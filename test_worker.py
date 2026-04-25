"""Integration test covering the worker, REST API, and broker together."""

import asyncio
import contextlib
import io
import json
import os
import shutil
import socket
import time
import uuid

import httpx
import pytest
import uvicorn
import websockets
from PIL import Image

import models
import worker
from broker_utils import manager
from database import SessionLocal
from main import app



def free_port() -> int:
    """Reserve an ephemeral local TCP port for the test server."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port



def make_test_image_bytes() -> bytes:
    """Create a small RGB image used by the worker integration test."""
    image = Image.new("RGB", (8, 8), color=(120, 40, 200))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()



def wait_until(callback, timeout: float = 5.0, interval: float = 0.05):
    """Poll until a callback returns a truthy value or timeout is reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = callback()
        if result:
            return result
        time.sleep(interval)
    return callback()



def clear_queued_messages(topic: str) -> None:
    """Delete queued broker rows for one topic after a test run."""
    db = SessionLocal()
    try:
        db.query(models.QueuedMessage).filter(models.QueuedMessage.topic == topic).delete()
        db.commit()
    finally:
        db.close()



def clear_bucket(bucket_id: str) -> None:
    """Remove test objects/bucket rows and storage artifacts for one bucket."""
    db = SessionLocal()
    try:
        db.query(models.ObjectModel).filter(models.ObjectModel.bucket_id == bucket_id).delete()
        bucket = db.get(models.Bucket, bucket_id)
        if bucket:
            db.delete(bucket)
        db.commit()
    finally:
        db.close()

    bucket_dir = os.path.join("storage", bucket_id)
    if os.path.isdir(bucket_dir):
        shutil.rmtree(bucket_dir)


async def wait_for_server(base_url: str, timeout: float = 10.0) -> None:
    """Wait until the FastAPI server responds on /docs."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                response = await client.get(f"{base_url}/docs")
            if response.status_code == 200:
                return
        except Exception:
            pass
        await asyncio.sleep(0.1)
    raise AssertionError("Server did not start in time")


async def wait_for_worker_connection(timeout: float = 10.0) -> None:
    """Wait until the worker subscribes to the image.jobs topic."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if len(manager.topics.get("image.jobs", set())) >= 1:
            return
        await asyncio.sleep(0.1)
    raise AssertionError("Worker did not subscribe to image.jobs in time")


async def collect_done_messages(uri: str, expected: int, timeout: float = 30.0) -> list[dict]:
    """Collect completion notifications from the image.done topic."""
    messages: list[dict] = []
    async with websockets.connect(uri) as ws:
        while len(messages) < expected:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            envelope = json.loads(raw)
            if envelope.get("action") != "deliver":
                continue
            payload = envelope.get("payload")
            if isinstance(payload, str):
                payload = json.loads(payload)
            messages.append(payload)
    return messages



def load_job_queue(topic: str) -> list[models.QueuedMessage]:
    """Load durable queue rows for one topic using a fresh session."""
    db = SessionLocal()
    try:
        return db.query(models.QueuedMessage).filter(models.QueuedMessage.topic == topic).all()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_worker_processes_ten_jobs_and_emits_ten_done_messages(monkeypatch):
    """The worker should process queued image jobs and ACK each durable message."""
    bucket_id = f"worker-bucket-{uuid.uuid4().hex[:8]}"
    object_keys = [f"img-{i}-{uuid.uuid4().hex[:6]}.png" for i in range(10)]
    jobs_topic = "image.jobs"
    done_topic = "image.done"

    clear_queued_messages(jobs_topic)
    clear_queued_messages(done_topic)
    clear_bucket(bucket_id)
    manager.topics.clear()
    manager.client_formats.clear()

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    broker_base_ws = f"ws://127.0.0.1:{port}"
    done_uri = f"{broker_base_ws}/ws/broker/image.done?mode=json&role=subscriber&durable=false"

    monkeypatch.setattr(worker, "API_BASE_URL", base_url)
    monkeypatch.setattr(worker, "BROKER_BASE_WS", broker_base_ws)

    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    server_task = asyncio.create_task(server.serve())

    worker_task = None
    collector_task = None
    try:
        await wait_for_server(base_url)
        worker_task = asyncio.create_task(worker.run())
        await wait_for_worker_connection()

        image_bytes = make_test_image_bytes()
        async with httpx.AsyncClient(timeout=10.0) as client:
            for object_key in object_keys:
                files = {"file": (object_key, image_bytes, "image/png")}
                response = await client.put(
                    f"{base_url}/buckets/{bucket_id}/objects/{object_key}",
                    params={"user_id": f"user-{object_key}"},
                    files=files,
                )
                assert response.status_code == 200

        collector_task = asyncio.create_task(collect_done_messages(done_uri, expected=10, timeout=30.0))
        await asyncio.sleep(0.3)

        async with httpx.AsyncClient(timeout=10.0) as client:
            for object_key in object_keys:
                response = await client.post(
                    f"{base_url}/buckets/{bucket_id}/objects/{object_key}/process",
                    json={"operation": "grayscale", "params": {}},
                )
                assert response.status_code == 200
                assert response.json()["status"] == "processing_started"

        done_messages = await asyncio.wait_for(collector_task, timeout=35.0)
        assert len(done_messages) == 10
        assert {msg["object_key"] for msg in done_messages} == set(object_keys)
        assert all(msg["status"] == "completed" for msg in done_messages)

        queued = wait_until(
            lambda: [message for message in load_job_queue(jobs_topic) if message.is_delivered],
            timeout=5.0,
        )
        assert len(load_job_queue(jobs_topic)) == 10
        assert len(queued) == 10
    finally:
        if collector_task is not None and not collector_task.done():
            collector_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await collector_task
        if worker_task is not None:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await server_task
        clear_queued_messages(jobs_topic)
        clear_queued_messages(done_topic)
        clear_bucket(bucket_id)
        for temp_dir in ("temp_in", "temp_out", "processed"):
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)
        manager.topics.clear()
        manager.client_formats.clear()
