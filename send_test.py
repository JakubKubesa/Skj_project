"""Tiny helper script that publishes one test image-processing job to the broker."""

import asyncio
import json

import websockets


async def send_test() -> None:
    """Publish one grayscale job to ``image.jobs`` for manual experiments."""
    uri = "ws://localhost:8000/ws/broker/image.jobs?mode=json&role=publisher&durable=true"
    task = {
        "operation": "grayscale",
        "object_key": "mojefoto.jpg",
        "bucket_id": "test-bucket",
        "params": {},
    }
    message = {
        "action": "publish",
        "topic": "image.jobs",
        "payload": task,
    }

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps(message))
        print("[send_test] Sent:", json.dumps(message))

        try:
            resp = await asyncio.wait_for(ws.recv(), timeout=3)
            print("[send_test] Received:", resp)
        except asyncio.TimeoutError:
            print("[send_test] No immediate response received.")


if __name__ == "__main__":
    asyncio.run(send_test())
