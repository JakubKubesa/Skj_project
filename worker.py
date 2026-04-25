"""Asynchronous image-processing worker for jobs published through the broker.

The worker subscribes to ``image.jobs``, downloads the referenced object from the
REST API, applies one NumPy-based transformation, uploads the result back into
its bucket, and publishes a status message to ``image.done``.
"""

import asyncio
import json
import os
import random
import re
import signal
import sys
from typing import Any, Optional

import httpx
import websockets
from pydantic import ValidationError

import schemas
from image_processor import process_image

API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
BROKER_BASE_WS = os.getenv("BROKER_BASE_WS", "ws://127.0.0.1:8000")
RECONNECT_BASE = 1.0
RECONNECT_MAX = 30.0
INTERNAL_HEADERS = {"x-internal-source": "true"}
ACK_ATTEMPTS = 3
ACK_RESPONSE_TIMEOUT = 2.0


def build_broker_uri(topic: str, role: str = "subscriber", durable: bool = True) -> str:
    """Construct the WebSocket URI used to talk to the in-process broker."""
    durable_value = "true" if durable else "false"
    return f"{BROKER_BASE_WS}/ws/broker/{topic}?mode=json&role={role}&durable={durable_value}"


def _fix_json_text(text: str) -> str:
    """Best-effort repair for slightly malformed JSON snippets during debugging."""
    t = text.replace("'", '"')
    pattern = re.compile(r'([\{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:\s*')
    return pattern.sub(r'\1"\2": ', t)


async def send_status(payload: schemas.WorkerStatusPayload) -> None:
    """Publish one worker status update to the ``image.done`` topic."""
    uri = build_broker_uri("image.done", role="publisher", durable=False)
    envelope = schemas.BrokerPublishMessage(
        action="publish",
        topic="image.done",
        payload=payload.model_dump(exclude_none=True),
    ).model_dump()
    try:
        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps(envelope))
    except Exception as exc:
        print(f"[WORKER] Chyba pri odesilani statusu: {exc}")


async def send_job_ack(message_id: int) -> bool:
    """Acknowledge one durable job message after processing is finished.

    The worker waits for the broker ACK response so the connection is not closed
    before the server has actually processed the acknowledgement.
    """
    uri = build_broker_uri("image.jobs", role="publisher", durable=True)
    envelope = schemas.BrokerAckMessage(action="ack", message_id=message_id).model_dump()

    for attempt in range(1, ACK_ATTEMPTS + 1):
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps(envelope))
                raw = await asyncio.wait_for(ws.recv(), timeout=ACK_RESPONSE_TIMEOUT)
                response = json.loads(raw)
                if response.get("action") == "ack" and response.get("message_id") == message_id:
                    print(f"[WORKER] ACK potvrzeno pro message_id={message_id}")
                    return True
                print(f"[WORKER] Neocekavana ACK odpoved pro {message_id}: {response}")
        except Exception as exc:
            print(f"[WORKER] Chyba pri odeslani ACK {message_id} (pokus {attempt}/{ACK_ATTEMPTS}): {exc}")
            if attempt < ACK_ATTEMPTS:
                await asyncio.sleep(0.2 * attempt)
    return False


async def download_image(bucket_id: str, object_key: str, temp_path: str) -> None:
    """Download the source image from the REST API into a temp file."""
    url = f"{API_BASE_URL}/buckets/{bucket_id}/objects/{object_key}"
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url, headers=INTERNAL_HEADERS)
        response.raise_for_status()
        with open(temp_path, "wb") as handle:
            handle.write(response.content)


async def upload_image(bucket_id: str, object_key: str, user_id: str, file_path: str) -> None:
    """Upload the processed image back to the REST API."""
    url = f"{API_BASE_URL}/buckets/{bucket_id}/objects/{object_key}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        with open(file_path, "rb") as handle:
            files = {"file": (object_key, handle, "application/octet-stream")}
            response = await client.put(url, params={"user_id": user_id}, files=files, headers=INTERNAL_HEADERS)
            response.raise_for_status()


async def ack_if_needed(message_id: Optional[int]) -> bool:
    """Send ACK only for durable broker messages that carry a message id."""
    if message_id is None:
        return True

    acked = await send_job_ack(int(message_id))
    if not acked:
        print(f"[WORKER] ACK {message_id} se nepodarilo potvrdit brokerem")
    return acked


def parse_incoming_message(raw: Any) -> dict[str, Any]:
    """Decode a JSON text frame into a Python dict, with a small repair fallback."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        fixed = _fix_json_text(str(raw))
        return json.loads(fixed)


async def handle_message(raw: Any) -> None:
    """Process one broker delivery message from ``image.jobs``.

    The handler validates both the broker envelope and the worker payload with
    Pydantic before touching the filesystem or performing CPU-bound work.
    """
    try:
        data = parse_incoming_message(raw)
    except Exception as exc:
        await send_status(
            schemas.WorkerStatusPayload(
                status="failed",
                bucket_id="unknown",
                error=f"invalid_json: {exc}",
            )
        )
        return

    action = data.get("action")
    if action in {"ack", "error"}:
        return

    message_id: Optional[int] = None
    payload_raw: Any
    if action == "deliver":
        try:
            deliver = schemas.BrokerDeliverMessage.model_validate(data)
        except ValidationError as exc:
            await send_status(
                schemas.WorkerStatusPayload(
                    status="failed",
                    bucket_id="unknown",
                    error=f"invalid_deliver_envelope: {exc.errors(include_url=False)}",
                )
            )
            return
        message_id = deliver.message_id
        payload_raw = deliver.payload
    else:
        payload_raw = data.get("payload", data)

    try:
        if isinstance(payload_raw, str):
            try:
                payload_data = json.loads(payload_raw)
            except json.JSONDecodeError:
                payload_data = json.loads(_fix_json_text(payload_raw))
        else:
            payload_data = payload_raw
    except Exception as exc:
        await ack_if_needed(message_id)
        await send_status(
            schemas.WorkerStatusPayload(
                status="failed",
                bucket_id="unknown",
                error=f"invalid_payload_json: {exc}",
            )
        )
        return

    try:
        job = schemas.WorkerJobPayload.model_validate(payload_data)
    except ValidationError as exc:
        bucket_value = payload_data.get("bucket_id", "unknown") if isinstance(payload_data, dict) else "unknown"
        object_value = payload_data.get("object_key") if isinstance(payload_data, dict) else None
        await ack_if_needed(message_id)
        await send_status(
            schemas.WorkerStatusPayload(
                status="failed",
                bucket_id=bucket_value,
                object_key=object_value,
                error=f"invalid_job_payload: {exc.errors(include_url=False)}",
            )
        )
        return

    print(f"[WORKER] Prijata uloha: {job.operation} pro objekt {job.object_key} (bucket: {job.bucket_id})")

    os.makedirs("temp_in", exist_ok=True)
    os.makedirs("temp_out", exist_ok=True)
    temp_input = os.path.join("temp_in", job.object_key)
    temp_output = os.path.join("temp_out", job.object_key)

    try:
        print(f"[WORKER] Stahuji {job.object_key} z gateway...")
        await download_image(job.bucket_id, job.object_key, temp_input)

        print(f"[WORKER] Zpracovavam operaci {job.operation}...")
        await asyncio.to_thread(process_image, temp_input, temp_output, job.operation, job.params)

        print(f"[WORKER] Nahravam upraveny soubor zpet...")
        await upload_image(job.bucket_id, job.object_key, job.user_id, temp_output)

        await ack_if_needed(message_id)
        await send_status(
            schemas.WorkerStatusPayload(
                status="completed",
                operation=job.operation,
                bucket_id=job.bucket_id,
                object_key=job.object_key,
            )
        )
    except Exception as exc:
        print(f"[WORKER] Chyba pri zpracovani {job.object_key}: {exc}")
        await ack_if_needed(message_id)
        await send_status(
            schemas.WorkerStatusPayload(
                status="failed",
                operation=job.operation,
                bucket_id=job.bucket_id,
                object_key=job.object_key,
                error=str(exc),
            )
        )


async def run(stop_event: Optional[asyncio.Event] = None, topic: str = "image.jobs") -> None:
    """Run the worker forever with reconnect/backoff logic.

    Args:
        stop_event: Optional event used by tests to stop the worker.
        topic: Broker topic to subscribe to.
    """
    backoff = RECONNECT_BASE
    stop = stop_event or asyncio.Event()

    def _signal(_signum, _frame):
        stop.set()

    try:
        signal.signal(signal.SIGINT, _signal)
        signal.signal(signal.SIGTERM, _signal)
    except Exception:
        pass

    while not stop.is_set():
        try:
            broker_uri = build_broker_uri(topic, role="subscriber", durable=True)
            print(f"[WORKER] Pokousim se pripojit k brokeru: {broker_uri}")
            async with websockets.connect(broker_uri) as ws:
                print(f"[WORKER] Pripojeno k brokeru: {broker_uri}")
                backoff = RECONNECT_BASE

                async for message in ws:
                    asyncio.create_task(handle_message(message))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[WORKER] Odpojeno / chyba pripojeni: {exc}")

        if stop.is_set():
            break

        wait_seconds = backoff + random.random()
        print(f"[WORKER] Znovu se pokusim pripojit za {wait_seconds:.1f}s")
        try:
            await asyncio.wait_for(stop.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            backoff = min(RECONNECT_MAX, backoff * 2)

    print("[WORKER] Ukoncuji sluzbu.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[WORKER] Preruseno uzivatelem.")
        try:
            sys.exit(0)
        except SystemExit:
            pass
