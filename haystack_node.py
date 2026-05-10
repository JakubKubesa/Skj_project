"""Standalone Haystack storage node for append-only volume writes.

The service subscribes to ``storage.write`` through the existing WebSocket
broker, appends binary payloads into rotating ``volume_<id>.dat`` files, and
publishes offset acknowledgements to ``storage.ack``.
"""

import asyncio
import json
import os
import random
import re
from collections import OrderedDict
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import msgpack
import uvicorn
import websockets
from fastapi import FastAPI, HTTPException, Path as FastAPIPath, Response
from pydantic import ValidationError

import schemas

BROKER_BASE_WS = os.getenv("BROKER_BASE_WS", "ws://127.0.0.1:8000")
WRITE_TOPIC = os.getenv("HAYSTACK_WRITE_TOPIC", "storage.write")
ACK_TOPIC = os.getenv("HAYSTACK_ACK_TOPIC", "storage.ack")
# Broker wire format (json or msgpack). Default to JSON for compatibility.
HAYSTACK_BROKER_MODE = os.getenv("HAYSTACK_BROKER_MODE", "msgpack").lower()
VOLUME_DIR = Path(os.getenv("HAYSTACK_VOLUME_DIR", "haystack_volumes"))
MAX_VOLUME_SIZE_BYTES = int(os.getenv("HAYSTACK_MAX_VOLUME_SIZE_BYTES", str(100 * 1024 * 1024)))
READ_MEDIA_TYPE = os.getenv("HAYSTACK_READ_MEDIA_TYPE", "application/octet-stream")
MESSAGE_CACHE_SIZE = int(os.getenv("HAYSTACK_MESSAGE_CACHE_SIZE", "10000"))
RECONNECT_BASE = 1.0
RECONNECT_MAX = 30.0
ACK_ATTEMPTS = 3
ACK_RESPONSE_TIMEOUT = 2.0


class HaystackVolumeStore:
    """Manage rotating append-only volume files."""

    _volume_pattern = re.compile(r"^volume_(\d+)\.dat$")

    def __init__(self, base_dir: Path, max_volume_size_bytes: int, message_cache_size: int = 10000) -> None:
        self.base_dir = Path(base_dir)
        self.max_volume_size_bytes = max(1, int(max_volume_size_bytes))
        self.message_cache_size = max(1, int(message_cache_size))
        self._active_handle = None
        self._active_volume_id: int | None = None
        self._lock = asyncio.Lock()
        self._processed_messages: OrderedDict[int, schemas.StorageAckPayload] = OrderedDict()

    async def startup(self) -> None:
        """Prepare the active volume on service startup."""
        await asyncio.to_thread(self._startup_sync)

    def _startup_sync(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        existing_ids = self._discover_existing_volume_ids_sync()
        starting_volume_id = existing_ids[-1] if existing_ids else 1
        print(f"[HAYSTACK] Starting volume store in {self.base_dir}, discovered volumes: {existing_ids}")
        self._open_volume_sync(starting_volume_id)

    async def shutdown(self) -> None:
        """Close the active volume file cleanly."""
        async with self._lock:
            await asyncio.to_thread(self._close_active_sync)

    def _discover_existing_volume_ids_sync(self) -> list[int]:
        ids: list[int] = []
        for candidate in self.base_dir.iterdir():
            match = self._volume_pattern.fullmatch(candidate.name)
            if match and candidate.is_file():
                ids.append(int(match.group(1)))
        return sorted(ids)

    def _volume_path(self, volume_id: int) -> Path:
        return self.base_dir / f"volume_{volume_id}.dat"

    def _open_volume_sync(self, volume_id: int) -> None:
        self._close_active_sync()
        path = self._volume_path(volume_id)
        print(f"[HAYSTACK] Opening volume file: {path}")
        handle = open(path, "ab+")
        handle.seek(0, os.SEEK_END)
        self._active_handle = handle
        self._active_volume_id = volume_id

    def _close_active_sync(self) -> None:
        if self._active_handle is not None:
            try:
                self._active_handle.close()
            finally:
                self._active_handle = None
                self._active_volume_id = None

    def _ensure_active_volume_sync(self) -> None:
        if self._active_handle is None or self._active_volume_id is None:
            self._startup_sync()

    def _rotate_volume_sync(self) -> None:
        next_volume_id = 1 if self._active_volume_id is None else self._active_volume_id + 1
        print(f"[HAYSTACK] Rotating volume: current={self._active_volume_id} next={next_volume_id}")
        self._open_volume_sync(next_volume_id)

    async def append_payload(
        self,
        payload: schemas.StorageWritePayload,
        *,
        broker_message_id: int | None = None,
    ) -> schemas.StorageAckPayload:
        """Append one object payload and return the written offset metadata."""
        async with self._lock:
            if broker_message_id is not None and broker_message_id in self._processed_messages:
                self._processed_messages.move_to_end(broker_message_id)
                return self._processed_messages[broker_message_id]

            ack_payload = await asyncio.to_thread(self._append_payload_sync, payload)
            if broker_message_id is not None:
                self._processed_messages[broker_message_id] = ack_payload
                self._processed_messages.move_to_end(broker_message_id)
                while len(self._processed_messages) > self.message_cache_size:
                    self._processed_messages.popitem(last=False)
            return ack_payload

    def _append_payload_sync(self, payload: schemas.StorageWritePayload) -> schemas.StorageAckPayload:
        self._ensure_active_volume_sync()
        if self._active_handle is None or self._active_volume_id is None:
            raise RuntimeError("Active volume is not available")

        current_end = self._active_handle.tell()
        if current_end > 0 and current_end + len(payload.data) > self.max_volume_size_bytes:
            self._rotate_volume_sync()
            current_end = self._active_handle.tell()

        offset = current_end
        # Log where we're writing and ensure data is flushed to disk
        vol_name = f"volume_{self._active_volume_id}.dat"
        print(f"[HAYSTACK] Writing {len(payload.data)} bytes to {vol_name} at offset {offset}")
        written = self._active_handle.write(payload.data)
        if written != len(payload.data):
            raise IOError("Volume write was incomplete")
        # Flush Python buffers and force OS to write to disk
        self._active_handle.flush()
        try:
            os.fsync(self._active_handle.fileno())
        except Exception:
            # fsync may fail on some filesystems or environments; log but continue
            print(f"[HAYSTACK] Warning: os.fsync failed for {vol_name}")

        return schemas.StorageAckPayload(
            object_id=payload.object_id,
            volume_id=self._active_volume_id,
            offset=offset,
            size=len(payload.data),
        )

    async def read_chunk(self, volume_id: int, offset: int, size: int) -> bytes:
        """Read an exact byte range from one volume file."""
        async with self._lock:
            await asyncio.to_thread(self._flush_if_active_sync, volume_id)
            path = self._volume_path(volume_id)
        return await asyncio.to_thread(self._read_chunk_sync, path, offset, size)

    def _flush_if_active_sync(self, volume_id: int) -> None:
        if self._active_handle is not None and self._active_volume_id == volume_id:
            self._active_handle.flush()

    @staticmethod
    def _read_chunk_sync(path: Path, offset: int, size: int) -> bytes:
        if not path.exists():
            raise FileNotFoundError(f"Volume file not found: {path.name}")

        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            total_size = handle.tell()
            if offset > total_size:
                raise ValueError("Offset is outside the selected volume")
            if offset + size > total_size:
                raise ValueError("Requested byte range exceeds the selected volume")

            handle.seek(offset)
            data = handle.read(size)
            if len(data) != size:
                raise ValueError("Could not read the requested byte range")
            return data


def build_broker_uri(topic: str, *, role: str = "subscriber", durable: bool = True, mode: str = "json") -> str:
    """Construct the WebSocket URI for one broker topic."""
    durable_value = "true" if durable else "false"
    return f"{BROKER_BASE_WS}/ws/broker/{topic}?mode={mode}&role={role}&durable={durable_value}"


def decode_wire_message(raw: Any) -> dict[str, Any]:
    """Decode one broker frame from JSON text or MessagePack bytes."""
    if isinstance(raw, (bytes, bytearray)):
        return msgpack.unpackb(raw, raw=False)
    return json.loads(raw)


async def publish_storage_ack(ack_payload: schemas.StorageAckPayload) -> None:
    """Publish one successful write acknowledgement to the broker."""
    uri = build_broker_uri(ACK_TOPIC, role="publisher", durable=False, mode="msgpack")
    envelope = schemas.BrokerPublishMessage(
        action="publish",
        topic=ACK_TOPIC,
        payload=ack_payload.model_dump(),
    ).model_dump()
    # Log the ack details for diagnostics
    try:
        print(f"[HAYSTACK] ACK -> object_id={ack_payload.object_id}, volume_id={ack_payload.volume_id}, offset={ack_payload.offset}, size={ack_payload.size}")
    except Exception:
        pass
    async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
        await ws.send(msgpack.packb(envelope, use_bin_type=True))


async def acknowledge_broker_message(message_id: int) -> bool:
    """Confirm durable processing of one broker message id."""
    uri = build_broker_uri(WRITE_TOPIC, role="publisher", durable=False, mode="msgpack")
    envelope = schemas.BrokerAckMessage(action="ack", message_id=message_id).model_dump()

    for attempt in range(1, ACK_ATTEMPTS + 1):
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(msgpack.packb(envelope, use_bin_type=True))
                raw = await asyncio.wait_for(ws.recv(), timeout=ACK_RESPONSE_TIMEOUT)
                response = decode_wire_message(raw)
                if response.get("action") == "ack" and response.get("message_id") == message_id:
                    return response.get("status") in {"ok", "ignored"}
        except Exception as exc:
            print(f"[HAYSTACK] ACK {message_id} failed on attempt {attempt}/{ACK_ATTEMPTS}: {exc}")
            if attempt < ACK_ATTEMPTS:
                await asyncio.sleep(0.2 * attempt)
    return False


async def handle_storage_delivery(raw: Any, store: HaystackVolumeStore) -> None:
    """Validate one broker delivery and append its payload into a volume."""
    try:
        data = decode_wire_message(raw)
    except Exception as exc:
        print(f"[HAYSTACK] Invalid broker frame: {exc}")
        return

    if not isinstance(data, dict):
        print(f"[HAYSTACK] Unsupported broker frame payload: {type(data).__name__}")
        return

    action = data.get("action")
    if action in {"ack", "error"}:
        return
    if action != "deliver":
        print(f"[HAYSTACK] Unsupported broker action: {action}")
        return

    message_id = data.get("message_id")
    try:
        deliver = schemas.BrokerDeliverMessage.model_validate(data)
        payload = schemas.StorageWritePayload.model_validate(deliver.payload)
    except ValidationError as exc:
        if isinstance(message_id, int):
            await acknowledge_broker_message(message_id)
        print(f"[HAYSTACK] Invalid storage.write payload: {exc.errors(include_url=False)}")
        return

    try:
        ack_payload = await store.append_payload(payload, broker_message_id=deliver.message_id)
        await publish_storage_ack(ack_payload)
        if deliver.message_id is not None:
            acked = await acknowledge_broker_message(deliver.message_id)
            if not acked:
                print(f"[HAYSTACK] Broker did not confirm ACK for message_id={deliver.message_id}")
    except Exception as exc:
        print(f"[HAYSTACK] Write handling failed for object_id={payload.object_id}: {exc}")


async def run_storage_subscriber(stop_event: asyncio.Event, store: HaystackVolumeStore) -> None:
    """Keep one durable subscription to storage.write alive in the background."""
    backoff = RECONNECT_BASE

    while not stop_event.is_set():
        try:
            # Use configured broker wire mode and enable websocket heartbeat pings
            broker_uri = build_broker_uri(WRITE_TOPIC, role="subscriber", durable=False, mode=HAYSTACK_BROKER_MODE)
            print(f"[HAYSTACK] Connecting to broker: {broker_uri}")
            # set ping_interval so the connection stays alive through intermediaries
            async with websockets.connect(broker_uri, ping_interval=20, ping_timeout=10) as ws:
                print(f"[HAYSTACK] Connected to broker: {broker_uri}")
                backoff = RECONNECT_BASE

                while not stop_event.is_set():
                    recv_task = asyncio.create_task(ws.recv())
                    stop_task = asyncio.create_task(stop_event.wait())
                    done, pending = await asyncio.wait(
                        {recv_task, stop_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    if stop_task in done:
                        recv_task.cancel()
                        await asyncio.gather(recv_task, return_exceptions=True)
                        break

                    raw = recv_task.result()
                    await handle_storage_delivery(raw, store)
        except asyncio.CancelledError:
            raise
        except websockets.exceptions.ConnectionClosed:
            if stop_event.is_set():
                break
            print("[HAYSTACK] Broker connection closed.")
        except Exception as exc:
            print(f"[HAYSTACK] Broker subscriber error: {exc}")

        if stop_event.is_set():
            break

        wait_seconds = backoff + random.random()
        print(f"[HAYSTACK] Reconnecting in {wait_seconds:.1f}s")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            backoff = min(RECONNECT_MAX, backoff * 2)

    print("[HAYSTACK] Subscriber stopped.")


store = HaystackVolumeStore(
    base_dir=VOLUME_DIR,
    max_volume_size_bytes=MAX_VOLUME_SIZE_BYTES,
    message_cache_size=MESSAGE_CACHE_SIZE,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the volume store and its broker subscriber task."""
    await store.startup()
    stop_event = asyncio.Event()
    subscriber_task = asyncio.create_task(run_storage_subscriber(stop_event, store))
    app.state.store = store
    app.state.stop_event = stop_event
    app.state.subscriber_task = subscriber_task
    try:
        yield
    finally:
        stop_event.set()
        subscriber_task.cancel()
        with suppress(asyncio.CancelledError):
            await subscriber_task
        await store.shutdown()


app = FastAPI(
    title="Haystack Storage Node",
    description="Append-only storage node backed by rotating volume files.",
    lifespan=lifespan,
)


@app.get("/volume/{volume_id}/{offset}/{size}")
async def read_volume_bytes(
    volume_id: int = FastAPIPath(..., ge=1),
    offset: int = FastAPIPath(..., ge=0),
    size: int = FastAPIPath(..., ge=0),
):
    """Read one exact byte range from a stored volume."""
    try:
        data = await app.state.store.read_chunk(volume_id, offset, size)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=416, detail=str(exc)) from exc
    return Response(content=data, media_type=READ_MEDIA_TYPE)


if __name__ == "__main__":
    uvicorn.run(
        "haystack_node:app",
        host=os.getenv("HAYSTACK_HOST", "127.0.0.1"),
        port=int(os.getenv("HAYSTACK_PORT", "8002")),
        reload=False,
    )
