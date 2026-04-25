"""Simple CLI client for manual broker publish/subscribe testing."""

import asyncio
import json
from typing import Any

import msgpack
import websockets



def parse_message(value: Any) -> Any:
    """Try to parse a JSON literal from CLI input; otherwise return the raw value."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value



def encode_message(message: dict[str, Any], mode: str) -> str | bytes:
    """Encode one broker envelope for JSON or MessagePack transport."""
    if mode == "msgpack":
        return msgpack.packb(message, use_bin_type=True)
    return json.dumps(message)



def decode_message(data: str | bytes, mode: str) -> dict[str, Any] | Any:
    """Decode one broker frame according to the selected wire format."""
    if isinstance(data, bytes):
        if mode == "msgpack":
            return msgpack.unpackb(data, raw=False)
        return json.loads(data.decode("utf-8"))
    return json.loads(data)


async def run_client(topic: str, mode: str, action: str, message: Any = None) -> None:
    """Run a small durable broker client for ad hoc manual testing."""
    if mode not in ("json", "msgpack"):
        raise ValueError("mode must be 'json' or 'msgpack'")
    if action not in ("subscribe", "publish"):
        raise ValueError("action must be 'subscribe' or 'publish'")

    uri = f"ws://localhost:8000/ws/broker/{topic}?mode={mode}"

    async with websockets.connect(uri) as ws:
        if action == "publish":
            payload = {"action": "publish", "topic": topic, "payload": parse_message(message)}
            await ws.send(encode_message(payload, mode))
            print(f"published ({mode}):", payload)
            return

        print(f"subscribed to topic={topic} mode={mode}")
        try:
            while True:
                data = await ws.recv()
                try:
                    obj = decode_message(data, mode)
                except Exception:
                    print("recv (raw):", data)
                    continue

                print(f"recv ({mode}):", obj)
                if isinstance(obj, dict) and obj.get("action") == "deliver":
                    message_id = obj.get("message_id")
                    if message_id is None:
                        continue
                    ack = {"action": "ack", "message_id": message_id}
                    await ws.send(encode_message(ack, mode))
                    print("ack:", message_id)
        except websockets.exceptions.ConnectionClosed:
            print("connection closed")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Durable message broker test client")
    p.add_argument("topic")
    p.add_argument("mode", choices=("json", "msgpack"))
    p.add_argument("action", choices=("subscribe", "publish"))
    p.add_argument("--message", default=None)
    args = p.parse_args()

    asyncio.run(run_client(args.topic, args.mode, args.action, args.message))
