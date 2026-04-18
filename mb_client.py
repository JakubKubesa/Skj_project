import asyncio
import json
from typing import Any

import msgpack
import websockets


async def run_client(topic: str, mode: str, action: str, message: Any = None) -> None:
    """Run a simple broker client.

    - mode: 'json' or 'msgpack'
    - action: 'subscribe' or 'publish'
    - message: payload for publish
    """
    uri = f"ws://localhost:8000/ws/broker/{topic}"

    if mode not in ("json", "msgpack"):
        raise ValueError("mode must be 'json' or 'msgpack'")
    if action not in ("subscribe", "publish"):
        raise ValueError("action must be 'subscribe' or 'publish'")

    async with websockets.connect(uri) as ws:
        if action == "publish":
            payload = {"action": "publish", "topic": topic, "payload": message}
            if mode == "json":
                await ws.send(json.dumps(payload))
                print("published (json):", payload)
            else:
                await ws.send(msgpack.packb(payload, use_bin_type=True))
                print("published (msgpack):", payload)
            return

        # subscribe
        print(f"subscribed to topic={topic} mode={mode}")
        try:
            while True:
                data = await ws.recv()
                # websockets returns str for text frames and bytes for binary
                if isinstance(data, bytes):
                    if mode == "msgpack":
                        obj = msgpack.unpackb(data, raw=False)
                        print("recv (msgpack):", obj)
                    else:
                        # unexpected binary in json mode — attempt to decode as utf-8
                        try:
                            text = data.decode("utf-8")
                            obj = json.loads(text)
                            print("recv (json-from-bytes):", obj)
                        except Exception:
                            print("recv (raw bytes):", data)
                else:
                    # text frame
                    if mode == "json":
                        try:
                            obj = json.loads(data)
                            print("recv (json):", obj)
                        except Exception:
                            print("recv (text):", data)
                    else:
                        # msgpack mode but received text — try parsing JSON
                        try:
                            obj = json.loads(data)
                            print("recv (json-as-text):", obj)
                        except Exception:
                            print("recv (text):", data)
        except websockets.exceptions.ConnectionClosed:
            print("connection closed")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Message broker test client")
    p.add_argument("topic")
    p.add_argument("mode", choices=("json", "msgpack"))
    p.add_argument("action", choices=("subscribe", "publish"))
    p.add_argument("--message", default=None)
    args = p.parse_args()

    asyncio.run(run_client(args.topic, args.mode, args.action, args.message))
