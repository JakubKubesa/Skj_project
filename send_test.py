import asyncio
import json

import websockets


async def send_test():
    uri = "ws://localhost:8000/ws/broker/image.jobs"

    # Prepare a correctly formatted payload (payload is a JSON string inside 'payload')
    task = {"operation": "grayscale", "object_id": "mojefoto.jpg", "bucket_id": "test-bucket", "params": {}}
    message = {
        "action": "publish",
        "topic": "image.jobs",
        "payload": json.dumps(task)
    }

    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps(message))
        print("[send_test] Sent:", json.dumps(message))

        # Optionally wait for a short response from broker
        try:
            resp = await asyncio.wait_for(ws.recv(), timeout=3)
            print("[send_test] Received:", resp)
        except asyncio.TimeoutError:
            print("[send_test] No immediate response received.")


if __name__ == "__main__":
    asyncio.run(send_test())
