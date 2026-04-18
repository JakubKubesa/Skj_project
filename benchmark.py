"""Benchmark script for the in-process message broker.

Runs N publishers and M subscribers concurrently. Each publisher sends
`messages_per_publisher` messages. Measures time from first send to when
all subscribers have received all messages.

Usage:
  python benchmark.py --mode json
  python benchmark.py --mode msgpack
"""
import argparse
import asyncio
import json
import time
from typing import Any

import msgpack
import websockets


async def subscriber(name: str, uri: str, mode: str, expected_msgs: int, connected_queue: asyncio.Queue) -> int:
    """Connect and receive expected_msgs messages, return number received."""
    recv_count = 0
    try:
        async with websockets.connect(uri) as ws:
            # signal that this subscriber is connected
            await connected_queue.put(1)

            while recv_count < expected_msgs:
                data = await ws.recv()
                # count message regardless of decoding success
                recv_count += 1
                # optional decode for validation, but keep lightweight
                try:
                    if isinstance(data, bytes):
                        if mode == "msgpack":
                            _ = msgpack.unpackb(data, raw=False)
                        else:
                            _ = data.decode("utf-8")
                    else:
                        if mode == "json":
                            _ = json.loads(data)
                except Exception:
                    # ignore decode errors for benchmark
                    pass
            # close connection
            await ws.close()
    except Exception as e:
        print(f"subscriber {name} error: {e}")
    return recv_count


async def publisher(name: str, uri: str, mode: str, messages: int, start_time_holder: dict, start_lock: asyncio.Lock, start_event: asyncio.Event) -> int:
    """Send `messages` messages. Set start_time_holder['t'] at first send."""
    sent = 0
    await start_event.wait()
    try:
        async with websockets.connect(uri) as ws:
            for i in range(messages):
                payload = {"action": "publish", "topic": uri.rsplit('/', 1)[-1], "payload": f"{name}-{i}"}
                if mode == "json":
                    data = json.dumps(payload)
                    # set start time exactly when first message is sent
                    async with start_lock:
                        if start_time_holder.get("t") is None:
                            start_time_holder["t"] = time.perf_counter()
                    await ws.send(data)
                else:
                    packed = msgpack.packb(payload, use_bin_type=True)
                    async with start_lock:
                        if start_time_holder.get("t") is None:
                            start_time_holder["t"] = time.perf_counter()
                    await ws.send(packed)
                sent += 1
    except Exception as e:
        print(f"publisher {name} error: {e}")
    return sent


async def run_benchmark(mode: str, topic: str, n_publishers: int = 5, n_subscribers: int = 5, messages_per_publisher: int = 10000):
    uri = f"ws://localhost:8000/ws/broker/{topic}"

    connected_queue: asyncio.Queue = asyncio.Queue()
    start_event = asyncio.Event()
    start_time_holder: dict = {"t": None}
    start_lock = asyncio.Lock()

    total_published = n_publishers * messages_per_publisher
    expected_per_subscriber = total_published

    # create subscriber tasks
    subs = [asyncio.create_task(subscriber(f"sub{i}", uri, mode, expected_per_subscriber, connected_queue)) for i in range(n_subscribers)]

    # wait until all subscribers have connected
    connected = 0
    while connected < n_subscribers:
        await connected_queue.get()
        connected += 1
    # now allow publishers to start
    start_event.set()

    # create publisher tasks
    pubs = [asyncio.create_task(publisher(f"pub{i}", uri, mode, messages_per_publisher, start_time_holder, start_lock, start_event)) for i in range(n_publishers)]

    # wait for all publishers to finish sending
    published_counts = await asyncio.gather(*pubs)

    # wait for all subscribers to finish receiving
    recv_counts = await asyncio.gather(*subs)

    end_t = time.perf_counter()
    start_t = start_time_holder.get("t") or end_t

    duration = end_t - start_t

    total_published_actual = sum(published_counts)
    total_received_actual = sum(recv_counts)

    print("--- Benchmark results ---")
    print(f"mode: {mode}")
    print(f"publishers: {n_publishers}, subscribers: {n_subscribers}, messages/publisher: {messages_per_publisher}")
    print(f"total published (intended): {total_published}")
    print(f"total published (actual): {total_published_actual}")
    print(f"total received (actual across all subscribers): {total_received_actual}")
    print(f"total time (s): {duration:.6f}")
    if duration > 0:
        print(f"throughput (published msg/s): {total_published_actual / duration:.2f}")
        print(f"throughput (received msg/s): {total_received_actual / duration:.2f}")
    else:
        print("duration too small to compute throughput")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("json", "msgpack"), required=True)
    p.add_argument("--topic", default="benchmark")
    p.add_argument("--publishers", type=int, default=5)
    p.add_argument("--subscribers", type=int, default=5)
    p.add_argument("--messages", type=int, default=10000, help="messages per publisher")
    args = p.parse_args()

    asyncio.run(run_benchmark(args.mode, args.topic, args.publishers, args.subscribers, args.messages))


if __name__ == "__main__":
    main()
