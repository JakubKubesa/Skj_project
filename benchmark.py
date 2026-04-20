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

import msgpack
import websockets


def mode_safe_timestamp() -> str:
    return str(int(time.time() * 1000))


async def subscriber(name: str, uri: str, mode: str, expected_msgs: int, connected_queue: asyncio.Queue, durable: bool) -> int:
    """Connect and receive expected_msgs messages, return number received."""
    recv_count = 0
    try:
        async with websockets.connect(uri) as ws:
            # signal that this subscriber is connected
            await connected_queue.put(1)
            print(f"{name}: connected")

            while recv_count < expected_msgs:
                data = await ws.recv()
                # optional decode for validation, but keep lightweight
                try:
                    obj = None
                    if isinstance(data, bytes):
                        if mode == "msgpack":
                            obj = msgpack.unpackb(data, raw=False)
                        else:
                            obj = json.loads(data.decode("utf-8"))
                    else:
                        if mode == "json":
                            obj = json.loads(data)
                        else:
                            obj = json.loads(data)

                    if isinstance(obj, dict) and obj.get("action") == "deliver":
                        recv_count += 1
                        message_id = obj.get("message_id")
                        if durable and message_id is not None:
                            ack = {"action": "ack", "message_id": message_id}
                            if mode == "msgpack":
                                await ws.send(msgpack.packb(ack, use_bin_type=True))
                            else:
                                await ws.send(json.dumps(ack))

                        if recv_count % 1000 == 0:
                            print(f"{name}: received {recv_count}/{expected_msgs}")
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
            topic = uri.rsplit("/ws/broker/", 1)[-1].split("?", 1)[0]
            print(f"{name}: connected")
            for i in range(messages):
                payload = {"action": "publish", "topic": topic, "payload": f"{name}-{i}"}
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
                if sent % 1000 == 0:
                    print(f"{name}: sent {sent}/{messages}")
    except Exception as e:
        print(f"publisher {name} error: {e}")
    return sent


async def run_benchmark(mode: str, topic: str, n_publishers: int = 5, n_subscribers: int = 5, messages_per_publisher: int = 10000, durable: bool = False):
    durable_value = "true" if durable else "false"
    subscriber_uri = f"ws://localhost:8000/ws/broker/{topic}?mode={mode}&role=subscriber&durable={durable_value}"
    publisher_uri = f"ws://localhost:8000/ws/broker/{topic}?mode={mode}&role=publisher&durable={durable_value}"
    print("--- Starting benchmark ---")
    print(f"topic: {topic}")
    print(f"subscriber uri: {subscriber_uri}")
    print(f"publisher uri: {publisher_uri}")
    print(f"mode: {mode}")
    print(f"durable: {durable}")
    print(f"publishers: {n_publishers}, subscribers: {n_subscribers}, messages/publisher: {messages_per_publisher}")

    connected_queue: asyncio.Queue = asyncio.Queue()
    start_event = asyncio.Event()
    start_time_holder: dict = {"t": None}
    start_lock = asyncio.Lock()

    total_published = n_publishers * messages_per_publisher
    expected_per_subscriber = total_published

    # create subscriber tasks
    subs = [asyncio.create_task(subscriber(f"sub{i}", subscriber_uri, mode, expected_per_subscriber, connected_queue, durable)) for i in range(n_subscribers)]

    # wait until all subscribers have connected
    connected = 0
    while connected < n_subscribers:
        try:
            await asyncio.wait_for(connected_queue.get(), timeout=10)
        except asyncio.TimeoutError:
            for task in subs:
                task.cancel()
            raise RuntimeError(
                "Timed out waiting for subscribers. Check that uvicorn is running on localhost:8000."
            )
        connected += 1
        print(f"subscribers connected: {connected}/{n_subscribers}")
    # now allow publishers to start
    start_event.set()
    print("all subscribers connected, starting publishers")

    # create publisher tasks
    pubs = [asyncio.create_task(publisher(f"pub{i}", publisher_uri, mode, messages_per_publisher, start_time_holder, start_lock, start_event)) for i in range(n_publishers)]

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
    print(f"durable: {durable}")
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
    p.add_argument("--topic", default=None)
    p.add_argument("--publishers", type=int, default=5)
    p.add_argument("--subscribers", type=int, default=5)
    p.add_argument("--messages", type=int, default=10000, help="messages per publisher")
    p.add_argument("--durable", action="store_true", help="Include durable queue DB persistence and ACK writes")
    args = p.parse_args()
    topic = args.topic or f"benchmark_{mode_safe_timestamp()}"

    asyncio.run(run_benchmark(args.mode, topic, args.publishers, args.subscribers, args.messages, args.durable))


if __name__ == "__main__":
    main()
