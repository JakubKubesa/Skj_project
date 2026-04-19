import json
import time
import threading
import queue

import pytest
from fastapi.testclient import TestClient

from main import app
from broker_utils import manager


def test_connection_and_disconnect():
    topic = "test_conn_1"
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}") as ws:
        # small pause to allow server-side accept/registration
        time.sleep(0.01)
        assert topic in manager.topics
        sockets = manager.topics.get(topic)
        assert sockets is not None and len(sockets) == 1

    # after exiting the context the disconnect handler should run
    time.sleep(0.05)
    assert topic not in manager.topics or len(manager.topics.get(topic, set())) == 0


def test_correct_routing():
    topic = "topic_A"
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}") as subscriber_ws, \
         client.websocket_connect(f"/ws/broker/{topic}") as publisher_ws:

        time.sleep(0.01)
        assert topic in manager.topics
        assert len(manager.topics[topic]) >= 2

        payload = {"action": "publish", "topic": topic, "payload": "hello-A"}
        publisher_ws.send_text(json.dumps(payload))

        received = subscriber_ws.receive_text()
        try:
            obj = json.loads(received)
        except Exception:
            obj = None

        assert isinstance(obj, dict)
        assert obj.get("action") == "publish"
        assert obj.get("topic") == topic
        assert obj.get("payload") == "hello-A"


def test_topic_isolation():
    topic_a = "topic_isolation_A"
    topic_b = "topic_isolation_B"
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic_a}") as sub_a, \
         client.websocket_connect(f"/ws/broker/{topic_b}") as sub_b, \
         client.websocket_connect(f"/ws/broker/{topic_a}") as pub_ws:

        time.sleep(0.01)

        payload = {"action": "publish", "topic": topic_a, "payload": "secret-A"}
        pub_ws.send_text(json.dumps(payload))

        msg_a = sub_a.receive_text()
        obj_a = json.loads(msg_a)
        assert obj_a.get("payload") == "secret-A"

        # ensure sub_b does NOT receive a message — attempt receive in a background thread
        q = queue.Queue()

        def try_recv():
            try:
                msg = sub_b.receive_text()
                q.put(msg)
            except Exception as e:
                q.put(e)

        t = threading.Thread(target=try_recv, daemon=True)
        t.start()
        t.join(timeout=0.2)

        got_b = not q.empty()
        assert not got_b, "Subscriber on topic B unexpectedly received a message for topic A"
