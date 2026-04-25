"""Integration tests for the in-process WebSocket broker."""

import json
import queue
import threading
import time
import uuid

import msgpack
import pytest
from fastapi.testclient import TestClient

import models
from broker_utils import manager
from database import SessionLocal
from main import app


@pytest.fixture(autouse=True)
def clean_broker_state():
    """Reset in-memory broker state and queued message rows between tests."""
    manager.topics.clear()
    manager.client_formats.clear()

    db = SessionLocal()
    try:
        db.query(models.QueuedMessage).delete()
        db.commit()
    finally:
        db.close()

    yield

    manager.topics.clear()
    manager.client_formats.clear()

    db = SessionLocal()
    try:
        db.query(models.QueuedMessage).delete()
        db.commit()
    finally:
        db.close()



def unique_topic(prefix: str) -> str:
    """Build a unique topic name so tests do not interfere with each other."""
    return f"{prefix}_{uuid.uuid4().hex}"



def get_queued_messages(topic: str) -> list[dict]:
    """Read durable broker rows for one topic from the database."""
    db = SessionLocal()
    try:
        messages = (
            db.query(models.QueuedMessage)
            .filter(models.QueuedMessage.topic == topic)
            .order_by(models.QueuedMessage.id.asc())
            .all()
        )
        return [
            {
                "id": message.id,
                "topic": message.topic,
                "is_delivered": message.is_delivered,
                "payload_format": message.payload_format,
            }
            for message in messages
        ]
    finally:
        db.close()



def wait_until(callback, timeout: float = 1.0):
    """Poll until a callback returns a truthy value or timeout is reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = callback()
        if result:
            return result
        time.sleep(0.01)
    return callback()



def connected_count(topic: str) -> int:
    """Return the number of active subscribers tracked for a topic."""
    return len(manager.topics.get(topic, set()))



def receive_json(ws) -> dict:
    """Read one JSON broker frame from a TestClient websocket."""
    return json.loads(ws.receive_text())



def receive_msgpack(ws) -> dict:
    """Read one MessagePack broker frame from a TestClient websocket."""
    return msgpack.unpackb(ws.receive_bytes(), raw=False)



def receive_nothing(ws, timeout: float = 0.2) -> bool:
    """Return True when no frame arrives within a short timeout window."""
    q = queue.Queue()

    def try_recv():
        try:
            q.put(ws.receive_text())
        except Exception as exc:
            q.put(exc)

    t = threading.Thread(target=try_recv, daemon=True)
    t.start()
    t.join(timeout=timeout)
    return q.empty()



def test_connection_and_disconnect():
    """Subscribers should appear in broker state and then disappear on close."""
    topic = unique_topic("test_conn")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json"):
        assert wait_until(lambda: connected_count(topic) == 1)

    time.sleep(0.05)
    assert topic not in manager.topics or len(manager.topics.get(topic, set())) == 0



def test_correct_routing():
    """A published message should be delivered to subscribers of the same topic."""
    topic = unique_topic("topic_a")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as ws:
        assert wait_until(lambda: connected_count(topic) >= 1)
        payload = {"action": "publish", "topic": topic, "payload": "hello-A"}
        ws.send_text(json.dumps(payload))

        received = receive_json(ws)
        assert received.get("action") == "deliver"
        assert received.get("topic") == topic
        assert isinstance(received.get("message_id"), int)
        assert received.get("payload") == "hello-A"



def test_topic_isolation():
    """Messages from topic A must not leak into topic B subscribers."""
    topic_a = unique_topic("topic_isolation_a")
    topic_b = unique_topic("topic_isolation_b")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic_a}?mode=json") as sub_a, client.websocket_connect(
        f"/ws/broker/{topic_b}?mode=json"
    ) as sub_b:
        assert wait_until(lambda: connected_count(topic_a) >= 1)
        assert wait_until(lambda: connected_count(topic_b) >= 1)
        payload = {"action": "publish", "topic": topic_a, "payload": "secret-A"}
        sub_a.send_text(json.dumps(payload))

        msg_a = receive_json(sub_a)
        assert msg_a.get("payload") == "secret-A"
        assert receive_nothing(sub_b)



def test_publish_persists_undelivered_message():
    """Durable publish should store the message until an ACK is received."""
    topic = unique_topic("persist")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as publisher_ws:
        payload = {"action": "publish", "topic": topic, "payload": {"temp": 22.5}}
        publisher_ws.send_text(json.dumps(payload))

        messages = wait_until(lambda: get_queued_messages(topic))
        assert len(messages) == 1
        assert messages[0]["is_delivered"] is False
        assert messages[0]["payload_format"] == "json"



def test_ack_marks_message_delivered():
    """ACK should mark the queued durable message as delivered."""
    topic = unique_topic("ack")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as ws:
        assert wait_until(lambda: connected_count(topic) >= 1)
        ws.send_text(json.dumps({"action": "publish", "topic": topic, "payload": "needs-ack"}))
        delivered = receive_json(ws)
        message_id = delivered["message_id"]

        ws.send_text(json.dumps({"action": "ack", "message_id": message_id}))

        messages = wait_until(lambda: [message for message in get_queued_messages(topic) if message["is_delivered"]])
        assert messages[0]["id"] == message_id



def test_invalid_ack_returns_protocol_error():
    """Malformed ACK messages should be rejected by broker validation."""
    topic = unique_topic("invalid_ack")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as ws:
        ws.send_text(json.dumps({"action": "ack", "message_id": "oops"}))
        error = receive_json(ws)
        assert error["action"] == "error"



def test_reconnect_receives_pending_message():
    """A reconnecting subscriber should receive previously undelivered data."""
    topic = unique_topic("reconnect")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as publisher_ws:
        publisher_ws.send_text(json.dumps({"action": "publish", "topic": topic, "payload": "stored"}))
        assert wait_until(lambda: get_queued_messages(topic))

    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as subscriber_ws:
        delivered = receive_json(subscriber_ws)
        assert delivered["action"] == "deliver"
        assert delivered["topic"] == topic
        assert delivered["payload"] == "stored"



def test_delivered_message_is_not_replayed_after_ack():
    """Once acknowledged, a durable message should no longer replay on reconnect."""
    topic = unique_topic("no_replay")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as publisher_ws:
        publisher_ws.send_text(json.dumps({"action": "publish", "topic": topic, "payload": "once"}))
        assert wait_until(lambda: get_queued_messages(topic))

    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as subscriber_ws:
        delivered = receive_json(subscriber_ws)
        subscriber_ws.send_text(json.dumps({"action": "ack", "message_id": delivered["message_id"]}))
        assert wait_until(lambda: [message for message in get_queued_messages(topic) if message["is_delivered"]])

    with client.websocket_connect(f"/ws/broker/{topic}?mode=json") as subscriber_ws:
        assert receive_nothing(subscriber_ws)



def test_msgpack_publish_deliver_and_ack():
    """The broker should support MessagePack delivery and acknowledgements too."""
    topic = unique_topic("msgpack")
    client = TestClient(app)
    with client.websocket_connect(f"/ws/broker/{topic}?mode=msgpack") as ws:
        assert wait_until(lambda: connected_count(topic) >= 1)
        payload = {"action": "publish", "topic": topic, "payload": {"temperature": 22.5}}
        ws.send_bytes(msgpack.packb(payload, use_bin_type=True))

        delivered = receive_msgpack(ws)
        assert delivered["action"] == "deliver"
        assert delivered["payload"] == {"temperature": 22.5}

        ws.send_bytes(msgpack.packb({"action": "ack", "message_id": delivered["message_id"]}, use_bin_type=True))
        messages = wait_until(lambda: [message for message in get_queued_messages(topic) if message["is_delivered"]])
        assert messages[0]["payload_format"] == "msgpack"
