"""Connection manager for the in-process WebSocket message broker.

The broker groups WebSocket subscribers by topic and remembers whether each
client speaks JSON or MessagePack on the wire.
"""

import asyncio
import json
from typing import Any, Dict, Optional, Set

import msgpack
from fastapi import WebSocket


class ConnectionManager:
    """Keep track of topic subscribers and send protocol messages to them.

    Attributes:
        topics: Mapping of topic name to currently subscribed WebSocket clients.
        client_formats: Preferred serialization format per WebSocket.
    """

    def __init__(self) -> None:
        self.topics: Dict[str, Set[WebSocket]] = {}
        self.client_formats: Dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, topic: str, mode: str = "json", subscribe: bool = True) -> None:
        """Accept a socket and optionally register it as a topic subscriber.

        Args:
            websocket: Connected client socket.
            topic: Topic name handled by the route.
            mode: Serialization format, either ``json`` or ``msgpack``.
            subscribe: Publishers can connect without being added to the
                subscriber set; subscribers are tracked in ``self.topics``.
        """
        await websocket.accept()
        async with self._lock:
            if subscribe:
                self.topics.setdefault(topic, set()).add(websocket)
                if topic == "storage.write":
                    try:
                        client = websocket.client
                    except Exception:
                        client = None
                    print(f"[BROKER] New subscriber for 'storage.write' (mode={mode}) client={client}")
            self.client_formats[websocket] = mode

    async def disconnect(self, websocket: WebSocket, topic: str) -> None:
        """Remove a socket from broker bookkeeping.

        Empty topics are removed to keep the in-memory structure compact.
        """
        async with self._lock:
            self.client_formats.pop(websocket, None)
            sockets = self.topics.get(topic)
            if not sockets:
                return
            sockets.discard(websocket)
            if not sockets:
                del self.topics[topic]

    async def send_protocol_message(self, websocket: WebSocket, message: Dict[str, Any]) -> None:
        """Serialize and send one protocol message to a single client."""
        mode = self.client_formats.get(websocket, "json")
        if mode == "msgpack":
            await websocket.send_bytes(msgpack.packb(message, use_bin_type=True))
        else:
            # Ensure bytes in message are converted to base64 so JSON is valid
            def _convert(obj):
                import base64
                if isinstance(obj, (bytes, bytearray)):
                    return {"__type": "bytes", "data": base64.b64encode(bytes(obj)).decode("ascii")}
                if isinstance(obj, dict):
                    return {k: _convert(v) for k, v in obj.items()}
                if isinstance(obj, list):
                    return [_convert(v) for v in obj]
                return obj

            safe = _convert(message)
            await websocket.send_text(json.dumps(safe))

    async def broadcast(self, message: Dict[str, Any], topic: str, sender: Optional[WebSocket] = None) -> None:
        """Send a protocol message to all subscribers of a topic.

        Args:
            message: Already validated broker envelope.
            topic: Target topic.
            sender: Optional socket to exclude from broadcast.
        """
        async with self._lock:
            receivers = {ws for ws in self.topics.get(topic, set()) if ws is not sender}

        async def send_one(ws: WebSocket) -> None:
            try:
                await self.send_protocol_message(ws, message)
            except Exception:
                try:
                    await self.disconnect(ws, topic)
                except Exception:
                    pass

        await asyncio.gather(*(send_one(ws) for ws in receivers))


manager = ConnectionManager()
