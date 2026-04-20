import asyncio
import json
from typing import Any, Dict, Set, Optional

import msgpack

from fastapi import WebSocket


class ConnectionManager:
    """
    Manages WebSocket connections grouped by topic.
    Uses dict[str, set[WebSocket]] to store connected sockets per topic.
    Stores the preferred wire format for each client.
    """
    def __init__(self) -> None:
        self.topics: Dict[str, Set[WebSocket]] = {}
        self.client_formats: Dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, topic: str, mode: str = "json", subscribe: bool = True) -> None:
        """Accept the socket and register it under the given topic."""
        await websocket.accept()
        async with self._lock:
            if subscribe:
                self.topics.setdefault(topic, set()).add(websocket)
            self.client_formats[websocket] = mode

    async def disconnect(self, websocket: WebSocket, topic: str) -> None:
        """Remove the socket from the topic; delete topic if empty."""
        async with self._lock:
            self.client_formats.pop(websocket, None)
            sockets = self.topics.get(topic)
            if not sockets:
                return
            sockets.discard(websocket)
            if not sockets:
                # remove empty topic to keep structure clean
                del self.topics[topic]

    async def send_protocol_message(self, websocket: WebSocket, message: Dict[str, Any]) -> None:
        """Send a protocol message using the recipient's preferred wire format."""
        mode = self.client_formats.get(websocket, "json")
        if mode == "msgpack":
            await websocket.send_bytes(msgpack.packb(message, use_bin_type=True))
        else:
            await websocket.send_text(json.dumps(message))

    async def broadcast(self, message: Dict[str, Any], topic: str, sender: Optional[WebSocket] = None) -> None:
        """Send a protocol message to all sockets in `topic` except `sender`.
        We copy the receivers under the lock and perform network I/O outside the lock.
        """
        async with self._lock:
            receivers = {ws for ws in self.topics.get(topic, set()) if ws is not sender}

        async def send_one(ws: WebSocket) -> None:
            try:
                await self.send_protocol_message(ws, message)
            except Exception:
                # on failure, attempt to remove the socket so state stays consistent
                try:
                    await self.disconnect(ws, topic)
                except Exception:
                    pass

        await asyncio.gather(*(send_one(ws) for ws in receivers))


# single manager instance to import/use from the app
manager = ConnectionManager()
