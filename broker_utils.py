import asyncio
from typing import Dict, Set, Optional, Union

from fastapi import WebSocket


class ConnectionManager:
    """
    Manages WebSocket connections grouped by topic.
    Uses dict[str, set[WebSocket]] to store connected sockets per topic.
    Supports broadcasting both text (str) and binary (bytes) messages.
    """
    def __init__(self) -> None:
        self.topics: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, topic: str) -> None:
        """Accept the socket and register it under the given topic."""
        await websocket.accept()
        async with self._lock:
            self.topics.setdefault(topic, set()).add(websocket)

    async def disconnect(self, websocket: WebSocket, topic: str) -> None:
        """Remove the socket from the topic; delete topic if empty."""
        async with self._lock:
            sockets = self.topics.get(topic)
            if not sockets:
                return
            sockets.discard(websocket)
            if not sockets:
                # remove empty topic to keep structure clean
                del self.topics[topic]

    async def broadcast(self, message: Union[str, bytes], topic: str, sender: Optional[WebSocket] = None) -> None:
        """Send `message` (text or bytes) to all sockets in `topic` except `sender`.
        We copy the receivers under the lock and perform network I/O outside the lock.
        """
        async with self._lock:
            receivers = {ws for ws in self.topics.get(topic, set()) if ws is not sender}

        for ws in receivers:
            try:
                if isinstance(message, bytes):
                    await ws.send_bytes(message)
                else:
                    await ws.send_text(message)
            except Exception:
                # on failure, attempt to remove the socket so state stays consistent
                try:
                    await self.disconnect(ws, topic)
                except Exception:
                    pass


# single manager instance to import/use from the app
manager = ConnectionManager()
