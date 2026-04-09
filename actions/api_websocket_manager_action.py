"""API WebSocket Connection Manager.

This module provides WebSocket connection management:
- Connection lifecycle
- Room/topic management
- Message routing
- Heartbeat/keepalive

Example:
    >>> from actions.api_websocket_manager_action import WebSocketManager
    >>> manager = WebSocketManager()
    >>> conn_id = manager.connect(ws, client_id="user_123")
    >>> manager.send_to_room("room_1", {"event": "message", "data": "hello"})
"""

from __future__ import annotations

import time
import json
import logging
import threading
import uuid
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class Connection:
    """A WebSocket connection."""
    conn_id: str
    client_id: str
    ws: Any
    rooms: set[str] = field(default_factory=set)
    connected_at: float = field(default_factory=time.time)
    last_ping: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_alive: bool = True


class WebSocketManager:
    """Manages WebSocket connections and message routing."""

    def __init__(
        self,
        heartbeat_interval: float = 30.0,
        heartbeat_timeout: float = 60.0,
        max_connections: int = 10000,
    ) -> None:
        """Initialize the WebSocket manager.

        Args:
            heartbeat_interval: Seconds between heartbeats.
            heartbeat_timeout: Seconds before considering connection dead.
            max_connections: Maximum concurrent connections.
        """
        self._connections: dict[str, Connection] = {}
        self._client_connections: dict[str, set[str]] = defaultdict(set)
        self._rooms: dict[str, set[str]] = defaultdict(set)
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._max_connections = max_connections
        self._lock = threading.RLock()
        self._stats = {"connects": 0, "disconnects": 0, "messages_sent": 0, "messages_received": 0}
        self._message_handlers: dict[str, Callable] = {}
        self._running = False
        self._heartbeat_thread: Optional[threading.Thread] = None

    def connect(
        self,
        ws: Any,
        client_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> str:
        """Register a new WebSocket connection.

        Args:
            ws: WebSocket object.
            client_id: Client identifier.
            metadata: Additional connection metadata.

        Returns:
            Connection ID.
        """
        with self._lock:
            if len(self._connections) >= self._max_connections:
                raise RuntimeError("Max connections reached")

            conn_id = str(uuid.uuid4())[:12]
            conn = Connection(
                conn_id=conn_id,
                client_id=client_id,
                ws=ws,
                metadata=metadata or {},
            )
            self._connections[conn_id] = conn
            self._client_connections[client_id].add(conn_id)
            self._stats["connects"] += 1

            logger.info("WebSocket connected: %s (client=%s)", conn_id, client_id)
            return conn_id

    def disconnect(self, conn_id: str, reason: str = "") -> None:
        """Disconnect a WebSocket connection.

        Args:
            conn_id: Connection ID.
            reason: Disconnect reason.
        """
        with self._lock:
            conn = self._connections.get(conn_id)
            if conn is None:
                return

            for room in conn.rooms:
                self._rooms[room].discard(conn_id)

            self._client_connections[conn.client_id].discard(conn_id)
            del self._connections[conn_id]
            conn.is_alive = False
            self._stats["disconnects"] += 1

            logger.info("WebSocket disconnected: %s (reason=%s)", conn_id, reason)

    def join_room(self, conn_id: str, room: str) -> bool:
        """Join a room/topic.

        Args:
            conn_id: Connection ID.
            room: Room name.

        Returns:
            True if joined successfully.
        """
        with self._lock:
            conn = self._connections.get(conn_id)
            if conn is None:
                return False

            conn.rooms.add(room)
            self._rooms[room].add(conn_id)
            logger.info("Connection %s joined room %s", conn_id, room)
            return True

    def leave_room(self, conn_id: str, room: str) -> bool:
        """Leave a room.

        Args:
            conn_id: Connection ID.
            room: Room name.

        Returns:
            True if left successfully.
        """
        with self._lock:
            conn = self._connections.get(conn_id)
            if conn is None:
                return False

            conn.rooms.discard(room)
            self._rooms[room].discard(conn_id)
            return True

    def send_to_connection(self, conn_id: str, message: dict[str, Any]) -> bool:
        """Send a message to a specific connection.

        Args:
            conn_id: Connection ID.
            message: Message dict.

        Returns:
            True if sent successfully.
        """
        with self._lock:
            conn = self._connections.get(conn_id)
            if conn is None or not conn.is_alive:
                return False

        try:
            conn.ws.send(json.dumps(message))
            self._stats["messages_sent"] += 1
            return True
        except Exception as e:
            logger.error("Failed to send to %s: %s", conn_id, e)
            return False

    def send_to_client(self, client_id: str, message: dict[str, Any]) -> int:
        """Send a message to all connections of a client.

        Args:
            client_id: Client identifier.
            message: Message dict.

        Returns:
            Number of messages sent.
        """
        with self._lock:
            conn_ids = list(self._client_connections.get(client_id, set()))

        sent = 0
        for conn_id in conn_ids:
            if self.send_to_connection(conn_id, message):
                sent += 1
        return sent

    def send_to_room(self, room: str, message: dict[str, Any]) -> int:
        """Broadcast a message to all connections in a room.

        Args:
            room: Room name.
            message: Message dict.

        Returns:
            Number of messages sent.
        """
        with self._lock:
            conn_ids = list(self._rooms.get(room, set()))

        sent = 0
        for conn_id in conn_ids:
            if self.send_to_connection(conn_id, message):
                sent += 1
        return sent

    def broadcast(self, message: dict[str, Any]) -> int:
        """Broadcast a message to all connections.

        Args:
            message: Message dict.

        Returns:
            Number of messages sent.
        """
        with self._lock:
            conn_ids = list(self._connections.keys())

        sent = 0
        for conn_id in conn_ids:
            if self.send_to_connection(conn_id, message):
                sent += 1
        return sent

    def register_handler(self, event_type: str, handler: Callable[[str, dict], None]) -> None:
        """Register a message handler for an event type.

        Args:
            event_type: Event type string.
            handler: Handler function (conn_id, message).
        """
        with self._lock:
            self._message_handlers[event_type] = handler

    def handle_message(self, conn_id: str, message: dict[str, Any]) -> None:
        """Handle an incoming message.

        Args:
            conn_id: Connection ID.
            message: Message dict.
        """
        self._stats["messages_received"] += 1
        event_type = message.get("type", "message")

        with self._lock:
            handler = self._message_handlers.get(event_type)

        if handler:
            try:
                handler(conn_id, message)
            except Exception as e:
                logger.error("Message handler error: %s", e)

    def ping(self, conn_id: str) -> bool:
        """Send a ping to a connection.

        Args:
            conn_id: Connection ID.

        Returns:
            True if ping sent.
        """
        with self._lock:
            conn = self._connections.get(conn_id)
            if conn is None:
                return False
            conn.last_ping = time.time()
        return True

    def start_heartbeat(self) -> None:
        """Start the heartbeat thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()
            logger.info("WebSocket heartbeat started")

    def stop_heartbeat(self) -> None:
        """Stop the heartbeat thread."""
        with self._lock:
            self._running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5.0)
        logger.info("WebSocket heartbeat stopped")

    def _heartbeat_loop(self) -> None:
        """Heartbeat check loop."""
        while self._running:
            now = time.time()
            timeout = now - self._heartbeat_timeout

            with self._lock:
                dead = [
                    conn_id for conn_id, conn in self._connections.items()
                    if conn.last_ping < timeout
                ]

            for conn_id in dead:
                self.disconnect(conn_id, reason="heartbeat_timeout")

            time.sleep(self._heartbeat_interval)

    def get_connection(self, conn_id: str) -> Optional[Connection]:
        """Get connection info."""
        with self._lock:
            return self._connections.get(conn_id)

    def list_connections(self, room: Optional[str] = None) -> list[Connection]:
        """List connections, optionally filtered by room."""
        with self._lock:
            if room:
                conn_ids = self._rooms.get(room, set())
                return [self._connections[c] for c in conn_ids if c in self._connections]
            return list(self._connections.values())

    def get_stats(self) -> dict[str, Any]:
        """Get WebSocket statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_connections": len(self._connections),
                "active_rooms": len(self._rooms),
            }
