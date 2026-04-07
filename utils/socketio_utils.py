"""
Socket.IO utilities for real-time bidirectional communication.

Provides Socket.IO client/server, room management, namespace
handling, event broadcasting, and authentication integration.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PacketType(Enum):
    """Socket.IO packet types."""
    CONNECT = 0
    DISCONNECT = 1
    EVENT = 2
    ACK = 3
    ERROR = 4
    BINARY_EVENT = 5
    BINARY_ACK = 6
    CONNECT_ERROR = 7


@dataclass
class SocketIOConfig:
    """Configuration for Socket.IO."""
    host: str = "localhost"
    port: int = 3000
    path: str = "/socket.io/"
    transports: list[str] = field(default_factory=lambda: ["polling", "websocket"])
    upgrade: bool = True
    rememberUpgrade: bool = False
    auth: dict[str, Any] = field(default_factory=dict)
    namespace: str = "/"
    ping_timeout: int = 20
    pong_timeout: int = 5


@dataclass
class SocketIOEvent:
    """Represents a Socket.IO event."""
    name: str
    data: Any
    namespace: str = "/"
    timestamp: float = field(default_factory=time.time)


class SocketIOClient:
    """Socket.IO client for connecting to a server."""

    def __init__(self, config: Optional[SocketIOConfig] = None) -> None:
        self.config = config or SocketIOConfig()
        self._handlers: dict[str, list[Callable[..., Any]]] = {}
        self._rooms: set[str] = set()
        self._connected = False
        self._session: Any = None

    def on(self, event: str) -> Callable:
        """Decorator to register an event handler."""
        def decorator(func: Callable[..., Any]) -> Callable:
            if event not in self._handlers:
                self._handlers[event] = []
            self._handlers[event].append(func)
            return func
        return decorator

    async def connect(self) -> bool:
        """Establish connection to the Socket.IO server."""
        try:
            import socketio
            self._session = socketio.AsyncClient()
            for event, handlers in self._handlers.items():
                for handler in handlers:
                    self._session.on(event, handler, namespace=self.config.namespace)
            await self._session.connect(
                f"http://{self.config.host}:{self.config.port}",
                transports=self.config.transports,
                auth=self.config.auth,
                socketio_path=self.config.path,
            )
            self._connected = True
            logger.info("Socket.IO connected to %s:%d", self.config.host, self.config.port)
            return True
        except ImportError:
            logger.warning("python-socketio not installed")
            self._connected = True
            return True
        except Exception as e:
            logger.error("Socket.IO connection failed: %s", e)
            return False

    async def emit(self, event: str, data: Any = None, namespace: Optional[str] = None, ack: Optional[Callable] = None) -> None:
        """Emit an event to the server."""
        if not self._connected:
            if not await self.connect():
                return
        ns = namespace or self.config.namespace
        if self._session:
            await self._session.emit(event, data or {}, namespace=ns, callback=ack)

    async def emit_with_ack(self, event: str, data: Any = None, timeout: float = 5.0) -> Any:
        """Emit an event and wait for acknowledgment."""
        if not self._connected:
            if not await self.connect():
                return None

        result = None
        ack_received = asyncio.Event()

        async def ack_callback(*args: Any) -> None:
            nonlocal result
            result = args[0] if args else None
            ack_received.set()

        if self._session:
            await self._session.emit(event, data or {}, callback=ack_callback)
            try:
                await asyncio.wait_for(ack_received.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("ACK timeout for event: %s", event)
        return result

    async def join_room(self, room: str) -> None:
        """Join a room."""
        await self.emit("join", {"room": room})
        self._rooms.add(room)

    async def leave_room(self, room: str) -> None:
        """Leave a room."""
        await self.emit("leave", {"room": room})
        self._rooms.discard(room)

    async def disconnect(self) -> None:
        """Disconnect from the server."""
        if self._session:
            await self._session.disconnect()
        self._connected = False
        logger.info("Socket.IO disconnected")

    @property
    def connected(self) -> bool:
        return self._connected


class SocketIOServer:
    """Socket.IO server for handling multiple clients."""

    def __init__(self, config: Optional[SocketIOConfig] = None) -> None:
        self.config = config or SocketIOConfig()
        self._app: Any = None
        self._io: Any = None
        self._handlers: dict[str, Callable[..., Any]] = {}
        self._rooms: dict[str, set[str]] = {}

    def on(self, event: str) -> Callable:
        """Decorator to register an event handler."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._handlers[event] = func
            return func
        return decorator

    async def emit(self, event: str, data: Any, room: Optional[str] = None, namespace: str = "/") -> int:
        """Emit an event to clients in a room or all clients."""
        if not self._io:
            return 0
        if room:
            return await self._io.emit(event, data, room=room, namespace=namespace)
        return await self._io.emit(event, data, namespace=namespace)

    async def broadcast(self, event: str, data: Any, include_self: bool = True) -> None:
        """Broadcast to all connected clients."""
        if self._io:
            await self._io.emit(event, data, include_self=include_self)

    async def save_session(self, sid: str, data: dict[str, Any]) -> None:
        """Save session data for a client."""
        if self._io:
            await self._io.save_session(sid, data)

    async def get_session(self, sid: str) -> dict[str, Any]:
        """Get session data for a client."""
        if self._io:
            return await self._io.get_session(sid)
        return {}

    def start(self, app: Any) -> bool:
        """Start the Socket.IO server attached to an ASGI app."""
        try:
            import socketio
            self._app = app
            self._io = socketio.AsyncServer(
                async_mode="asgi",
                always_connect=False,
                cors_allowed_origins="*",
            )
            for event, handler in self._handlers.items():
                self._io.on(event, handler, namespace=self.config.namespace)

            @self._io.on("connect", namespace=self.config.namespace)
            async def connect(sid: str, env: dict) -> bool:
                logger.info("Client connected: %s", sid)
                return True

            @self._io.on("disconnect", namespace=self.config.namespace)
            async def disconnect(sid: str) -> None:
                logger.info("Client disconnected: %s", sid)

            return True
        except ImportError:
            logger.warning("python-socketio not installed")
            return False
        except Exception as e:
            logger.error("Failed to start Socket.IO server: %s", e)
            return False

    def attach_to_app(self, app: Any) -> None:
        """Attach the Socket.IO server to an ASGI app."""
        if self._io and self._app:
            self._app.add_socketio_middleware(self._io, self.config.path)


class NamespaceHandler:
    """Handler for Socket.IO namespace events."""

    def __init__(self, namespace: str = "/") -> None:
        self.namespace = namespace
        self._handlers: dict[str, Callable[..., Any]] = {}

    def on(self, event: str) -> Callable:
        """Decorator for event handlers within this namespace."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._handlers[event] = func
            return func
        return decorator

    async def trigger_event(self, event: str, *args: Any) -> None:
        """Trigger an event handler."""
        handler = self._handlers.get(event)
        if handler:
            await handler(*args)
