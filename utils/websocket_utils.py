"""
WebSocket utilities for client and server implementations.

Provides async WebSocket client/server helpers, message framing,
reconnection logic, ping/pong handling, and multi-protocol support.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

import websockets
from websockets.client import WebSocketClientProtocol
from websockets.server import WebSocketServerProtocol

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """WebSocket message types."""
    TEXT = auto()
    BINARY = auto()
    PING = auto()
    PONG = auto()
    CLOSE = auto()


@dataclass
class WebSocketMessage:
    """Represents a WebSocket message with metadata."""
    payload: str | bytes
    msg_type: MessageType = MessageType.TEXT
    timestamp: float = field(default_factory=time.time)
    opcode: int = 0

    @property
    def is_text(self) -> bool:
        return self.msg_type == MessageType.TEXT

    @property
    def is_binary(self) -> bool:
        return self.msg_type == MessageType.BINARY

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload": self.payload.decode() if isinstance(self.payload, bytes) else self.payload,
            "msg_type": self.msg_type.name,
            "timestamp": self.timestamp,
            "opcode": self.opcode,
        }


@dataclass
class WebSocketConfig:
    """Configuration for WebSocket connections."""
    uri: str = "ws://localhost:8765"
    timeout: float = 30.0
    max_size: int = 10 * 1024 * 1024  # 10MB
    ping_interval: float = 20.0
    ping_timeout: float = 10.0
    close_timeout: float = 5.0
    max_reconnects: int = 10
    reconnect_delay: float = 1.0
    reconnect_multiplier: float = 1.5
    max_reconnect_delay: float = 60.0


class WebSocketClient(ABC):
    """Async WebSocket client with auto-reconnect and message routing."""

    def __init__(self, config: Optional[WebSocketConfig] = None) -> None:
        self.config = config or WebSocketConfig()
        self._conn: Optional[WebSocketClientProtocol] = None
        self._running: bool = False
        self._reconnect_count: int = 0
        self._handlers: dict[str, Callable[..., Coroutine[Any, Any, None]]] = {}
        self._default_handler: Optional[Callable[..., Coroutine[Any, Any, None]]] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def on(self, event: str) -> Callable:
        """Decorator to register an event handler."""
        def decorator(func: Callable[..., Coroutine[Any, Any, None]]) -> Callable:
            self._handlers[event] = func
            return func
        return decorator

    def on_message(self, func: Callable[..., Coroutine[Any, Any, None]]) -> Callable:
        """Decorator to register the default message handler."""
        self._default_handler = func
        return func

    async def connect(self) -> bool:
        """Establish connection to the WebSocket server."""
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._reconnect_count = 0
        try:
            self._conn = await websockets.connect(
                self.config.uri,
                timeout=self.config.timeout,
                max_size=self.config.max_size,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
                close_timeout=self.config.close_timeout,
            )
            logger.info("WebSocket connected to %s", self.config.uri)
            return True
        except Exception as e:
            logger.error("Failed to connect: %s", e)
            return False

    async def _reconnect(self) -> None:
        """Attempt to reconnect with exponential backoff."""
        delay = self.config.reconnect_delay
        while self._running and self._reconnect_count < self.config.max_reconnects:
            await asyncio.sleep(delay)
            logger.info("Reconnecting (attempt %d)...", self._reconnect_count + 1)
            if await self.connect():
                self._reconnect_count = 0
                return
            self._reconnect_count += 1
            delay = min(delay * self.config.reconnect_multiplier, self.config.max_reconnect_delay)
        logger.error("Max reconnects reached, giving up")

    async def listen(self) -> None:
        """Start listening for messages."""
        if not self._conn:
            if not await self.connect():
                raise RuntimeError("Cannot start listening: not connected")
        try:
            async for raw_msg in self._conn:
                msg = self._parse_message(raw_msg)
                asyncio.create_task(self._dispatch(msg))
        except websockets.exceptions.ConnectionClosed:
            logger.warning("Connection closed")
        finally:
            if self._running:
                await self._reconnect()

    def _parse_message(self, raw: Any) -> WebSocketMessage:
        """Parse a raw WebSocket message."""
        if isinstance(raw, str):
            return WebSocketMessage(payload=raw, msg_type=MessageType.TEXT, opcode=1)
        elif isinstance(raw, bytes):
            return WebSocketMessage(payload=raw, msg_type=MessageType.BINARY, opcode=2)
        return WebSocketMessage(payload=str(raw), msg_type=MessageType.TEXT, opcode=1)

    async def _dispatch(self, msg: WebSocketMessage) -> None:
        """Dispatch message to appropriate handler."""
        try:
            data = json.loads(msg.payload) if msg.is_text else msg.payload
            if isinstance(data, dict) and "event" in data:
                handler = self._handlers.get(data["event"])
                if handler:
                    await handler(data)
                    return
            if self._default_handler:
                await self._default_handler(msg)
        except json.JSONDecodeError:
            if self._default_handler:
                await self._default_handler(msg)
        except Exception as e:
            logger.error("Handler error: %s", e)

    async def send(self, payload: str | bytes, opcode: int = 1) -> None:
        """Send a message through the connection."""
        if self._conn:
            await self._conn.send(payload)

    async def send_json(self, data: dict[str, Any]) -> None:
        """Send a JSON-encoded message."""
        await self.send(json.dumps(data))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the WebSocket connection."""
        self._running = False
        if self._conn:
            await self._conn.close(code, reason)


class WebSocketServer:
    """Async WebSocket server with broadcast and room support."""

    def __init__(self, host: str = "localhost", port: int = 8765) -> None:
        self.host = host
        self.port = port
        self._clients: set[WebSocketServerProtocol] = set()
        self._rooms: dict[str, set[WebSocketServerProtocol]] = {}
        self._running: bool = False
        self._handlers: dict[str, Callable[..., Coroutine[Any, Any, None]]] = {}
        self._default_handler: Optional[Callable[..., Coroutine[Any, Any, None]]] = None

    def on(self, event: str) -> Callable:
        """Decorator to register an event handler."""
        def decorator(func: Callable[..., Coroutine[Any, Any, None]]) -> Callable:
            self._handlers[event] = func
            return func
        return decorator

    async def _broadcast(self, message: str | bytes, room: Optional[str] = None) -> int:
        """Broadcast message to all clients or a specific room."""
        targets = self._rooms.get(room, self._clients) if room else self._clients
        count = 0
        for client in list(targets):
            try:
                await client.send(message)
                count += 1
            except Exception:
                self._clients.discard(client)
        return count

    async def _handle_client(self, conn: WebSocketServerProtocol, path: str) -> None:
        """Handle individual client connection."""
        self._clients.add(conn)
        logger.info("Client connected from %s", conn.remote_address)
        try:
            async for raw_msg in conn:
                msg = self._parse_message(raw_msg)
                asyncio.create_task(self._dispatch(conn, msg))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._clients.discard(conn)
            for room in self._rooms.values():
                room.discard(conn)
            logger.info("Client disconnected")

    def _parse_message(self, raw: Any) -> WebSocketMessage:
        if isinstance(raw, str):
            return WebSocketMessage(payload=raw, msg_type=MessageType.TEXT, opcode=1)
        elif isinstance(raw, bytes):
            return WebSocketMessage(payload=raw, msg_type=MessageType.BINARY, opcode=2)
        return WebSocketMessage(payload=str(raw), msg_type=MessageType.TEXT, opcode=1)

    async def _dispatch(self, conn: WebSocketServerProtocol, msg: WebSocketMessage) -> None:
        """Dispatch message to appropriate handler."""
        try:
            if msg.is_text:
                data = json.loads(msg.payload)
                if isinstance(data, dict) and "event" in data:
                    handler = self._handlers.get(data["event"])
                    if handler:
                        await handler(conn, data)
                        return
            if self._default_handler:
                await self._default_handler(conn, msg)
        except json.JSONDecodeError:
            if self._default_handler:
                await self._default_handler(conn, msg)
        except Exception as e:
            logger.error("Server handler error: %s", e)

    async def start(self) -> None:
        """Start the WebSocket server."""
        self._running = True
        async with websockets.serve(self._handle_client, self.host, self.port):
            logger.info("WebSocket server started on %s:%d", self.host, self.port)
            await asyncio.Future()  # run forever

    def broadcast(self, message: str | bytes) -> Coroutine[Any, Any, int]:
        """Broadcast to all connected clients."""
        return self._broadcast(message)

    def broadcast_room(self, room: str, message: str | bytes) -> Coroutine[Any, Any, int]:
        """Broadcast to clients in a specific room."""
        return self._broadcast(message, room=room)
