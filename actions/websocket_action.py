"""
WebSocket Action Module

Provides WebSocket client and server functionality for real-time communication
in UI automation workflows. Supports text/binary messages, ping/pong, and
connection management.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class WebSocketState(Enum):
    """WebSocket connection state."""
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()
    DISCONNECTED = auto()
    ERROR = auto()


class MessageType(Enum):
    """WebSocket message type."""
    TEXT = auto()
    BINARY = auto()
    PING = auto()
    PONG = auto()
    CLOSE = auto()


@dataclass
class WebSocketMessage:
    """Represents a WebSocket message."""
    type: MessageType
    data: str | bytes
    timestamp: float = field(default_factory=lambda: asyncio.get_event_loop().time())
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WebSocketConfig:
    """WebSocket connection configuration."""
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    ping_interval: float = 30.0
    ping_timeout: float = 10.0
    close_timeout: float = 10.0
    max_message_size: int = 10 * 1024 * 1024
    receive_timeout: float = 60.0
    ssl_context: Optional[Any] = None


class WebSocketClient:
    """
    WebSocket client for real-time communication.

    Example:
        >>> async def main():
        ...     client = WebSocketClient(WebSocketConfig("wss://echo.websocket.org"))
        ...     await client.connect()
        ...     await client.send_text("Hello")
        ...     response = await client.receive()
        ...     await client.disconnect()
        ...     return response
    """

    def __init__(self, config: WebSocketConfig) -> None:
        self.config = config
        self.state = WebSocketState.DISCONNECTED
        self._websocket: Optional[Any] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._message_queue: asyncio.Queue[WebSocketMessage] = asyncio.Queue()
        self._handlers: dict[MessageType, list[Callable]] = {
            msg_type: [] for msg_type in MessageType
        }
        self._close_event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self.state == WebSocketState.CONNECTED

    async def connect(self) -> None:
        """Establish WebSocket connection."""
        if self.state != WebSocketState.DISCONNECTED:
            raise RuntimeError(f"Cannot connect in state {self.state}")

        self.state = WebSocketState.CONNECTING

        try:
            import websockets

            parsed = urlparse(self.config.url)
            extra_headers = (
                self.config.headers if self.config.headers else None
            )

            self._websocket = await websockets.connect(
                self.config.url,
                extra_headers=extra_headers,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
                close_timeout=self.config.close_timeout,
                max_size=self.config.max_message_size,
                ssl=self.config.ssl_context,
            )
            self.state = WebSocketState.CONNECTED
            self._receive_task = asyncio.create_task(self._receive_loop())
            logger.info(f"Connected to {self.config.url}")
        except Exception as e:
            self.state = WebSocketState.ERROR
            logger.error(f"Connection failed: {e}")
            raise

    async def disconnect(self, code: int = 1000, reason: str = "Normal closure") -> None:
        """Close WebSocket connection."""
        if self.state not in (WebSocketState.CONNECTED, WebSocketState.CONNECTING):
            return

        self.state = WebSocketState.DISCONNECTING
        self._close_event.set()

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._websocket:
            try:
                await self._websocket.close(code=code, reason=reason)
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self._websocket = None

        self.state = WebSocketState.DISCONNECTED
        logger.info(f"Disconnected from {self.config.url}")

    async def send_text(self, data: str) -> None:
        """Send text message."""
        if not self.is_connected:
            raise RuntimeError("Not connected")
        await self._websocket.send(data)
        logger.debug(f"Sent text message: {len(data)} chars")

    async def send_binary(self, data: bytes) -> None:
        """Send binary message."""
        if not self.is_connected:
            raise RuntimeError("Not connected")
        await self._websocket.send(data)
        logger.debug(f"Sent binary message: {len(data)} bytes")

    async def send_json(self, data: dict[str, Any]) -> None:
        """Send JSON-encoded message."""
        text = json.dumps(data)
        await self.send_text(text)

    async def receive(self, timeout: Optional[float] = None) -> WebSocketMessage:
        """Receive next message from queue."""
        try:
            return await asyncio.wait_for(
                self._message_queue.get(),
                timeout=timeout or self.config.receive_timeout,
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Receive timeout")

    def add_handler(
        self,
        message_type: MessageType,
        handler: Callable[[WebSocketMessage], None],
    ) -> None:
        """Add message handler for specific type."""
        self._handlers[message_type].append(handler)

    def remove_handler(
        self,
        message_type: MessageType,
        handler: Callable[[WebSocketMessage], None],
    ) -> None:
        """Remove message handler."""
        if handler in self._handlers[message_type]:
            self._handlers[message_type].remove(handler)

    async def _receive_loop(self) -> None:
        """Background task to receive messages."""
        try:
            async for raw_message in self._websocket:
                message: WebSocketMessage

                if isinstance(raw_message, str):
                    message = WebSocketMessage(
                        type=MessageType.TEXT,
                        data=raw_message,
                    )
                else:
                    message = WebSocketMessage(
                        type=MessageType.BINARY,
                        data=raw_message,
                    )

                for handler in self._handlers[message.type]:
                    try:
                        handler(message)
                    except Exception as e:
                        logger.error(f"Handler error: {e}")

                await self._message_queue.put(message)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Receive loop error: {e}")
            self.state = WebSocketState.ERROR


class WebSocketPool:
    """
    Pool of WebSocket connections for load balancing.

    Example:
        >>> pool = WebSocketPool([config1, config2, config3])
        >>> await pool.connect_all()
        >>> client = await pool.get_connection()
    """

    def __init__(self, configs: list[WebSocketConfig]) -> None:
        self.configs = configs
        self._clients: list[WebSocketClient] = []
        self._available: asyncio.Queue[WebSocketClient] = asyncio.Queue()
        self._connected: set[WebSocketClient] = set()

    async def connect_all(self) -> None:
        """Connect all clients in pool."""
        for config in self.configs:
            client = WebSocketClient(config)
            await client.connect()
            self._clients.append(client)
            self._connected.add(client)
            await self._available.put(client)
        logger.info(f"Connected {len(self._clients)} WebSocket clients")

    async def get_connection(self) -> WebSocketClient:
        """Get next available connection from pool."""
        return await self._available.get()

    async def return_connection(self, client: WebSocketClient) -> None:
        """Return connection to pool."""
        if client.is_connected:
            await self._available.put(client)
        else:
            self._connected.discard(client)
            new_client = WebSocketClient(client.config)
            await new_client.connect()
            self._connected.add(new_client)
            self._clients.append(new_client)
            await self._available.put(new_client)

    async def close_all(self) -> None:
        """Close all connections."""
        for client in self._clients:
            await client.disconnect()
        self._clients.clear()
        self._connected.clear()

    async def broadcast(self, message: str) -> None:
        """Broadcast message to all connected clients."""
        tasks = [
            client.send_text(message) for client in self._connected
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    def __len__(self) -> int:
        return len(self._clients)

    def __repr__(self) -> str:
        return f"WebSocketPool(connections={len(self._clients)}, available={self._available.qsize()})"
