"""WebSocket action for real-time bidirectional communication.

This module provides comprehensive WebSocket support:
- Client and server implementations
- Auto-reconnection with backoff
- Heartbeat/ping-pong management
- Message framing and protocols
- Binary and text message handling
- Secure connections (WSS)
- Room/topic-based broadcasting

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import base64
import hashlib
import json
import logging
import secrets
import ssl
import struct
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

try:
    import websockets
    from websockets.client import WebSocketClientProtocol
    from websockets.server import WebSocketServerProtocol, serve as ws_serve
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    WebSocketClientProtocol = None
    WebSocketServerProtocol = None
    ws_serve = None

logger = logging.getLogger(__name__)


class Opcode(Enum):
    """WebSocket opcode values."""
    CONTINUATION = 0x0
    TEXT = 0x1
    BINARY = 0x2
    CLOSE = 0x8
    PING = 0x9
    PONG = 0xA


class MessageType(Enum):
    """Application message types."""
    TEXT = "text"
    BINARY = "binary"
    JSON = "json"
    PING = "ping"
    PONG = "pong"
    CLOSE = "close"


@dataclass
class WebSocketMessage:
    """WebSocket message wrapper."""
    type: MessageType
    data: Any
    opcode: int
    timestamp: float = field(default_factory=time.time)
    client_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ClientInfo:
    """Connected client information."""
    client_id: str
    connected_at: float
    remote_address: str
    headers: Dict[str, str] = field(default_factory=dict)
    subscribed_rooms: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RoomInfo:
    """Chat room / topic information."""
    name: str
    created_at: float
    clients: Set[str] = field(default_factory=set)
    max_clients: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServerConfig:
    """WebSocket server configuration."""
    host: str = "localhost"
    port: int = 8080
    ssl_context: Optional[Any] = None
    ping_interval: float = 30.0
    ping_timeout: float = 10.0
    max_message_size: int = 10 * 1024 * 1024
    max_queue_size: int = 100
    compression: Optional[str] = "deflate"
    origins: Optional[List[str]] = None


@dataclass
class ClientConfig:
    """WebSocket client configuration."""
    endpoint: str
    ssl_context: Optional[Any] = None
    ping_interval: float = 30.0
    ping_timeout: float = 10.0
    max_message_size: int = 10 * 1024 * 1024
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 10
    base_reconnect_delay: float = 1.0
    max_reconnect_delay: float = 60.0
    heartbeat_interval: float = 5.0


class RoomManager:
    """Manages rooms/topics for broadcasting."""

    def __init__(self):
        """Initialize room manager."""
        self._rooms: Dict[str, RoomInfo] = {}
        self._client_rooms: Dict[str, Set[str]] = {}

    def create_room(
        self,
        name: str,
        max_clients: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> RoomInfo:
        """Create a new room.

        Args:
            name: Room name
            max_clients: Maximum clients allowed
            metadata: Room metadata

        Returns:
            Created room info
        """
        room = RoomInfo(
            name=name,
            created_at=time.time(),
            clients=set(),
            max_clients=max_clients,
            metadata=metadata or {}
        )
        self._rooms[name] = room
        return room

    def delete_room(self, name: str) -> bool:
        """Delete a room.

        Args:
            name: Room name

        Returns:
            True if deleted
        """
        if name in self._rooms:
            del self._rooms[name]
            return True
        return False

    def join_room(self, client_id: str, room_name: str) -> bool:
        """Add client to room.

        Args:
            client_id: Client ID
            room_name: Room name

        Returns:
            True if joined successfully
        """
        if room_name not in self._rooms:
            return False

        room = self._rooms[room_name]
        if room.max_clients and len(room.clients) >= room.max_clients:
            return False

        room.clients.add(client_id)
        if client_id not in self._client_rooms:
            self._client_rooms[client_id] = set()
        self._client_rooms[client_id].add(room_name)
        return True

    def leave_room(self, client_id: str, room_name: str) -> bool:
        """Remove client from room.

        Args:
            client_id: Client ID
            room_name: Room name

        Returns:
            True if left
        """
        if room_name in self._rooms:
            room = self._rooms[room_name]
            room.clients.discard(client_id)

            if client_id in self._client_rooms:
                self._client_rooms[client_id].discard(room_name)
            return True
        return False

    def leave_all_rooms(self, client_id: str) -> Set[str]:
        """Remove client from all rooms.

        Args:
            client_id: Client ID

        Returns:
            Set of room names left
        """
        if client_id in self._client_rooms:
            rooms = self._client_rooms[client_id].copy()
            for room_name in rooms:
                if room_name in self._rooms:
                    self._rooms[room_name].clients.discard(client_id)
            del self._client_rooms[client_id]
            return rooms
        return set()

    def get_room_clients(self, room_name: str) -> Set[str]:
        """Get clients in room.

        Args:
            room_name: Room name

        Returns:
            Set of client IDs
        """
        if room_name in self._rooms:
            return self._rooms[room_name].clients.copy()
        return set()

    def get_rooms(self) -> Dict[str, RoomInfo]:
        """Get all rooms.

        Returns:
            Dictionary of rooms
        """
        return self._rooms.copy()


class WebSocketClient:
    """WebSocket client with auto-reconnection.

    Provides a robust WebSocket client with automatic reconnection,
    heartbeat management, and message routing.
    """

    def __init__(
        self,
        config: ClientConfig,
        on_message: Optional[Callable[[WebSocketMessage], None]] = None,
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[Optional[Exception]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        """Initialize WebSocket client.

        Args:
            config: Client configuration
            on_message: Message callback
            on_connect: Connect callback
            on_disconnect: Disconnect callback
            on_error: Error callback
        """
        self.config = config
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_error = on_error

        self._ws: Optional[WebSocketClientProtocol] = None
        self._running = False
        self._reconnect_attempts = 0
        self._last_pong: float = time.time()
        self._message_queue: asyncio.Queue = asyncio.Queue()

    async def connect(self) -> bool:
        """Connect to WebSocket server.

        Returns:
            True if connected
        """
        try:
            self._ws = await websockets.connect(
                self.config.endpoint,
                ssl=self.config.ssl_context,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
                max_size=self.config.max_message_size,
                close_timeout=5.0
            )
            self._running = True
            self._reconnect_attempts = 0
            logger.info(f"Connected to {self.config.endpoint}")

            if self.on_connect:
                self.on_connect()

            return True

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            if self.on_error:
                self.on_error(e)
            return False

    async def disconnect(self, code: int = 1000, reason: str = "Normal closure") -> None:
        """Disconnect from server.

        Args:
            code: Close code
            reason: Close reason
        """
        self._running = False
        if self._ws:
            try:
                await self._ws.close(code, reason)
            except Exception:
                pass
            self._ws = None

    async def send(
        self,
        data: Any,
        message_type: MessageType = MessageType.TEXT
    ) -> bool:
        """Send message to server.

        Args:
            data: Message data
            message_type: Message type

        Returns:
            True if sent successfully
        """
        if not self._ws or not self._running:
            return False

        try:
            if message_type == MessageType.JSON:
                data = json.dumps(data)
                message_type = MessageType.TEXT

            if message_type == MessageType.TEXT:
                await self._ws.send(data)
            else:
                await self._ws.send(data)

            return True

        except Exception as e:
            logger.error(f"Send failed: {e}")
            if self.on_error:
                self.on_error(e)
            return False

    async def receive(self) -> Optional[WebSocketMessage]:
        """Receive next message.

        Returns:
            WebSocket message or None
        """
        if not self._ws or not self._running:
            return None

        try:
            data = await self._ws.recv()
            message_type = MessageType.BINARY

            if isinstance(data, str):
                message_type = MessageType.TEXT
                try:
                    json.loads(data)
                    message_type = MessageType.JSON
                except (json.JSONDecodeError, TypeError):
                    pass

            return WebSocketMessage(
                type=message_type,
                data=data,
                opcode=Opcode.TEXT.value if message_type == MessageType.TEXT else Opcode.BINARY.value
            )

        except websockets.ConnectionClosed:
            await self._handle_disconnect(None)
            return None
        except Exception as e:
            logger.error(f"Receive failed: {e}")
            if self.on_error:
                self.on_error(e)
            return None

    async def run(self) -> None:
        """Run client message loop."""
        while self._running:
            message = await self.receive()
            if message and self.on_message:
                self.on_message(message)

            if self.config.auto_reconnect:
                await self._attempt_reconnect()

    async def _attempt_reconnect(self) -> None:
        """Attempt to reconnect with backoff."""
        if not self._running:
            return

        if self._reconnect_attempts >= self.config.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached")
            await self._handle_disconnect(Exception("Max reconnection attempts reached"))
            return

        delay = min(
            self.config.base_reconnect_delay * (2 ** self._reconnect_attempts),
            self.config.max_reconnect_delay
        )

        logger.info(f"Reconnecting in {delay}s (attempt {self._reconnect_attempts + 1})")
        await asyncio.sleep(delay)

        self._reconnect_attempts += 1
        await self.connect()

    async def _handle_disconnect(self, error: Optional[Exception]) -> None:
        """Handle disconnection."""
        self._running = False
        if self.on_disconnect:
            self.on_disconnect(error)

    async def start_heartbeat(self) -> None:
        """Start heartbeat loop."""
        while self._running:
            await asyncio.sleep(self.config.heartbeat_interval)

            if self._ws and self._running:
                try:
                    pong_waiter = await self._ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.config.ping_timeout)
                    self._last_pong = time.time()
                except Exception:
                    await self._handle_disconnect(Exception("Heartbeat failed"))


class WebSocketServer:
    """WebSocket server with room support.

    Provides a server implementation with room/topic management,
    broadcasting, and client tracking.
    """

    def __init__(
        self,
        config: ServerConfig,
        on_message: Optional[Callable[[WebSocketMessage], None]] = None,
        on_connect: Optional[Callable[[ClientInfo], None]] = None,
        on_disconnect: Optional[Callable[[ClientInfo, Optional[Exception]], None]] = None,
    ):
        """Initialize WebSocket server.

        Args:
            config: Server configuration
            on_message: Message callback
            on_connect: Connect callback
            on_disconnect: Disconnect callback
        """
        self.config = config
        self.on_message = on_message
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._clients: Dict[str, ClientInfo] = {}
        self._room_manager = RoomManager()
        self._server = None

    def get_client(self, client_id: str) -> Optional[ClientInfo]:
        """Get client info.

        Args:
            client_id: Client ID

        Returns:
            Client info or None
        """
        return self._clients.get(client_id)

    def get_all_clients(self) -> Dict[str, ClientInfo]:
        """Get all connected clients.

        Returns:
            Dictionary of clients
        """
        return self._clients.copy()

    def create_room(self, name: str, **kwargs) -> RoomInfo:
        """Create a room.

        Args:
            name: Room name
            **kwargs: Additional room options

        Returns:
            Room info
        """
        return self._room_manager.create_room(name, **kwargs)

    async def broadcast_to_room(
        self,
        room_name: str,
        data: Any,
        message_type: MessageType = MessageType.TEXT,
        exclude: Optional[Set[str]] = None
    ) -> int:
        """Broadcast message to room.

        Args:
            room_name: Room name
            data: Message data
            message_type: Message type
            exclude: Client IDs to exclude

        Returns:
            Number of clients message was sent to
        """
        client_ids = self._room_manager.get_room_clients(room_name)
        if exclude:
            client_ids -= exclude

        count = 0
        for client_id in client_ids:
            if await self.send_to_client(client_id, data, message_type):
                count += 1

        return count

    async def broadcast_all(
        self,
        data: Any,
        message_type: MessageType = MessageType.TEXT,
        exclude: Optional[Set[str]] = None
    ) -> int:
        """Broadcast message to all clients.

        Args:
            data: Message data
            message_type: Message type
            exclude: Client IDs to exclude

        Returns:
            Number of clients message was sent to
        """
        exclude = exclude or set()
        count = 0

        for client_id in list(self._clients.keys()):
            if client_id not in exclude:
                if await self.send_to_client(client_id, data, message_type):
                    count += 1

        return count

    async def send_to_client(
        self,
        client_id: str,
        data: Any,
        message_type: MessageType = MessageType.TEXT
    ) -> bool:
        """Send message to specific client.

        Args:
            client_id: Client ID
            data: Message data
            message_type: Message type

        Returns:
            True if sent
        """
        client = self._clients.get(client_id)
        if not client or not hasattr(client, '_ws'):
            return False

        try:
            ws = getattr(client, '_ws', None)
            if not ws:
                return False

            if message_type == MessageType.JSON:
                data = json.dumps(data)

            await ws.send(data)
            return True

        except Exception as e:
            logger.error(f"Send to client {client_id} failed: {e}")
            return False

    async def kick_client(self, client_id: str, reason: str = "Kicked") -> bool:
        """Kick a client.

        Args:
            client_id: Client ID
            reason: Kick reason

        Returns:
            True if kicked
        """
        client = self._clients.get(client_id)
        if not client:
            return False

        try:
            ws = getattr(client, '_ws', None)
            if ws:
                await ws.close(1008, reason)
            return True
        except Exception:
            return False

    async def start(self) -> None:
        """Start WebSocket server."""
        self._server = await ws_serve(
            self._handle_client,
            host=self.config.host,
            port=self.config.port,
            ssl=self.config.ssl_context,
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            max_size=self.config.max_message_size,
            compression=self.config.compression,
            origins=self.config.origins,
        )
        logger.info(f"WebSocket server started on {self.config.host}:{self.config.port}")

    async def stop(self) -> None:
        """Stop WebSocket server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("WebSocket server stopped")

    async def _handle_client(self, ws: WebSocketServerProtocol, path: str) -> None:
        """Handle client connection.

        Args:
            ws: WebSocket protocol
            path: Connection path
        """
        client_id = str(uuid.uuid4())
        client_info = ClientInfo(
            client_id=client_id,
            connected_at=time.time(),
            remote_address=f"{ws.remote_address[0]}:{ws.remote_address[1]}",
            headers={}
        )
        client_info._ws = ws

        self._clients[client_id] = client_info

        if self.on_connect:
            await self._safe_callback(self.on_connect, client_info)

        try:
            async for raw_message in ws:
                if isinstance(raw_message, bytes):
                    message_type = MessageType.BINARY
                    data = raw_message
                else:
                    message_type = MessageType.TEXT
                    data = raw_message

                try:
                    json.loads(data)
                    message_type = MessageType.JSON
                except (json.JSONDecodeError, TypeError):
                    pass

                message = WebSocketMessage(
                    type=message_type,
                    data=data,
                    opcode=Opcode.TEXT.value if message_type == MessageType.TEXT else Opcode.BINARY.value,
                    client_id=client_id
                )

                if self.on_message:
                    await self._safe_callback(self.on_message, message)

        except websockets.ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Client handler error: {e}")
        finally:
            self._room_manager.leave_all_rooms(client_id)
            del self._clients[client_id]

            if self.on_disconnect:
                await self._safe_callback(self.on_disconnect, client_info, None)

    async def _safe_callback(self, callback: Callable, *args) -> None:
        """Safely execute callback.

        Args:
            callback: Callback function
            *args: Callback arguments
        """
        try:
            if asyncio.iscoroutinefunction(callback):
                await callback(*args)
            else:
                callback(*args)
        except Exception as e:
            logger.error(f"Callback error: {e}")


class WebSocketProtocol:
    """WebSocket protocol utilities.

    Provides utilities for WebSocket framing, masking,
    and protocol-level operations.
    """

    @staticmethod
    def mask_data(key: bytes, data: bytes) -> bytes:
        """Mask data with mask key.

        Args:
            key: 4-byte mask key
            data: Data to mask

        Returns:
            Masked data
        """
        key_array = bytearray(key)
        data_array = bytearray(data)

        for i in range(len(data_array)):
            data_array[i] ^= key_array[i % 4]

        return bytes(data_array)

    @staticmethod
    def pack_frame(
        opcode: int,
        payload: bytes,
        fin: bool = True,
        masked: bool = False,
        mask_key: Optional[bytes] = None
    ) -> bytes:
        """Pack WebSocket frame.

        Args:
            opcode: Frame opcode
            payload: Frame payload
            fin: FIN bit
            masked: MASK bit
            mask_key: Mask key if masked

        Returns:
            Packed frame bytes
        """
        first_byte = (0x80 if fin else 0x00) | opcode

        payload_len = len(payload)
        if payload_len < 126:
            second_byte = (0x80 if masked else 0x00) | payload_len
            header = bytes([first_byte, second_byte])
        elif payload_len < 65536:
            second_byte = (0x80 if masked else 0x00) | 126
            header = struct.pack("!BBH", first_byte, second_byte, payload_len)
        else:
            second_byte = (0x80 if masked else 0x00) | 127
            header = struct.pack("!BBQ", first_byte, second_byte, payload_len)

        if masked and mask_key:
            payload = WebSocketProtocol.mask_data(mask_key, payload)
            return header + mask_key + payload

        return header + payload

    @staticmethod
    def generate_mask_key() -> bytes:
        """Generate random mask key.

        Returns:
            4-byte mask key
        """
        return secrets.token_bytes(4)

    @staticmethod
    def compute_accept_key(key: str) -> str:
        """Compute WebSocket accept key.

        Args:
            key: Sec-WebSocket-Key from client

        Returns:
            Sec-WebSocket-Accept key
        """
        GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        accept_key = base64.b64encode(
            hashlib.sha1((key + GUID).encode()).digest()
        ).decode()
        return accept_key


# Convenience functions

async def create_client(
    endpoint: str,
    on_message: Optional[Callable] = None,
    **kwargs
) -> WebSocketClient:
    """Create WebSocket client.

    Args:
        endpoint: WebSocket endpoint URL
        on_message: Message callback
        **kwargs: Additional client options

    Returns:
        WebSocket client instance
    """
    config = ClientConfig(endpoint=endpoint, **kwargs)
    client = WebSocketClient(config, on_message=on_message)
    await client.connect()
    return client


async def create_server(
    host: str = "localhost",
    port: int = 8080,
    on_message: Optional[Callable] = None,
    on_connect: Optional[Callable] = None,
    **kwargs
) -> WebSocketServer:
    """Create WebSocket server.

    Args:
        host: Server host
        port: Server port
        on_message: Message callback
        on_connect: Connect callback
        **kwargs: Additional server options

    Returns:
        WebSocket server instance
    """
    config = ServerConfig(host=host, port=port, **kwargs)
    server = WebSocketServer(
        config,
        on_message=on_message,
        on_connect=on_connect
    )
    await server.start()
    return server
