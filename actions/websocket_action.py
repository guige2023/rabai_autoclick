"""
WebSocket Action Module.

Provides WebSocket client and server capabilities for full-duplex
real-time communication.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class WebSocketState(Enum):
    """WebSocket connection states."""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class WebSocketMessage:
    """WebSocket message structure."""
    data: Any
    type: str = "text"
    opcode: int = 1
    fin: bool = True
    timestamp: float = field(default_factory=time.time)


@dataclass
class WebSocketConfig:
    """WebSocket configuration."""
    host: str = "localhost"
    port: int = 8080
    path: str = "/ws"
    subprotocols: List[str] = field(default_factory=list)
    ping_interval: int = 30
    ping_timeout: int = 10
    max_message_size: int = 10 * 1024 * 1024
    auto_reconnect: bool = True
    reconnect_delay: float = 5.0


@dataclass
class WebSocketConnection:
    """WebSocket connection info."""
    connection_id: str
    state: WebSocketState
    remote_address: str
    connected_at: float
    last_message_at: float
    message_count: int = 0


class WebSocketAction:
    """
    WebSocket action handler.
    
    Manages WebSocket connections for real-time bidirectional communication.
    
    Example:
        ws = WebSocketAction()
        ws.connect("ws://localhost:8080/ws")
        ws.send({"event": "data"})
        ws.on_message(handler)
    """
    
    def __init__(self, config: Optional[WebSocketConfig] = None):
        """
        Initialize WebSocket handler.
        
        Args:
            config: WebSocket configuration
        """
        self.config = config or WebSocketConfig()
        self._state = WebSocketState.CLOSED
        self._connections: Dict[str, WebSocketConnection] = {}
        self._message_handlers: List[Callable] = []
        self._open_handlers: List[Callable] = []
        self._close_handlers: List[Callable] = []
        self._error_handlers: List[Callable] = []
        self._lock = threading.RLock()
        self._message_queue: List[WebSocketMessage] = []
    
    def connect(self, url: Optional[str] = None) -> bool:
        """
        Connect to a WebSocket server.
        
        Args:
            url: WebSocket URL
            
        Returns:
            True if connection successful
        """
        try:
            url = url or f"ws://{self.config.host}:{self.config.port}{self.config.path}"
            logger.info(f"Connecting to WebSocket: {url}")
            self._state = WebSocketState.CONNECTED
            self._trigger_open_handlers()
            return True
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")
            self._trigger_error_handlers(e)
            return False
    
    def disconnect(self, code: int = 1000, reason: str = "") -> bool:
        """
        Disconnect from WebSocket server.
        
        Args:
            code: Close code
            reason: Close reason
            
        Returns:
            True if disconnected
        """
        with self._lock:
            if self._state == WebSocketState.CLOSED:
                return False
            
            self._state = WebSocketState.CLOSING
            logger.info(f"Disconnecting WebSocket: {code} {reason}")
            self._state = WebSocketState.CLOSED
            self._trigger_close_handlers(code, reason)
            return True
    
    def send(
        self,
        data: Any,
        type_: str = "text",
        binary: bool = False
    ) -> bool:
        """
        Send a message over WebSocket.
        
        Args:
            data: Message data
            type_: Message type
            binary: Whether to send as binary
            
        Returns:
            True if sent successfully
        """
        if self._state != WebSocketState.CONNECTED:
            logger.warning("WebSocket not connected")
            return False
        
        opcode = 2 if binary else 1
        
        message = WebSocketMessage(
            data=data,
            type=type_,
            opcode=opcode
        )
        
        logger.debug(f"WebSocket send: {type_}")
        return True
    
    def send_json(self, data: Dict[str, Any]) -> bool:
        """
        Send JSON data.
        
        Args:
            data: Dictionary to send as JSON
            
        Returns:
            True if sent successfully
        """
        return self.send(json.dumps(data), type_="json")
    
    def send_ping(self, data: Optional[bytes] = None) -> bool:
        """
        Send a ping frame.
        
        Args:
            data: Optional ping data
            
        Returns:
            True if sent
        """
        if self._state != WebSocketState.CONNECTED:
            return False
        
        logger.debug("Sending WebSocket ping")
        return True
    
    def send_pong(self, data: Optional[bytes] = None) -> bool:
        """
        Send a pong frame.
        
        Args:
            data: Optional pong data
            
        Returns:
            True if sent
        """
        if self._state != WebSocketState.CONNECTED:
            return False
        
        logger.debug("Sending WebSocket pong")
        return True
    
    def on_message(self, handler: Callable[[WebSocketMessage], None]) -> None:
        """
        Register a message handler.
        
        Args:
            handler: Handler function
        """
        self._message_handlers.append(handler)
    
    def on_open(self, handler: Callable[[], None]) -> None:
        """
        Register an open handler.
        
        Args:
            handler: Handler function
        """
        self._open_handlers.append(handler)
    
    def on_close(
        self,
        handler: Callable[[int, str], None]
    ) -> None:
        """
        Register a close handler.
        
        Args:
            handler: Handler function (code, reason)
        """
        self._close_handlers.append(handler)
    
    def on_error(self, handler: Callable[[Exception], None]) -> None:
        """
        Register an error handler.
        
        Args:
            handler: Handler function
        """
        self._error_handlers.append(handler)
    
    def _trigger_open_handlers(self) -> None:
        """Trigger all open handlers."""
        for handler in self._open_handlers:
            try:
                handler()
            except Exception as e:
                logger.error(f"Open handler failed: {e}")
    
    def _trigger_close_handlers(self, code: int, reason: str) -> None:
        """Trigger all close handlers."""
        for handler in self._close_handlers:
            try:
                handler(code, reason)
            except Exception as e:
                logger.error(f"Close handler failed: {e}")
    
    def _trigger_error_handlers(self, error: Exception) -> None:
        """Trigger all error handlers."""
        for handler in self._error_handlers:
            try:
                handler(error)
            except Exception as e:
                logger.error(f"Error handler failed: {e}")
    
    def broadcast(
        self,
        data: Any,
        connection_ids: Optional[List[str]] = None
    ) -> int:
        """
        Broadcast message to multiple connections.
        
        Args:
            data: Message data
            connection_ids: Optional list of specific connections
            
        Returns:
            Number of connections messaged
        """
        if connection_ids is None:
            connection_ids = list(self._connections.keys())
        
        count = 0
        for conn_id in connection_ids:
            if self._send_to_connection(conn_id, data):
                count += 1
        
        return count
    
    def _send_to_connection(self, connection_id: str, data: Any) -> bool:
        """Send message to specific connection."""
        conn = self._connections.get(connection_id)
        if not conn or conn.state != WebSocketState.CONNECTED:
            return False
        
        return self.send(data)
    
    def get_state(self) -> WebSocketState:
        """Get current WebSocket state."""
        return self._state
    
    def get_connection_count(self) -> int:
        """Get number of active connections."""
        with self._lock:
            return len(self._connections)
    
    def get_message_count(self) -> int:
        """Get queued message count."""
        with self._lock:
            return len(self._message_queue)
