"""WebSocket action module for RabAI AutoClick.

Provides WebSocket client operations for real-time bidirectional
communication with servers.
"""

import sys
import os
import json
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from queue import Queue, Empty

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class WebSocketConfig:
    """WebSocket client configuration."""
    url: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    ping_interval: float = 30.0
    ping_timeout: float = 10.0
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 5
    reconnect_delay: float = 1.0
    ssl_verify: bool = True


class WebSocketConnection:
    """Manages WebSocket connection lifecycle."""
    
    def __init__(self, config: WebSocketConfig):
        self.config = config
        self._socket = None
        self._connected = False
        self._message_queue: Queue = Queue()
        self._running = False
        self._receive_thread: Optional[threading.Thread] = None
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> tuple:
        """Establish WebSocket connection."""
        try:
            import ssl
            
            if sys.version_info >= (3, 10):
                import asyncio
                return False, "WebSocket requires asyncio-based implementation"
            
            try:
                import websocket
                ws = websocket.WebSocket(
                    sslopt={"cert_reqs": ssl.CERT_NONE} if not self.config.ssl_verify else {}
                )
                
                headers = []
                for k, v in self.config.headers.items():
                    headers.append(f"{k}: {v}")
                
                ws.connect(
                    self.config.url,
                    timeout=self.config.timeout,
                    headers=headers
                )
                
                self._socket = ws
                self._connected = True
                self._running = True
                self._receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
                self._receive_thread.start()
                
                return True, "Connected"
                
            except ImportError:
                return False, "websocket-client not installed. Install with: pip install websocket-client"
                
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def _receive_loop(self) -> None:
        """Background thread for receiving messages."""
        while self._running and self._connected:
            try:
                message = self._socket.recv()
                if message:
                    self._message_queue.put(message)
            except Exception:
                if self._running:
                    self._connected = False
                break
    
    def send(self, data: Any) -> tuple:
        """Send message through WebSocket."""
        if not self._connected:
            return False, "Not connected"
        
        try:
            if isinstance(data, (dict, list)):
                message = json.dumps(data)
            else:
                message = str(data)
            
            self._socket.send(message)
            return True, "Sent"
        except Exception as e:
            return False, f"Send error: {str(e)}"
    
    def receive(self, timeout: float = 1.0) -> tuple:
        """Receive message from queue."""
        try:
            message = self._message_queue.get(timeout=timeout)
            return True, message
        except Empty:
            return False, None
    
    def disconnect(self) -> None:
        """Close WebSocket connection."""
        self._running = False
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        self._connected = False
    
    def get_queue_size(self) -> int:
        """Get number of messages in queue."""
        return self._message_queue.qsize()


class WebSocketAction(BaseAction):
    """Action for WebSocket operations.
    
    Features:
        - Connect to WebSocket servers
        - Send text/binary messages
        - Receive messages with timeout
        - Auto-reconnect on disconnect
        - Message queuing
        - Ping/pong heartbeat
        - SSL/TLS support
    
    Note: Requires websocket-client library.
    Install with: pip install websocket-client
    """
    
    def __init__(self, config: Optional[WebSocketConfig] = None):
        """Initialize WebSocket action.
        
        Args:
            config: WebSocket configuration.
        """
        super().__init__()
        self.config = config or WebSocketConfig()
        self._connection: Optional[WebSocketConnection] = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, send,
                           receive, broadcast, status)
                - url: WebSocket URL
                - message: Message to send
                - headers: Optional headers dict
                - timeout: Receive timeout in seconds
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "send":
                return self._send(params)
            elif operation == "receive":
                return self._receive(params)
            elif operation == "broadcast":
                return self._broadcast(params)
            elif operation == "status":
                return self._status(params)
            elif operation == "ping":
                return self._ping(params)
            elif operation == "drain_queue":
                return self._drain_queue(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish WebSocket connection."""
        url = params.get("url", self.config.url)
        headers = params.get("headers", self.config.headers)
        
        if not url:
            return ActionResult(success=False, message="url is required")
        
        if self._connection and self._connection.is_connected:
            return ActionResult(success=True, message="Already connected")
        
        config = WebSocketConfig(
            url=url,
            headers=headers,
            timeout=params.get("timeout", self.config.timeout),
            ping_interval=params.get("ping_interval", self.config.ping_interval),
            auto_reconnect=params.get("auto_reconnect", self.config.auto_reconnect),
            ssl_verify=params.get("ssl_verify", self.config.ssl_verify)
        )
        
        self._connection = WebSocketConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to {url}",
                data={"url": url, "connected": True}
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close WebSocket connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _send(self, params: Dict[str, Any]) -> ActionResult:
        """Send message through WebSocket."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        message = params.get("message", "")
        if not message:
            return ActionResult(success=False, message="message is required")
        
        success, result = self._connection.send(message)
        
        if success:
            return ActionResult(
                success=True,
                message="Message sent",
                data={"sent": True}
            )
        else:
            return ActionResult(success=False, message=result)
    
    def _receive(self, params: Dict[str, Any]) -> ActionResult:
        """Receive message from WebSocket."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        timeout = params.get("timeout", 5.0)
        
        messages = []
        end_time = time.time() + timeout
        
        while time.time() < end_time:
            success, message = self._connection.receive(timeout=0.1)
            
            if success and message is not None:
                try:
                    parsed = json.loads(message)
                    messages.append({"type": "json", "data": parsed})
                except json.JSONDecodeError:
                    messages.append({"type": "text", "data": message})
            
            if messages:
                break
        
        if messages:
            return ActionResult(
                success=True,
                message=f"Received {len(messages)} message(s)",
                data={"messages": messages, "count": len(messages)}
            )
        else:
            return ActionResult(
                success=False,
                message="No messages received (timeout)",
                data={"messages": [], "count": 0}
            )
    
    def _broadcast(self, params: Dict[str, Any]) -> ActionResult:
        """Send broadcast message to multiple connections."""
        connections = params.get("connections", [])
        message = params.get("message", "")
        
        if not connections:
            return ActionResult(success=False, message="connections list required")
        if not message:
            return ActionResult(success=False, message="message is required")
        
        results = []
        success_count = 0
        fail_count = 0
        
        for conn in connections:
            if conn.is_connected:
                ok, _ = conn.send(message)
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            else:
                fail_count += 1
            results.append({"connected": conn.is_connected})
        
        return ActionResult(
            success=fail_count == 0,
            message=f"Broadcast: {success_count} sent, {fail_count} failed",
            data={
                "sent": success_count,
                "failed": fail_count,
                "results": results
            }
        )
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get WebSocket connection status."""
        if not self._connection:
            return ActionResult(
                success=True,
                message="Not connected",
                data={"connected": False}
            )
        
        return ActionResult(
            success=True,
            message="Connected" if self._connection.is_connected else "Disconnected",
            data={
                "connected": self._connection.is_connected,
                "queue_size": self._connection.get_queue_size(),
                "url": self._connection.config.url
            }
        )
    
    def _ping(self, params: Dict[str, Any]) -> ActionResult:
        """Send ping to WebSocket server."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        try:
            if hasattr(self._connection._socket, 'ping'):
                self._connection._socket.ping()
                return ActionResult(success=True, message="Ping sent")
            else:
                return ActionResult(
                    success=False,
                    message="Ping not supported by this connection type"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Ping failed: {str(e)}")
    
    def _drain_queue(self, params: Dict[str, Any]) -> ActionResult:
        """Drain all messages from queue."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        max_messages = params.get("max_messages", 100)
        
        messages = []
        for _ in range(max_messages):
            success, message = self._connection.receive(timeout=0.01)
            if not success or message is None:
                break
            try:
                parsed = json.loads(message)
                messages.append({"type": "json", "data": parsed})
            except json.JSONDecodeError:
                messages.append({"type": "text", "data": message})
        
        return ActionResult(
            success=True,
            message=f"Drained {len(messages)} messages",
            data={"messages": messages, "count": len(messages)}
        )
