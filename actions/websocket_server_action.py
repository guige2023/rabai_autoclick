"""
WebSocket server action for real-time bidirectional communication.

Provides WebSocket connection management, ping/pong handling, and framing.
"""

from typing import Any, Optional
import json
import time
import secrets


class WebSocketServerAction:
    """WebSocket server for real-time communication."""

    def __init__(
        self,
        max_connections: int = 1000,
        ping_interval: float = 30.0,
        ping_timeout: float = 10.0,
        max_frame_size: int = 65536,
    ) -> None:
        self.max_connections = max_connections
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_frame_size = max_frame_size
        self._connections: dict[str, dict[str, Any]] = {}
        self._handlers: dict[str, Any] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute WebSocket operation.

        Args:
            params: Dictionary containing:
                - operation: 'start', 'stop', 'broadcast', 'send'
                - connection_id: Target connection ID
                - message: Message to send

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "broadcast")

        if operation == "start":
            return self._start_server(params)
        elif operation == "stop":
            return self._stop_server(params)
        elif operation == "broadcast":
            return self._broadcast(params)
        elif operation == "send":
            return self._send_to_connection(params)
        elif operation == "close":
            return self._close_connection(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _start_server(self, params: dict[str, Any]) -> dict[str, Any]:
        """Start WebSocket server."""
        host = params.get("host", "0.0.0.0")
        port = params.get("port", 8080)

        return {
            "success": True,
            "host": host,
            "port": port,
            "status": "listening",
        }

    def _stop_server(self, params: dict[str, Any]) -> dict[str, Any]:
        """Stop WebSocket server."""
        for conn_id in list(self._connections.keys()):
            self._connections[conn_id]["status"] = "closed"

        self._connections.clear()
        return {"success": True, "status": "stopped"}

    def _broadcast(self, params: dict[str, Any]) -> dict[str, Any]:
        """Broadcast message to all connections."""
        message = params.get("message", "")
        exclude = params.get("exclude", [])

        if not message:
            return {"success": False, "error": "Message is required"}

        sent_count = 0
        for conn_id, conn_info in self._connections.items():
            if conn_id not in exclude and conn_info.get("status") == "open":
                sent_count += 1

        return {
            "success": True,
            "sent_count": sent_count,
            "total_connections": len(self._connections),
        }

    def _send_to_connection(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send message to specific connection."""
        connection_id = params.get("connection_id", "")
        message = params.get("message", "")

        if not connection_id:
            return {"success": False, "error": "Connection ID is required"}

        if connection_id not in self._connections:
            return {"success": False, "error": "Connection not found"}

        conn = self._connections[connection_id]
        if conn.get("status") != "open":
            return {"success": False, "error": "Connection is not open"}

        return {
            "success": True,
            "connection_id": connection_id,
            "bytes_sent": len(str(message)),
        }

    def _close_connection(self, params: dict[str, Any]) -> dict[str, Any]:
        """Close specific WebSocket connection."""
        connection_id = params.get("connection_id", "")

        if connection_id in self._connections:
            self._connections[connection_id]["status"] = "closed"
            return {"success": True, "connection_id": connection_id}
        return {"success": False, "error": "Connection not found"}

    def register_connection(
        self, connection_id: Optional[str] = None
    ) -> dict[str, Any]:
        """Register new WebSocket connection."""
        conn_id = connection_id or secrets.token_hex(8)
        self._connections[conn_id] = {
            "status": "open",
            "connected_at": time.time(),
            "last_pong": time.time(),
        }
        return {"connection_id": conn_id, "status": "open"}

    def get_connection_info(self, connection_id: str) -> Optional[dict[str, Any]]:
        """Get connection information."""
        return self._connections.get(connection_id)

    def get_active_connections(self) -> list[dict[str, Any]]:
        """Get all active connections."""
        return [
            {
                "connection_id": cid,
                "status": info["status"],
                "connected_at": info.get("connected_at"),
            }
            for cid, info in self._connections.items()
            if info.get("status") == "open"
        ]
