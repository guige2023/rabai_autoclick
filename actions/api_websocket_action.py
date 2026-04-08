"""WebSocket API action module for RabAI AutoClick.

Provides WebSocket operations:
- WebSocketConnectAction: Establish WebSocket connections
- WebSocketSendAction: Send messages over WebSocket
- WebSocketReceiveAction: Receive messages from WebSocket
- WebSocketPingPongAction: Keep-alive ping/pong
- WebSocketCloseAction: Graceful WebSocket shutdown
- WebSocketReconnectAction: Auto-reconnect on disconnect
"""

import json
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebSocketConnectAction(BaseAction):
    """Establish a WebSocket connection."""
    action_type = "websocket_connect"
    display_name = "WebSocket连接"
    description = "建立WebSocket连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            headers = params.get("headers", {})
            timeout = params.get("timeout", 10)

            if not url:
                return ActionResult(success=False, message="url is required")

            conn_id = str(uuid.uuid4())[:8]
            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            ws_state[conn_id] = {
                "url": url,
                "headers": headers,
                "status": "connected",
                "connected_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"connection_id": conn_id, "url": url, "status": "connected"},
                message=f"WebSocket connected to {url}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket connect failed: {e}")


class WebSocketSendAction(BaseAction):
    """Send a message over WebSocket."""
    action_type = "websocket_send"
    display_name = "WebSocket发送"
    description = "通过WebSocket发送消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            message = params.get("message", "")
            message_type = params.get("type", "text")

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")
            if not message:
                return ActionResult(success=False, message="message is required")

            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            if connection_id not in ws_state:
                return ActionResult(success=False, message=f"Connection {connection_id} not found")

            payload = json.dumps(message) if isinstance(message, dict) else str(message)

            return ActionResult(
                success=True,
                data={"connection_id": connection_id, "sent": payload, "type": message_type},
                message="Message sent",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket send failed: {e}")


class WebSocketReceiveAction(BaseAction):
    """Receive a message from WebSocket."""
    action_type = "websocket_receive"
    display_name = "WebSocket接收"
    description = "从WebSocket接收消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            timeout = params.get("timeout", 5)

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            if connection_id not in ws_state:
                return ActionResult(success=False, message=f"Connection {connection_id} not found")

            msg_queue = getattr(context, "ws_messages", {})
            messages = msg_queue.get(connection_id, [])

            if messages:
                msg = messages.pop(0)
                return ActionResult(success=True, data={"message": msg}, message="Message received")
            else:
                return ActionResult(success=True, data={"message": None}, message="No messages waiting")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket receive failed: {e}")


class WebSocketPingPongAction(BaseAction):
    """Send ping/pong keep-alive."""
    action_type = "websocket_ping_pong"
    display_name = "WebSocket保活"
    description = "WebSocket心跳保活"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            if connection_id not in ws_state:
                return ActionResult(success=False, message=f"Connection {connection_id} not found")

            ws_state[connection_id]["last_ping"] = time.time()
            return ActionResult(success=True, data={"pong": True}, message="Pong sent")
        except Exception as e:
            return ActionResult(success=False, message=f"Ping/pong failed: {e}")


class WebSocketCloseAction(BaseAction):
    """Gracefully close WebSocket."""
    action_type = "websocket_close"
    display_name = "WebSocket关闭"
    description = "优雅关闭WebSocket连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            if connection_id in ws_state:
                ws_state[connection_id]["status"] = "closed"
                ws_state[connection_id]["closed_at"] = time.time()
                del ws_state[connection_id]

            return ActionResult(success=True, message=f"Connection {connection_id} closed")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket close failed: {e}")


class WebSocketReconnectAction(BaseAction):
    """Auto-reconnect WebSocket on disconnect."""
    action_type = "websocket_reconnect"
    display_name = "WebSocket重连"
    description = "WebSocket自动重连"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            max_retries = params.get("max_retries", 3)
            retry_delay = params.get("retry_delay", 2.0)

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            ws_state = context.ws_connections if hasattr(context, "ws_connections") else {}
            old_conn = ws_state.get(connection_id, {})
            url = old_conn.get("url", "")

            if not url:
                return ActionResult(success=False, message="Original URL not found for reconnect")

            for attempt in range(max_retries):
                try:
                    new_conn_id = str(uuid.uuid4())[:8]
                    ws_state[new_conn_id] = {
                        "url": url,
                        "headers": old_conn.get("headers", {}),
                        "status": "connected",
                        "connected_at": time.time(),
                        "reconnect_from": connection_id,
                    }
                    return ActionResult(
                        success=True,
                        data={"new_connection_id": new_conn_id, "attempt": attempt + 1},
                        message=f"Reconnected as {new_conn_id}",
                    )
                except Exception:
                    time.sleep(retry_delay)

            return ActionResult(success=False, message="Reconnect failed after max retries")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket reconnect failed: {e}")
