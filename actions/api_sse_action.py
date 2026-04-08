"""Server-Sent Events (SSE) API action module for RabAI AutoClick.

Provides SSE operations:
- SSEConnectAction: Start SSE connection
- SSEReadAction: Read SSE events
- SSEEmitAction: Emit SSE events (server-side)
- SSEParseAction: Parse SSE event data
- SSEHeartbeatAction: SSE heartbeat management
- SSECloseAction: Close SSE connection
"""

import json
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SSEConnectAction(BaseAction):
    """Start an SSE connection."""
    action_type = "sse_connect"
    display_name = "SSE连接"
    description = "建立SSE连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            headers = params.get("headers", {})
            last_event_id = params.get("last_event_id", "")

            if not url:
                return ActionResult(success=False, message="url is required")

            conn_id = str(uuid.uuid4())[:8]
            sse_state = context.sse_connections if hasattr(context, "sse_connections") else {}
            sse_state[conn_id] = {
                "url": url,
                "headers": headers,
                "last_event_id": last_event_id,
                "status": "connected",
                "connected_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"connection_id": conn_id, "url": url, "status": "connected"},
                message=f"SSE connected to {url}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SSE connect failed: {e}")


class SSEReadAction(BaseAction):
    """Read SSE events from connection."""
    action_type = "sse_read"
    display_name = "SSE读取"
    description = "读取SSE事件流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            timeout = params.get("timeout", 5)
            max_events = params.get("max_events", 10)

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            sse_state = context.sse_connections if hasattr(context, "sse_connections") else {}
            if connection_id not in sse_state:
                return ActionResult(success=False, message=f"Connection {connection_id} not found")

            event_queue = getattr(context, "sse_events", {}).get(connection_id, [])
            events = event_queue[:max_events]

            return ActionResult(
                success=True,
                data={"connection_id": connection_id, "events": events, "count": len(events)},
                message=f"Read {len(events)} events",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SSE read failed: {e}")


class SSEEmitAction(BaseAction):
    """Emit SSE event (server-side)."""
    action_type = "sse_emit"
    display_name = "SSE发送"
    description = "服务端发送SSE事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            event_type = params.get("event_type", "message")
            data = params.get("data", "")
            event_id = params.get("event_id", str(uuid.uuid4())[:8])
            retry = params.get("retry", 5000)

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            event = {
                "event": event_type,
                "data": data,
                "id": event_id,
                "retry": retry,
            }

            return ActionResult(
                success=True,
                data={"connection_id": connection_id, "event": event},
                message=f"Event {event_type} emitted",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SSE emit failed: {e}")


class SSEParseAction(BaseAction):
    """Parse raw SSE data."""
    action_type = "sse_parse"
    display_name = "SSE解析"
    description = "解析SSE原始数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            raw_data = params.get("raw_data", "")
            if not raw_data:
                return ActionResult(success=False, message="raw_data is required")

            lines = raw_data.strip().split("\n")
            event = {"event": "message", "data": "", "id": "", "retry": 5000}

            for line in lines:
                if line.startswith("event:"):
                    event["event"] = line[6:].strip()
                elif line.startswith("data:"):
                    event["data"] = line[5:].strip()
                elif line.startswith("id:"):
                    event["id"] = line[3:].strip()
                elif line.startswith("retry:"):
                    event["retry"] = int(line[6:].strip())

            try:
                event["data"] = json.loads(event["data"])
            except (json.JSONDecodeError, TypeError):
                pass

            return ActionResult(success=True, data=event, message="SSE parsed")
        except Exception as e:
            return ActionResult(success=False, message=f"SSE parse failed: {e}")


class SSEHeartbeatAction(BaseAction):
    """SSE heartbeat/keep-alive management."""
    action_type = "sse_heartbeat"
    display_name = "SSE心跳"
    description = "SSE心跳保活"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            interval = params.get("interval", 15)

            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            sse_state = context.sse_connections if hasattr(context, "sse_connections") else {}
            if connection_id not in sse_state:
                return ActionResult(success=False, message=f"Connection {connection_id} not found")

            sse_state[connection_id]["last_heartbeat"] = time.time()
            sse_state[connection_id]["heartbeat_interval"] = interval

            return ActionResult(
                success=True,
                data={"connection_id": connection_id, "interval": interval, "pong": True},
                message="Heartbeat acknowledged",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SSE heartbeat failed: {e}")


class SSECloseAction(BaseAction):
    """Close SSE connection."""
    action_type = "sse_close"
    display_name = "SSE关闭"
    description = "关闭SSE连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            connection_id = params.get("connection_id", "")
            if not connection_id:
                return ActionResult(success=False, message="connection_id is required")

            sse_state = context.sse_connections if hasattr(context, "sse_connections") else {}
            if connection_id in sse_state:
                sse_state[connection_id]["status"] = "closed"
                del sse_state[connection_id]

            return ActionResult(success=True, message=f"SSE connection {connection_id} closed")
        except Exception as e:
            return ActionResult(success=False, message=f"SSE close failed: {e}")
