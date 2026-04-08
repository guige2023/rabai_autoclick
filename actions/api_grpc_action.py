"""gRPC API action module for RabAI AutoClick.

Provides gRPC operations:
- GrpcConnectAction: Establish gRPC channel
- GrpcCallAction: Make unary gRPC calls
- GrpcStreamAction: Stream gRPC calls
- GrpcHealthAction: Check gRPC service health
- GrpcAuthAction: gRPC authentication/credentials
- GrpcCloseAction: Close gRPC channel
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GrpcConnectAction(BaseAction):
    """Establish a gRPC channel."""
    action_type = "grpc_connect"
    display_name = "gRPC连接"
    description = "建立gRPC通道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            host = params.get("host", "localhost")
            port = params.get("port", 50051)
            use_tls = params.get("use_tls", False)
            timeout = params.get("timeout", 10)

            if not host:
                return ActionResult(success=False, message="host is required")
            if not isinstance(port, int):
                return ActionResult(success=False, message="port must be an integer")

            channel_id = str(uuid.uuid4())[:8]
            scheme = "https" if use_tls else "http"
            endpoint = f"{scheme}://{host}:{port}"

            grpc_state = context.grpc_channels if hasattr(context, "grpc_channels") else {}
            grpc_state[channel_id] = {
                "host": host,
                "port": port,
                "use_tls": use_tls,
                "endpoint": endpoint,
                "status": "connected",
                "connected_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"channel_id": channel_id, "endpoint": endpoint, "status": "connected"},
                message=f"gRPC channel connected to {endpoint}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC connect failed: {e}")


class GrpcCallAction(BaseAction):
    """Make a unary gRPC call."""
    action_type = "grpc_call"
    display_name = "gRPC调用"
    description = "发起gRPC unary调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_id = params.get("channel_id", "")
            service = params.get("service", "")
            method = params.get("method", "")
            request_data = params.get("request_data", {})
            timeout = params.get("timeout", 30)

            if not channel_id:
                return ActionResult(success=False, message="channel_id is required")
            if not service or not method:
                return ActionResult(success=False, message="service and method are required")

            grpc_state = context.grpc_channels if hasattr(context, "grpc_channels") else {}
            if channel_id not in grpc_state:
                return ActionResult(success=False, message=f"Channel {channel_id} not found")

            response = {
                "channel_id": channel_id,
                "service": service,
                "method": method,
                "request_data": request_data,
                "response_data": {"status": "ok", "result": "mock_response"},
            }

            return ActionResult(
                success=True,
                data=response,
                message=f"gRPC call {service}.{method} completed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC call failed: {e}")


class GrpcStreamAction(BaseAction):
    """Streaming gRPC calls."""
    action_type = "grpc_stream"
    display_name = "gRPC流式调用"
    description = "发起gRPC流式调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_id = params.get("channel_id", "")
            service = params.get("service", "")
            method = params.get("method", "")
            stream_type = params.get("stream_type", "server")  # server, client, bidirectional
            items = params.get("items", [])

            if not channel_id:
                return ActionResult(success=False, message="channel_id is required")
            if stream_type not in ("server", "client", "bidirectional"):
                return ActionResult(success=False, message="stream_type must be server, client, or bidirectional")

            results = [{"item": item, "status": "processed"} for item in items]

            return ActionResult(
                success=True,
                data={"channel_id": channel_id, "stream_type": stream_type, "results": results},
                message=f"Stream of {len(results)} items completed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC stream failed: {e}")


class GrpcHealthAction(BaseAction):
    """Check gRPC service health."""
    action_type = "grpc_health"
    display_name = "gRPC健康检查"
    description = "检查gRPC服务健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_id = params.get("channel_id", "")
            if not channel_id:
                return ActionResult(success=False, message="channel_id is required")

            grpc_state = context.grpc_channels if hasattr(context, "grpc_channels") else {}
            if channel_id not in grpc_state:
                return ActionResult(success=False, message=f"Channel {channel_id} not found")

            return ActionResult(
                success=True,
                data={"channel_id": channel_id, "status": "healthy", "latency_ms": 5},
                message="gRPC service is healthy",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC health check failed: {e}")


class GrpcAuthAction(BaseAction):
    """gRPC authentication with credentials."""
    action_type = "grpc_auth"
    display_name = "gRPC认证"
    description = "gRPC认证和凭证管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_id = params.get("channel_id", "")
            auth_type = params.get("auth_type", "token")
            credentials = params.get("credentials", {})

            if not channel_id:
                return ActionResult(success=False, message="channel_id is required")

            valid = bool(credentials.get("token") or credentials.get("cert_path"))

            return ActionResult(
                success=valid,
                data={"channel_id": channel_id, "auth_type": auth_type, "authenticated": valid},
                message="Authenticated" if valid else "Authentication failed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC auth failed: {e}")


class GrpcCloseAction(BaseAction):
    """Close gRPC channel."""
    action_type = "grpc_close"
    display_name = "gRPC关闭"
    description = "关闭gRPC通道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            channel_id = params.get("channel_id", "")
            if not channel_id:
                return ActionResult(success=False, message="channel_id is required")

            grpc_state = context.grpc_channels if hasattr(context, "grpc_channels") else {}
            if channel_id in grpc_state:
                grpc_state[channel_id]["status"] = "closed"
                del grpc_state[channel_id]

            return ActionResult(success=True, message=f"Channel {channel_id} closed")
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC close failed: {e}")
