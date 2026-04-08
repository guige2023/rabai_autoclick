"""API adapter action module for RabAI AutoClick.

Provides API adapter operations:
- AdapterCreateAction: Create API adapter
- AdapterTransformAction: Transform request/response
- AdapterConnectAction: Connect to external API
- AdapterDisconnectAction: Disconnect adapter
- AdapterHealthAction: Check adapter health
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AdapterCreateAction(BaseAction):
    """Create an API adapter."""
    action_type = "adapter_create"
    display_name = "创建适配器"
    description = "创建API适配器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            adapter_type = params.get("type", "http")
            config = params.get("config", {})

            if not name:
                return ActionResult(success=False, message="name is required")

            adapter_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "api_adapters"):
                context.api_adapters = {}
            context.api_adapters[adapter_id] = {
                "adapter_id": adapter_id,
                "name": name,
                "type": adapter_type,
                "config": config,
                "status": "created",
                "created_at": time.time(),
                "connected_at": None,
            }

            return ActionResult(
                success=True,
                data={"adapter_id": adapter_id, "name": name, "type": adapter_type},
                message=f"Adapter {adapter_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adapter create failed: {e}")


class AdapterTransformAction(BaseAction):
    """Transform request/response via adapter."""
    action_type = "adapter_transform"
    display_name = "适配器转换"
    description = "通过适配器转换请求/响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            adapter_id = params.get("adapter_id", "")
            data = params.get("data", {})
            direction = params.get("direction", "request")

            if not adapter_id:
                return ActionResult(success=False, message="adapter_id is required")

            adapters = getattr(context, "api_adapters", {})
            if adapter_id not in adapters:
                return ActionResult(success=False, message=f"Adapter {adapter_id} not found")

            if isinstance(data, dict):
                transformed = {f"{direction}_ transformed": v for k, v in data.items()}
            else:
                transformed = f"{direction}_transformed({data})"

            return ActionResult(
                success=True,
                data={"adapter_id": adapter_id, "direction": direction, "transformed": transformed},
                message=f"Transformed {direction} via adapter {adapter_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adapter transform failed: {e}")


class AdapterConnectAction(BaseAction):
    """Connect adapter to external API."""
    action_type = "adapter_connect"
    display_name = "适配器连接"
    description = "连接适配器到外部API"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            adapter_id = params.get("adapter_id", "")
            endpoint = params.get("endpoint", "")

            if not adapter_id:
                return ActionResult(success=False, message="adapter_id is required")

            adapters = getattr(context, "api_adapters", {})
            if adapter_id not in adapters:
                return ActionResult(success=False, message=f"Adapter {adapter_id} not found")

            adapters[adapter_id]["status"] = "connected"
            adapters[adapter_id]["endpoint"] = endpoint
            adapters[adapter_id]["connected_at"] = time.time()

            return ActionResult(
                success=True,
                data={"adapter_id": adapter_id, "endpoint": endpoint, "status": "connected"},
                message=f"Adapter {adapter_id} connected to {endpoint}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adapter connect failed: {e}")


class AdapterDisconnectAction(BaseAction):
    """Disconnect adapter."""
    action_type = "adapter_disconnect"
    display_name = "适配器断开"
    description = "断开适配器连接"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            adapter_id = params.get("adapter_id", "")
            if not adapter_id:
                return ActionResult(success=False, message="adapter_id is required")

            adapters = getattr(context, "api_adapters", {})
            if adapter_id not in adapters:
                return ActionResult(success=False, message=f"Adapter {adapter_id} not found")

            adapters[adapter_id]["status"] = "disconnected"
            adapters[adapter_id]["disconnected_at"] = time.time()

            return ActionResult(success=True, data={"adapter_id": adapter_id, "status": "disconnected"}, message=f"Adapter {adapter_id} disconnected")
        except Exception as e:
            return ActionResult(success=False, message=f"Adapter disconnect failed: {e}")


class AdapterHealthAction(BaseAction):
    """Check adapter health."""
    action_type = "adapter_health"
    display_name = "适配器健康检查"
    description = "检查适配器健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            adapter_id = params.get("adapter_id", "")
            if not adapter_id:
                return ActionResult(success=False, message="adapter_id is required")

            adapters = getattr(context, "api_adapters", {})
            if adapter_id not in adapters:
                return ActionResult(success=False, message=f"Adapter {adapter_id} not found")

            adapter = adapters[adapter_id]
            status = adapter.get("status", "unknown")
            healthy = status == "connected"

            return ActionResult(
                success=True,
                data={"adapter_id": adapter_id, "status": status, "healthy": healthy},
                message=f"Adapter {adapter_id}: {status}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adapter health failed: {e}")
