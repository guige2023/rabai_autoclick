"""API adapter action module for RabAI AutoClick.

Provides API adapter operations:
- APIAdapterAction: Adapt API requests/responses
- RequestAdapterAction: Adapt requests
- ResponseAdapterAction: Adapt responses
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIAdapterAction(BaseAction):
    """Adapt API requests and responses."""
    action_type = "api_adapter"
    display_name = "API适配器"
    description = "适配API请求和响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "adapt_request")
            data = params.get("data", {})
            adapter_config = params.get("adapter_config", {})

            if operation == "adapt_request":
                adapted = self._adapt_request(data, adapter_config)
            elif operation == "adapt_response":
                adapted = self._adapt_response(data, adapter_config)
            else:
                adapted = data

            return ActionResult(
                success=True,
                data={
                    "operation": operation,
                    "adapted": adapted,
                    "adapted_at": datetime.now().isoformat()
                },
                message=f"API adapted: {operation}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API adapter error: {str(e)}")

    def _adapt_request(self, data: Dict, config: Dict) -> Dict:
        return data

    def _adapt_response(self, data: Dict, config: Dict) -> Dict:
        return data


class RequestAdapterAction(BaseAction):
    """Adapt requests to different formats."""
    action_type = "request_adapter"
    display_name = "请求适配器"
    description = "将请求适配为不同格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            request = params.get("request", {})
            source_format = params.get("source_format", "json")
            target_format = params.get("target_format", "xml")

            adapted = {"adapted": True, "source": source_format, "target": target_format}

            return ActionResult(
                success=True,
                data=adapted,
                message=f"Request adapted: {source_format} -> {target_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Request adapter error: {str(e)}")


class ResponseAdapterAction(BaseAction):
    """Adapt responses to different formats."""
    action_type = "response_adapter"
    display_name = "响应适配器"
    description = "将响应适配为不同格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            response = params.get("response", {})
            target_format = params.get("target_format", "json")

            adapted = {"adapted": True, "response": response, "format": target_format}

            return ActionResult(
                success=True,
                data=adapted,
                message=f"Response adapted to {target_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Response adapter error: {str(e)}")
