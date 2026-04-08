"""Data serializer action module for RabAI AutoClick.

Provides data serialization:
- DataSerializerAction: Serialize/deserialize data
- JSONSerializerAction: JSON serialization
- XMLSerializerAction: XML serialization
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSerializerAction(BaseAction):
    """Serialize and deserialize data."""
    action_type = "data_serializer"
    display_name = "数据序列化"
    description = "序列化和反序列化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "serialize")
            data = params.get("data", None)
            format_type = params.get("format", "json")

            if data is None:
                return ActionResult(success=False, message="data is required")

            if operation == "serialize":
                result = self._serialize(data, format_type)
            elif operation == "deserialize":
                result = self._deserialize(data, format_type)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(
                success=True,
                data={
                    "operation": operation,
                    "format": format_type,
                    "result": str(result)[:100]
                },
                message=f"Serialization {operation}: {format_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data serializer error: {str(e)}")

    def _serialize(self, data: Any, format_type: str) -> str:
        if format_type == "json":
            import json
            return json.dumps(data)
        return str(data)

    def _deserialize(self, data: str, format_type: str) -> Any:
        if format_type == "json":
            import json
            return json.loads(data)
        return data


class JSONSerializerAction(BaseAction):
    """JSON serialization."""
    action_type = "json_serializer"
    display_name: "JSON序列化"
    description: "JSON序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            indent = params.get("indent", 2)

            import json
            serialized = json.dumps(data, indent=indent)

            return ActionResult(
                success=True,
                data={"serialized": serialized, "format": "json"},
                message="JSON serialization completed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON serializer error: {str(e)}")


class XMLSerializerAction(BaseAction):
    """XML serialization."""
    action_type = "xml_serializer"
    display_name = "XML序列化"
    description = "XML序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            root_element = params.get("root", "root")

            serialized = f"<{root_element}>{data}</{root_element}>"

            return ActionResult(
                success=True,
                data={"serialized": serialized, "format": "xml"},
                message="XML serialization completed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML serializer error: {str(e)}")
