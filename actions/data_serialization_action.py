"""Data serialization action module for RabAI AutoClick.

Provides serialization operations:
- SerializeAction: Serialize data to format
- DeserializeAction: Deserialize data from format
- SchemaSerializeAction: Serialize with schema
- FormatDetectAction: Detect data format
"""

import json
import pickle
import base64
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SerializeAction(BaseAction):
    """Serialize data to specified format."""
    action_type = "serialize"
    display_name = "序列化"
    description = "将数据序列化为指定格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            format_type = params.get("format", "json")

            if not data:
                return ActionResult(success=False, message="data is required")

            if format_type == "json":
                serialized = json.dumps(data)
            elif format_type == "pickle":
                serialized = base64.b64encode(pickle.dumps(data)).decode("ascii")
            elif format_type == "base64":
                serialized = base64.b64encode(str(data).encode()).decode("ascii")
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            return ActionResult(
                success=True,
                data={"format": format_type, "serialized": serialized, "size": len(serialized)},
                message=f"Serialized to {format_type}: {len(serialized)} chars",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Serialize failed: {e}")


class DeserializeAction(BaseAction):
    """Deserialize data from format."""
    action_type = "deserialize"
    display_name = "反序列化"
    description = "从格式反序列化数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            format_type = params.get("format", "json")

            if not data:
                return ActionResult(success=False, message="data is required")

            if format_type == "json":
                deserialized = json.loads(data)
            elif format_type == "pickle":
                deserialized = pickle.loads(base64.b64decode(data))
            elif format_type == "base64":
                deserialized = base64.b64decode(data).decode("utf-8")
            else:
                return ActionResult(success=False, message=f"Unsupported format: {format_type}")

            return ActionResult(
                success=True,
                data={"format": format_type, "deserialized": deserialized},
                message=f"Deserialized from {format_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Deserialize failed: {e}")


class SchemaSerializeAction(BaseAction):
    """Serialize data with embedded schema."""
    action_type = "schema_serialize"
    display_name = "Schema序列化"
    description = "带Schema的序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            format_type = params.get("format", "json")

            if not data:
                return ActionResult(success=False, message="data is required")

            wrapped = {"schema": schema, "data": data, "version": "1.0"}
            serialized = json.dumps(wrapped) if format_type == "json" else str(wrapped)

            return ActionResult(
                success=True,
                data={"serialized": serialized, "format": format_type},
                message=f"Schema-serialized: {len(serialized)} chars",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema serialize failed: {e}")


class FormatDetectAction(BaseAction):
    """Detect data serialization format."""
    action_type = "format_detect"
    display_name = "格式检测"
    description = "检测数据序列化格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            if not data:
                return ActionResult(success=False, message="data is required")

            data_str = str(data).strip()
            detected = "unknown"

            if data_str.startswith("{") or data_str.startswith("["):
                detected = "json"
            elif all(c in "0123456789abcdefABCDEF" for c in data_str[:32]) and len(data_str) in (32, 40, 64):
                detected = "hash"
            elif ":" in data_str and "\n" in data_str:
                detected = "text"

            return ActionResult(
                success=True,
                data={"detected_format": detected, "sample": data_str[:50]},
                message=f"Detected format: {detected}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format detect failed: {e}")
