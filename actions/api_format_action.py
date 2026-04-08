"""API Format Action Module.

Handles API request/response format negotiation,
content type conversion, and serialization.
"""

from __future__ import annotations

import sys
import os
import time
import json
import base64
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    XML = "xml"
    YAML = "yaml"
    MSGPACK = "msgpack"
    PROTOBUF = "protobuf"


from enum import Enum


@dataclass
class FormatConfig:
    """Format configuration."""
    input_format: SerializationFormat = SerializationFormat.JSON
    output_format: SerializationFormat = SerializationFormat.JSON
    pretty_print: bool = True
    validate_schema: bool = False


class APIFormatAction(BaseAction):
    """
    API format negotiation and serialization.

    Handles conversion between formats, encoding,
    and content type management.

    Example:
        fmt = APIFormatAction()
        result = fmt.execute(ctx, {"action": "convert", "data": {}, "to_format": "xml"})
    """
    action_type = "api_format"
    display_name = "API格式处理"
    description = "API请求/响应格式协商和序列化处理"

    def __init__(self) -> None:
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "convert":
                return self._convert(params)
            elif action == "serialize":
                return self._serialize(params)
            elif action == "deserialize":
                return self._deserialize(params)
            elif action == "detect_format":
                return self._detect_format(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Format error: {str(e)}")

    def _convert(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        from_format = params.get("from_format", "json")
        to_format = params.get("to_format", "xml")

        if from_format == to_format:
            return ActionResult(success=True, message="Same format, no conversion needed", data={"data": data})

        converted = self._do_convert(data, from_format, to_format)

        return ActionResult(success=True, message=f"Converted {from_format} to {to_format}", data={"data": converted, "format": to_format})

    def _serialize(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        format_str = params.get("format", "json")
        pretty = params.get("pretty", True)

        if format_str == "json":
            serialized = json.dumps(data, indent=2 if pretty else None)
        else:
            serialized = str(data)

        return ActionResult(success=True, data={"serialized": serialized, "format": format_str})

    def _deserialize(self, params: Dict[str, Any]) -> ActionResult:
        data_str = params.get("data_str", "")
        format_str = params.get("format", "json")

        if format_str == "json":
            try:
                deserialized = json.loads(data_str)
                return ActionResult(success=True, data={"data": deserialized})
            except json.JSONDecodeError as e:
                return ActionResult(success=False, message=f"Parse error: {e}")

        return ActionResult(success=True, data={"data": data_str})

    def _detect_format(self, params: Dict[str, Any]) -> ActionResult:
        data_str = params.get("data_str", "")

        detected = "json"
        if data_str.strip().startswith("<"):
            detected = "xml"
        elif data_str.strip().startswith("---"):
            detected = "yaml"

        return ActionResult(success=True, data={"format": detected, "confidence": 0.9})

    def _do_convert(self, data: Any, from_fmt: str, to_fmt: str) -> Any:
        if to_fmt == "xml":
            return self._to_xml(data)
        elif to_fmt == "json":
            return data
        return data

    def _to_xml(self, data: Any, root: str = "root") -> str:
        if isinstance(data, dict):
            items = [f"<{k}>{self._to_xml(v, k)}</{k}>" for k, v in data.items()]
            return f"<{root}>{''.join(items)}</{root}>"
        elif isinstance(data, list):
            items = [self._to_xml(item, "item") for item in data]
            return f"<{root}>{''.join(items)}</{root}>"
        else:
            return str(data)
