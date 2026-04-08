"""Data serializer action module for RabAI AutoClick.

Provides serialization:
- DataSerializer: Serialize/deserialize data
- JSONSerializer: JSON serialization
- MessagePackSerializer: MessagePack serialization
- XMLSerializer: XML serialization
- SchemaSerializer: Schema-based serialization
"""

import json
import pickle
import base64
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SerializationFormat(Enum):
    """Serialization formats."""
    JSON = "json"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    XML = "xml"
    YAML = "yaml"
    BASE64 = "base64"


@dataclass
class SerializationResult:
    """Serialization result."""
    success: bool
    data: Any
    format: str
    size: int
    error: Optional[str] = None


class DataSerializer:
    """General data serializer."""

    def __init__(self):
        self._serializers: Dict[SerializationFormat, Callable] = {
            SerializationFormat.JSON: self._json_serialize,
            SerializationFormat.PICKLE: self._pickle_serialize,
            SerializationFormat.BASE64: self._base64_serialize,
        }

        self._deserializers: Dict[SerializationFormat, Callable] = {
            SerializationFormat.JSON: self._json_deserialize,
            SerializationFormat.PICKLE: self._pickle_deserialize,
            SerializationFormat.BASE64: self._base64_deserialize,
        }

    def serialize(self, data: Any, format: SerializationFormat = SerializationFormat.JSON) -> SerializationResult:
        """Serialize data."""
        try:
            serializer = self._serializers.get(format, self._json_serialize)
            result = serializer(data)

            if isinstance(result, str):
                result = result.encode()

            return SerializationResult(
                success=True,
                data=result,
                format=format.value,
                size=len(result),
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=format.value,
                size=0,
                error=str(e),
            )

    def deserialize(self, data: Any, format: SerializationFormat = SerializationFormat.JSON) -> SerializationResult:
        """Deserialize data."""
        try:
            deserializer = self._deserializers.get(format, self._json_deserialize)
            result = deserializer(data)

            return SerializationResult(
                success=True,
                data=result,
                format=format.value,
                size=len(data) if isinstance(data, (str, bytes)) else 0,
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=format.value,
                size=0,
                error=str(e),
            )

    def _json_serialize(self, data: Any) -> bytes:
        """JSON serialize."""
        return json.dumps(data, ensure_ascii=False).encode("utf-8")

    def _json_deserialize(self, data: Any) -> Any:
        """JSON deserialize."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def _pickle_serialize(self, data: Any) -> bytes:
        """Pickle serialize."""
        return pickle.dumps(data)

    def _pickle_deserialize(self, data: Any) -> Any:
        """Pickle deserialize."""
        return pickle.loads(data)

    def _base64_serialize(self, data: Any) -> bytes:
        """Base64 serialize."""
        json_data = self._json_serialize(data)
        return base64.b64encode(json_data)

    def _base64_deserialize(self, data: Any) -> Any:
        """Base64 deserialize."""
        if isinstance(data, str):
            data = data.encode()
        json_data = base64.b64decode(data)
        return self._json_deserialize(json_data)


class SchemaSerializer:
    """Schema-based serializer."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema

    def serialize(self, data: Dict) -> Dict:
        """Serialize data according to schema."""
        result = {}
        for field_name, field_schema in self.schema.items():
            if field_name in data:
                value = data[field_name]
                result[field_name] = self._serialize_field(value, field_schema)
        return result

    def deserialize(self, data: Dict) -> Dict:
        """Deserialize data according to schema."""
        result = {}
        for field_name, field_schema in self.schema.items():
            if field_name in data:
                value = data[field_name]
                result[field_name] = self._deserialize_field(value, field_schema)
        return result

    def _serialize_field(self, value: Any, schema: Dict) -> Any:
        """Serialize field value."""
        field_type = schema.get("type")

        if field_type == "string":
            return str(value)
        elif field_type == "number":
            return float(value) if value is not None else None
        elif field_type == "integer":
            return int(value) if value is not None else None
        elif field_type == "boolean":
            return bool(value)
        elif field_type == "array":
            if isinstance(value, list):
                item_schema = schema.get("items", {})
                return [self._serialize_field(item, item_schema) for item in value]
            return []
        elif field_type == "object":
            if isinstance(value, dict):
                nested = SchemaSerializer(schema.get("properties", {}))
                return nested.serialize(value)
            return {}

        return value

    def _deserialize_field(self, value: Any, schema: Dict) -> Any:
        """Deserialize field value."""
        return self._serialize_field(value, schema)


class DataSerializerAction(BaseAction):
    """Data serializer action."""
    action_type = "data_serializer"
    display_name = "数据序列化器"
    description = "数据序列化和反序列化"

    def __init__(self):
        super().__init__()
        self._serializer = DataSerializer()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "serialize")

            if operation == "serialize":
                return self._serialize(params)
            elif operation == "deserialize":
                return self._deserialize(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Serializer error: {str(e)}")

    def _serialize(self, params: Dict) -> ActionResult:
        """Serialize data."""
        data = params.get("data")
        format_str = params.get("format", "json").upper()

        if data is None:
            return ActionResult(success=False, message="data is required")

        try:
            fmt = SerializationFormat[format_str]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown format: {format_str}")

        result = self._serializer.serialize(data, fmt)

        if result.success:
            output = result.data
            if isinstance(output, bytes):
                output = base64.b64encode(output).decode() if format_str != "BASE64" else output.decode()

            return ActionResult(
                success=True,
                message=f"Serialized to {result.format}, size: {result.size}",
                data={
                    "format": result.format,
                    "size": result.size,
                    "data": output,
                },
            )
        else:
            return ActionResult(success=False, message=f"Serialization failed: {result.error}")

    def _deserialize(self, params: Dict) -> ActionResult:
        """Deserialize data."""
        data = params.get("data")
        format_str = params.get("format", "json").upper()

        if data is None:
            return ActionResult(success=False, message="data is required")

        try:
            fmt = SerializationFormat[format_str]
        except KeyError:
            return ActionResult(success=False, message=f"Unknown format: {format_str}")

        result = self._serializer.deserialize(data, fmt)

        if result.success:
            return ActionResult(
                success=True,
                message=f"Deserialized from {result.format}",
                data={"data": result.data, "format": result.format},
            )
        else:
            return ActionResult(success=False, message=f"Deserialization failed: {result.error}")
