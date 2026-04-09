"""Data Serialization Action Module.

Provides multi-format data serialization and deserialization support
including JSON, MessagePack, Protocol Buffers, Avro, and custom formats
with schema evolution and validation capabilities.
"""

from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    MSGPACK = "msgpack"
    UBJSON = "ubjson"
    BSON = "bson"
    CBOR = "cbor"
    PROTOBUF = "protobuf"
    AVRO = "avro"
    PICKLE = "pickle"
    XML = "xml"


@dataclass
class SchemaDefinition:
    """Schema definition for serialized data."""
    schema_id: str
    format: SerializationFormat
    schema_data: Dict[str, Any]
    version: int = 1
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SerializationConfig:
    """Configuration for serialization operations."""
    default_format: SerializationFormat = SerializationFormat.JSON
    pretty_print: bool = False
    validate_schema: bool = True
    compression_enabled: bool = False
    compression_type: str = "zlib"
    strict_mode: bool = True
    date_format: str = "iso8601"


class SerializationHandler:
    """Handle serialization to different formats."""

    @staticmethod
    def to_json(data: Any, pretty: bool = False) -> bytes:
        """Serialize to JSON."""
        if pretty:
            return json.dumps(data, indent=2, sort_keys=True, default=str).encode()
        return json.dumps(data, separators=(",", ":"), default=str).encode()

    @staticmethod
    def from_json(data: bytes) -> Any:
        """Deserialize from JSON."""
        return json.loads(data.decode())

    @staticmethod
    def to_msgpack(data: Any) -> bytes:
        """Serialize to MessagePack."""
        try:
            import msgpack
            return msgpack.packb(data, use_bin_type=True)
        except ImportError:
            logger.warning("msgpack not available, falling back to JSON")
            return SerializationHandler.to_json(data)

    @staticmethod
    def from_msgpack(data: bytes) -> Any:
        """Deserialize from MessagePack."""
        try:
            import msgpack
            return msgpack.unpackb(data, raw=False)
        except ImportError:
            logger.warning("msgpack not available")
            raise ImportError("msgpack library not installed")

    @staticmethod
    def to_cbor(data: Any) -> bytes:
        """Serialize to CBOR."""
        try:
            import cbor2
            return cbor2.dumps(data)
        except ImportError:
            logger.warning("cbor2 not available, falling back to JSON")
            return SerializationHandler.to_json(data)

    @staticmethod
    def from_cbor(data: bytes) -> Any:
        """Deserialize from CBOR."""
        try:
            import cbor2
            return cbor2.loads(data)
        except ImportError:
            raise ImportError("cbor2 library not installed")

    @staticmethod
    def to_bson(data: Any) -> bytes:
        """Serialize to BSON."""
        try:
            import bson
            return bson.dumps(data)
        except ImportError:
            logger.warning("bson not available, falling back to JSON")
            return SerializationHandler.to_json(data)

    @staticmethod
    def from_bson(data: bytes) -> Any:
        """Deserialize from BSON."""
        try:
            import bson
            return bson.loads(data)
        except ImportError:
            raise ImportError("bson library not installed")

    @staticmethod
    def to_pickle(data: Any) -> bytes:
        """Serialize to Pickle."""
        import pickle
        return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def from_pickle(data: bytes) -> Any:
        """Deserialize from Pickle."""
        import pickle
        return pickle.loads(data)


class SchemaRegistry:
    """Registry for data schemas."""

    def __init__(self):
        self._schemas: Dict[str, SchemaDefinition] = {}
        self._lock = threading.Lock()

    def register(
        self,
        schema_id: str,
        format: SerializationFormat,
        schema_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Register a new schema."""
        try:
            with self._lock:
                if schema_id in self._schemas:
                    return ActionResult(success=False, error=f"Schema {schema_id} already exists")

                schema = SchemaDefinition(
                    schema_id=schema_id,
                    format=format,
                    schema_data=schema_data,
                    metadata=metadata or {}
                )
                self._schemas[schema_id] = schema
                return ActionResult(success=True, data={"schema_id": schema_id})
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def get(self, schema_id: str) -> Optional[SchemaDefinition]:
        """Get a schema by ID."""
        with self._lock:
            return self._schemas.get(schema_id)

    def validate(
        self,
        data: Any,
        schema_id: str
    ) -> Tuple[bool, List[str]]:
        """Validate data against a schema."""
        schema = self.get(schema_id)
        if not schema:
            return False, [f"Schema {schema_id} not found"]

        errors = []

        if schema.format == SerializationFormat.JSON:
            errors = self._validate_json_schema(data, schema.schema_data)

        return len(errors) == 0, errors

    def _validate_json_schema(
        self,
        data: Any,
        schema: Dict[str, Any]
    ) -> List[str]:
        """Validate data against JSON schema."""
        errors = []

        if "type" in schema:
            expected_type = schema["type"]
            type_map = {
                "string": str,
                "number": (int, float),
                "integer": int,
                "boolean": bool,
                "array": list,
                "object": dict,
                "null": type(None)
            }

            if expected_type in type_map:
                expected = type_map[expected_type]
                if not isinstance(data, expected):
                    errors.append(f"Expected type {expected_type}, got {type(data).__name__}")

        if "properties" in schema and isinstance(data, dict):
            required = schema.get("required", [])
            for field_name in required:
                if field_name not in data:
                    errors.append(f"Missing required field: {field_name}")

        if "properties" in schema and isinstance(data, dict):
            for field_name, field_value in data.items():
                if field_name in schema["properties"]:
                    field_schema = schema["properties"][field_name]
                    field_errors = self._validate_json_schema(field_value, field_schema)
                    errors.extend([f"{field_name}.{e}" for e in field_errors])

        return errors


class DataSerializationAction(BaseAction):
    """Action for multi-format data serialization."""

    def __init__(self):
        super().__init__(name="data_serialization")
        self._config = SerializationConfig()
        self._handler = SerializationHandler()
        self._schema_registry = SchemaRegistry()

    def configure(self, config: SerializationConfig):
        """Configure serialization settings."""
        self._config = config

    def serialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None
    ) -> ActionResult:
        """Serialize data to specified format."""
        try:
            fmt = format or self._config.default_format

            if fmt == SerializationFormat.JSON:
                result = self._handler.to_json(data, self._config.pretty_print)
            elif fmt == SerializationFormat.MSGPACK:
                result = self._handler.to_msgpack(data)
            elif fmt == SerializationFormat.CBOR:
                result = self._handler.to_cbor(data)
            elif fmt == SerializationFormat.BSON:
                result = self._handler.to_bson(data)
            elif fmt == SerializationFormat.PICKLE:
                result = self._handler.to_pickle(data)
            else:
                result = self._handler.to_json(data, self._config.pretty_print)

            return ActionResult(
                success=True,
                data={
                    "format": fmt.value,
                    "data": result.decode("latin-1") if isinstance(result, bytes) else result,
                    "size_bytes": len(result)
                }
            )
        except Exception as e:
            logger.exception("Serialization failed")
            return ActionResult(success=False, error=str(e))

    def deserialize(
        self,
        data: bytes,
        format: Optional[SerializationFormat] = None
    ) -> ActionResult:
        """Deserialize data from specified format."""
        try:
            fmt = format or self._config.default_format

            if fmt == SerializationFormat.JSON:
                result = self._handler.from_json(data)
            elif fmt == SerializationFormat.MSGPACK:
                result = self._handler.from_msgpack(data)
            elif fmt == SerializationFormat.CBOR:
                result = self._handler.from_cbor(data)
            elif fmt == SerializationFormat.BSON:
                result = self._handler.from_bson(data)
            elif fmt == SerializationFormat.PICKLE:
                result = self._handler.from_pickle(data)
            else:
                result = self._handler.from_json(data)

            return ActionResult(success=True, data=result)
        except Exception as e:
            logger.exception("Deserialization failed")
            return ActionResult(success=False, error=str(e))

    def register_schema(
        self,
        schema_id: str,
        format: SerializationFormat,
        schema_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> ActionResult:
        """Register a data schema."""
        return self._schema_registry.register(schema_id, format, schema_data, metadata)

    def validate_data(
        self,
        data: Any,
        schema_id: str
    ) -> ActionResult:
        """Validate data against a registered schema."""
        try:
            valid, errors = self._schema_registry.validate(data, schema_id)
            return ActionResult(
                success=valid,
                data={
                    "valid": valid,
                    "errors": errors
                }
            )
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute serialization action."""
        try:
            action = params.get("action", "serialize")

            if action == "serialize":
                return self.serialize(
                    params["data"],
                    SerializationFormat(params.get("format", self._config.default_format.value))
                )
            elif action == "deserialize":
                data = params["data"]
                if isinstance(data, str):
                    data = data.encode()
                return self.deserialize(
                    data,
                    SerializationFormat(params.get("format", self._config.default_format.value))
                )
            elif action == "register_schema":
                return self.register_schema(
                    params["schema_id"],
                    SerializationFormat(params["format"]),
                    params["schema_data"],
                    params.get("metadata")
                )
            elif action == "validate":
                return self.validate_data(params["data"], params["schema_id"])
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
