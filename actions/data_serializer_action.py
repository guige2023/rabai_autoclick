"""Data serializer action module.

Provides serialization and deserialization:
- DataSerializer: Serialize/deserialize data
- CompactSerializer: Compact binary serialization
- SchemaSerializer: Schema-based serialization
- SerializerRegistry: Registry for serializers
"""

from __future__ import annotations

import json
import base64
import pickle
import zlib
from typing import Any, Callable, Dict, List, Optional, Type
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    """Serialization format."""
    JSON = "json"
    BINARY = "binary"
    COMPACT = "compact"
    BASE64 = "base64"
    PICKLE = "pickle"


@dataclass
class SerializationResult:
    """Result of a serialization operation."""
    success: bool
    data: Any
    format: SerializationFormat
    size_bytes: int = 0
    error: Optional[str] = None


class DataSerializer:
    """General-purpose data serializer."""

    def __init__(
        self,
        default_format: SerializationFormat = SerializationFormat.JSON,
        compression: bool = False,
        compression_level: int = 6,
    ):
        self.default_format = default_format
        self.compression = compression
        self.compression_level = compression_level
        self._encoders: Dict[SerializationFormat, Callable] = {}
        self._decoders: Dict[SerializationFormat, Callable] = {}
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default serialization handlers."""
        self._encoders[SerializationFormat.JSON] = lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8")
        self._decoders[SerializationFormat.JSON] = lambda d: json.loads(d.decode("utf-8"))

        self._encoders[SerializationFormat.BINARY] = lambda d: str(d).encode("utf-8")
        self._decoders[SerializationFormat.BINARY] = lambda d: d.decode("utf-8")

        self._encoders[SerializationFormat.BASE64] = lambda d: base64.b64encode(json.dumps(d).encode("utf-8"))
        self._decoders[SerializationFormat.BASE64] = lambda d: json.loads(base64.b64decode(d).decode("utf-8"))

        self._encoders[SerializationFormat.PICKLE] = lambda d: pickle.dumps(d)
        self._decoders[SerializationFormat.PICKLE] = lambda d: pickle.loads(d)

    def serialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
    ) -> SerializationResult:
        """Serialize data to specified format."""
        fmt = format or self.default_format
        try:
            encoded = self._encoders[fmt](data)
            if self.compression:
                encoded = zlib.compress(encoded, level=self.compression_level)
            return SerializationResult(
                success=True,
                data=encoded,
                format=fmt,
                size_bytes=len(encoded),
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=fmt,
                error=str(e),
            )

    def deserialize(
        self,
        data: Any,
        format: SerializationFormat,
    ) -> SerializationResult:
        """Deserialize data from specified format."""
        try:
            decoded_data = data
            if self.compression:
                try:
                    decoded_data = zlib.decompress(decoded_data)
                except zlib.error:
                    pass
            result = self._decoders[format](decoded_data)
            return SerializationResult(
                success=True,
                data=result,
                format=format,
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=format,
                error=str(e),
            )

    def register_encoder(
        self,
        format: SerializationFormat,
        encoder: Callable[[Any], bytes],
    ) -> None:
        """Register a custom encoder."""
        self._encoders[format] = encoder

    def register_decoder(
        self,
        format: SerializationFormat,
        decoder: Callable[[bytes], Any],
    ) -> None:
        """Register a custom decoder."""
        self._decoders[format] = decoder


class SchemaSerializer:
    """Schema-based serializer with validation."""

    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        self.schema = schema or {}

    def serialize(
        self,
        data: Dict[str, Any],
        validate: bool = True,
    ) -> SerializationResult:
        """Serialize data with schema validation."""
        if validate:
            errors = self._validate(data)
            if errors:
                return SerializationResult(
                    success=False,
                    data=None,
                    format=SerializationFormat.JSON,
                    error=f"Validation errors: {errors}",
                )
        try:
            json_str = json.dumps(data, ensure_ascii=False)
            return SerializationResult(
                success=True,
                data=json_str,
                format=SerializationFormat.JSON,
                size_bytes=len(json_str.encode("utf-8")),
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=SerializationFormat.JSON,
                error=str(e),
            )

    def deserialize(
        self,
        data: str,
        validate: bool = True,
    ) -> SerializationResult:
        """Deserialize data with schema validation."""
        try:
            parsed = json.loads(data)
            if validate:
                errors = self._validate(parsed)
                if errors:
                    return SerializationResult(
                        success=False,
                        data=None,
                        format=SerializationFormat.JSON,
                        error=f"Validation errors: {errors}",
                    )
            return SerializationResult(
                success=True,
                data=parsed,
                format=SerializationFormat.JSON,
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                data=None,
                format=SerializationFormat.JSON,
                error=str(e),
            )

    def _validate(self, data: Dict[str, Any]) -> List[str]:
        """Validate data against schema."""
        errors = []
        for field_name, field_schema in self.schema.items():
            if field_schema.get("required", False) and field_name not in data:
                errors.append(f"Missing required field: {field_name}")
        return errors


class SerializerRegistry:
    """Registry for serializer instances."""

    _instance: Optional["SerializerRegistry"] = None

    def __init__(self):
        self._serializers: Dict[str, DataSerializer] = {}
        self._schemas: Dict[str, SchemaSerializer] = {}

    @classmethod
    def get_instance(cls) -> "SerializerRegistry":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register_serializer(
        self,
        name: str,
        serializer: DataSerializer,
    ) -> None:
        """Register a serializer."""
        self._serializers[name] = serializer

    def get_serializer(self, name: str) -> Optional[DataSerializer]:
        """Get a registered serializer."""
        return self._serializers.get(name)

    def register_schema(
        self,
        name: str,
        schema: Dict[str, Any],
    ) -> SchemaSerializer:
        """Register a schema."""
        self._schemas[name] = SchemaSerializer(schema)
        return self._schemas[name]

    def get_schema_serializer(self, name: str) -> Optional[SchemaSerializer]:
        """Get a schema serializer."""
        return self._schemas.get(name)


def serialize_json(data: Any, compress: bool = False) -> bytes:
    """Convenience function to serialize to JSON."""
    serializer = DataSerializer(compression=compress)
    result = serializer.serialize(data, SerializationFormat.JSON)
    if not result.success:
        raise ValueError(result.error)
    return result.data


def deserialize_json(data: bytes, compressed: bool = False) -> Any:
    """Convenience function to deserialize from JSON."""
    serializer = DataSerializer(compression=compressed)
    result = serializer.deserialize(data, SerializationFormat.JSON)
    if not result.success:
        raise ValueError(result.error)
    return result.data
