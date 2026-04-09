"""Data Serializer Action Module.

Serialize/deserialize data with support for multiple formats.
"""

from __future__ import annotations

import base64
import json
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Generic, TypeVar
import yaml

T = TypeVar("T")


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    YAML = "yaml"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    BASE64_JSON = "base64_json"


class SerializerError(Exception):
    """Serializer error."""
    pass


class Serializer(ABC, Generic[T]):
    """Abstract serializer interface."""

    @abstractmethod
    def serialize(self, data: T) -> str | bytes:
        """Serialize data to string or bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: str | bytes) -> T:
        """Deserialize data from string or bytes."""
        pass


class JSONSerializer(Serializer[Any]):
    """JSON serializer with datetime support."""

    def __init__(self, indent: int | None = None, ensure_ascii: bool = False) -> None:
        self.indent = indent
        self.ensure_ascii = ensure_ascii

    def serialize(self, data: Any) -> str:
        def default_handler(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return {"__datetime__": True, "value": obj.isoformat()}
            if isinstance(obj, bytes):
                return {"__bytes__": True, "value": base64.b64encode(obj).decode()}
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        try:
            return json.dumps(data, indent=self.indent, ensure_ascii=self.ensure_ascii, default=default_handler)
        except Exception as e:
            raise SerializerError(f"JSON serialization failed: {e}") from e

    def deserialize(self, data: str | bytes) -> Any:
        def object_hook(obj: dict) -> Any:
            if "__datetime__" in obj:
                return datetime.fromisoformat(obj["value"])
            if "__bytes__" in obj:
                return base64.b64decode(obj["value"])
            return obj
        try:
            return json.loads(data, object_hook=object_hook)
        except Exception as e:
            raise SerializerError(f"JSON deserialization failed: {e}") from e


class YAMLSerializer(Serializer[Any]):
    """YAML serializer."""

    def __init__(self) -> None:
        self._yaml = yaml.SafeDumper
        self._yaml_add_representer(datetime, lambda dumper, dt: dumper.represent_scalar("tag:yaml.org,2002:timestamp", dt.isoformat()))

    def serialize(self, data: Any) -> str:
        try:
            return yaml.dump(data, Dumper=self._yaml, default_flow_style=False, allow_unicode=True)
        except Exception as e:
            raise SerializerError(f"YAML serialization failed: {e}") from e

    def deserialize(self, data: str | bytes) -> Any:
        try:
            return yaml.safe_load(data)
        except Exception as e:
            raise SerializerError(f"YAML deserialization failed: {e}") from e


class PickleSerializer(Serializer[Any]):
    """Pickle serializer for Python objects."""

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self.protocol = protocol

    def serialize(self, data: Any) -> bytes:
        try:
            return pickle.dumps(data, protocol=self.protocol)
        except Exception as e:
            raise SerializerError(f"Pickle serialization failed: {e}") from e

    def deserialize(self, data: str | bytes) -> Any:
        try:
            return pickle.loads(data)
        except Exception as e:
            raise SerializerError(f"Pickle deserialization failed: {e}") from e


class Base64JSONSerializer(Serializer[Any]):
    """JSON serializer wrapped in base64."""

    def __init__(self) -> None:
        self.json_serializer = JSONSerializer()

    def serialize(self, data: Any) -> str:
        json_str = self.json_serializer.serialize(data)
        return base64.b64encode(json_str.encode()).decode()

    def deserialize(self, data: str | bytes) -> Any:
        decoded = base64.b64decode(data.encode()).decode()
        return self.json_serializer.deserialize(decoded)


class SerializerFactory:
    """Factory for creating serializers."""

    _serializers: dict[SerializationFormat, type[Serializer]] = {
        SerializationFormat.JSON: JSONSerializer,
        SerializationFormat.YAML: YAMLSerializer,
        SerializationFormat.PICKLE: PickleSerializer,
        SerializationFormat.BASE64_JSON: Base64JSONSerializer,
    }

    @classmethod
    def create(cls, format: SerializationFormat) -> Serializer:
        """Create a serializer for the given format."""
        serializer_class = cls._serializers.get(format)
        if not serializer_class:
            raise ValueError(f"Unknown format: {format}")
        return serializer_class()

    @classmethod
    def register(cls, format: SerializationFormat, serializer_class: type[Serializer]) -> None:
        """Register a custom serializer."""
        cls._serializers[format] = serializer_class
