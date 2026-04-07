"""Serialization utilities: pickle, JSON, msgpack, and base64 encoding/decoding."""

from __future__ import annotations

import base64
import json
import pickle
from dataclasses import is_dataclass
from typing import Any

__all__ = [
    "Serializer",
    "JSONSerializer",
    "PickleSerializer",
    "Base64Serializer",
    "MsgPackSerializer",
    "serialize",
    "deserialize",
]


class Serializer:
    """Abstract serializer interface."""

    def serialize(self, obj: Any) -> bytes:
        raise NotImplementedError

    def deserialize(self, data: bytes) -> Any:
        raise NotImplementedError


class JSONSerializer(Serializer):
    """JSON serialization with dataclass support."""

    def __init__(self, indent: int | None = None) -> None:
        self.indent = indent

    def serialize(self, obj: Any) -> bytes:
        return json.dumps(obj, indent=self.indent, default=self._default).encode("utf-8")

    def deserialize(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))

    @staticmethod
    def _default(obj: Any) -> Any:
        if is_dataclass(obj):
            return obj.__dict__
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        if hasattr(obj, "__slots__"):
            return {s: getattr(obj, s) for s in obj.__slots__ if hasattr(obj, s)}
        return str(obj)


class PickleSerializer(Serializer):
    """Pickle-based serialization (not secure for untrusted input)."""

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self.protocol = protocol

    def serialize(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=self.protocol)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)


class Base64Serializer(Serializer):
    """Base64 wrapper for any serializer."""

    def __init__(self, inner: Serializer) -> None:
        self.inner = inner

    def serialize(self, obj: Any) -> bytes:
        return base64.b64encode(self.inner.serialize(obj))

    def deserialize(self, data: bytes) -> Any:
        return self.inner.deserialize(base64.b64decode(data))


def serialize(obj: Any, format: str = "json") -> bytes:
    """Serialize an object to bytes."""
    if format == "json":
        return JSONSerializer().serialize(obj)
    elif format == "pickle":
        return PickleSerializer().serialize(obj)
    else:
        raise ValueError(f"Unknown format: {format}")


def deserialize(data: bytes, format: str = "json") -> Any:
    """Deserialize bytes to an object."""
    if format == "json":
        return JSONSerializer().deserialize(data)
    elif format == "pickle":
        return PickleSerializer().deserialize(data)
    else:
        raise ValueError(f"Unknown format: {format}")
