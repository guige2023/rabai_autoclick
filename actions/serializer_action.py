"""serializer action module for rabai_autoclick.

Provides data serialization utilities: JSON, pickle, msgpack,
binary, and custom serializers with schema support.
"""

from __future__ import annotations

import base64
import io
import json
import pickle
import struct
from dataclasses import dataclass, is_dataclass
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Sequence, Union
from enum import Enum

__all__ = [
    "Serializer",
    "JsonSerializer",
    "PickleSerializer",
    "MsgpackSerializer",
    "BinarySerializer",
    "Base64Serializer",
    "SchemaSerializer",
    "serialize",
    "deserialize",
    "to_json",
    "from_json",
    "to_pickle",
    "from_pickle",
    "to_base64",
    "from_base64",
    "SerializationError",
    "CompressionType",
    "Compressor",
    "GzipCompressor",
    "Lz4Compressor",
]


class SerializationError(Exception):
    """Raised when serialization fails."""
    pass


class CompressionType(Enum):
    """Compression algorithms."""
    NONE = "none"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"


class Compressor:
    """Compression utility."""

    @staticmethod
    def compress_gzip(data: bytes) -> bytes:
        """Compress bytes using gzip."""
        import gzip
        return gzip.compress(data)

    @staticmethod
    def decompress_gzip(data: bytes) -> bytes:
        """Decompress gzip bytes."""
        import gzip
        return gzip.decompress(data)

    @staticmethod
    def compress_lz4(data: bytes) -> bytes:
        """Compress bytes using lz4."""
        try:
            import lz4.frame
            return lz4.frame.compress(data)
        except ImportError:
            raise SerializationError("lz4 not available")

    @staticmethod
    def decompress_lz4(data: bytes) -> bytes:
        """Decompress lz4 bytes."""
        try:
            import lz4.frame
            return lz4.frame.decompress(data)
        except ImportError:
            raise SerializationError("lz4 not available")

    @staticmethod
    def compress_zstd(data: bytes) -> bytes:
        """Compress bytes using zstd."""
        try:
            import zstandard as zstd
            cctx = zstd.ZstdCompressor()
            return cctx.compress(data)
        except ImportError:
            raise SerializationError("zstd not available")

    @staticmethod
    def decompress_zstd(data: bytes) -> bytes:
        """Decompress zstd bytes."""
        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(data)
        except ImportError:
            raise SerializationError("zstd not available")


class Serializer:
    """Base serializer interface."""

    def serialize(self, obj: Any) -> bytes:
        """Serialize object to bytes."""
        raise NotImplementedError

    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to object."""
        raise NotImplementedError


class JsonSerializer(Serializer):
    """JSON serializer with options."""

    def __init__(
        self,
        indent: Optional[int] = None,
        ensure_ascii: bool = False,
        sort_keys: bool = False,
    ) -> None:
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys

    def serialize(self, obj: Any) -> bytes:
        """Serialize to JSON bytes."""
        try:
            text = json.dumps(
                obj,
                indent=self.indent,
                ensure_ascii=self.ensure_ascii,
                sort_keys=self.sort_keys,
                default=self._default_handler,
            )
            return text.encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(f"JSON serialization failed: {e}")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from JSON bytes."""
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeError) as e:
            raise SerializationError(f"JSON deserialization failed: {e}")

    def _default_handler(self, obj: Any) -> Any:
        """Handle non-serializable objects."""
        if is_dataclass(obj):
            return dataclass_to_dict(obj)
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        if hasattr(obj, "__slots__"):
            return {slot: getattr(obj, slot) for slot in obj.__slots__ if hasattr(obj, slot)}
        return str(obj)


class PickleSerializer(Serializer):
    """Pickle serializer with protocol options."""

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self.protocol = protocol

    def serialize(self, obj: Any) -> bytes:
        """Serialize to pickle bytes."""
        try:
            return pickle.dumps(obj, protocol=self.protocol)
        except pickle.PickleError as e:
            raise SerializationError(f"Pickle serialization failed: {e}")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from pickle bytes."""
        try:
            return pickle.loads(data)
        except pickle.UnpicklingError as e:
            raise SerializationError(f"Pickle deserialization failed: {e}")


class MsgpackSerializer(Serializer):
    """MessagePack serializer."""

    def __init__(self) -> None:
        try:
            import msgpack
            self._msgpack = msgpack
        except ImportError:
            raise SerializationError("msgpack not installed")

    def serialize(self, obj: Any) -> bytes:
        """Serialize to MessagePack bytes."""
        try:
            return self._msgpack.packb(obj, use_bin_type=True)
        except Exception as e:
            raise SerializationError(f"MessagePack serialization failed: {e}")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from MessagePack bytes."""
        try:
            return self._msgpack.unpackb(data, raw=False)
        except Exception as e:
            raise SerializationError(f"MessagePack deserialization failed: {e}")


class BinarySerializer(Serializer):
    """Binary serializer using struct for fixed-size records."""

    def __init__(self, fmt: str = "!I") -> None:
        self.fmt = fmt
        self._size = struct.calcsize(fmt)

    def serialize(self, obj: Any) -> bytes:
        """Serialize to binary using struct format."""
        try:
            if isinstance(obj, (list, tuple)):
                return struct.pack(self.fmt, *obj)
            return struct.pack(self.fmt, obj)
        except struct.error as e:
            raise SerializationError(f"Binary serialization failed: {e}")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from binary using struct format."""
        try:
            if len(data) < self._size:
                raise SerializationError(f"Data too short: {len(data)} < {self._size}")
            return struct.unpack(self.fmt, data[:self._size])[0]
        except struct.error as e:
            raise SerializationError(f"Binary deserialization failed: {e}")

    def size(self) -> int:
        """Return the size of one packed record."""
        return self._size


class Base64Serializer(Serializer):
    """Base64 wrapper serializer for transmitting binary as text."""

    def __init__(self, inner: Optional[Serializer] = None) -> None:
        self._inner = inner or PickleSerializer()

    def serialize(self, obj: Any) -> str:
        """Serialize and encode as base64 string."""
        data = self._inner.serialize(obj)
        return base64.b64encode(data).decode("ascii")

    def deserialize(self, data: str) -> Any:
        """Decode base64 and deserialize."""
        raw = base64.b64decode(data.encode("ascii"))
        return self._inner.deserialize(raw)


class SchemaSerializer(Serializer):
    """Schema-based serializer with validation."""

    def __init__(self, schema: Dict[str, type]) -> None:
        self.schema = schema

    def serialize(self, obj: Any) -> bytes:
        """Serialize with schema validation."""
        if isinstance(obj, dict):
            self._validate(obj)
            return json.dumps(obj).encode("utf-8")
        raise SerializationError("SchemaSerializer only supports dicts")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize and validate against schema."""
        obj = json.loads(data.decode("utf-8"))
        if isinstance(obj, dict):
            self._validate(obj)
        return obj

    def _validate(self, obj: dict) -> None:
        """Validate object against schema."""
        for key, expected_type in self.schema.items():
            if key not in obj:
                raise SerializationError(f"Missing required field: {key}")
            if not isinstance(obj[key], expected_type):
                raise SerializationError(
                    f"Field {key} has wrong type: expected {expected_type.__name__}, "
                    f"got {type(obj[key]).__name__}"
                )


def dataclass_to_dict(obj: Any) -> dict:
    """Convert dataclass to dict recursively."""
    if not is_dataclass(obj):
        return obj
    result = {}
    for field_name in getattr(obj, "__dataclass_fields__", {}):
        value = getattr(obj, field_name)
        if is_dataclass(value):
            result[field_name] = dataclass_to_dict(value)
        elif isinstance(value, (list, tuple)):
            result[field_name] = [
                dataclass_to_dict(v) if is_dataclass(v) else v
                for v in value
            ]
        elif isinstance(value, dict):
            result[field_name] = {
                k: dataclass_to_dict(v) if is_dataclass(v) else v
                for k, v in value.items()
            }
        else:
            result[field_name] = value
    return result


def to_json(obj: Any, **kwargs: Any) -> str:
    """Serialize object to JSON string."""
    return json.dumps(obj, default=_json_default, **kwargs)


def from_json(text: str) -> Any:
    """Deserialize JSON string to object."""
    return json.loads(text)


def to_pickle(obj: Any, protocol: int = pickle.HIGHEST_PROTOCOL) -> bytes:
    """Serialize object to pickle bytes."""
    return pickle.dumps(obj, protocol=protocol)


def from_pickle(data: bytes) -> Any:
    """Deserialize pickle bytes to object."""
    return pickle.loads(data)


def to_base64(obj: Any) -> str:
    """Serialize object to base64 string."""
    data = to_pickle(obj)
    return base64.b64encode(data).decode("ascii")


def from_base64(text: str) -> Any:
    """Deserialize base64 string to object."""
    data = base64.b64decode(text.encode("ascii"))
    return from_pickle(data)


def serialize(
    obj: Any,
    format: str = "json",
    **kwargs: Any,
) -> Union[bytes, str]:
    """Serialize object to specified format.

    Args:
        obj: Object to serialize.
        format: Format name ("json", "pickle", "msgpack", "base64").
        **kwargs: Format-specific options.

    Returns:
        Serialized bytes or string.
    """
    if format == "json":
        return JsonSerializer(**kwargs).serialize(obj)
    elif format == "pickle":
        return PickleSerializer(**kwargs).serialize(obj)
    elif format == "msgpack":
        return MsgpackSerializer().serialize(obj)
    elif format == "base64":
        return Base64Serializer().serialize(obj)
    else:
        raise SerializationError(f"Unknown format: {format}")


def deserialize(
    data: Union[bytes, str],
    format: str = "json",
    **kwargs: Any,
) -> Any:
    """Deserialize from specified format.

    Args:
        data: Serialized data.
        format: Format name ("json", "pickle", "msgpack", "base64").
        **kwargs: Format-specific options.

    Returns:
        Deserialized object.
    """
    if format == "json":
        return JsonSerializer(**kwargs).deserialize(data)
    elif format == "pickle":
        return PickleSerializer(**kwargs).deserialize(data)
    elif format == "msgpack":
        return MsgpackSerializer().deserialize(data)
    elif format == "base64":
        return Base64Serializer().deserialize(data)
    else:
        raise SerializationError(f"Unknown format: {format}")


def _json_default(obj: Any) -> Any:
    """Default JSON encoder fallback."""
    if is_dataclass(obj):
        return dataclass_to_dict(obj)
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    if hasattr(obj, "__slots__"):
        return {slot: getattr(obj, slot) for slot in obj.__slots__ if hasattr(obj, slot)}
    try:
        return str(obj)
    except Exception:
        return repr(obj)
