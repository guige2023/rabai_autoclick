"""
Data Serializer Action Module.

Universal data serialization with support for multiple
formats, schema evolution, and compression.
"""

import json
import zlib
import base64
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    MSGPACK = "msgpack"
    CBOR = "cbor"
    UBJSON = "ubjson"
    CUSTOM = "custom"


class CompressionType(Enum):
    """Compression types."""
    NONE = "none"
    ZLIB = "zlib"
    GZIP = "gzip"
    LZ4 = "lz4"


@dataclass
class SerializerConfig:
    """Serializer configuration."""
    format: SerializationFormat = SerializationFormat.JSON
    compression: CompressionType = CompressionType.NONE
    indent: Optional[int] = None
    ensure_ascii: bool = False
    custom_encoder: Optional[Callable] = None
    schema_version: str = "1.0"


@dataclass
class SerializationResult:
    """Result of serialization operation."""
    success: bool
    data: Any
    format: SerializationFormat
    compressed: bool
    size_bytes: int


class DataSerializerAction:
    """
    Universal data serializer with multiple format support.

    Example:
        serializer = DataSerializerAction()
        serializer.configure(format=SerializationFormat.MSGPACK, compression=CompressionType.ZLIB)
        serialized = serializer.serialize(data)
        data = serializer.deserialize(serialized)
    """

    def __init__(self, config: Optional[SerializerConfig] = None):
        """
        Initialize data serializer.

        Args:
            config: Serializer configuration.
        """
        self.config = config or SerializerConfig()
        self._custom_serializers: dict[type, Callable] = {}

    def configure(
        self,
        format: Optional[SerializationFormat] = None,
        compression: Optional[CompressionType] = None,
        indent: Optional[int] = None
    ) -> None:
        """Update serializer configuration."""
        if format is not None:
            self.config.format = format
        if compression is not None:
            self.config.compression = compression
        if indent is not None:
            self.config.indent = indent

    def register_type(
        self,
        type_cls: type,
        serializer: Callable,
        deserializer: Callable
    ) -> None:
        """
        Register custom type serializer.

        Args:
            type_cls: Type class to register.
            serializer: Function to serialize type.
            deserializer: Function to deserialize type.
        """
        self._custom_serializers[type_cls] = {
            "serialize": serializer,
            "deserialize": deserializer
        }

    def serialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
        compression: Optional[CompressionType] = None
    ) -> Any:
        """
        Serialize data to specified format.

        Args:
            data: Data to serialize.
            format: Output format (uses config default if None).
            compression: Compression type (uses config default if None).

        Returns:
            Serialized data.
        """
        fmt = format or self.config.format
        comp = compression or self.config.compression

        try:
            serialized = self._serialize_impl(data, fmt)

            if comp != CompressionType.NONE:
                serialized = self._compress(serialized, comp)

            return serialized

        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            raise

    def deserialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
        compression: Optional[CompressionType] = None
    ) -> Any:
        """
        Deserialize data from specified format.

        Args:
            data: Serialized data.
            format: Input format (uses config default if None).
            compression: Compression type (uses config default if None).

        Returns:
            Deserialized data.
        """
        fmt = format or self.config.format
        comp = compression or self.config.compression

        try:
            if comp != CompressionType.NONE:
                data = self._decompress(data, comp)

            return self._deserialize_impl(data, fmt)

        except Exception as e:
            logger.error(f"Deserialization failed: {e}")
            raise

    def serialize_to_bytes(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None
    ) -> bytes:
        """
        Serialize to bytes.

        Args:
            data: Data to serialize.
            format: Output format.

        Returns:
            Serialized bytes.
        """
        fmt = format or self.config.format
        serialized = self._serialize_impl(data, fmt)

        if isinstance(serialized, str):
            return serialized.encode("utf-8")

        return serialized

    def serialize_to_base64(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None
    ) -> str:
        """
        Serialize and encode to base64.

        Args:
            data: Data to serialize.
            format: Output format.

        Returns:
            Base64-encoded string.
        """
        bytes_data = self.serialize_to_bytes(data, format)
        return base64.b64encode(bytes_data).decode("ascii")

    def deserialize_from_base64(
        self,
        encoded: str,
        format: Optional[SerializationFormat] = None
    ) -> Any:
        """
        Deserialize from base64-encoded string.

        Args:
            encoded: Base64-encoded string.
            format: Input format.

        Returns:
            Deserialized data.
        """
        bytes_data = base64.b64decode(encoded.encode("ascii"))
        return self.deserialize(bytes_data, format)

    def _serialize_impl(self, data: Any, fmt: SerializationFormat) -> Any:
        """Internal serialization implementation."""
        if fmt == SerializationFormat.JSON:
            return self._to_json(data)

        elif fmt == SerializationFormat.MSGPACK:
            try:
                import msgpack
                return msgpack.packb(data, use_bin_type=True)
            except ImportError:
                raise ImportError("msgpack is required. Install with: pip install msgpack")

        elif fmt == SerializationFormat.CBOR:
            try:
                import cbor2
                return cbor2.dumps(data)
            except ImportError:
                raise ImportError("cbor2 is required. Install with: pip install cbor2")

        elif fmt == SerializationFormat.UBJSON:
            try:
                import ubjson
                return ubjson.dumpb(data)
            except ImportError:
                raise ImportError("ubjson is required. Install with: pip install ubjson")

        raise ValueError(f"Unsupported format: {fmt}")

    def _deserialize_impl(self, data: Any, fmt: SerializationFormat) -> Any:
        """Internal deserialization implementation."""
        if fmt == SerializationFormat.JSON:
            return self._from_json(data)

        elif fmt == SerializationFormat.MSGPACK:
            try:
                import msgpack
                return msgpack.unpackb(data, raw=False)
            except ImportError:
                raise ImportError("msgpack is required")

        elif fmt == SerializationFormat.CBOR:
            try:
                import cbor2
                return cbor2.loads(data)
            except ImportError:
                raise ImportError("cbor2 is required")

        elif fmt == SerializationFormat.UBJSON:
            try:
                import ubjson
                return ubjson.loadb(data)
            except ImportError:
                raise ImportError("ubjson is required")

        raise ValueError(f"Unsupported format: {fmt}")

    def _to_json(self, data: Any) -> str:
        """Serialize to JSON string."""
        default_handler = None

        if self._custom_serializers:
            def default(o):
                for type_cls, handlers in self._custom_serializers.items():
                    if isinstance(o, type_cls):
                        return {"__type__": type_cls.__name__, "data": handlers["serialize"](o)}
                raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

            default_handler = default

        return json.dumps(
            data,
            indent=self.config.indent,
            ensure_ascii=self.config.ensure_ascii,
            default=default_handler
        )

    def _from_json(self, data: Any) -> Any:
        """Deserialize from JSON."""
        if isinstance(data, str):
            return json.loads(data)
        return json.loads(data.decode("utf-8"))

    def _compress(self, data: Any, compression: CompressionType) -> bytes:
        """Compress serialized data."""
        if isinstance(data, str):
            data = data.encode("utf-8")

        if compression == CompressionType.ZLIB:
            return zlib.compress(data)

        elif compression == CompressionType.GZIP:
            import gzip
            return gzip.compress(data)

        elif compression == CompressionType.LZ4:
            try:
                import lz4.frame
                return lz4.frame.compress(data)
            except ImportError:
                raise ImportError("lz4 is required. Install with: pip install lz4")

        return data

    def _decompress(self, data: bytes, compression: CompressionType) -> Any:
        """Decompress serialized data."""
        if compression == CompressionType.ZLIB:
            return zlib.decompress(data)

        elif compression == CompressionType.GZIP:
            import gzip
            return gzip.decompress(data)

        elif compression == CompressionType.LZ4:
            try:
                import lz4.frame
                return lz4.frame.decompress(data)
            except ImportError:
                raise ImportError("lz4 is required")

        return data

    def get_size(self, data: Any, format: Optional[SerializationFormat] = None) -> int:
        """
        Get serialized size in bytes.

        Args:
            data: Data to measure.
            format: Serialization format.

        Returns:
            Size in bytes.
        """
        serialized = self._serialize_impl(data, format or self.config.format)
        if isinstance(serialized, str):
            return len(serialized.encode("utf-8"))
        return len(serialized)
