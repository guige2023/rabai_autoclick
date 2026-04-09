"""Data Serializer Action Module.

Provides serialization capabilities for data objects including
JSON, pickle, and custom serialization formats.

Example:
    >>> from actions.data.data_serializer_action import DataSerializerAction
    >>> action = DataSerializerAction()
    >>> serialized = action.serialize(data, format="json")
"""

from __future__ import annotations

import json
import pickle
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import base64
import threading


class SerializationFormat(Enum):
    """Serialization format types."""
    JSON = "json"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    CUSTOM = "custom"


@dataclass
class SerializerConfig:
    """Configuration for serialization.
    
    Attributes:
        default_format: Default serialization format
        custom_serializer: Custom serialization function
        compression: Enable compression
        encoding: Character encoding
    """
    default_format: SerializationFormat = SerializationFormat.JSON
    custom_serializer: Optional[Callable[[Any], bytes]] = None
    compression: bool = False
    encoding: str = "utf-8"


@dataclass
class SerializationResult:
    """Result of serialization operation.
    
    Attributes:
        data: Serialized data
        format: Format used
        original_size: Original size in bytes
        serialized_size: Serialized size in bytes
        compression_ratio: Compression ratio
    """
    data: Any
    format: SerializationFormat
    original_size: int
    serialized_size: int
    compression_ratio: float = 1.0


class DataSerializerAction:
    """Data serializer for various formats.
    
    Provides serialization with support for multiple
    formats and compression options.
    
    Attributes:
        config: Serializer configuration
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[SerializerConfig] = None,
    ) -> None:
        """Initialize serializer action.
        
        Args:
            config: Serializer configuration
        """
        self.config = config or SerializerConfig()
        self._lock = threading.Lock()
    
    def serialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
    ) -> SerializationResult:
        """Serialize data.
        
        Args:
            data: Data to serialize
            format: Serialization format
        
        Returns:
            SerializationResult
        """
        format = format or self.config.default_format
        original_size = self._get_size(data)
        
        if format == SerializationFormat.JSON:
            serialized = self._serialize_json(data)
        elif format == SerializationFormat.PICKLE:
            serialized = self._serialize_pickle(data)
        elif format == SerializationFormat.CUSTOM:
            serialized = self._serialize_custom(data)
        else:
            serialized = self._serialize_json(data)
        
        if self.config.compression:
            serialized = self._compress(serialized)
        
        serialized_size = len(serialized) if isinstance(serialized, bytes) else len(str(serialized))
        
        return SerializationResult(
            data=serialized,
            format=format,
            original_size=original_size,
            serialized_size=serialized_size,
            compression_ratio=serialized_size / original_size if original_size > 0 else 1.0,
        )
    
    def deserialize(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
    ) -> Any:
        """Deserialize data.
        
        Args:
            data: Data to deserialize
            format: Serialization format
        
        Returns:
            Deserialized object
        """
        format = format or self.config.default_format
        
        if self.config.compression:
            data = self._decompress(data)
        
        if format == SerializationFormat.JSON:
            return self._deserialize_json(data)
        elif format == SerializationFormat.PICKLE:
            return self._deserialize_pickle(data)
        elif format == SerializationFormat.CUSTOM:
            return self._deserialize_custom(data)
        else:
            return self._deserialize_json(data)
    
    def _serialize_json(self, data: Any) -> bytes:
        """Serialize to JSON.
        
        Args:
            data: Data to serialize
        
        Returns:
            JSON bytes
        """
        json_str = json.dumps(data, ensure_ascii=False)
        return json_str.encode(self.config.encoding)
    
    def _deserialize_json(self, data: Any) -> Any:
        """Deserialize from JSON.
        
        Args:
            data: Data to deserialize
        
        Returns:
            Deserialized object
        """
        if isinstance(data, bytes):
            data = data.decode(self.config.encoding)
        return json.loads(data)
    
    def _serialize_pickle(self, data: Any) -> bytes:
        """Serialize to pickle.
        
        Args:
            data: Data to serialize
        
        Returns:
            Pickle bytes
        """
        return pickle.dumps(data)
    
    def _deserialize_pickle(self, data: Any) -> Any:
        """Deserialize from pickle.
        
        Args:
            data: Data to deserialize
        
        Returns:
            Deserialized object
        """
        return pickle.loads(data)
    
    def _serialize_custom(self, data: Any) -> bytes:
        """Serialize using custom serializer.
        
        Args:
            data: Data to serialize
        
        Returns:
            Serialized bytes
        """
        if self.config.custom_serializer:
            return self.config.custom_serializer(data)
        return str(data).encode(self.config.encoding)
    
    def _deserialize_custom(self, data: Any) -> Any:
        """Deserialize using custom deserializer.
        
        Args:
            data: Data to deserialize
        
        Returns:
            Deserialized object
        """
        return data
    
    def _compress(self, data: bytes) -> bytes:
        """Compress data.
        
        Args:
            data: Data to compress
        
        Returns:
            Compressed data
        """
        import zlib
        return base64.b64encode(zlib.compress(data))
    
    def _decompress(self, data: Any) -> bytes:
        """Decompress data.
        
        Args:
            data: Data to decompress
        
        Returns:
            Decompressed data
        """
        import zlib
        if isinstance(data, str):
            data = data.encode()
        return zlib.decompress(base64.b64decode(data))
    
    def _get_size(self, data: Any) -> int:
        """Get size of data.
        
        Args:
            data: Data
        
        Returns:
            Size in bytes
        """
        if isinstance(data, bytes):
            return len(data)
        if isinstance(data, str):
            return len(data.encode(self.config.encoding))
        return len(str(data))
    
    def set_custom_serializer(
        self,
        serializer: Callable[[Any], bytes],
    ) -> None:
        """Set custom serializer function.
        
        Args:
            serializer: Custom serialization function
        """
        self.config.custom_serializer = serializer
    
    def serialize_to_string(
        self,
        data: Any,
        format: Optional[SerializationFormat] = None,
    ) -> str:
        """Serialize to string representation.
        
        Args:
            data: Data to serialize
            format: Serialization format
        
        Returns:
            String representation
        """
        result = self.serialize(data, format)
        
        if isinstance(result.data, bytes):
            return base64.b64encode(result.data).decode("ascii")
        return str(result.data)
