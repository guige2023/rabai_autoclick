"""
Data Serializer Action Module.

Provides multi-format serialization/deserialization with
schema evolution support.
"""

import asyncio
import base64
import json
import pickle
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar
import zlib

T = TypeVar("T")


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    XML = "xml"
    PICKLE = "pickle"
    MSGPACK = "msgpack"
    CBOR = "cbor"
    UBJSON = "ubjson"


@dataclass
class SchemaVersion:
    """Schema version for evolution."""
    version: int
    transform: Optional[Callable[[dict], dict]] = None
    validator: Optional[Callable[[dict], bool]] = None


@dataclass
class SerializerConfig:
    """Serializer configuration."""
    format: SerializationFormat = SerializationFormat.JSON
    pretty: bool = False
    compression: bool = False
    schema_versions: list[SchemaVersion] = field(default_factory=list)
    current_version: int = 1


@dataclass
class SerializationResult:
    """Serialization result."""
    success: bool
    data: Any = None
    format: SerializationFormat = SerializationFormat.JSON
    size: int = 0
    compressed: bool = False
    error: Optional[str] = None


class JSONSerializer:
    """JSON serializer."""

    def __init__(self, pretty: bool = False):
        self.pretty = pretty

    def serialize(self, data: Any) -> bytes:
        """Serialize to JSON bytes."""
        if self.pretty:
            return json.dumps(data, indent=2, ensure_ascii=False).encode()
        return json.dumps(data, ensure_ascii=False).encode()

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from JSON bytes."""
        return json.loads(data.decode())


class XMLSerializer:
    """XML serializer."""

    def __init__(self, root_name: str = "root"):
        self.root_name = root_name

    def _dict_to_xml(self, data: Any, element: ET.Element) -> None:
        """Convert dict to XML element."""
        if isinstance(data, dict):
            for key, value in data.items():
                sub_element = ET.SubElement(element, str(key))
                self._dict_to_xml(value, sub_element)
        elif isinstance(data, list):
            for item in data:
                item_element = ET.SubElement(element, "item")
                self._dict_to_xml(item, item_element)
        else:
            element.text = str(data) if data is not None else ""

    def _xml_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dict."""
        result = {}

        for child in element:
            child_data = self._xml_to_dict(child)

            if child.tag == "item":
                if element.tag not in result:
                    result[element.tag] = []
                result[element.tag].append(child_data)
            else:
                result[child.tag] = child_data

        if not result and element.text:
            return element.text

        return result if result else None

    def serialize(self, data: Any) -> bytes:
        """Serialize to XML bytes."""
        root = ET.Element(self.root_name)
        self._dict_to_xml(data, root)
        return ET.tostring(root, encoding="utf-8")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from XML bytes."""
        root = ET.fromstring(data)
        return {root.tag: self._xml_to_dict(root)}


class PickleSerializer:
    """Pickle serializer."""

    def serialize(self, data: Any) -> bytes:
        """Serialize to pickle bytes."""
        return pickle.dumps(data)

    def deserialize(self, data: bytes) -> Any:
        """Deserialize from pickle bytes."""
        return pickle.loads(data)


class CompressionWrapper:
    """Compression wrapper for serializers."""

    def __init__(self, serializer: Any, level: int = 6):
        self.serializer = serializer
        self.level = level

    def serialize_compress(self, data: Any) -> tuple[bytes, int]:
        """Serialize and compress."""
        serialized = self.serializer.serialize(data)
        compressed = zlib.compress(serialized, level=self.level)
        return compressed, len(serialized)

    def decompress_deserialize(self, data: bytes) -> Any:
        """Decompress and deserialize."""
        decompressed = zlib.decompress(data)
        return self.serializer.deserialize(decompressed)


class DataSerializer:
    """Multi-format data serializer."""

    def __init__(self, config: Optional[SerializerConfig] = None):
        self.config = config or SerializerConfig()
        self._serializer = self._create_serializer()
        self._compression = CompressionWrapper(
            self._serializer,
            level=6
        ) if self.config.compression else None

    def _create_serializer(self) -> Any:
        """Create appropriate serializer."""
        if self.config.format == SerializationFormat.JSON:
            return JSONSerializer(pretty=self.config.pretty)
        elif self.config.format == SerializationFormat.XML:
            return XMLSerializer()
        elif self.config.format == SerializationFormat.PICKLE:
            return PickleSerializer()
        else:
            return JSONSerializer(pretty=self.config.pretty)

    def serialize(self, data: Any) -> SerializationResult:
        """Serialize data."""
        try:
            if self._compression:
                serialized, original_size = self._compression.serialize_compress(data)
                return SerializationResult(
                    success=True,
                    data=base64.b64encode(serialized).decode(),
                    format=self.config.format,
                    size=len(serialized),
                    compressed=True
                )
            else:
                serialized = self._serializer.serialize(data)
                return SerializationResult(
                    success=True,
                    data=serialized.decode() if isinstance(serialized, bytes) else serialized,
                    format=self.config.format,
                    size=len(serialized)
                )
        except Exception as e:
            return SerializationResult(
                success=False,
                error=str(e)
            )

    def deserialize(self, data: Any) -> SerializationResult:
        """Deserialize data."""
        try:
            if isinstance(data, str) and self.config.compression:
                decoded = base64.b64decode(data)
                result = self._compression.decompress_deserialize(decoded)
            elif isinstance(data, bytes):
                result = self._serializer.deserialize(data)
            else:
                result = self._serializer.deserialize(data.encode())
            return SerializationResult(
                success=True,
                data=result,
                format=self.config.format
            )
        except Exception as e:
            return SerializationResult(
                success=False,
                error=str(e)
            )

    def migrate_version(
        self,
        data: dict,
        from_version: int,
        to_version: int
    ) -> dict:
        """Migrate data between schema versions."""
        current_version = from_version
        result = data.copy()

        while current_version < to_version:
            next_version = current_version + 1
            version_spec = next(
                (v for v in self.config.schema_versions if v.version == next_version),
                None
            )

            if version_spec and version_spec.transform:
                result = version_spec.transform(result)

            current_version = next_version

        return result


class DataSerializerAction:
    """
    Multi-format serialization with schema evolution.

    Example:
        config = SerializerConfig(
            format=SerializationFormat.JSON,
            pretty=True,
            compression=True
        )

        serializer = DataSerializerAction(config)
        result = serializer.serialize(data)
        data = serializer.deserialize(result.data)
    """

    def __init__(
        self,
        format: SerializationFormat = SerializationFormat.JSON,
        **kwargs: Any
    ):
        config = SerializerConfig(format=format, **kwargs)
        self._serializer = DataSerializer(config)

    def serialize(self, data: Any) -> SerializationResult:
        """Serialize data."""
        return self._serializer.serialize(data)

    def deserialize(self, data: Any) -> SerializationResult:
        """Deserialize data."""
        return self._serializer.deserialize(data)

    def add_schema_version(
        self,
        version: int,
        transform: Optional[Callable[[dict], dict]] = None,
        validator: Optional[Callable[[dict], bool]] = None
    ) -> "DataSerializerAction":
        """Add schema version."""
        self._serializer.config.schema_versions.append(
            SchemaVersion(version, transform, validator)
        )
        return self

    def migrate(
        self,
        data: dict,
        from_version: int,
        to_version: int
    ) -> dict:
        """Migrate between versions."""
        return self._serializer.migrate_version(data, from_version, to_version)
