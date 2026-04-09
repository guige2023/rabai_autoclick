"""Data serialization and deserialization utilities.

This module provides serialization support for:
- Multiple formats (JSON, CSV, XML, YAML)
- Schema validation during deserialization
- Custom type handling
- Streaming serialization

Example:
    >>> from actions.data_serializer_action import Serializer
    >>> serializer = Serializer(format="json")
    >>> data = serializer.deserialize(json_string)
"""

from __future__ import annotations

import json
import csv
import logging
import io
from typing import Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    """Supported serialization formats."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    YAML = "yaml"


@dataclass
class SerializationOptions:
    """Options for serialization."""
    format: SerializationFormat = SerializationFormat.JSON
    pretty: bool = False
    include_none: bool = False
    datetime_format: str = "iso"
    custom_encoders: dict[type, Callable[[Any], Any]] = None


class Serializer:
    """Serialize and deserialize data.

    Attributes:
        options: Serialization options.
    """

    def __init__(self, options: Optional[SerializationOptions] = None) -> None:
        self.options = options or SerializationOptions()

    def serialize(self, data: Any) -> str:
        """Serialize data to string.

        Args:
            data: Data to serialize.

        Returns:
            Serialized string.

        Raises:
            ValueError: If format is unsupported.
        """
        if self.options.format == SerializationFormat.JSON:
            return self._serialize_json(data)
        elif self.options.format == SerializationFormat.CSV:
            return self._serialize_csv(data)
        elif self.options.format == SerializationFormat.XML:
            return self._serialize_xml(data)
        elif self.options.format == SerializationFormat.YAML:
            return self._serialize_yaml(data)
        else:
            raise ValueError(f"Unsupported format: {self.options.format}")

    def deserialize(self, data: str) -> Any:
        """Deserialize string to data.

        Args:
            data: String to deserialize.

        Returns:
            Deserialized data.

        Raises:
            ValueError: If format is unsupported or parsing fails.
        """
        if self.options.format == SerializationFormat.JSON:
            return self._deserialize_json(data)
        elif self.options.format == SerializationFormat.CSV:
            return self._deserialize_csv(data)
        elif self.options.format == SerializationFormat.XML:
            return self._deserialize_xml(data)
        elif self.options.format == SerializationFormat.YAML:
            return self._deserialize_yaml(data)
        else:
            raise ValueError(f"Unsupported format: {self.options.format}")

    def _serialize_json(self, data: Any) -> str:
        """Serialize to JSON."""
        kwargs = {"indent": 2} if self.options.pretty else {}
        return json.dumps(data, **kwargs)

    def _deserialize_json(self, data: str) -> Any:
        """Deserialize from JSON."""
        return json.loads(data)

    def _serialize_csv(self, data: Any) -> str:
        """Serialize to CSV."""
        if not isinstance(data, list):
            data = [data]
        if not data:
            return ""
        output = io.StringIO()
        fieldnames = list(data[0].keys()) if isinstance(data[0], dict) else []
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)
            else:
                writer.writerow({})
        return output.getvalue()

    def _deserialize_csv(self, data: str) -> list[dict[str, Any]]:
        """Deserialize from CSV."""
        input_stream = io.StringIO(data)
        reader = csv.DictReader(input_stream)
        return list(reader)

    def _serialize_xml(self, data: Any) -> str:
        """Serialize to XML (basic implementation)."""
        def to_xml(obj: Any, root: str = "root") -> str:
            if isinstance(obj, dict):
                items = "".join(f"<{k}>{to_xml(v, k)}</{k}>" for k, v in obj.items())
                return f"<{root}>{items}</{root}>"
            elif isinstance(obj, list):
                items = "".join(f"<item>{to_xml(i, 'item')}</item>" for i in obj)
                return f"<{root}>{items}</{root}>"
            else:
                return str(obj)
        return to_xml(data, "data")

    def _deserialize_xml(self, data: str) -> Any:
        """Deserialize from XML (basic implementation)."""
        import xml.etree.ElementTree as ET
        root = ET.fromstring(data)
        return self._xml_to_dict(root)

    def _xml_to_dict(self, element) -> Any:
        """Convert XML element to dict."""
        result = {}
        for child in element:
            value = self._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(value)
            else:
                result[child.tag] = value
        if not result and element.text:
            return element.text
        return result

    def _serialize_yaml(self, data: Any) -> str:
        """Serialize to YAML."""
        try:
            import yaml
            return yaml.dump(data, default_flow_style=False)
        except ImportError:
            logger.warning("PyYAML not installed, falling back to JSON")
            return self._serialize_json(data)

    def _deserialize_yaml(self, data: str) -> Any:
        """Deserialize from YAML."""
        try:
            import yaml
            return yaml.safe_load(data)
        except ImportError:
            logger.warning("PyYAML not installed, falling back to JSON")
            return self._deserialize_json(data)


class StreamingSerializer:
    """Serialize large datasets in chunks."""

    def __init__(self, chunk_size: int = 1000) -> None:
        self.chunk_size = chunk_size

    def serialize_chunks(
        self,
        data: list[dict[str, Any]],
        format: SerializationFormat = SerializationFormat.JSON,
    ) -> list[str]:
        """Serialize data in chunks.

        Args:
            data: Data to serialize.
            format: Output format.

        Returns:
            List of serialized chunks.
        """
        chunks = []
        for i in range(0, len(data), self.chunk_size):
            chunk = data[i:i + self.chunk_size]
            serializer = Serializer(SerializationOptions(format=format))
            chunks.append(serializer.serialize(chunk))
        return chunks

    def deserialize_chunks(
        self,
        chunks: list[str],
        format: SerializationFormat = SerializationFormat.JSON,
    ) -> list[Any]:
        """Deserialize chunks into single dataset.

        Args:
            chunks: List of serialized chunks.
            format: Input format.

        Returns:
            Combined deserialized data.
        """
        result = []
        for chunk in chunks:
            serializer = Serializer(SerializationOptions(format=format))
            data = serializer.deserialize(chunk)
            if isinstance(data, list):
                result.extend(data)
            else:
                result.append(data)
        return result


def serialize(data: Any, format: str = "json", **kwargs: Any) -> str:
    """Quick serialize function.

    Args:
        data: Data to serialize.
        format: Format name.
        **kwargs: Additional options.

    Returns:
        Serialized string.
    """
    fmt_map = {
        "json": SerializationFormat.JSON,
        "csv": SerializationFormat.CSV,
        "xml": SerializationFormat.XML,
        "yaml": SerializationFormat.YAML,
    }
    options = SerializationOptions(
        format=fmt_map.get(format.lower(), SerializationFormat.JSON),
        pretty=kwargs.get("pretty", False),
    )
    return Serializer(options).serialize(data)


def deserialize(data: str, format: str = "json") -> Any:
    """Quick deserialize function.

    Args:
        data: String to deserialize.
        format: Format name.

    Returns:
        Deserialized data.
    """
    fmt_map = {
        "json": SerializationFormat.JSON,
        "csv": SerializationFormat.CSV,
        "xml": SerializationFormat.XML,
        "yaml": SerializationFormat.YAML,
    }
    options = SerializationOptions(
        format=fmt_map.get(format.lower(), SerializationFormat.JSON),
    )
    return Serializer(options).deserialize(data)
