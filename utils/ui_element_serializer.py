"""
UI Element Serializer.

Serialize and deserialize UI element trees to/from JSON, XML,
or other formats for logging, debugging, or cross-process communication.

Usage:
    from utils.ui_element_serializer import ElementSerializer, deserialize

    tree = bridge.build_accessibility_tree(app)
    serialized = ElementSerializer.to_json(tree)
    restored = deserialize(serialized)
"""

from __future__ import annotations

import json
import base64
import zlib
from typing import Optional, Dict, Any, List, Union, Iterator
from dataclasses import dataclass, field
from enum import Enum, auto

try:
    import lxml.etree as ET
    HAS_LXML = True
except ImportError:
    HAS_LXML = False


class SerializationFormat(Enum):
    """Output format for serialization."""
    JSON = auto()
    COMPACT_JSON = auto()
    XML = auto()
    SUMMARY = auto()


@dataclass
class SerializationOptions:
    """Options for serialization."""
    format: SerializationFormat = SerializationFormat.JSON
    include_empty: bool = False
    max_depth: Optional[int] = None
    redact_sensitive: bool = False
    compress: bool = False
    attribute_blacklist: List[str] = field(default_factory=lambda: ["password", "ssn"])


class ElementSerializer:
    """
    Serialize UI element trees to portable formats.

    Supports JSON (readable and compact), XML, and summary formats.
    Can optionally compress output with zlib.

    Example:
        serializer = ElementSerializer(format=SerializationFormat.COMPACT_JSON)
        output = serializer.serialize(tree)
        print(output)
    """

    def __init__(
        self,
        format: SerializationFormat = SerializationFormat.JSON,
        include_empty: bool = False,
        max_depth: Optional[int] = None,
    ) -> None:
        """
        Initialize the serializer.

        Args:
            format: Output format (JSON, COMPACT_JSON, XML, SUMMARY).
            include_empty: Whether to include empty/null fields.
            max_depth: Maximum tree depth to serialize (None = unlimited).
        """
        self._format = format
        self._include_empty = include_empty
        self._max_depth = max_depth
        self._blacklist: List[str] = ["password", "ssn", "credit_card"]

    def serialize(
        self,
        element: Dict[str, Any],
        options: Optional[SerializationOptions] = None,
    ) -> str:
        """
        Serialize a UI element tree.

        Args:
            element: Element dictionary to serialize.
            options: Optional serialization options.

        Returns:
            Serialized string in the configured format.
        """
        opts = options or SerializationOptions(format=self._format)
        cleaned = self._clean_element(element, depth=0, opts=opts)

        if opts.format == SerializationFormat.JSON:
            return json.dumps(cleaned, indent=2, ensure_ascii=False)
        elif opts.format == SerializationFormat.COMPACT_JSON:
            return json.dumps(cleaned, ensure_ascii=False, separators=(",", ":"))
        elif opts.format == SerializationFormat.XML:
            return self._to_xml(cleaned)
        elif opts.format == SerializationFormat.SUMMARY:
            return self._to_summary(cleaned)
        else:
            return json.dumps(cleaned, indent=2, ensure_ascii=False)

    def _clean_element(
        self,
        element: Dict[str, Any],
        depth: int,
        opts: SerializationOptions,
    ) -> Any:
        """Recursively clean and filter element data."""
        if self._max_depth is not None and depth > self._max_depth:
            return {"__truncated__": True}

        if not isinstance(element, dict):
            return element

        result: Dict[str, Any] = {}
        for key, value in element.items():
            if key in opts.attribute_blacklist:
                result[key] = "[REDACTED]"
                continue

            if value is None and not opts.include_empty:
                continue

            if isinstance(value, dict):
                result[key] = self._clean_element(value, depth + 1, opts)
            elif isinstance(value, list):
                result[key] = [
                    self._clean_element(v, depth + 1, opts)
                    if isinstance(v, dict) else v
                    for v in value
                    if opts.include_empty or v is not None
                ]
            else:
                result[key] = value

        return result

    def _to_xml(self, element: Dict[str, Any], root: str = "element") -> str:
        """Convert element dictionary to XML string."""
        if not HAS_LXML:
            return self._to_xml_fallback(element, root)

        def dict_to_xml(d: Dict[str, Any], parent: ET.Element) -> None:
            for key, value in d.items():
                if isinstance(value, dict):
                    child = ET.SubElement(parent, str(key))
                    dict_to_xml(value, child)
                elif isinstance(value, list):
                    for item in value:
                        child = ET.SubElement(parent, str(key))
                        if isinstance(item, dict):
                            dict_to_xml(item, child)
                        else:
                            child.text = str(item)
                else:
                    child = ET.SubElement(parent, str(key))
                    child.text = str(value) if value is not None else ""

        root_elem = ET.Element(root)
        dict_to_xml(element, root_elem)
        return ET.tostring(root_elem, pretty_print=True, encoding="unicode")

    def _to_xml_fallback(self, element: Dict[str, Any], root: str = "element") -> str:
        """Fallback XML serializer without lxml."""
        lines = [f"<{root}>"]
        for key, value in element.items():
            if isinstance(value, dict):
                lines.append(f"  <{key}>")
                for k, v in value.items():
                    lines.append(f"    <{k}>{v}</{k}>")
                lines.append(f"  </{key}>")
            elif isinstance(value, list):
                for item in value:
                    lines.append(f"  <{key}>{item}</{key}>")
            else:
                lines.append(f"  <{key}>{value}</{key}>")
        lines.append(f"</{root}>")
        return "\n".join(lines)

    def _to_summary(self, element: Dict[str, Any]) -> str:
        """Create a one-line summary of the element."""
        parts = []
        for key in ["role", "title", "value", "description"]:
            val = element.get(key)
            if val:
                parts.append(f"{key}={val!r}")
        return f"Element({' '.join(parts)})"

    def to_json(element: Dict[str, Any]) -> str:
        """Class method to quickly serialize to JSON."""
        serializer = ElementSerializer(format=SerializationFormat.JSON)
        return serializer.serialize(element)

    def to_compact_json(element: Dict[str, Any]) -> str:
        """Class method to quickly serialize to compact JSON."""
        serializer = ElementSerializer(format=SerializationFormat.COMPACT_JSON)
        return serializer.serialize(element)


def deserialize(
    data: str,
    format: Optional[SerializationFormat] = None,
) -> Dict[str, Any]:
    """
    Deserialize a serialized element back to a dictionary.

    Args:
        data: Serialized string (JSON or XML).
        format: Optional format hint (auto-detected if None).

    Returns:
        Element dictionary.
    """
    if format == SerializationFormat.XML:
        return _deserialize_xml(data)
    else:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            try:
                return _deserialize_xml(data)
            except Exception as e:
                raise ValueError(f"Cannot deserialize data: {e}")


def _deserialize_xml(data: str) -> Dict[str, Any]:
    """Deserialize XML string to dictionary."""
    if HAS_LXML:
        root = ET.fromstring(data)
        return _xml_to_dict(root)
    else:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(data)
        return _xml_to_dict_simple(root)


def _xml_to_dict(element: Any) -> Dict[str, Any]:
    """Convert XML element to dictionary."""
    result: Dict[str, Any] = {}
    for attr in element.attrib:
        result[f"@{attr}"] = element.attrib[attr]

    if element.text and element.text.strip():
        if len(element) == 0:
            return element.text.strip()
        result["#text"] = element.text.strip()

    for child in element:
        child_data = _xml_to_dict(child)
        tag = child.tag
        if tag in result:
            if not isinstance(result[tag], list):
                result[tag] = [result[tag]]
            result[tag].append(child_data)
        else:
            result[tag] = child_data

    return result


def _xml_to_dict_simple(element: Any) -> Dict[str, Any]:
    """Simpler XML to dict without lxml."""
    result = {}
    if element.text and element.text.strip():
        return element.text.strip()
    for child in element:
        child_data = _xml_to_dict_simple(child)
        if child.tag in result:
            existing = result[child.tag]
            if isinstance(existing, list):
                existing.append(child_data)
            else:
                result[child.tag] = [existing, child_data]
        else:
            result[child.tag] = child_data
    return result


def compress_serialized(data: str) -> str:
    """
    Compress serialized data with zlib + base64.

    Args:
        data: JSON or other string to compress.

    Returns:
        Base64-encoded compressed string.
    """
    compressed = zlib.compress(data.encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


def decompress_serialized(data: str) -> str:
    """
    Decompress data compressed with compress_serialized.

    Args:
        data: Base64-encoded compressed string.

    Returns:
        Decompressed string.
    """
    compressed = base64.b64decode(data.encode("ascii"))
    return zlib.decompress(compressed).decode("utf-8")
