"""
Data Marshaling and Encoding Module.

Handles data serialization to/from various formats (JSON, XML, YAML,
Protobuf, MessagePack, Pickle). Supports schema validation and
data transformation during marshaling.

Author: AutoGen
"""
from __future__ import annotations

import base64
import json
import logging
import pickle
import xml.etree.ElementTree as ET
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, date, time
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple, Type, Union
import yaml

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    JSON = auto()
    YAML = auto()
    XML = auto()
    MSGPACK = auto()
    PROTOBUF = auto()
    PICKLE = auto()
    CBOR = auto()
    UBJSON = auto()


@dataclass
class MarshalOptions:
    """Options for data marshaling."""
    format: SerializationFormat = SerializationFormat.JSON
    indent: Optional[int] = 2
    include_metadata: bool = False
    validate_schema: bool = False
    datetime_format: str = "iso"
    bytes_encoding: str = "base64"
    strict_types: bool = False
    enum_style: str = "name"


@dataclass
class SchemaDefinition:
    """Schema for data validation during marshaling."""
    type_name: str
    fields: Dict[str, Tuple[str, bool, Any]] = field(default_factory=dict)
    required_fields: FrozenSet[str] = field(default_factory=frozenset)
    field_types: Dict[str, Type] = field(default_factory=dict)


class DataMarshalError(Exception):
    """Error during data marshaling/unmarshaling."""
    pass


class JSONMarshaler:
    """JSON serialization with enhanced type support."""

    def __init__(self, options: MarshalOptions):
        self.options = options
        self._datetime_handlers: Dict[str, Callable[[Any], str]] = {
            "iso": lambda dt: dt.isoformat() if hasattr(dt, "isoformat") else str(dt),
            "unix": lambda dt: str(int(dt.timestamp())) if hasattr(dt, "timestamp") else str(dt),
            "rfc3339": lambda dt: dt.isoformat() + "Z" if hasattr(dt, "isoformat") else str(dt),
        }

    def marshal(self, data: Any) -> bytes:
        dt_handler = self._datetime_handlers.get(
            self.options.datetime_format, self._datetime_handlers["iso"]
        )

        def default_serializer(obj: Any) -> Any:
            if isinstance(obj, (datetime, date, time)):
                return dt_handler(obj)
            if isinstance(obj, bytes):
                if self.options.bytes_encoding == "base64":
                    return base64.b64encode(obj).decode()
                return obj.decode("utf-8", errors="replace")
            if isinstance(obj, set):
                return list(obj)
            if isinstance(obj, Enum):
                if self.options.enum_style == "name":
                    return obj.name
                return obj.value
            if hasattr(obj, "__dataclass_fields__"):
                return self._dataclass_to_dict(obj, default_serializer)
            return str(obj)

        try:
            content = json.dumps(
                data,
                indent=self.options.indent,
                default=default_serializer,
                ensure_ascii=False,
            )
            return content.encode("utf-8")
        except Exception as exc:
            raise DataMarshalError(f"JSON marshal failed: {exc}") from exc

    def unmarshal(self, data: bytes) -> Any:
        try:
            obj = json.loads(data.decode("utf-8"))
            if self.options.include_metadata:
                obj = self._inject_metadata(obj)
            return obj
        except Exception as exc:
            raise DataMarshalError(f"JSON unmarshal failed: {exc}") from exc

    def _dataclass_to_dict(self, obj: Any, serializer: Callable) -> Dict:
        result = {}
        for name, field_def in obj.__dataclass_fields__.items():
            value = getattr(obj, name)
            result[name] = serializer(value)
        return result

    def _inject_metadata(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            obj["_marshal_timestamp"] = datetime.utcnow().isoformat()
            obj["_marshal_format"] = "json"
        return obj


class XMLMarshaler:
    """XML serialization with element and attribute support."""

    ROOT_TAG = "data"
    ITEM_TAG = "item"
    TYPE_ATTR = "type"

    def __init__(self, options: MarshalOptions):
        self.options = options

    def marshal(self, data: Any) -> bytes:
        root = self._build_element(self.ROOT_TAG, data)
        return ET.tostring(root, encoding="utf-8", xml_declaration=True)

    def unmarshal(self, data: bytes) -> Any:
        try:
            root = ET.fromstring(data)
            return self._parse_element(root)
        except Exception as exc:
            raise DataMarshalError(f"XML unmarshal failed: {exc}") from exc

    def _build_element(self, tag: str, value: Any) -> ET.Element:
        elem = ET.Element(tag)

        if isinstance(value, dict):
            for k, v in value.items():
                child = self._build_element(str(k), v)
                elem.append(child)
        elif isinstance(value, (list, tuple)):
            for item in value:
                child = self._build_element(self.ITEM_TAG, item)
                elem.append(child)
        elif isinstance(value, Enum):
            elem.set(self.TYPE_ATTR, "enum")
            elem.text = value.name
        elif isinstance(value, (int, float, str, bool)):
            if value is not None:
                elem.text = str(value)
                if isinstance(value, bool):
                    elem.set(self.TYPE_ATTR, "bool")
                elif isinstance(value, int):
                    elem.set(self.TYPE_ATTR, "int")
                elif isinstance(value, float):
                    elem.set(self.TYPE_ATTR, "float")
        elif value is None:
            elem.set(self.TYPE_ATTR, "null")
        else:
            elem.text = str(value)

        return elem

    def _parse_element(self, elem: ET.Element) -> Any:
        if len(elem) == 0:
            text = elem.text.strip() if elem.text else ""
            type_attr = elem.get(self.TYPE_ATTR, "str")

            if type_attr == "null":
                return None
            elif type_attr == "bool":
                return text.lower() in ("true", "1")
            elif type_attr == "int":
                return int(text) if text else 0
            elif type_attr == "float":
                return float(text) if text else 0.0
            elif type_attr == "enum":
                return text
            return text

        children_by_tag: Dict[str, List[Any]] = {}
        for child in elem:
            tag = child.tag
            if tag not in children_by_tag:
                children_by_tag[tag] = []
            children_by_tag[tag].append(self._parse_element(child))

        if len(children_by_tag) == 1 and self.ITEM_TAG in children_by_tag:
            return children_by_tag[self.ITEM_TAG]

        return dict(children_by_tag)


class YAMLMarshaler:
    """YAML serialization."""

    def __init__(self, options: MarshalOptions):
        self.options = options

    def marshal(self, data: Any) -> bytes:
        try:
            content = yaml.dump(
                data,
                indent=self.options.indent or 2,
                default_flow_style=False,
                allow_unicode=True,
            )
            return content.encode("utf-8")
        except Exception as exc:
            raise DataMarshalError(f"YAML marshal failed: {exc}") from exc

    def unmarshal(self, data: bytes) -> Any:
        try:
            return yaml.safe_load(data.decode("utf-8"))
        except Exception as exc:
            raise DataMarshalError(f"YAML unmarshal failed: {exc}") from exc


class MsgPackMarshaler:
    """MessagePack binary serialization."""

    def __init__(self, options: MarshalOptions):
        self.options = options

    def marshal(self, data: Any) -> bytes:
        try:
            import msgpack
            return msgpack.packb(data, use_bin_type=True)
        except ImportError:
            raise DataMarshalError("msgpack not installed")
        except Exception as exc:
            raise DataMarshalError(f"MsgPack marshal failed: {exc}") from exc

    def unmarshal(self, data: bytes) -> Any:
        try:
            import msgpack
            return msgpack.unpackb(data, raw=False)
        except ImportError:
            raise DataMarshalError("msgpack not installed")
        except Exception as exc:
            raise DataMarshalError(f"MsgPack unmarshal failed: {exc}") from exc


class PickleMarshaler:
    """Python pickle serialization (not for untrusted data)."""

    def __init__(self, options: MarshalOptions):
        self.options = options

    def marshal(self, data: Any) -> bytes:
        try:
            return pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            raise DataMarshalError(f"Pickle marshal failed: {exc}") from exc

    def unmarshal(self, data: bytes) -> Any:
        try:
            return pickle.loads(data)
        except Exception as exc:
            raise DataMarshalError(f"Pickle unmarshal failed: {exc}") from exc


class DataMarshaler:
    """
    Unified data marshaling interface supporting multiple formats.
    """

    MARSHALERS: Dict[SerializationFormat, Type] = {
        SerializationFormat.JSON: JSONMarshaler,
        SerializationFormat.XML: XMLMarshaler,
        SerializationFormat.YAML: YAMLMarshaler,
        SerializationFormat.MSGPACK: MsgPackMarshaler,
        SerializationFormat.PICKLE: PickleMarshaler,
    }

    def __init__(self, options: Optional[MarshalOptions] = None):
        self.options = options or MarshalOptions()
        self._current_marshaler: Optional[Any] = None
        self._update_marshaler()

    def set_format(self, fmt: SerializationFormat) -> None:
        self.options.format = fmt
        self._update_marshaler()

    def _update_marshaler(self) -> None:
        marshaler_cls = self.MARSHALERS.get(self.options.format)
        if marshaler_cls:
            self._current_marshaler = marshaler_cls(self.options)

    def marshal(self, data: Any) -> bytes:
        if self._current_marshaler is None:
            raise DataMarshalError("No marshaler configured")
        return self._current_marshaler.marshal(data)

    def unmarshal(self, data: bytes) -> Any:
        if self._current_marshaler is None:
            raise DataMarshalError("No marshaler configured")
        return self._current_marshaler.unmarshal(data)

    def marshal_to_string(self, data: Any) -> str:
        return self.marshal(data).decode("utf-8")

    def unmarshal_from_string(self, data: str) -> Any:
        return self.unmarshal(data.encode("utf-8"))

    def convert(
        self, data: bytes, from_format: SerializationFormat, to_format: SerializationFormat
    ) -> bytes:
        """Convert data from one format to another."""
        old_format = self.options.format
        try:
            self.set_format(from_format)
            obj = self.unmarshal(data)
            self.set_format(to_format)
            return self.marshal(obj)
        finally:
            self.set_format(old_format)

    def validate_against_schema(
        self, data: Any, schema: SchemaDefinition
    ) -> Tuple[bool, List[str]]:
        """Validate data against a schema definition."""
        errors: List[str] = []

        for field_name in schema.required_fields:
            if field_name not in data:
                errors.append(f"Missing required field: {field_name}")

        for field_name, (expected_type, required, default) in schema.fields.items():
            if field_name not in data:
                if required:
                    errors.append(f"Missing required field: {field_name}")
                continue

            value = data[field_name]
            if value is None:
                continue

            if not isinstance(value, schema.field_types.get(field_name, str)):
                errors.append(
                    f"Field '{field_name}' has wrong type: "
                    f"expected {schema.field_types.get(field_name, str).__name__}, "
                    f"got {type(value).__name__}"
                )

        return (len(errors) == 0, errors)
