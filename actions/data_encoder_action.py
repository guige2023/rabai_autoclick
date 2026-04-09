"""
Data Encoder Module.

Provides multi-format data encoding, decoding, and transformation
for API request/response handling.
"""

from __future__ import annotations

import base64
import json
import pickle
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EncodingFormat(Enum):
    """Supported encoding formats."""
    JSON = "json"
    XML = "xml"
    YAML = "yaml"
    MSGPACK = "msgpack"
    PROTOBUF = "protobuf"
    AVRO = "avro"
    BASE64 = "base64"
    URL_ENCODED = "url_encoded"
    MULTIPART = "multipart"
    PICKLE = "pickle"
    UBJSON = "ubjson"


@dataclass
class EncodingConfig:
    """Configuration for encoding operations."""
    format: EncodingFormat = EncodingFormat.JSON
    indent: Optional[int] = None
    ensure_ascii: bool = False
    url_safe: bool = True
    strict: bool = True
    schema: Optional[Dict[str, Any]] = None


@dataclass
class EncodedData:
    """Container for encoded data."""
    format: EncodingFormat
    data: Union[bytes, str]
    content_type: str
    encoding: str = "utf-8"
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataEncoder:
    """
    Multi-format data encoder/decoder.
    
    Example:
        encoder = DataEncoder(EncodingConfig(format=EncodingFormat.JSON))
        
        # Encode
        encoded = encoder.encode({"key": "value"})
        
        # Decode
        decoded = encoder.decode(encoded)
    """
    
    def __init__(self, config: Optional[EncodingConfig] = None) -> None:
        """
        Initialize the encoder.
        
        Args:
            config: Encoding configuration.
        """
        self.config = config or EncodingConfig()
        self._content_types = {
            EncodingFormat.JSON: "application/json",
            EncodingFormat.XML: "application/xml",
            EncodingFormat.YAML: "application/x-yaml",
            EncodingFormat.MSGPACK: "application/msgpack",
            EncodingFormat.BASE64: "text/plain",
            EncodingFormat.URL_ENCODED: "application/x-www-form-urlencoded",
            EncodingFormat.PICKLE: "application/octet-stream",
        }
        
    def encode(self, data: Any) -> EncodedData:
        """
        Encode data to specified format.
        
        Args:
            data: Data to encode.
            
        Returns:
            EncodedData container.
        """
        if self.config.format == EncodingFormat.JSON:
            return self._encode_json(data)
        elif self.config.format == EncodingFormat.XML:
            return self._encode_xml(data)
        elif self.config.format == EncodingFormat.YAML:
            return self._encode_yaml(data)
        elif self.config.format == EncodingFormat.BASE64:
            return self._encode_base64(data)
        elif self.config.format == EncodingFormat.URL_ENCODED:
            return self._encode_url(data)
        elif self.config.format == EncodingFormat.PICKLE:
            return self._encode_pickle(data)
        else:
            return self._encode_json(data)
            
    def decode(self, encoded: EncodedData) -> Any:
        """
        Decode data from EncodedData container.
        
        Args:
            encoded: EncodedData to decode.
            
        Returns:
            Decoded data.
        """
        if encoded.format == EncodingFormat.JSON:
            return self._decode_json(encoded)
        elif encoded.format == EncodingFormat.XML:
            return self._decode_xml(encoded)
        elif encoded.format == EncodingFormat.YAML:
            return self._decode_yaml(encoded)
        elif encoded.format == EncodingFormat.BASE64:
            return self._decode_base64(encoded)
        elif encoded.format == EncodingFormat.URL_ENCODED:
            return self._decode_url(encoded)
        elif encoded.format == EncodingFormat.PICKLE:
            return self._decode_pickle(encoded)
        else:
            return self._decode_json(encoded)
            
    def encode_to_string(self, data: Any) -> str:
        """Encode data to string format."""
        encoded = self.encode(data)
        if isinstance(encoded.data, bytes):
            return encoded.data.decode(encoded.encoding)
        return encoded.data
        
    def decode_from_string(self, data: str, format: Optional[EncodingFormat] = None) -> Any:
        """Decode data from string format."""
        fmt = format or self.config.format
        encoded_data = EncodedData(
            format=fmt,
            data=data,
            content_type=self._content_types.get(fmt, "text/plain"),
        )
        return self.decode(encoded_data)
        
    def _encode_json(self, data: Any) -> EncodedData:
        """Encode to JSON."""
        try:
            json_str = json.dumps(
                data,
                indent=self.config.indent,
                ensure_ascii=self.config.ensure_ascii,
                default=str,
            )
            return EncodedData(
                format=EncodingFormat.JSON,
                data=json_str.encode(self.config.encoding) if self.config.indent else json_str,
                content_type=self._content_types[EncodingFormat.JSON],
                encoding=self.config.encoding,
            )
        except Exception as e:
            logger.error(f"JSON encoding failed: {e}")
            raise ValueError(f"JSON encoding failed: {e}")
            
    def _decode_json(self, encoded: EncodedData) -> Any:
        """Decode from JSON."""
        try:
            data = encoded.data
            if isinstance(data, bytes):
                data = data.decode(encoded.encoding)
            return json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding failed: {e}")
            raise ValueError(f"JSON decoding failed: {e}")
            
    def _encode_xml(self, data: Any) -> EncodedData:
        """Encode to XML."""
        try:
            if isinstance(data, dict):
                root = self._dict_to_xml("root", data)
            else:
                root = ET.Element("root")
                root.text = str(data)
                
            xml_str = ET.tostring(root, encoding=self.config.encoding, xml_declaration=True)
            
            return EncodedData(
                format=EncodingFormat.XML,
                data=xml_str,
                content_type=self._content_types[EncodingFormat.XML],
                encoding=self.config.encoding,
            )
        except Exception as e:
            logger.error(f"XML encoding failed: {e}")
            raise ValueError(f"XML encoding failed: {e}")
            
    def _dict_to_xml(self, tag: str, data: Union[dict, list, Any]) -> ET.Element:
        """Convert dictionary to XML element."""
        element = ET.Element(tag)
        
        if isinstance(data, dict):
            for key, value in data.items():
                child = self._dict_to_xml(key, value)
                element.append(child)
        elif isinstance(data, list):
            for item in data:
                child = self._dict_to_xml("item", item)
                element.append(child)
        else:
            element.text = str(data)
            
        return element
        
    def _decode_xml(self, encoded: EncodedData) -> Any:
        """Decode from XML."""
        try:
            data = encoded.data
            if isinstance(data, bytes):
                data = data.decode(encoded.encoding)
                
            root = ET.fromstring(data)
            return self._xml_to_dict(root)
        except ET.ParseError as e:
            logger.error(f"XML decoding failed: {e}")
            raise ValueError(f"XML decoding failed: {e}")
            
    def _xml_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dictionary."""
        result: Dict[str, Any] = {}
        
        for child in element:
            child_data = self._xml_to_dict(child)
            
            if child.tag in result:
                # Multiple children with same tag -> list
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
                
        if not result and element.text:
            return element.text
            
        return result
        
    def _encode_yaml(self, data: Any) -> EncodedData:
        """Encode to YAML."""
        try:
            import yaml
            
            yaml_str = yaml.dump(
                data,
                indent=self.config.indent or 2,
                default_flow_style=False,
                allow_unicode=not self.config.ensure_ascii,
            )
            
            return EncodedData(
                format=EncodingFormat.YAML,
                data=yaml_str,
                content_type=self._content_types[EncodingFormat.YAML],
                encoding=self.config.encoding,
            )
        except ImportError:
            logger.warning("YAML library not installed, using JSON fallback")
            return self._encode_json(data)
        except Exception as e:
            logger.error(f"YAML encoding failed: {e}")
            raise ValueError(f"YAML encoding failed: {e}")
            
    def _decode_yaml(self, encoded: EncodedData) -> Any:
        """Decode from YAML."""
        try:
            import yaml
            
            data = encoded.data
            if isinstance(data, bytes):
                data = data.decode(encoded.encoding)
                
            return yaml.safe_load(data)
        except ImportError:
            logger.warning("YAML library not installed")
            raise ValueError("YAML library not installed")
        except Exception as e:
            logger.error(f"YAML decoding failed: {e}")
            raise ValueError(f"YAML decoding failed: {e}")
            
    def _encode_base64(self, data: Any) -> EncodedData:
        """Encode to Base64."""
        try:
            json_data = json.dumps(data, default=str)
            encoded = base64.urlsafe_b64encode(json_data.encode(self.config.encoding))
            
            return EncodedData(
                format=EncodingFormat.BASE64,
                data=encoded,
                content_type=self._content_types[EncodingFormat.BASE64],
                encoding=self.config.encoding,
            )
        except Exception as e:
            logger.error(f"Base64 encoding failed: {e}")
            raise ValueError(f"Base64 encoding failed: {e}")
            
    def _decode_base64(self, encoded: EncodedData) -> Any:
        """Decode from Base64."""
        try:
            data = encoded.data
            if isinstance(data, str):
                data = data.encode("utf-8")
                
            decoded = base64.urlsafe_b64decode(data)
            return json.loads(decoded.decode(self.config.encoding))
        except Exception as e:
            logger.error(f"Base64 decoding failed: {e}")
            raise ValueError(f"Base64 decoding failed: {e}")
            
    def _encode_url(self, data: Any) -> EncodedData:
        """Encode to URL-safe format."""
        try:
            from urllib.parse import urlencode
            
            if isinstance(data, dict):
                encoded_str = urlencode(data, safe="")
            else:
                encoded_str = str(data)
                
            return EncodedData(
                format=EncodingFormat.URL_ENCODED,
                data=encoded_str,
                content_type=self._content_types[EncodingFormat.URL_ENCODED],
                encoding=self.config.encoding,
            )
        except Exception as e:
            logger.error(f"URL encoding failed: {e}")
            raise ValueError(f"URL encoding failed: {e}")
            
    def _decode_url(self, encoded: EncodedData) -> Any:
        """Decode from URL-safe format."""
        try:
            from urllib.parse import parse_qs
            
            data = encoded.data
            if isinstance(data, bytes):
                data = data.decode(encoded.encoding)
                
            parsed = parse_qs(data)
            # Unwrap single-element lists
            return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
        except Exception as e:
            logger.error(f"URL decoding failed: {e}")
            raise ValueError(f"URL decoding failed: {e}")
            
    def _encode_pickle(self, data: Any) -> EncodedData:
        """Encode to Pickle."""
        try:
            encoded = pickle.dumps(data)
            
            return EncodedData(
                format=EncodingFormat.PICKLE,
                data=encoded,
                content_type=self._content_types[EncodingFormat.PICKLE],
                encoding=self.config.encoding,
            )
        except Exception as e:
            logger.error(f"Pickle encoding failed: {e}")
            raise ValueError(f"Pickle encoding failed: {e}")
            
    def _decode_pickle(self, encoded: EncodedData) -> Any:
        """Decode from Pickle."""
        try:
            data = encoded.data
            if isinstance(data, str):
                data = data.encode("latin-1")
                
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Pickle decoding failed: {e}")
            raise ValueError(f"Pickle decoding failed: {e}")
            
    def get_content_type(self, format: Optional[EncodingFormat] = None) -> str:
        """Get content type for format."""
        fmt = format or self.config.format
        return self._content_types.get(fmt, "application/octet-stream")


class SchemaEncoder:
    """
    Schema-based encoder with validation.
    
    Example:
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"},
            },
            "required": ["name"]
        }
        
        encoder = SchemaEncoder(schema)
        encoded = encoder.encode({"name": "John", "age": 30}, format=EncodingFormat.JSON)
    """
    
    def __init__(self, schema: Dict[str, Any]) -> None:
        """
        Initialize schema encoder.
        
        Args:
            schema: JSON Schema definition.
        """
        self.schema = schema
        
    def encode(self, data: Any, format: EncodingFormat = EncodingFormat.JSON) -> EncodedData:
        """
        Encode data with schema validation.
        
        Args:
            data: Data to encode.
            format: Target format.
            
        Returns:
            EncodedData container.
        """
        # Validate against schema
        errors = self._validate(data, self.schema)
        if errors:
            raise ValueError(f"Schema validation failed: {errors}")
            
        encoder = DataEncoder(EncodingConfig(format=format))
        return encoder.encode(data)
        
    def _validate(self, data: Any, schema: Dict[str, Any]) -> List[str]:
        """Validate data against schema."""
        errors = []
        
        schema_type = schema.get("type")
        
        if schema_type == "object":
            if not isinstance(data, dict):
                errors.append("Expected object")
                return errors
                
            required = schema.get("required", [])
            for field in required:
                if field not in data:
                    errors.append(f"Required field missing: {field}")
                    
            properties = schema.get("properties", {})
            for field, value in data.items():
                if field in properties:
                    field_errors = self._validate(value, properties[field])
                    errors.extend(f"{field}.{e}" for e in field_errors)
                    
        elif schema_type == "array":
            if not isinstance(data, list):
                errors.append("Expected array")
            else:
                items_schema = schema.get("items", {})
                for i, item in enumerate(data):
                    item_errors = self._validate(item, items_schema)
                    errors.extend(f"[{i}].{e}" for e in item_errors)
                    
        elif schema_type == "string":
            if not isinstance(data, str):
                errors.append("Expected string")
            else:
                if "pattern" in schema:
                    import re
                    if not re.match(schema["pattern"], data):
                        errors.append(f"Does not match pattern: {schema['pattern']}")
                if "minLength" in schema and len(data) < schema["minLength"]:
                    errors.append(f"String too short (min: {schema['minLength']})")
                if "maxLength" in schema and len(data) > schema["maxLength"]:
                    errors.append(f"String too long (max: {schema['maxLength']})")
                    
        elif schema_type == "integer" or schema_type == "number":
            if not isinstance(data, (int, float)) or isinstance(data, bool):
                errors.append(f"Expected number, got {type(data).__name__}")
            else:
                if "minimum" in schema and data < schema["minimum"]:
                    errors.append(f"Number too small (min: {schema['minimum']})")
                if "maximum" in schema and data > schema["maximum"]:
                    errors.append(f"Number too large (max: {schema['maximum']})")
                    
        elif schema_type == "boolean":
            if not isinstance(data, bool):
                errors.append("Expected boolean")
                
        elif schema_type == "null":
            if data is not None:
                errors.append("Expected null")
                
        return errors
