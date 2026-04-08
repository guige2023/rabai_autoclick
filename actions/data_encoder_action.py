"""Data Encoder Action Module.

Provides data encoding/decoding for various formats:
Base64, URL encoding, JSON pointers, and custom encoders.
"""
from __future__ import annotations

import base64
import json
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class EncodingFormat(Enum):
    """Encoding format."""
    BASE64 = "base64"
    URL = "url"
    JSON_POINTER = "json_pointer"
    CSV = "csv"
    HEX = "hex"
    CUSTOM = "custom"


@dataclass
class EncoderConfig:
    """Encoder configuration."""
    format: EncodingFormat
    custom_encoder: Optional[Callable[[Any], str]] = None
    custom_decoder: Optional[Callable[[str], Any]] = None


class DataEncoderAction:
    """Multi-format data encoder.

    Example:
        encoder = DataEncoderAction()

        encoded = encoder.encode({"data": "value"}, EncodingFormat.BASE64)
        decoded = encoder.decode(encoded, EncodingFormat.BASE64)

        pointer = encoder.create_pointer("/data")
    """

    def __init__(self) -> None:
        self._encoders: Dict[EncodingFormat, Callable] = {}
        self._decoders: Dict[EncodingFormat, Callable] = {}
        self._register_default_encoders()

    def _register_default_encoders(self) -> None:
        """Register default encoders/decoders."""
        self._encoders[EncodingFormat.BASE64] = self._encode_base64
        self._decoders[EncodingFormat.BASE64] = self._decode_base64

        self._encoders[EncodingFormat.URL] = self._encode_url
        self._decoders[EncodingFormat.URL] = self._decode_url

        self._encoders[EncodingFormat.HEX] = self._encode_hex
        self._decoders[EncodingFormat.HEX] = self._decode_hex

    def encode(
        self,
        data: Any,
        format: EncodingFormat,
    ) -> str:
        """Encode data.

        Args:
            data: Data to encode
            format: Target encoding format

        Returns:
            Encoded string
        """
        if format in self._encoders:
            return self._encoders[format](data)

        if format == EncodingFormat.JSON_POINTER:
            return self._encode_json_pointer(data)

        if format == EncodingFormat.CSV:
            return self._encode_csv(data)

        raise ValueError(f"No encoder for format: {format}")

    def decode(
        self,
        data: str,
        format: EncodingFormat,
    ) -> Any:
        """Decode data.

        Args:
            data: Encoded data
            format: Source encoding format

        Returns:
            Decoded data
        """
        if format in self._decoders:
            return self._decoders[format](data)

        if format == EncodingFormat.JSON_POINTER:
            return self._decode_json_pointer(data)

        if format == EncodingFormat.CSV:
            return self._decode_csv(data)

        raise ValueError(f"No decoder for format: {format}")

    def _encode_base64(self, data: Any) -> str:
        """Encode to Base64."""
        if isinstance(data, dict):
            data = json.dumps(data)
        if isinstance(data, str):
            data = data.encode("utf-8")
        return base64.b64encode(data).decode("ascii")

    def _decode_base64(self, data: str) -> Any:
        """Decode from Base64."""
        decoded = base64.b64decode(data.encode("ascii"))
        try:
            return json.loads(decoded.decode("utf-8"))
        except:
            return decoded.decode("utf-8")

    def _encode_url(self, data: Any) -> str:
        """URL encode."""
        if isinstance(data, dict):
            return urllib.parse.urlencode(data)
        return urllib.parse.quote(str(data))

    def _decode_url(self, data: str) -> Dict[str, str]:
        """URL decode."""
        return dict(urllib.parse.parse_qsl(data))

    def _encode_hex(self, data: Any) -> str:
        """Encode to hex."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        elif isinstance(data, int):
            return hex(data)
        return data.hex()

    def _decode_hex(self, data: str) -> bytes:
        """Decode from hex."""
        return bytes.fromhex(data)

    def _encode_json_pointer(self, data: Any) -> str:
        """Encode as JSON pointer."""
        return json.dumps(data)

    def _decode_json_pointer(self, data: str) -> Any:
        """Decode JSON pointer."""
        return json.loads(data)

    def _encode_csv(self, data: Any) -> str:
        """Encode as CSV."""
        if isinstance(data, list):
            return ",".join(str(v) for v in data)
        elif isinstance(data, dict):
            return ",".join(str(v) for v in data.values())
        return str(data)

    def _decode_csv(self, data: str) -> List[str]:
        """Decode CSV."""
        return data.split(",")

    def create_pointer(self, path: str, data: Any) -> str:
        """Create JSON pointer reference.

        Args:
            path: JSON pointer path
            data: Data to encode

        Returns:
            JSON string with $ref
        """
        return json.dumps({
            "$ref": path,
            "value": data,
        })

    def parse_pointer(self, json_str: str) -> Tuple[str, Any]:
        """Parse JSON pointer reference.

        Args:
            json_str: JSON string with $ref

        Returns:
            Tuple of (path, value)
        """
        obj = json.loads(json_str)
        return obj.get("$ref", ""), obj.get("value")

    def extract_pointer(self, data: Any, pointer: str) -> Any:
        """Extract value using JSON pointer.

        Args:
            data: JSON data
            pointer: JSON pointer (e.g., "/foo/bar")

        Returns:
            Extracted value
        """
        if not pointer.startswith("/"):
            return data

        parts = pointer.split("/")
        parts = [p.replace("~1", "/").replace("~0", "~") for p in parts[1:]]

        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None

        return current
