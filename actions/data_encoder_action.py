"""Data Encoder Action Module.

Provides encoding, decoding, and serialization for various data formats
including JSON, base64, URL encoding, hex, and custom encodings.
"""

from __future__ import annotations

import base64
import json
import logging
import quopri
import urllib.parse
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class EncodingType(Enum):
    """Supported encoding types."""
    BASE64 = "base64"
    BASE64_URL_SAFE = "base64_url_safe"
    BASE16 = "hex"
    BASE32 = "base32"
    BASE85 = "base85"
    URL = "url"
    URL_COMPONENT = "url_component"
    HTML = "html"
    JSON = "json"
    XML = "xml"
    QUOTED_PRINTABLE = "quoted_printable"
    MIME = "mime"
    UTF8 = "utf8"
    ASCII = "ascii"
    UNICODE_ESCAPE = "unicode_escape"


@dataclass
class EncoderConfig:
    """Configuration for encoding operations."""
    encoding: str = "utf-8"
    encoding_type: EncodingType = EncodingType.UTF8
    error_handling: str = "strict"  # strict, ignore, replace
    indent: Optional[int] = None
    sort_keys: bool = False


class Base64Encoder:
    """Base64 encoding/decoding."""

    @staticmethod
    def encode(data: bytes) -> str:
        """Encode bytes to base64 string."""
        return base64.b64encode(data).decode("ascii")

    @staticmethod
    def decode(data: str) -> bytes:
        """Decode base64 string to bytes."""
        # Handle URL-safe base64
        data = data.replace("-", "+").replace("_", "/")
        padding = len(data) % 4
        if padding:
            data += "=" * (4 - padding)
        return base64.b64decode(data)

    @staticmethod
    def encode_url_safe(data: bytes) -> str:
        """Encode bytes to URL-safe base64 string."""
        return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")

    @staticmethod
    def decode_url_safe(data: str) -> bytes:
        """Decode URL-safe base64 string to bytes."""
        padding = len(data) % 4
        if padding:
            data += "=" * (4 - padding)
        return base64.urlsafe_b64decode(data)


class HexEncoder:
    """Hexadecimal encoding/decoding."""

    @staticmethod
    def encode(data: bytes) -> str:
        """Encode bytes to hex string."""
        return data.hex()

    @staticmethod
    def decode(data: str) -> bytes:
        """Decode hex string to bytes."""
        return bytes.fromhex(data)


class Base32Encoder:
    """Base32 encoding/decoding."""

    @staticmethod
    def encode(data: bytes) -> str:
        """Encode bytes to base32 string."""
        return base64.b32encode(data).decode("ascii")

    @staticmethod
    def decode(data: str) -> bytes:
        """Decode base32 string to bytes."""
        return base64.b32decode(data.upper())


class Base85Encoder:
    """Base85 encoding/decoding."""

    @staticmethod
    def encode(data: bytes) -> str:
        """Encode bytes to base85 string."""
        return base64.b85encode(data).decode("ascii")

    @staticmethod
    def decode(data: str) -> bytes:
        """Decode base85 string to bytes."""
        return base64.b85decode(data)


class URLEncoder:
    """URL encoding/decoding."""

    @staticmethod
    def encode(data: str, safe: str = "") -> str:
        """Encode string to URL-safe format."""
        return urllib.parse.quote(data, safe=safe)

    @staticmethod
    def decode(data: str) -> str:
        """Decode URL-encoded string."""
        return urllib.parse.unquote(data)

    @staticmethod
    def encode_component(data: str) -> str:
        """Encode string as URL component."""
        return urllib.parse.quote_plus(data)

    @staticmethod
    def decode_component(data: str) -> str:
        """Decode URL component."""
        return urllib.parse.unquote_plus(data)

    @staticmethod
    def encode_dict(data: Dict[str, Any]) -> str:
        """Encode dictionary to URL query string."""
        return urllib.parse.urlencode(data)

    @staticmethod
    def decode_dict(data: str) -> Dict[str, str]:
        """Decode URL query string to dictionary."""
        return dict(urllib.parse.parse_qsl(data))


class HTMLEncoder:
    """HTML entity encoding/decoding."""

    HTML_ENTITIES = {
        "<": "&lt;",
        ">": "&gt;",
        "&": "&amp;",
        '"': "&quot;",
        "'": "&#39;",
    }

    HTML_ENTITIES_REVERSE = {v: k for k, v in HTML_ENTITIES.items()}

    @classmethod
    def encode(cls, data: str) -> str:
        """Encode string with HTML entities."""
        result = data
        for char, entity in cls.HTML_ENTITIES.items():
            result = result.replace(char, entity)
        return result

    @classmethod
    def decode(cls, data: str) -> str:
        """Decode HTML entities."""
        result = data
        for entity, char in cls.HTML_ENTITIES_REVERSE.items():
            result = result.replace(entity, char)
        return result

    @classmethod
    def encode_all(cls, data: str) -> str:
        """Encode all non-ASCII characters."""
        return data.encode("ascii", "xmlcharrefreplace").decode("ascii")


class JSONEncoder:
    """JSON encoding/decoding."""

    @staticmethod
    def encode(
        data: Any,
        indent: Optional[int] = None,
        sort_keys: bool = False
    ) -> str:
        """Encode data to JSON string."""
        return json.dumps(
            data,
            indent=indent,
            sort_keys=sort_keys,
            ensure_ascii=False
        )

    @staticmethod
    def decode(data: str) -> Any:
        """Decode JSON string to data."""
        return json.loads(data)


class QuotedPrintableEncoder:
    """Quoted-printable encoding/decoding."""

    @staticmethod
    def encode(data: bytes) -> str:
        """Encode bytes to quoted-printable string."""
        return quopri.encodestring(data).decode("ascii")

    @staticmethod
    def decode(data: str) -> bytes:
        """Decode quoted-printable string to bytes."""
        return quopri.decodestring(data.encode("ascii"))


class EncoderChain:
    """Chain multiple encodings together."""

    def __init__(self):
        self._encoders: List[tuple[int, EncodingType, Dict[str, Any]]] = []

    def add_encoding(
        self,
        encoding_type: EncodingType,
        priority: int = 0,
        config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add an encoding step to the chain."""
        self._encoders.append((priority, encoding_type, config or {}))
        self._encoders.sort(key=lambda x: x[0])

    async def encode(self, data: Union[bytes, str]) -> str:
        """Encode data through the chain."""
        current: Union[bytes, str] = data if isinstance(data, bytes) else data.encode("utf-8")

        for _, enc_type, config in self._encoders:
            if enc_type == EncodingType.BASE64:
                current = Base64Encoder.encode(current if isinstance(current, bytes) else current.encode())
            elif enc_type == EncodingType.BASE64_URL_SAFE:
                current = Base64Encoder.encode_url_safe(current if isinstance(current, bytes) else current.encode())
            elif enc_type == EncodingType.BASE16:
                current = HexEncoder.encode(current if isinstance(current, bytes) else current.encode())
            elif enc_type == EncodingType.BASE32:
                current = Base32Encoder.encode(current if isinstance(current, bytes) else current.encode())
            elif enc_type == EncodingType.BASE85:
                current = Base85Encoder.encode(current if isinstance(current, bytes) else current.encode())
            elif enc_type == EncodingType.URL:
                current = URLEncoder.encode(current if isinstance(current, str) else current.decode())
            elif enc_type == EncodingType.URL_COMPONENT:
                current = URLEncoder.encode_component(current if isinstance(current, str) else current.decode())
            elif enc_type == EncodingType.HTML:
                current = HTMLEncoder.encode(current if isinstance(current, str) else current.decode())
            elif enc_type == EncodingType.JSON:
                current = JSONEncoder.encode(current)
            elif enc_type == EncodingType.UTF8:
                if isinstance(current, bytes):
                    current = current.decode("utf-8")
            elif enc_type == EncodingType.QUOTED_PRINTABLE:
                current = QuotedPrintableEncoder.encode(current if isinstance(current, bytes) else current.encode())

        return current if isinstance(current, str) else current.decode()

    async def decode(self, data: str) -> Union[bytes, str]:
        """Decode data through the chain in reverse."""
        current = data

        for _, enc_type, config in reversed(self._encoders):
            if enc_type == EncodingType.BASE64:
                current = Base64Encoder.decode(current)
            elif enc_type == EncodingType.BASE64_URL_SAFE:
                current = Base64Encoder.decode_url_safe(current)
            elif enc_type == EncodingType.BASE16:
                current = HexEncoder.decode(current)
            elif enc_type == EncodingType.BASE32:
                current = Base32Encoder.decode(current)
            elif enc_type == EncodingType.BASE85:
                current = Base85Encoder.decode(current)
            elif enc_type == EncodingType.URL:
                current = URLEncoder.decode(current)
            elif enc_type == EncodingType.URL_COMPONENT:
                current = URLEncoder.decode_component(current)
            elif enc_type == EncodingType.HTML:
                current = HTMLEncoder.decode(current)
            elif enc_type == EncodingType.JSON:
                current = JSONEncoder.decode(current)
            elif enc_type == EncodingType.UTF8:
                if isinstance(current, bytes):
                    current = current.decode("utf-8")

        return current


class DataEncoderAction:
    """Main action class for data encoding."""

    def __init__(self):
        self._chain = EncoderChain()

    def add_encoding(self, encoding_type: EncodingType, priority: int = 0) -> None:
        """Add an encoding to the chain."""
        self._chain.add_encoding(encoding_type, priority)

    def encode(
        self,
        data: Union[bytes, str],
        encoding_type: EncodingType
    ) -> str:
        """Encode data using specified encoding."""
        encoders = {
            EncodingType.BASE64: lambda d: Base64Encoder.encode(d if isinstance(d, bytes) else d.encode()),
            EncodingType.BASE64_URL_SAFE: lambda d: Base64Encoder.encode_url_safe(d if isinstance(d, bytes) else d.encode()),
            EncodingType.BASE16: lambda d: HexEncoder.encode(d if isinstance(d, bytes) else d.encode()),
            EncodingType.BASE32: lambda d: Base32Encoder.encode(d if isinstance(d, bytes) else d.encode()),
            EncodingType.BASE85: lambda d: Base85Encoder.encode(d if isinstance(d, bytes) else d.encode()),
            EncodingType.URL: lambda d: URLEncoder.encode(d if isinstance(d, str) else d.decode()),
            EncodingType.URL_COMPONENT: lambda d: URLEncoder.encode_component(d if isinstance(d, str) else d.decode()),
            EncodingType.HTML: lambda d: HTMLEncoder.encode(d if isinstance(d, str) else d.decode()),
            EncodingType.JSON: lambda d: JSONEncoder.encode(d),
            EncodingType.UTF8: lambda d: d if isinstance(d, str) else d.decode("utf-8"),
            EncodingType.QUOTED_PRINTABLE: lambda d: QuotedPrintableEncoder.encode(d if isinstance(d, bytes) else d.encode()),
        }

        encoder = encoders.get(encoding_type)
        if not encoder:
            raise ValueError(f"Unsupported encoding: {encoding_type}")
        return encoder(data)

    def decode(
        self,
        data: str,
        encoding_type: EncodingType
    ) -> Union[bytes, str]:
        """Decode data using specified encoding."""
        decoders = {
            EncodingType.BASE64: Base64Encoder.decode,
            EncodingType.BASE64_URL_SAFE: Base64Encoder.decode_url_safe,
            EncodingType.BASE16: HexEncoder.decode,
            EncodingType.BASE32: Base32Encoder.decode,
            EncodingType.BASE85: Base85Encoder.decode,
            EncodingType.URL: URLEncoder.decode,
            EncodingType.URL_COMPONENT: URLEncoder.decode_component,
            EncodingType.HTML: HTMLEncoder.decode,
            EncodingType.JSON: JSONEncoder.decode,
            EncodingType.QUOTED_PRINTABLE: QuotedPrintableEncoder.decode,
        }

        decoder = decoders.get(encoding_type)
        if not decoder:
            raise ValueError(f"Unsupported encoding: {encoding_type}")
        return decoder(data)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data encoder action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform (encode, decode, chain)
                - data: Data to encode/decode
                - encoding: Encoding type
                - indent: JSON indent level
                - sort_keys: Sort JSON keys

        Returns:
            Dictionary with encoding results.
        """
        operation = context.get("operation", "encode")

        if operation == "encode":
            data = context.get("data", "")
            enc_str = context.get("encoding", "base64")
            try:
                enc_type = EncodingType(enc_str)
            except ValueError:
                return {"success": False, "error": f"Unknown encoding: {enc_str}"}

            try:
                result = self.encode(data, enc_type)
                return {
                    "success": True,
                    "encoded": result,
                    "encoding": enc_str
                }
            except Exception as e:
                return {"success": False, "error": str(e)}

        elif operation == "decode":
            data = context.get("data", "")
            enc_str = context.get("encoding", "base64")
            try:
                enc_type = EncodingType(enc_str)
            except ValueError:
                return {"success": False, "error": f"Unknown encoding: {enc_str}"}

            try:
                result = self.decode(data, enc_type)
                return {
                    "success": True,
                    "decoded": result,
                    "encoding": enc_str
                }
            except Exception as e:
                return {"success": False, "error": str(e)}

        elif operation == "chain_encode":
            data = context.get("data", "")
            encodings = context.get("encodings", [])

            temp_chain = EncoderChain()
            for i, enc_str in enumerate(encodings):
                try:
                    enc_type = EncodingType(enc_str)
                    temp_chain.add_encoding(enc_type, priority=i)
                except ValueError:
                    return {"success": False, "error": f"Unknown encoding: {enc_str}"}

            try:
                result = await temp_chain.encode(data)
                return {
                    "success": True,
                    "encoded": result,
                    "chain": encodings
                }
            except Exception as e:
                return {"success": False, "error": str(e)}

        elif operation == "url_encode_dict":
            data = context.get("data", {})
            try:
                result = URLEncoder.encode_dict(data)
                return {"success": True, "encoded": result}
            except Exception as e:
                return {"success": False, "error": str(e)}

        elif operation == "url_decode_dict":
            data = context.get("data", "")
            try:
                result = URLEncoder.decode_dict(data)
                return {"success": True, "decoded": result}
            except Exception as e:
                return {"success": False, "error": str(e)}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
