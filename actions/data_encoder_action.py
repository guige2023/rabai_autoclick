"""
Data Encoder Action Module.

Encoding and decoding for data serialization,
supports JSON, Base64, URL encoding, and custom formats.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass
from enum import Enum
import logging
import base64
import urllib.parse
import json
import pickle

logger = logging.getLogger(__name__)


class EncodingType(Enum):
    """Encoding types."""
    JSON = "json"
    BASE64 = "base64"
    URL = "url"
    PICKLE = "pickle"
    HEX = "hex"
    CUSTOM = "custom"


@dataclass
class EncodingConfig:
    """Encoding configuration."""
    encoding: EncodingType
    custom_encoder: Optional[Callable[[Any], Any]] = None
    custom_decoder: Optional[Callable[[Any], Any]] = None


class DataEncoderAction:
    """
    Data encoding and decoding utilities.

    Encodes/decodes data for storage or transmission,
    supports chained encoding operations.

    Example:
        encoder = DataEncoderAction()
        encoded = encoder.encode(data, EncodingType.BASE64)
        decoded = encoder.decode(encoded, EncodingType.BASE64)
    """

    def __init__(self, default_encoding: EncodingType = EncodingType.JSON) -> None:
        self.default_encoding = default_encoding
        self._encoders: dict[EncodingType, Callable[[Any], Any]] = {
            EncodingType.JSON: self._encode_json,
            EncodingType.BASE64: self._encode_base64,
            EncodingType.URL: self._encode_url,
            EncodingType.PICKLE: self._encode_pickle,
            EncodingType.HEX: self._encode_hex,
        }
        self._decoders: dict[EncodingType, Callable[[Any], Any]] = {
            EncodingType.JSON: self._decode_json,
            EncodingType.BASE64: self._decode_base64,
            EncodingType.URL: self._decode_url,
            EncodingType.PICKLE: self._decode_pickle,
            EncodingType.HEX: self._decode_hex,
        }

    def encode(
        self,
        data: Any,
        encoding: Optional[EncodingType] = None,
    ) -> str:
        """Encode data to string."""
        enc_type = encoding or self.default_encoding

        encoder = self._encoders.get(enc_type)
        if encoder:
            return encoder(data)

        raise ValueError(f"No encoder for {enc_type}")

    def decode(
        self,
        data: str,
        encoding: Optional[EncodingType] = None,
    ) -> Any:
        """Decode string back to data."""
        enc_type = encoding or self.default_encoding

        decoder = self._decoders.get(enc_type)
        if decoder:
            return decoder(data)

        raise ValueError(f"No decoder for {enc_type}")

    def encode_chain(
        self,
        data: Any,
        encodings: list[EncodingType],
    ) -> str:
        """Apply multiple encodings in sequence."""
        result = data
        for enc in encodings:
            result = self.encode(result, enc)
        return result

    def decode_chain(
        self,
        data: str,
        encodings: list[EncodingType],
    ) -> Any:
        """Reverse multiple encodings in sequence."""
        result = data
        for enc in reversed(encodings):
            result = self.decode(result, enc)
        return result

    def _encode_json(self, data: Any) -> str:
        """Encode to JSON."""
        return json.dumps(data, default=str, ensure_ascii=False)

    def _decode_json(self, data: str) -> Any:
        """Decode from JSON."""
        return json.loads(data)

    def _encode_base64(self, data: Any) -> str:
        """Encode to Base64."""
        if isinstance(data, str):
            data = data.encode()
        elif not isinstance(data, bytes):
            data = str(data).encode()

        return base64.b64encode(data).decode("ascii")

    def _decode_base64(self, data: str) -> Any:
        """Decode from Base64."""
        decoded = base64.b64decode(data)
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded

    def _encode_url(self, data: Any) -> str:
        """Encode for URL."""
        return urllib.parse.quote(str(data))

    def _decode_url(self, data: str) -> str:
        """Decode from URL."""
        return urllib.parse.unquote(data)

    def _encode_pickle(self, data: Any) -> str:
        """Encode using pickle."""
        pickled = pickle.dumps(data)
        return base64.b64encode(pickled).decode("ascii")

    def _decode_pickle(self, data: str) -> Any:
        """Decode using pickle."""
        decoded = base64.b64decode(data)
        return pickle.loads(decoded)

    def _encode_hex(self, data: Any) -> str:
        """Encode to hex."""
        if isinstance(data, str):
            data = data.encode()
        elif not isinstance(data, bytes):
            data = str(data).encode()

        return data.hex()

    def _decode_hex(self, data: str) -> str:
        """Decode from hex."""
        decoded = bytes.fromhex(data)
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded

    def compress_encode(
        self,
        data: Any,
        encoding: EncodingType = EncodingType.BASE64,
    ) -> str:
        """Compress then encode data."""
        import zlib

        if isinstance(data, str):
            data = data.encode()

        compressed = zlib.compress(data)
        return self.encode(compressed, encoding)

    def decode_decompress(
        self,
        data: str,
        encoding: EncodingType = EncodingType.BASE64,
        output_type: str = "str",
    ) -> Any:
        """Decode then decompress data."""
        import zlib

        decoded = self.decode(data, encoding)

        if isinstance(decoded, str):
            decoded = decoded.encode()

        decompressed = zlib.decompress(decoded)

        if output_type == "str":
            return decompressed.decode("utf-8")
        elif output_type == "bytes":
            return decompressed
        else:
            return decompressed
