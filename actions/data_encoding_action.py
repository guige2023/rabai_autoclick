"""
Data Encoding Action - Encodes and decodes data.

This module provides encoding/decoding capabilities for
various formats including base64, URL, and custom encodings.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any


@dataclass
class EncodingConfig:
    """Configuration for encoding."""
    encoding: str = "utf-8"
    base64_padding: bool = True


class DataEncoder:
    """Encodes and decodes data."""
    
    def __init__(self, config: EncodingConfig | None = None) -> None:
        self.config = config or EncodingConfig()
    
    def encode_base64(self, data: str) -> str:
        """Encode string to base64."""
        encoded = base64.b64encode(data.encode(self.config.encoding))
        return encoded.decode(self.config.encoding)
    
    def decode_base64(self, data: str) -> str:
        """Decode base64 to string."""
        decoded = base64.b64decode(data.encode(self.config.encoding))
        return decoded.decode(self.config.encoding)
    
    def encode_url(self, data: str) -> str:
        """URL encode a string."""
        import urllib.parse
        return urllib.parse.quote(data)
    
    def decode_url(self, data: str) -> str:
        """URL decode a string."""
        import urllib.parse
        return urllib.parse.unquote(data)
    
    def encode_json(self, data: Any) -> str:
        """Encode data as JSON string."""
        return json.dumps(data, ensure_ascii=False, default=str)
    
    def decode_json(self, data: str) -> Any:
        """Decode JSON string to data."""
        return json.loads(data)


class DataEncodingAction:
    """Data encoding action for automation workflows."""
    
    def __init__(self) -> None:
        self.encoder = DataEncoder()
    
    def encode(self, data: str, format: str = "base64") -> str:
        """Encode data."""
        if format == "base64":
            return self.encoder.encode_base64(data)
        elif format == "url":
            return self.encoder.encode_url(data)
        elif format == "json":
            return self.encoder.encode_json(data)
        return data
    
    def decode(self, data: str, format: str = "base64") -> str:
        """Decode data."""
        if format == "base64":
            return self.encoder.decode_base64(data)
        elif format == "url":
            return self.encoder.decode_url(data)
        elif format == "json":
            return self.encoder.decode_json(data)
        return data


__all__ = ["EncodingConfig", "DataEncoder", "DataEncodingAction"]
