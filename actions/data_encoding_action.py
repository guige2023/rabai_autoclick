"""
Data Encoding Action Module

Provides data encoding and decoding capabilities.
Supports base64, URL encoding, JSON, and custom encoding schemes.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class EncodingType(Enum):
    """Encoding type."""
    BASE64 = "base64"
    URL = "url"
    JSON = "json"
    HEX = "hex"
    UTF8 = "utf8"
    CUSTOM = "custom"


@dataclass
class EncodingResult:
    """Result of an encoding operation."""
    success: bool
    original: Any
    encoded: Any
    encoding_type: EncodingType
    duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class DecodingResult:
    """Result of a decoding operation."""
    success: bool
    encoded: Any
    decoded: Any
    encoding_type: EncodingType
    duration_ms: float = 0.0
    error: Optional[str] = None


class DataEncodingAction:
    """
    Data encoding and decoding utilities.
    
    Example:
        encoder = DataEncodingAction()
        
        result = encoder.encode("Hello World", EncodingType.BASE64)
        decoded = encoder.decode(result.encoded, EncodingType.BASE64)
    """
    
    def __init__(self):
        self._stats = {
            "total_encodes": 0,
            "total_decodes": 0,
            "successful_encodes": 0,
            "successful_decodes": 0,
            "failed_operations": 0
        }
    
    def encode(
        self,
        data: Any,
        encoding_type: EncodingType = EncodingType.BASE64,
        custom_encoder: Optional[Callable[[Any], Any]] = None
    ) -> EncodingResult:
        """
        Encode data.
        
        Args:
            data: Data to encode
            encoding_type: Type of encoding
            custom_encoder: Optional custom encoder function
            
        Returns:
            EncodingResult with encoded data
        """
        start_time = datetime.now()
        self._stats["total_encodes"] += 1
        
        try:
            if custom_encoder:
                encoded = custom_encoder(data)
            elif encoding_type == EncodingType.BASE64:
                if isinstance(data, str):
                    data = data.encode('utf-8')
                encoded = base64.b64encode(data).decode('ascii')
            elif encoding_type == EncodingType.URL:
                encoded = urllib.parse.quote(str(data))
            elif encoding_type == EncodingType.HEX:
                if isinstance(data, str):
                    data = data.encode('utf-8')
                encoded = data.hex()
            elif encoding_type == EncodingType.UTF8:
                if isinstance(data, str):
                    encoded = data.encode('utf-8').decode('utf-8')
                else:
                    encoded = data
            elif encoding_type == EncodingType.JSON:
                encoded = json.dumps(data)
            else:
                encoded = str(data)
            
            self._stats["successful_encodes"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return EncodingResult(
                success=True,
                original=data,
                encoded=encoded,
                encoding_type=encoding_type,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["failed_operations"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return EncodingResult(
                success=False,
                original=data,
                encoded=None,
                encoding_type=encoding_type,
                duration_ms=duration_ms,
                error=str(e)
            )
    
    def decode(
        self,
        data: Any,
        encoding_type: EncodingType = EncodingType.BASE64,
        custom_decoder: Optional[Callable[[Any], Any]] = None
    ) -> DecodingResult:
        """
        Decode data.
        
        Args:
            data: Data to decode
            encoding_type: Type of encoding
            custom_decoder: Optional custom decoder function
            
        Returns:
            DecodingResult with decoded data
        """
        start_time = datetime.now()
        self._stats["total_decodes"] += 1
        
        try:
            if custom_decoder:
                decoded = custom_decoder(data)
            elif encoding_type == EncodingType.BASE64:
                if isinstance(data, str):
                    data = data.encode('ascii')
                decoded = base64.b64decode(data)
                if isinstance(decoded, bytes):
                    decoded = decoded.decode('utf-8')
            elif encoding_type == EncodingType.URL:
                decoded = urllib.parse.unquote(str(data))
            elif encoding_type == EncodingType.HEX:
                decoded = bytes.fromhex(data)
                if isinstance(decoded, bytes):
                    decoded = decoded.decode('utf-8')
            elif encoding_type == EncodingType.UTF8:
                decoded = data.decode('utf-8') if isinstance(data, bytes) else data
            elif encoding_type == EncodingType.JSON:
                decoded = json.loads(data)
            else:
                decoded = data
            
            self._stats["successful_decodes"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return DecodingResult(
                success=True,
                encoded=data,
                decoded=decoded,
                encoding_type=encoding_type,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["failed_operations"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return DecodingResult(
                success=False,
                encoded=data,
                decoded=None,
                encoding_type=encoding_type,
                duration_ms=duration_ms,
                error=str(e)
            )
    
    def encode_dict(
        self,
        data: dict,
        keys: Optional[list[str]] = None,
        encoding_type: EncodingType = EncodingType.BASE64
    ) -> dict:
        """
        Encode specific keys in a dictionary.
        
        Args:
            data: Dictionary to encode
            keys: Keys to encode (None = encode all string values)
            encoding_type: Type of encoding
            
        Returns:
            Dictionary with encoded values
        """
        result = data.copy()
        
        for key, value in result.items():
            if keys is None or key in keys:
                if isinstance(value, str):
                    result[key] = self.encode(value, encoding_type).encoded
            elif isinstance(value, dict):
                result[key] = self.encode_dict(value, keys, encoding_type)
        
        return result
    
    def decode_dict(
        self,
        data: dict,
        keys: Optional[list[str]] = None,
        encoding_type: EncodingType = EncodingType.BASE64
    ) -> dict:
        """Decode specific keys in a dictionary."""
        result = data.copy()
        
        for key, value in result.items():
            if keys is None or key in keys:
                if isinstance(value, str):
                    result[key] = self.decode(value, encoding_type).decoded
            elif isinstance(value, dict):
                result[key] = self.decode_dict(value, keys, encoding_type)
        
        return result
    
    def encode_batch(
        self,
        data: list[Any],
        encoding_type: EncodingType = EncodingType.BASE64
    ) -> list[EncodingResult]:
        """Encode a batch of data."""
        return [self.encode(item, encoding_type) for item in data]
    
    def decode_batch(
        self,
        data: list[Any],
        encoding_type: EncodingType = EncodingType.BASE64
    ) -> list[DecodingResult]:
        """Decode a batch of data."""
        return [self.decode(item, encoding_type) for item in data]
    
    def get_stats(self) -> dict[str, Any]:
        """Get encoding statistics."""
        return {
            **self._stats,
            "success_rate": (
                (self._stats["successful_encodes"] + self._stats["successful_decodes"]) /
                max(1, self._stats["total_encodes"] + self._stats["total_decodes"])
            )
        }
