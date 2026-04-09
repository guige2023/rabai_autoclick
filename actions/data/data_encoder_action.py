"""Data Encoder Action Module.

Provides data encoding capabilities for various formats including
base64, hexadecimal, URL encoding, and custom encoding schemes.

Example:
    >>> from actions.data.data_encoder_action import DataEncoderAction
    >>> action = DataEncoderAction()
    >>> encoded = action.encode(data, format="base64")
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import codecs
import threading


class EncodingFormat(Enum):
    """Encoding format types."""
    BASE64 = "base64"
    HEX = "hex"
    URL = "url"
    HTML = "html"
    JSON = "json"
    UTF8 = "utf8"
    UNICODE = "unicode"
    CUSTOM = "custom"


@dataclass
class EncoderConfig:
    """Configuration for encoding.
    
    Attributes:
        default_format: Default encoding format
        custom_encoder: Custom encoding function
        encoding_errors: Error handling mode
        include_schema: Include format schema in output
    """
    default_format: EncodingFormat = EncodingFormat.BASE64
    custom_encoder: Optional[Callable[[Any], str]] = None
    encoding_errors: str = "strict"
    include_schema: bool = False


@dataclass
class EncodingResult:
    """Result of encoding operation.
    
    Attributes:
        data: Encoded data
        format: Encoding format used
        original_type: Original data type
        encoded_type: Encoded data type
        metadata: Additional metadata
    """
    data: Any
    format: EncodingFormat
    original_type: str
    encoded_type: str
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataEncoderAction:
    """Data encoder for various formats.
    
    Provides encoding capabilities for data transformation
    with support for multiple encoding schemes.
    
    Attributes:
        config: Encoder configuration
        _cache: Encoding cache
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[EncoderConfig] = None,
    ) -> None:
        """Initialize encoder action.
        
        Args:
            config: Encoder configuration
        """
        self.config = config or EncoderConfig()
        self._cache: Dict[str, str] = {}
        self._lock = threading.Lock()
    
    def encode(
        self,
        data: Any,
        format: Optional[EncodingFormat] = None,
        **kwargs: Any,
    ) -> EncodingResult:
        """Encode data.
        
        Args:
            data: Data to encode
            format: Encoding format
            **kwargs: Additional format-specific arguments
        
        Returns:
            EncodingResult
        """
        import time
        start = time.time()
        
        format = format or self.config.default_format
        
        original_type = type(data).__name__
        
        if format == EncodingFormat.BASE64:
            result = self._encode_base64(data)
        elif format == EncodingFormat.HEX:
            result = self._encode_hex(data)
        elif format == EncodingFormat.URL:
            result = self._encode_url(data)
        elif format == EncodingFormat.HTML:
            result = self._encode_html(data)
        elif format == EncodingFormat.JSON:
            result = self._encode_json(data)
        elif format == EncodingFormat.UTF8:
            result = self._encode_utf8(data)
        elif format == EncodingFormat.UNICODE:
            result = self._encode_unicode(data)
        elif format == EncodingFormat.CUSTOM:
            result = self._encode_custom(data)
        else:
            result = str(data)
        
        encoded_type = type(result).__name__
        
        return EncodingResult(
            data=result,
            format=format,
            original_type=original_type,
            encoded_type=encoded_type,
            duration=time.time() - start,
            metadata={"original_size": self._get_size(data)},
        )
    
    def _encode_base64(self, data: Any) -> str:
        """Encode to base64.
        
        Args:
            data: Data to encode
        
        Returns:
            Base64 encoded string
        """
        if isinstance(data, str):
            data = data.encode(self.config.encoding_errors)
        elif not isinstance(data, bytes):
            data = str(data).encode(self.config.encoding_errors)
        
        encoded = base64.b64encode(data)
        return encoded.decode("ascii")
    
    def _encode_hex(self, data: Any) -> str:
        """Encode to hexadecimal.
        
        Args:
            data: Data to encode
        
        Returns:
            Hex encoded string
        """
        if isinstance(data, str):
            data = data.encode(self.config.encoding_errors)
        elif not isinstance(data, bytes):
            data = str(data).encode(self.config.encoding_errors)
        
        return data.hex()
    
    def _encode_url(self, data: Any) -> str:
        """Encode for URL.
        
        Args:
            data: Data to encode
        
        Returns:
            URL encoded string
        """
        return urllib.parse.quote(str(data), safe="")
    
    def _encode_html(self, data: Any) -> str:
        """Encode for HTML.
        
        Args:
            data: Data to encode
        
        Returns:
            HTML encoded string
        """
        result = str(data)
        result = result.replace("&", "&amp;")
        result = result.replace("<", "&lt;")
        result = result.replace(">", "&gt;")
        result = result.replace('"', "&quot;")
        result = result.replace("'", "&#39;")
        return result
    
    def _encode_json(self, data: Any) -> str:
        """Encode to JSON.
        
        Args:
            data: Data to encode
        
        Returns:
            JSON string
        """
        return json.dumps(data, ensure_ascii=False)
    
    def _encode_utf8(self, data: Any) -> str:
        """Encode to UTF-8.
        
        Args:
            data: Data to encode
        
        Returns:
            UTF-8 string
        """
        if isinstance(data, bytes):
            return data.decode("utf-8", errors=self.config.encoding_errors)
        return str(data).encode("utf-8", errors=self.config.encoding_errors).decode("utf-8")
    
    def _encode_unicode(self, data: Any) -> str:
        """Encode to Unicode escape sequences.
        
        Args:
            data: Data to encode
        
        Returns:
            Unicode escaped string
        """
        result = ""
        for char in str(data):
            if ord(char) > 127:
                result += f"\\u{ord(char):04x}"
            else:
                result += char
        return result
    
    def _encode_custom(self, data: Any) -> str:
        """Encode using custom encoder.
        
        Args:
            data: Data to encode
        
        Returns:
            Custom encoded string
        """
        if self.config.custom_encoder:
            return self.config.custom_encoder(data)
        return str(data)
    
    def batch_encode(
        self,
        data_list: List[Any],
        format: Optional[EncodingFormat] = None,
    ) -> List[EncodingResult]:
        """Encode multiple data items.
        
        Args:
            data_list: List of data to encode
            format: Encoding format
        
        Returns:
            List of EncodingResult
        """
        return [self.encode(data, format) for data in data_list]
    
    def encode_dict(
        self,
        data: Dict[str, Any],
        format: Optional[EncodingFormat] = None,
        keys_only: bool = False,
        values_only: bool = False,
    ) -> Dict[str, Any]:
        """Encode dictionary contents.
        
        Args:
            data: Dictionary to encode
            format: Encoding format
            keys_only: Only encode keys
            values_only: Only encode values
        
        Returns:
            Encoded dictionary
        """
        result = {}
        
        for key, value in data.items():
            encoded_key = self.encode(key, format).data if not values_only else key
            encoded_value = self.encode(value, format).data if not keys_only else value
            
            result[encoded_key] = encoded_value
        
        return result
    
    def _get_size(self, data: Any) -> int:
        """Get size of data.
        
        Args:
            data: Data
        
        Returns:
            Size in bytes
        """
        if isinstance(data, bytes):
            return len(data)
        if isinstance(data, str):
            return len(data.encode("utf-8"))
        return len(str(data))
    
    def set_custom_encoder(
        self,
        encoder: Callable[[Any], str],
    ) -> None:
        """Set custom encoder function.
        
        Args:
            encoder: Custom encoding function
        """
        self.config.custom_encoder = encoder
    
    def clear_cache(self) -> None:
        """Clear encoding cache."""
        with self._lock:
            self._cache.clear()
