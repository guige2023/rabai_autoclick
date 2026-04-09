"""Data Decoder Action Module.

Provides data decoding capabilities for various formats including
base64, hexadecimal, URL decoding, and custom decoding schemes.

Example:
    >>> from actions.data.data_decoder_action import DataDecoderAction
    >>> action = DataDecoderAction()
    >>> decoded = action.decode(data, format="base64")
"""

from __future__ import annotations

import base64
import json
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import codecs
import threading


class DecodingFormat(Enum):
    """Decoding format types."""
    BASE64 = "base64"
    HEX = "hex"
    URL = "url"
    HTML = "html"
    JSON = "json"
    UTF8 = "utf8"
    UNICODE = "unicode"
    CUSTOM = "custom"


@dataclass
class DecoderConfig:
    """Configuration for decoding.
    
    Attributes:
        default_format: Default decoding format
        custom_decoder: Custom decoding function
        encoding_errors: Error handling mode
        strict_mode: Strict format checking
    """
    default_format: DecodingFormat = DecodingFormat.BASE64
    custom_decoder: Optional[Callable[[str], Any]] = None
    encoding_errors: str = "strict"
    strict_mode: bool = True


@dataclass
class DecodingResult:
    """Result of decoding operation.
    
    Attributes:
        data: Decoded data
        format: Decoding format used
        original_type: Original data type
        decoded_type: Decoded data type
        metadata: Additional metadata
    """
    data: Any
    format: DecodingFormat
    original_type: str
    decoded_type: str
    duration: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataDecoderAction:
    """Data decoder for various formats.
    
    Provides decoding capabilities for data transformation
    with support for multiple decoding schemes.
    
    Attributes:
        config: Decoder configuration
        _cache: Decoding cache
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[DecoderConfig] = None,
    ) -> None:
        """Initialize decoder action.
        
        Args:
            config: Decoder configuration
        """
        self.config = config or DecoderConfig()
        self._cache: Dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def decode(
        self,
        data: Any,
        format: Optional[DecodingFormat] = None,
        **kwargs: Any,
    ) -> DecodingResult:
        """Decode data.
        
        Args:
            data: Data to decode
            format: Decoding format
            **kwargs: Additional format-specific arguments
        
        Returns:
            DecodingResult
        """
        import time
        start = time.time()
        
        format = format or self.config.default_format
        
        original_type = type(data).__name__
        
        if format == DecodingFormat.BASE64:
            result = self._decode_base64(data)
        elif format == DecodingFormat.HEX:
            result = self._decode_hex(data)
        elif format == DecodingFormat.URL:
            result = self._decode_url(data)
        elif format == DecodingFormat.HTML:
            result = self._decode_html(data)
        elif format == DecodingFormat.JSON:
            result = self._decode_json(data)
        elif format == DecodingFormat.UTF8:
            result = self._decode_utf8(data)
        elif format == DecodingFormat.UNICODE:
            result = self._decode_unicode(data)
        elif format == DecodingFormat.CUSTOM:
            result = self._decode_custom(data)
        else:
            result = data
        
        decoded_type = type(result).__name__
        
        return DecodingResult(
            data=result,
            format=format,
            original_type=original_type,
            decoded_type=decoded_type,
            duration=time.time() - start,
            metadata={"original_size": len(str(data))},
        )
    
    def _decode_base64(self, data: Any) -> Any:
        """Decode from base64.
        
        Args:
            data: Data to decode
        
        Returns:
            Decoded data
        """
        if isinstance(data, str):
            data = data.strip()
            
            missing_padding = len(data) % 4
            if missing_padding:
                data += "=" * (4 - missing_padding)
            
            decoded = base64.b64decode(data)
        else:
            decoded = base64.b64decode(data)
        
        if kwargs.get("as_bytes"):
            return decoded
        
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded
    
    def _decode_hex(self, data: Any) -> Any:
        """Decode from hexadecimal.
        
        Args:
            data: Data to decode
        
        Returns:
            Decoded data
        """
        if isinstance(data, str):
            data = data.encode("ascii")
        
        decoded = bytes.fromhex(data)
        
        if kwargs.get("as_bytes"):
            return decoded
        
        try:
            return decoded.decode("utf-8")
        except UnicodeDecodeError:
            return decoded
    
    def _decode_url(self, data: Any) -> str:
        """Decode from URL encoding.
        
        Args:
            data: Data to decode
        
        Returns:
            URL decoded string
        """
        return urllib.parse.unquote(str(data))
    
    def _decode_html(self, data: Any) -> str:
        """Decode from HTML encoding.
        
        Args:
            data: Data to decode
        
        Returns:
            HTML decoded string
        """
        result = str(data)
        
        html_entities = {
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#39;": "'",
            "&apos;": "'",
            "&nbsp;": " ",
        }
        
        for entity, char in html_entities.items():
            result = result.replace(entity, char)
        
        import re
        html_numeric = re.compile(r"&#(\d+);")
        result = html_numeric.sub(
            lambda m: chr(int(m.group(1))), result
        )
        
        html_hex = re.compile(r"&#x([0-9a-fA-F]+);")
        result = html_hex.sub(
            lambda m: chr(int(m.group(1), 16)), result
        )
        
        return result
    
    def _decode_json(self, data: Any) -> Any:
        """Decode from JSON.
        
        Args:
            data: Data to decode
        
        Returns:
            Parsed JSON data
        """
        return json.loads(str(data))
    
    def _decode_utf8(self, data: Any) -> str:
        """Decode from UTF-8.
        
        Args:
            data: Data to decode
        
        Returns:
            UTF-8 string
        """
        if isinstance(data, bytes):
            return data.decode("utf-8", errors=self.config.encoding_errors)
        return str(data)
    
    def _decode_unicode(self, data: Any) -> str:
        """Decode Unicode escape sequences.
        
        Args:
            data: Data to decode
        
        Returns:
            Decoded string
        """
        import re
        hex_pattern = re.compile(r"\\u([0-9a-fA-F]{4})")
        
        def replace_hex(m):
            return chr(int(m.group(1), 16))
        
        result = hex_pattern.sub(replace_hex, str(data))
        
        return result
    
    def _decode_custom(self, data: Any) -> Any:
        """Decode using custom decoder.
        
        Args:
            data: Data to decode
        
        Returns:
            Custom decoded data
        """
        if self.config.custom_decoder:
            return self.config.custom_decoder(str(data))
        return data
    
    def batch_decode(
        self,
        data_list: List[Any],
        format: Optional[DecodingFormat] = None,
    ) -> List[DecodingResult]:
        """Decode multiple data items.
        
        Args:
            data_list: List of data to decode
            format: Decoding format
        
        Returns:
            List of DecodingResult
        """
        return [self.decode(data, format) for data in data_list]
    
    def decode_dict(
        self,
        data: Dict[str, Any],
        format: Optional[DecodingFormat] = None,
        keys_only: bool = False,
        values_only: bool = False,
    ) -> Dict[str, Any]:
        """Decode dictionary contents.
        
        Args:
            data: Dictionary to decode
            format: Decoding format
            keys_only: Only decode keys
            values_only: Only decode values
        
        Returns:
            Decoded dictionary
        """
        result = {}
        
        for key, value in data.items():
            decoded_key = self.decode(key, format).data if not values_only else key
            decoded_value = self.decode(value, format).data if not keys_only else value
            
            result[decoded_key] = decoded_value
        
        return result
    
    def auto_detect(self, data: Any) -> DecodingResult:
        """Auto-detect format and decode.
        
        Args:
            data: Data to decode
        
        Returns:
            DecodingResult with detected format
        """
        data_str = str(data).strip()
        
        if len(data_str) >= 4:
            try:
                missing = len(data_str) % 4
                if missing:
                    data_str += "=" * (4 - missing)
                base64.b64decode(data_str)
                return self.decode(data, DecodingFormat.BASE64)
            except Exception:
                pass
        
        if all(c in "0123456789abcdefABCDEF" for c in data_str):
            if len(data_str) % 2 == 0:
                try:
                    bytes.fromhex(data_str)
                    return self.decode(data, DecodingFormat.HEX)
                except Exception:
                    pass
        
        if "%" in data_str:
            return self.decode(data, DecodingFormat.URL)
        
        if data_str.startswith("{"):
            return self.decode(data, DecodingFormat.JSON)
        
        if "&" in data_str or "<" in data_str:
            return self.decode(data, DecodingFormat.HTML)
        
        return self.decode(data, DecodingFormat.UTF8)
    
    def set_custom_decoder(
        self,
        decoder: Callable[[str], Any],
    ) -> None:
        """Set custom decoder function.
        
        Args:
            decoder: Custom decoding function
        """
        self.config.custom_decoder = decoder
    
    def clear_cache(self) -> None:
        """Clear decoding cache."""
        with self._lock:
            self._cache.clear()
