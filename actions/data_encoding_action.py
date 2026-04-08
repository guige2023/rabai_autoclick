# Copyright (c) 2024. coded by claude
"""Data Encoding Action Module.

Provides encoding and decoding utilities for API data including
base64, URL encoding, HTML escaping, and custom encodings.
"""
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import base64
import urllib.parse
import html
import json
import logging

logger = logging.getLogger(__name__)


class EncodingType(Enum):
    BASE64 = "base64"
    URL = "url"
    HTML = "html"
    JSON = "json"
    UTF8 = "utf8"
    HEX = "hex"


@dataclass
class EncodingResult:
    success: bool
    original: str
    encoded: str
    encoding_type: EncodingType
    error: Optional[str] = None


class DataEncoder:
    @staticmethod
    def encode_base64(data: str) -> EncodingResult:
        try:
            encoded = base64.b64encode(data.encode()).decode()
            return EncodingResult(success=True, original=data, encoded=encoded, encoding_type=EncodingType.BASE64)
        except Exception as e:
            return EncodingResult(success=False, original=data, encoded="", encoding_type=EncodingType.BASE64, error=str(e))

    @staticmethod
    def decode_base64(data: str) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            decoded = base64.b64decode(data.encode()).decode()
            return True, decoded, None
        except Exception as e:
            return False, None, str(e)

    @staticmethod
    def encode_url(data: str, safe: str = "") -> EncodingResult:
        try:
            encoded = urllib.parse.quote(data, safe=safe)
            return EncodingResult(success=True, original=data, encoded=encoded, encoding_type=EncodingType.URL)
        except Exception as e:
            return EncodingResult(success=False, original=data, encoded="", encoding_type=EncodingType.URL, error=str(e))

    @staticmethod
    def decode_url(data: str) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            decoded = urllib.parse.unquote(data)
            return True, decoded, None
        except Exception as e:
            return False, None, str(e)

    @staticmethod
    def encode_html(data: str) -> EncodingResult:
        try:
            encoded = html.escape(data)
            return EncodingResult(success=True, original=data, encoded=encoded, encoding_type=EncodingType.HTML)
        except Exception as e:
            return EncodingResult(success=False, original=data, encoded="", encoding_type=EncodingType.HTML, error=str(e))

    @staticmethod
    def decode_html(data: str) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            decoded = html.unescape(data)
            return True, decoded, None
        except Exception as e:
            return False, None, str(e)

    @staticmethod
    def encode_hex(data: str) -> EncodingResult:
        try:
            encoded = data.encode().hex()
            return EncodingResult(success=True, original=data, encoded=encoded, encoding_type=EncodingType.HEX)
        except Exception as e:
            return EncodingResult(success=False, original=data, encoded="", encoding_type=EncodingType.HEX, error=str(e))

    @staticmethod
    def decode_hex(data: str) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            decoded = bytes.fromhex(data).decode()
            return True, decoded, None
        except Exception as e:
            return False, None, str(e)

    @staticmethod
    def encode_json(data: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
        try:
            encoded = json.dumps(data, ensure_ascii=False)
            return True, encoded, None
        except Exception as e:
            return False, None, str(e)

    @staticmethod
    def decode_json(data: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        try:
            decoded = json.loads(data)
            return True, decoded, None
        except Exception as e:
            return False, None, str(e)
