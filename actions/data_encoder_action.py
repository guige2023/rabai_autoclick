"""Data encoder action module for RabAI AutoClick.

Provides data encoding/decoding operations:
- Base64EncoderAction: Base64 encoding/decoding
- URLEncoderAction: URL encoding/decoding
- HTMLEncoderAction: HTML entity encoding
- HexEncoderAction: Hexadecimal encoding
- UnicodeEncoderAction: Unicode normalization encoding
"""

import base64
import urllib.parse
import html
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncoderAction(BaseAction):
    """Base64 encoding/decoding."""
    action_type = "data_base64_encoder"
    display_name = "Base64编码器"
    description = "Base64编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "encode")
            encoding = params.get("encoding", "utf-8")

            if action == "encode":
                if isinstance(data, str):
                    encoded = base64.b64encode(data.encode(encoding)).decode(encoding)
                elif isinstance(data, bytes):
                    encoded = base64.b64encode(data).decode(encoding)
                else:
                    encoded = base64.b64encode(str(data).encode(encoding)).decode(encoding)

                return ActionResult(
                    success=True,
                    data={
                        "encoded": encoded,
                        "action": "encode",
                        "original_length": len(str(data)),
                        "encoded_length": len(encoded)
                    },
                    message=f"Base64 encoded: {len(encoded)} chars"
                )

            elif action == "decode":
                try:
                    if isinstance(data, str):
                        decoded = base64.b64decode(data.encode(encoding)).decode(encoding)
                    else:
                        decoded = base64.b64decode(data).decode(encoding)

                    return ActionResult(
                        success=True,
                        data={
                            "decoded": decoded,
                            "action": "decode",
                            "decoded_length": len(decoded)
                        },
                        message=f"Base64 decoded: {len(decoded)} chars"
                    )
                except Exception as e:
                    return ActionResult(success=False, message=f"Base64 decode error: {str(e)}")

            elif action == "url_safe":
                if isinstance(data, str):
                    encoded = base64.urlsafe_b64encode(data.encode(encoding)).decode(encoding)
                else:
                    encoded = base64.urlsafe_b64encode(str(data).encode(encoding)).decode(encoding)

                return ActionResult(
                    success=True,
                    data={"encoded": encoded, "action": "url_safe"},
                    message=f"Base64 URL-safe encoded: {len(encoded)} chars"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Base64 encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"action": "encode", "encoding": "utf-8"}


class URLEncoderAction(BaseAction):
    """URL encoding/decoding."""
    action_type = "data_url_encoder"
    display_name = "URL编码器"
    description = "URL编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "encode")
            safe_chars = params.get("safe_chars", "")
            quote_via = params.get("quote_via", "quote")

            if action == "encode":
                if quote_via == "quote":
                    encoded = urllib.parse.quote(str(data), safe=safe_chars)
                elif quote_via == "quote_plus":
                    encoded = urllib.parse.quote_plus(str(data), safe=safe_chars)
                else:
                    encoded = urllib.parse.quote(str(data), safe=safe_chars)

                return ActionResult(
                    success=True,
                    data={
                        "encoded": encoded,
                        "action": "encode",
                        "original_length": len(str(data)),
                        "encoded_length": len(encoded)
                    },
                    message=f"URL encoded: {len(encoded)} chars"
                )

            elif action == "decode":
                try:
                    if quote_via == "quote_plus":
                        decoded = urllib.parse.unquote_plus(str(data))
                    else:
                        decoded = urllib.parse.unquote(str(data))

                    return ActionResult(
                        success=True,
                        data={
                            "decoded": decoded,
                            "action": "decode",
                            "decoded_length": len(decoded)
                        },
                        message=f"URL decoded: {len(decoded)} chars"
                    )
                except Exception as e:
                    return ActionResult(success=False, message=f"URL decode error: {str(e)}")

            elif action == "encode_dict":
                if not isinstance(data, dict):
                    return ActionResult(success=False, message="data must be a dict for encode_dict action")
                encoded = urllib.parse.urlencode(data, safe=safe_chars)
                return ActionResult(
                    success=True,
                    data={"encoded": encoded, "action": "encode_dict"},
                    message=f"URL encoded dict: {len(encoded)} chars"
                )

            elif action == "parse_qs":
                parsed = urllib.parse.parse_qs(str(data))
                return ActionResult(
                    success=True,
                    data={"parsed": parsed, "action": "parse_qs"},
                    message=f"URL parsed {len(parsed)} params"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"URL encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"action": "encode", "safe_chars": "", "quote_via": "quote"}


class HTMLEncoderAction(BaseAction):
    """HTML entity encoding."""
    action_type = "data_html_encoder"
    display_name = "HTML编码器"
    description = "HTML实体编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "encode")
            entities_map = params.get("entities_map", {})

            if action == "encode":
                if entities_map:
                    encoded = str(data)
                    for char, entity in entities_map.items():
                        encoded = encoded.replace(char, entity)
                else:
                    encoded = html.escape(str(data))

                return ActionResult(
                    success=True,
                    data={
                        "encoded": encoded,
                        "action": "encode",
                        "original_length": len(str(data)),
                        "encoded_length": len(encoded)
                    },
                    message=f"HTML encoded: {len(encoded)} chars"
                )

            elif action == "decode":
                decoded = html.unescape(str(data))
                return ActionResult(
                    success=True,
                    data={
                        "decoded": decoded,
                        "action": "decode",
                        "decoded_length": len(decoded)
                    },
                    message=f"HTML decoded: {len(decoded)} chars"
                )

            elif action == "encode_all":
                import re
                encoded = str(data)
                encoded = re.sub(r"[&<>\"']", lambda m: {
                    "&": "&amp;",
                    "<": "&lt;",
                    ">": "&gt;",
                    '"': "&quot;",
                    "'": "&#39;"
                }.get(m.group(), m.group()), encoded)

                return ActionResult(
                    success=True,
                    data={"encoded": encoded, "action": "encode_all"},
                    message=f"HTML fully encoded: {len(encoded)} chars"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"HTML encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"action": "encode", "entities_map": {}}


class HexEncoderAction(BaseAction):
    """Hexadecimal encoding."""
    action_type = "data_hex_encoder"
    display_name = "Hex编码器"
    description = "十六进制编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "encode")
            uppercase = params.get("uppercase", True)
            separator = params.get("separator", "")

            if action == "encode":
                data_bytes = str(data).encode("utf-8")
                if uppercase:
                    encoded = data_bytes.hex().upper()
                else:
                    encoded = data_bytes.hex()

                if separator:
                    encoded = separator.join(encoded[i:i+2] for i in range(0, len(encoded), 2))

                return ActionResult(
                    success=True,
                    data={
                        "encoded": encoded,
                        "action": "encode",
                        "original_length": len(data_bytes),
                        "encoded_length": len(encoded)
                    },
                    message=f"Hex encoded: {len(encoded)} chars"
                )

            elif action == "decode":
                clean_data = data
                if separator:
                    clean_data = data.replace(separator, "")

                try:
                    decoded = bytes.fromhex(clean_data).decode("utf-8")
                    return ActionResult(
                        success=True,
                        data={
                            "decoded": decoded,
                            "action": "decode",
                            "decoded_length": len(decoded)
                        },
                        message=f"Hex decoded: {len(decoded)} chars"
                    )
                except ValueError as e:
                    return ActionResult(success=False, message=f"Hex decode error: {str(e)}")

            elif action == "int_to_hex":
                num = int(data)
                encoded = hex(num)
                if uppercase:
                    encoded = encoded.upper()
                return ActionResult(
                    success=True,
                    data={"encoded": encoded, "action": "int_to_hex"},
                    message=f"Integer {num} -> {encoded}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Hex encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"action": "encode", "uppercase": True, "separator": ""}


class UnicodeEncoderAction(BaseAction):
    """Unicode normalization encoding."""
    action_type = "data_unicode_encoder"
    display_name = "Unicode编码器"
    description = "Unicode标准化编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "normalize")
            form = params.get("form", "NFC")

            if action == "normalize":
                import unicodedata
                normalized = unicodedata.normalize(form, str(data))

                return ActionResult(
                    success=True,
                    data={
                        "normalized": normalized,
                        "action": "normalize",
                        "form": form,
                        "original_length": len(str(data)),
                        "normalized_length": len(normalized)
                    },
                    message=f"Unicode normalized ({form}): {len(normalized)} chars"
                )

            elif action == "encode_utf8":
                encoded = str(data).encode("utf-8")
                return ActionResult(
                    success=True,
                    data={
                        "encoded": encoded,
                        "action": "encode_utf8",
                        "bytes_length": len(encoded)
                    },
                    message=f"UTF-8 encoded: {len(encoded)} bytes"
                )

            elif action == "decode_utf8":
                if isinstance(data, bytes):
                    decoded = data.decode("utf-8")
                else:
                    decoded = bytes(data, "utf-8").decode("utf-8")

                return ActionResult(
                    success=True,
                    data={
                        "decoded": decoded,
                        "action": "decode_utf8",
                        "chars_length": len(decoded)
                    },
                    message=f"UTF-8 decoded: {len(decoded)} chars"
                )

            elif action == "codepoints":
                codepoints = [ord(c) for c in str(data)]
                return ActionResult(
                    success=True,
                    data={
                        "codepoints": codepoints,
                        "action": "codepoints",
                        "count": len(codepoints)
                    },
                    message=f"Extracted {len(codepoints)} codepoints"
                )

            elif action == "from_codepoints":
                if isinstance(data, list):
                    decoded = "".join(chr(cp) for cp in data)
                else:
                    decoded = "".join(chr(cp) for cp in data)

                return ActionResult(
                    success=True,
                    data={
                        "decoded": decoded,
                        "action": "from_codepoints",
                        "chars_length": len(decoded)
                    },
                    message=f"Built string from {len(data)} codepoints"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Unicode encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"action": "normalize", "form": "NFC"}
