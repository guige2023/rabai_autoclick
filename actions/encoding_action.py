"""Encoding/decoding action module for RabAI AutoClick.

Provides encoding operations:
- EncodeBase64Action: Base64 encode/decode
- EncodeHexAction: Hex encode/decode
- EncodeUrlAction: URL encode/decode
- EncodeHtmlAction: HTML encode/decode
- EncodeJsonAction: JSON encode/decode
- EncodeUnicodeAction: Unicode normalization
"""

import base64
import urllib.parse
import html
import unicodedata
import json
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EncodeBase64Action(BaseAction):
    """Base64 encode/decode."""
    action_type = "encode_base64"
    display_name = "Base64编码"
    description = "Base64编码/解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            action = params.get("action", "encode")

            if not text:
                return ActionResult(success=False, message="text is required")

            if action == "encode":
                encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
                return ActionResult(success=True, message="Base64 encoded", data={"result": encoded})

            elif action == "decode":
                try:
                    decoded = base64.b64decode(text.encode("ascii")).decode("utf-8")
                    return ActionResult(success=True, message="Base64 decoded", data={"result": decoded})
                except Exception as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Base64 error: {str(e)}")


class EncodeHexAction(BaseAction):
    """Hex encode/decode."""
    action_type = "encode_hex"
    display_name = "Hex编码"
    description = "Hex编码/解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            action = params.get("action", "encode")
            separator = params.get("separator", "")

            if not text:
                return ActionResult(success=False, message="text is required")

            if action == "encode":
                if separator:
                    hex_str = separator.join(f"{ord(c):02x}" for c in text)
                else:
                    hex_str = text.encode("utf-8").hex()
                return ActionResult(success=True, message="Hex encoded", data={"result": hex_str})

            elif action == "decode":
                try:
                    clean_hex = text.replace(separator, "") if separator else text
                    decoded = bytes.fromhex(clean_hex).decode("utf-8")
                    return ActionResult(success=True, message="Hex decoded", data={"result": decoded})
                except Exception as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Hex error: {str(e)}")


class EncodeUrlAction(BaseAction):
    """URL encode/decode."""
    action_type = "encode_url"
    display_name = "URL编码"
    description = "URL编码/解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            action = params.get("action", "encode")
            safe_chars = params.get("safe_chars", "")

            if not text:
                return ActionResult(success=False, message="text is required")

            if action == "encode":
                encoded = urllib.parse.quote(text, safe=safe_chars)
                return ActionResult(success=True, message="URL encoded", data={"result": encoded})

            elif action == "decode":
                try:
                    decoded = urllib.parse.unquote(text)
                    return ActionResult(success=True, message="URL decoded", data={"result": decoded})
                except Exception as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            elif action == "encode_component":
                encoded = urllib.parse.quote_plus(text)
                return ActionResult(success=True, message="URL component encoded", data={"result": encoded})

            elif action == "decode_component":
                try:
                    decoded = urllib.parse.unquote_plus(text)
                    return ActionResult(success=True, message="URL component decoded", data={"result": decoded})
                except Exception as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"URL encode error: {str(e)}")


class EncodeHtmlAction(BaseAction):
    """HTML encode/decode."""
    action_type = "encode_html"
    display_name = "HTML编码"
    description = "HTML编码/解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            action = params.get("action", "encode")

            if not text:
                return ActionResult(success=False, message="text is required")

            if action == "encode":
                encoded = html.escape(text)
                return ActionResult(success=True, message="HTML encoded", data={"result": encoded})

            elif action == "decode":
                try:
                    decoded = html.unescape(text)
                    return ActionResult(success=True, message="HTML decoded", data={"result": decoded})
                except Exception as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"HTML encode error: {str(e)}")


class EncodeJsonAction(BaseAction):
    """JSON encode/decode."""
    action_type = "encode_json"
    display_name = "JSON编码"
    description = "JSON编码/解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            action = params.get("action", "encode")
            indent = params.get("indent", 2)

            if action == "encode":
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except:
                        return ActionResult(success=False, message="Invalid JSON string")

                encoded = json.dumps(data, indent=indent, ensure_ascii=False)
                return ActionResult(success=True, message="JSON encoded", data={"result": encoded})

            elif action == "decode":
                if not isinstance(data, str):
                    data = str(data)

                try:
                    decoded = json.loads(data)
                    return ActionResult(success=True, message="JSON decoded", data={"result": decoded})
                except json.JSONDecodeError as e:
                    return ActionResult(success=False, message=f"Decode error: {str(e)}")

            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"JSON encode error: {str(e)}")


class EncodeUnicodeAction(BaseAction):
    """Unicode normalization."""
    action_type = "encode_unicode"
    display_name = "Unicode规范化"
    description = "Unicode规范化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            form = params.get("form", "NFC")

            if not text:
                return ActionResult(success=False, message="text is required")

            form_map = {
                "NFC": unicodedata.normalize("NFC", text),
                "NFD": unicodedata.normalize("NFD", text),
                "NFKC": unicodedata.normalize("NFKC", text),
                "NFKD": unicodedata.normalize("NFKD", text)
            }

            if form not in form_map:
                return ActionResult(success=False, message=f"Unknown form: {form}")

            normalized = form_map[form]

            return ActionResult(
                success=True,
                message=f"Unicode normalized to {form}",
                data={"result": normalized, "form": form}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Unicode error: {str(e)}")
