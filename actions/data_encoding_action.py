"""Data encoding action module for RabAI AutoClick.

Provides data encoding and transformation operations:
- Base64EncodeAction: Base64 encoding/decoding
- UrlEncodeAction: URL encoding/decoding
- HtmlEncodeAction: HTML encoding/decoding
- HexEncodeAction: Hex encoding/decoding
"""

import base64
import urllib.parse
import html
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Base64EncodeAction(BaseAction):
    """Base64 encoding/decoding."""
    action_type = "base64_encode"
    display_name = "Base64编码"
    description = "Base64编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", "")
            field = params.get("field", None)

            if field:
                if isinstance(context, dict):
                    data = context.get(field, "")
                elif hasattr(context, field):
                    data = getattr(context, field)

            if not data:
                return ActionResult(success=False, message="data is required")

            if action == "encode":
                if isinstance(data, str):
                    encoded = base64.b64encode(data.encode("utf-8")).decode("ascii")
                else:
                    encoded = base64.b64encode(data).decode("ascii")
                return ActionResult(success=True, message="Base64 encoded", data={"encoded": encoded})

            elif action == "decode":
                if isinstance(data, str):
                    try:
                        decoded = base64.b64decode(data.encode("ascii")).decode("utf-8")
                    except Exception:
                        decoded = base64.b64decode(data.encode("ascii"))
                else:
                    decoded = base64.b64decode(data)
                return ActionResult(success=True, message="Base64 decoded", data={"decoded": decoded})

            elif action == "encode_batch":
                if not isinstance(data, list):
                    data = [data]
                encoded_batch = []
                for item in data:
                    if isinstance(item, str):
                        encoded_batch.append(base64.b64encode(item.encode("utf-8")).decode("ascii"))
                    else:
                        encoded_batch.append(base64.b64encode(item).decode("ascii"))
                return ActionResult(success=True, message=f"Base64 encoded {len(encoded_batch)} items", data={"encoded": encoded_batch, "count": len(encoded_batch)})

            elif action == "decode_batch":
                if not isinstance(data, list):
                    data = [data]
                decoded_batch = []
                for item in data:
                    try:
                        decoded_batch.append(base64.b64decode(item.encode("ascii")).decode("utf-8"))
                    except Exception:
                        decoded_batch.append(base64.b64decode(item.encode("ascii")))
                return ActionResult(success=True, message=f"Base64 decoded {len(decoded_batch)} items", data={"decoded": decoded_batch, "count": len(decoded_batch)})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Base64Encode error: {e}")


class UrlEncodeAction(BaseAction):
    """URL encoding/decoding."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "URL编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", "")
            safe_chars = params.get("safe_chars", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            if action == "encode":
                if safe_chars:
                    encoded = urllib.parse.quote(str(data), safe=safe_chars)
                else:
                    encoded = urllib.parse.quote(str(data))
                return ActionResult(success=True, message="URL encoded", data={"encoded": encoded})

            elif action == "decode":
                decoded = urllib.parse.unquote(str(data))
                return ActionResult(success=True, message="URL decoded", data={"decoded": decoded})

            elif action == "encode_dict":
                if not isinstance(data, dict):
                    return ActionResult(success=False, message="data must be dict for encode_dict")
                encoded = urllib.parse.urlencode(data)
                return ActionResult(success=True, message="URL encoded dict", data={"encoded": encoded})

            elif action == "decode_dict":
                decoded = urllib.parse.parse_qs(str(data))
                decoded_flat = {k: v[0] if len(v) == 1 else v for k, v in decoded.items()}
                return ActionResult(success=True, message="URL decoded to dict", data={"decoded": decoded_flat})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"UrlEncode error: {e}")


class HtmlEncodeAction(BaseAction):
    """HTML encoding/decoding."""
    action_type = "html_encode"
    display_name = "HTML编码"
    description = "HTML编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            if action == "encode":
                encoded = html.escape(str(data))
                return ActionResult(success=True, message="HTML encoded", data={"encoded": encoded})

            elif action == "decode":
                decoded = html.unescape(str(data))
                return ActionResult(success=True, message="HTML decoded", data={"decoded": decoded})

            elif action == "encode_batch":
                if not isinstance(data, list):
                    data = [data]
                encoded_batch = [html.escape(str(item)) for item in data]
                return ActionResult(success=True, message=f"HTML encoded {len(encoded_batch)} items", data={"encoded": encoded_batch, "count": len(encoded_batch)})

            elif action == "decode_batch":
                if not isinstance(data, list):
                    data = [data]
                decoded_batch = [html.unescape(str(item)) for item in data]
                return ActionResult(success=True, message=f"HTML decoded {len(decoded_batch)} items", data={"decoded": decoded_batch, "count": len(decoded_batch)})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"HtmlEncode error: {e}")


class HexEncodeAction(BaseAction):
    """Hex encoding/decoding."""
    action_type = "hex_encode"
    display_name = "Hex编码"
    description = "十六进制编码和解码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "encode")
            data = params.get("data", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            if action == "encode":
                if isinstance(data, str):
                    encoded = data.encode("utf-8").hex()
                else:
                    encoded = data.hex()
                return ActionResult(success=True, message="Hex encoded", data={"encoded": encoded})

            elif action == "decode":
                if isinstance(data, str):
                    decoded = bytes.fromhex(data).decode("utf-8")
                else:
                    decoded = bytes.fromhex(data).decode("utf-8")
                return ActionResult(success=True, message="Hex decoded", data={"decoded": decoded})

            elif action == "encode_batch":
                if not isinstance(data, list):
                    data = [data]
                encoded_batch = []
                for item in data:
                    if isinstance(item, str):
                        encoded_batch.append(item.encode("utf-8").hex())
                    else:
                        encoded_batch.append(item.hex())
                return ActionResult(success=True, message=f"Hex encoded {len(encoded_batch)} items", data={"encoded": encoded_batch, "count": len(encoded_batch)})

            elif action == "decode_batch":
                if not isinstance(data, list):
                    data = [data]
                decoded_batch = []
                for item in data:
                    try:
                        decoded_batch.append(bytes.fromhex(item).decode("utf-8"))
                    except Exception:
                        decoded_batch.append(bytes.fromhex(item))
                return ActionResult(success=True, message=f"Hex decoded {len(decoded_batch)} items", data={"decoded": decoded_batch, "count": len(decoded_batch)})

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"HexEncode error: {e}")
