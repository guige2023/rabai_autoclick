"""Data encoding action module for RabAI AutoClick.

Provides data encoding:
- DataEncodingAction: Encode/decode data
- Base64EncoderAction: Base64 encoding
- URLEncoderAction: URL encoding
"""

import base64
import urllib.parse
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataEncodingAction(BaseAction):
    """Encode and decode data."""
    action_type = "data_encoding"
    display_name = "数据编码"
    description = "编码和解码数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "encode")
            data = params.get("data", "")
            encoding = params.get("encoding", "base64")

            if operation == "encode":
                if encoding == "base64":
                    result = base64.b64encode(data.encode()).decode()
                elif encoding == "url":
                    result = urllib.parse.quote(data)
                else:
                    result = data
            else:
                if encoding == "base64":
                    result = base64.b64decode(data.encode()).decode()
                elif encoding == "url":
                    result = urllib.parse.unquote(data)
                else:
                    result = data

            return ActionResult(
                success=True,
                data={
                    "operation": operation,
                    "encoding": encoding,
                    "result": result
                },
                message=f"Encoding {operation}: {encoding}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data encoding error: {str(e)}")


class Base64EncoderAction(BaseAction):
    """Base64 encoding."""
    action_type = "base64_encoder"
    display_name = "Base64编码"
    description = "Base64编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")

            encoded = base64.b64encode(data.encode()).decode()

            return ActionResult(
                success=True,
                data={"encoded": encoded},
                message=f"Base64 encoded: {len(encoded)} chars"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Base64 encoder error: {str(e)}")


class URLEncoderAction(BaseAction):
    """URL encoding."""
    action_type = "url_encoder"
    display_name = "URL编码"
    description = "URL编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")

            encoded = urllib.parse.quote(data)
            decoded = urllib.parse.unquote(encoded)

            return ActionResult(
                success=True,
                data={
                    "original": data,
                    "encoded": encoded,
                    "decoded": decoded
                },
                message=f"URL encoded: {encoded[:50]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL encoder error: {str(e)}")
