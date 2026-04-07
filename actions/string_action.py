"""String operations action module for RabAI AutoClick.

Provides string operations:
- StringCaseAction: Case conversion
- StringTrimAction: Trim whitespace
- StringReplaceAction: Find and replace
- StringSplitAction: Split strings
- StringJoinAction: Join strings
- StringPadAction: Padding strings
"""

import re
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringCaseAction(BaseAction):
    """Case conversion."""
    action_type = "string_case"
    display_name = "大小写转换"
    description = "字符串大小写转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            operation = params.get("operation", "lower")

            if not text:
                return ActionResult(success=False, message="text is required")

            if operation == "lower":
                result = text.lower()
            elif operation == "upper":
                result = text.upper()
            elif operation == "title":
                result = text.title()
            elif operation == "capitalize":
                result = text.capitalize()
            elif operation == "swapcase":
                result = text.swapcase()
            elif operation == "casefold":
                result = text.casefold()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"Converted to {operation}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Case error: {str(e)}")


class StringTrimAction(BaseAction):
    """Trim whitespace."""
    action_type = "string_trim"
    display_name = "去除空白"
    description = "去除字符串空白"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            operation = params.get("operation", "both")

            if not text:
                return ActionResult(success=False, message="text is required")

            if operation == "both":
                result = text.strip()
            elif operation == "left":
                result = text.lstrip()
            elif operation == "right":
                result = text.rstrip()
            elif operation == "normalize":
                result = " ".join(text.split())
            elif operation == "remove_all":
                result = text.replace(" ", "")
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"Trimmed with {operation}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Trim error: {str(e)}")


class StringReplaceAction(BaseAction):
    """Find and replace."""
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "查找替换字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            old = params.get("old", "")
            new = params.get("new", "")
            count = params.get("count", -1)
            case_sensitive = params.get("case_sensitive", True)

            if not text:
                return ActionResult(success=False, message="text is required")

            if count == -1:
                if case_sensitive:
                    result = text.replace(old, new)
                else:
                    result = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
            else:
                if case_sensitive:
                    result = text.replace(old, new, count)
                else:
                    result = re.sub(re.escape(old), new, text, count=count, flags=re.IGNORECASE)

            return ActionResult(success=True, message=f"Replaced", data={"result": result, "original": text})

        except Exception as e:
            return ActionResult(success=False, message=f"Replace error: {str(e)}")


class StringSplitAction(BaseAction):
    """Split strings."""
    action_type = "string_split"
    display_name = "字符串分割"
    description = "分割字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            delimiter = params.get("delimiter", None)
            max_split = params.get("max_split", 0)

            if not text:
                return ActionResult(success=False, message="text is required")

            if delimiter is None:
                parts = text.split()
            elif delimiter == "":
                parts = list(text)
            else:
                if max_split > 0:
                    parts = text.split(delimiter, max_split)
                else:
                    parts = text.split(delimiter)

            return ActionResult(success=True, message=f"Split into {len(parts)} parts", data={"parts": parts, "count": len(parts)})

        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")


class StringJoinAction(BaseAction):
    """Join strings."""
    action_type = "string_join"
    display_name = "字符串连接"
    description = "连接字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            strings = params.get("strings", [])
            delimiter = params.get("delimiter", "")
            prefix = params.get("prefix", "")
            suffix = params.get("suffix", "")

            if not strings:
                return ActionResult(success=False, message="strings list is required")

            if prefix or suffix:
                strings = [f"{prefix}{s}{suffix}" for s in strings]

            result = delimiter.join(strings)

            return ActionResult(success=True, message=f"Joined {len(strings)} strings", data={"result": result, "count": len(strings)})

        except Exception as e:
            return ActionResult(success=False, message=f"Join error: {str(e)}")


class StringPadAction(BaseAction):
    """Padding strings."""
    action_type = "string_pad"
    display_name = "字符串填充"
    description = "填充字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            text = params.get("text", "")
            width = params.get("width", 10)
            fill_char = params.get("fill_char", " ")
            operation = params.get("operation", "left")

            if not text:
                return ActionResult(success=False, message="text is required")

            if len(text) >= width:
                return ActionResult(success=True, message="Text already meets width", data={"result": text})

            if operation == "left":
                result = text.rjust(width, fill_char)
            elif operation == "right":
                result = text.ljust(width, fill_char)
            elif operation == "center":
                result = text.center(width, fill_char)
            elif operation == "zfill":
                result = text.zfill(width)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

            return ActionResult(success=True, message=f"Padded to width {width}", data={"result": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Pad error: {str(e)}")
