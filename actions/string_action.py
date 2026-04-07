"""String manipulation action module for RabAI AutoClick.

Provides string operations:
- StringUpperAction: Convert to uppercase
- StringLowerAction: Convert to lowercase
- StringStripAction: Strip whitespace
- StringReplaceAction: Replace substring
- StringSplitAction: Split string
- StringJoinAction: Join strings
- StringRegexAction: Regex replace
- StringContainsAction: Check if contains
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringUpperAction(BaseAction):
    """Convert string to uppercase."""
    action_type = "string_upper"
    display_name = "字符串大写"
    description = "转换为大写"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute uppercase."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'upper_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            result = str(resolved).upper()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result, data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Upper error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'upper_result'}


class StringLowerAction(BaseAction):
    """Convert string to lowercase."""
    action_type = "string_lower"
    display_name = "字符串小写"
    description = "转换为小写"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute lowercase."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'lower_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            result = str(resolved).lower()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result, data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Lower error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lower_result'}


class StringStripAction(BaseAction):
    """Strip whitespace."""
    action_type = "string_strip"
    display_name = "字符串去空格"
    description = "去除首尾空格"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute strip."""
        value = params.get('value', '')
        chars = params.get('chars', None)
        side = params.get('side', 'both')  # both, left, right
        output_var = params.get('output_var', 'strip_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_chars = context.resolve_value(chars) if context else chars
            resolved_side = context.resolve_value(side) if context else side

            if resolved_chars:
                if resolved_side == 'left':
                    result = str(resolved).lstrip(resolved_chars)
                elif resolved_side == 'right':
                    result = str(resolved).rstrip(resolved_chars)
                else:
                    result = str(resolved).strip(resolved_chars)
            else:
                if resolved_side == 'left':
                    result = str(resolved).lstrip()
                elif resolved_side == 'right':
                    result = str(resolved).rstrip()
                else:
                    result = str(resolved).strip()

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result[:100], data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Strip error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'side': 'both', 'output_var': 'strip_result'}


class StringReplaceAction(BaseAction):
    """Replace substring."""
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "替换字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute replace."""
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        count = params.get('count', -1)
        output_var = params.get('output_var', 'replace_result')

        if not value or not old:
            return ActionResult(success=False, message="value and old are required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_old = context.resolve_value(old) if context else old
            resolved_new = context.resolve_value(new) if context else new
            resolved_count = context.resolve_value(count) if context else count

            result = str(resolved).replace(resolved_old, resolved_new, resolved_count)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Replaced {resolved_old} -> {resolved_new}", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Replace error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'old']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'new': '', 'count': -1, 'output_var': 'replace_result'}


class StringSplitAction(BaseAction):
    """Split string."""
    action_type = "string_split"
    display_name = "字符串分割"
    description = "分割字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute split."""
        value = params.get('value', '')
        separator = params.get('separator', ' ')
        maxsplit = params.get('maxsplit', -1)
        output_var = params.get('output_var', 'split_result')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_sep = context.resolve_value(separator) if context else separator
            resolved_max = context.resolve_value(maxsplit) if context else maxsplit

            if resolved_max >= 0:
                parts = str(resolved).split(resolved_sep, resolved_max)
            else:
                parts = str(resolved).split(resolved_sep)

            result = {'parts': parts, 'count': len(parts)}
            if context:
                context.set(output_var, parts)
            return ActionResult(success=True, message=f"Split into {len(parts)} parts", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Split error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': ' ', 'maxsplit': -1, 'output_var': 'split_result'}


class StringJoinAction(BaseAction):
    """Join strings."""
    action_type = "string_join"
    display_name = "字符串连接"
    description = "连接字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute join."""
        strings = params.get('strings', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'join_result')

        if not strings:
            return ActionResult(success=False, message="strings is required")

        try:
            resolved_strings = context.resolve_value(strings) if context else strings
            resolved_sep = context.resolve_value(separator) if context else separator

            result = resolved_sep.join(str(s) for s in resolved_strings)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result[:100], data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Join error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['strings']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'join_result'}


class StringRegexAction(BaseAction):
    """Regex replace."""
    action_type = "string_regex"
    display_name = "正则替换"
    description = "正则表达式替换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex replace."""
        value = params.get('value', '')
        pattern = params.get('pattern', '')
        replacement = params.get('replacement', '')
        flags = params.get('flags', 0)  # 0=none, 1=IGNORECASE, 2=MULTILINE
        output_var = params.get('output_var', 'regex_result')

        if not value or not pattern:
            return ActionResult(success=False, message="value and pattern are required")

        try:
            import re as re_module

            resolved = context.resolve_value(value) if context else value
            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_replacement = context.resolve_value(replacement) if context else replacement
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            result = re_module.sub(resolved_pattern, resolved_replacement, str(resolved), flags=flags_int)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Regex replaced", data={'result': result})
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'replacement': '', 'flags': 0, 'output_var': 'regex_result'}


class StringContainsAction(BaseAction):
    """Check if string contains substring."""
    action_type = "string_contains"
    display_name = "字符串包含检查"
    description = "检查字符串是否包含"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute contains check."""
        value = params.get('value', '')
        substring = params.get('substring', '')
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'contains_result')

        if not value or not substring:
            return ActionResult(success=False, message="value and substring are required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_sub = context.resolve_value(substring) if context else substring
            resolved_cs = context.resolve_value(case_sensitive) if context else case_sensitive

            if resolved_cs:
                result = resolved_sub in str(resolved)
            else:
                result = resolved_sub.lower() in str(resolved).lower()

            result_data = {'contains': result, 'substring': resolved_sub, 'case_sensitive': resolved_cs}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Contains: {result}", data=result_data)
        except Exception as e:
            return ActionResult(success=False, message=f"Contains error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'case_sensitive': True, 'output_var': 'contains_result'}
