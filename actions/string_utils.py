"""String utilities action module for RabAI AutoClick.

Provides string manipulation actions including transform,
split, join, replace, format, and encoding operations.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StringTransformAction(BaseAction):
    """Transform string case and format.
    
    Supports uppercase, lowercase, title case, capitalize,
    swap case, and strip operations.
    """
    action_type = "string_transform"
    display_name = "字符串转换"
    description = "转换字符串大小写和格式"

    VALID_MODES = [
        "upper", "lower", "title", "capitalize",
        "swapcase", "strip", "lstrip", "rstrip",
        "reverse", "normalize_spaces"
    ]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform a string.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, mode, strip_chars,
                   save_to_var.
        
        Returns:
            ActionResult with transformed string.
        """
        text = params.get('text', '')
        mode = params.get('mode', 'strip')
        strip_chars = params.get('strip_chars', None)
        save_to_var = params.get('save_to_var', None)

        if mode not in self.VALID_MODES:
            return ActionResult(
                success=False,
                message=f"Invalid mode: {mode}. Valid: {self.VALID_MODES}"
            )

        original = text

        if mode == 'upper':
            text = text.upper()
        elif mode == 'lower':
            text = text.lower()
        elif mode == 'title':
            text = text.title()
        elif mode == 'capitalize':
            text = text.capitalize()
        elif mode == 'swapcase':
            text = text.swapcase()
        elif mode == 'strip':
            text = text.strip(strip_chars)
        elif mode == 'lstrip':
            text = text.lstrip(strip_chars)
        elif mode == 'rstrip':
            text = text.rstrip(strip_chars)
        elif mode == 'reverse':
            text = text[::-1]
        elif mode == 'normalize_spaces':
            text = ' '.join(text.split())

        result_data = {
            'original': original,
            'result': text,
            'mode': mode,
            'length': len(text)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"转换成功: {mode}",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text', 'mode']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'strip_chars': None,
            'save_to_var': None
        }


class StringSplitAction(BaseAction):
    """Split string by delimiter or pattern.
    
    Supports fixed delimiter, regex pattern, and max splits.
    Can output as list or numbered dict.
    """
    action_type = "string_split"
    display_name = "字符串分割"
    description = "按分隔符或正则分割字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Split a string.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, delimiter, pattern,
                   maxsplit, as_dict, save_to_var.
        
        Returns:
            ActionResult with split parts.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', None)
        pattern = params.get('pattern', None)
        maxsplit = params.get('maxsplit', 0)
        as_dict = params.get('as_dict', False)
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(
                success=False,
                message="Input text is empty"
            )

        if delimiter is None and pattern is None:
            return ActionResult(
                success=False,
                message="Either 'delimiter' or 'pattern' must be specified"
            )

        # Perform split
        if pattern:
            try:
                parts = re.split(pattern, text, maxsplit=maxsplit if maxsplit else 0)
            except re.error as e:
                return ActionResult(
                    success=False,
                    message=f"Invalid regex pattern: {e}"
                )
        else:
            parts = text.split(delimiter, maxsplit=maxsplit if maxsplit else -1)

        # Clean up parts
        parts = [p for p in parts if p]

        result_data = {
            'parts': parts,
            'count': len(parts),
            'as_dict': as_dict
        }

        if as_dict:
            result_data['parts_dict'] = {str(i+1): p for i, p in enumerate(parts)}

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"分割成功: {len(parts)} 部分",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'delimiter': None,
            'pattern': None,
            'maxsplit': 0,
            'as_dict': False,
            'save_to_var': None
        }


class StringJoinAction(BaseAction):
    """Join list of strings with separator.
    
    Supports custom separators, prefix, suffix,
    and filtering empty parts.
    """
    action_type = "string_join"
    display_name = "字符串连接"
    description = "用分隔符连接字符串列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Join strings.
        
        Args:
            context: Execution context.
            params: Dict with keys: parts, separator, prefix,
                   suffix, skip_empty, save_to_var.
        
        Returns:
            ActionResult with joined string.
        """
        parts = params.get('parts', [])
        separator = params.get('separator', '')
        prefix = params.get('prefix', '')
        suffix = params.get('suffix', '')
        skip_empty = params.get('skip_empty', True)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(parts, (list, tuple, str)):
            return ActionResult(
                success=False,
                message=f"Parts must be list or string, got {type(parts).__name__}"
            )

        # Handle string input (split behavior)
        if isinstance(parts, str):
            if separator:
                parts = parts.split(separator)
            else:
                parts = list(parts)

        # Filter empty if requested
        if skip_empty:
            parts = [p for p in parts if p]

        # Join
        result = separator.join(str(p) for p in parts)

        # Apply prefix/suffix
        if prefix:
            result = prefix + result
        if suffix:
            result = result + suffix

        result_data = {
            'result': result,
            'parts_count': len(parts),
            'length': len(result)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"连接成功: {len(parts)} 部分 -> {len(result)} 字符",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['parts', 'separator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'prefix': '',
            'suffix': '',
            'skip_empty': True,
            'save_to_var': None
        }


class StringReplaceAction(BaseAction):
    """Replace substrings in string.
    
    Supports fixed string replacement, regex pattern replacement,
    case-insensitive mode, and count limit.
    """
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "替换字符串中的子串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Replace substrings in string.
        
        Args:
            context: Execution context.
            params: Dict with keys: text, old, new, pattern,
                   count, ignore_case, regex, save_to_var.
        
        Returns:
            ActionResult with replaced string.
        """
        text = params.get('text', '')
        old = params.get('old', None)
        new = params.get('new', '')
        pattern = params.get('pattern', None)
        count = params.get('count', 0)
        ignore_case = params.get('ignore_case', False)
        regex = params.get('regex', False)
        save_to_var = params.get('save_to_var', None)

        if not text:
            return ActionResult(
                success=False,
                message="Input text is empty"
            )

        original = text

        if pattern:
            flags = re.IGNORECASE if ignore_case else 0
            try:
                if regex:
                    text = re.sub(pattern, new, text, count=count if count else 0, flags=flags)
                else:
                    text = re.sub(re.escape(pattern), new, text, count=count if count else 0, flags=flags)
            except re.error as e:
                return ActionResult(
                    success=False,
                    message=f"Invalid regex pattern: {e}"
                )
        elif old is not None:
            if ignore_case:
                pattern_obj = re.compile(re.escape(old), re.IGNORECASE)
                text = pattern_obj.sub(new, text, count=count if count else 0)
            else:
                text = text.replace(old, new, count if count else -1)
        else:
            return ActionResult(
                success=False,
                message="Either 'old' or 'pattern' must be specified"
            )

        result_data = {
            'original': original,
            'result': text,
            'replacements': original.count(old) if old and not pattern else None
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"替换成功",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'old': None,
            'new': '',
            'pattern': None,
            'count': 0,
            'ignore_case': False,
            'regex': False,
            'save_to_var': None
        }


class StringFormatAction(BaseAction):
    """Format string with template placeholders.
    
    Supports Python format string syntax, f-string style,
    and dictionary/key-value variable substitution.
    """
    action_type = "string_format"
    display_name = "字符串格式化"
    description = "格式化字符串模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Format a string template.
        
        Args:
            context: Execution context.
            params: Dict with keys: template, vars (dict),
                   save_to_var.
        
        Returns:
            ActionResult with formatted string.
        """
        template = params.get('template', '')
        vars_dict = params.get('vars', {})
        save_to_var = params.get('save_to_var', None)

        if not template:
            return ActionResult(
                success=False,
                message="Template string is empty"
            )

        # Merge context variables if available
        if hasattr(context, 'variables'):
            merged_vars = dict(context.variables)
            merged_vars.update(vars_dict or {})
        else:
            merged_vars = vars_dict or {}

        try:
            result = template
            # Replace {key} placeholders
            for key, value in merged_vars.items():
                placeholder = '{' + key + '}'
                if placeholder in result:
                    result = result.replace(placeholder, str(value))

            # Handle $variable style
            for key, value in merged_vars.items():
                dollar_style = '$' + key
                if dollar_style in result:
                    result = result.replace(dollar_style, str(value))

            result_data = {
                'result': result,
                'template': template,
                'vars_used': list(merged_vars.keys())
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message="格式化成功",
                data=result_data
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['template']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'vars': {},
            'save_to_var': None
        }
