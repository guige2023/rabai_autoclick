"""String action module for RabAI AutoClick.

Provides string operations:
- StringUpperAction: Convert to uppercase
- StringLowerAction: Convert to lowercase
- StringTitleAction: Title case
- StringStripAction: Strip whitespace
- StringReplaceAction: Replace substring
- StringSplitAction: Split string
- StringJoinAction: Join strings
- StringContainsAction: Check substring
- StringLengthAction: Get length
- StringReverseAction: Reverse string
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringUpperAction(BaseAction):
    """Convert to uppercase."""
    action_type = "string_upper"
    display_name = "转大写"
    description = "转换为大写"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upper.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with uppercase string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            result = resolved_text.upper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大写: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转大写失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class StringLowerAction(BaseAction):
    """Convert to lowercase."""
    action_type = "string_lower"
    display_name = "转小写"
    description = "转换为小写"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lower.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with lowercase string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            result = resolved_text.lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"小写: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转小写失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class StringTitleAction(BaseAction):
    """Title case."""
    action_type = "string_title"
    display_name = "首字母大写"
    description = "首字母大写"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with title string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            result = resolved_text.title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标题: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"首字母大写失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class StringStripAction(BaseAction):
    """Strip whitespace."""
    action_type = "string_strip"
    display_name = "去除空格"
    description = "去除首尾空格"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip.

        Args:
            context: Execution context.
            params: Dict with text, chars, output_var.

        Returns:
            ActionResult with stripped string.
        """
        text = params.get('text', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_chars = context.resolve_value(chars) if chars else None

            if resolved_chars:
                result = resolved_text.strip(resolved_chars)
            else:
                result = resolved_text.strip()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去空格: [{result}]",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"去除空格失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'output_var': 'string_result'}


class StringReplaceAction(BaseAction):
    """Replace substring."""
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "替换字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace.

        Args:
            context: Execution context.
            params: Dict with text, old, new, output_var.

        Returns:
            ActionResult with replaced string.
        """
        text = params.get('text', '')
        old = params.get('old', '')
        new = params.get('new', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)

            result = resolved_text.replace(resolved_old, resolved_new)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已替换: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串替换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class StringSplitAction(BaseAction):
    """Split string."""
    action_type = "string_split"
    display_name = "字符串分割"
    description = "分割字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split.

        Args:
            context: Execution context.
            params: Dict with text, delimiter, maxsplit, output_var.

        Returns:
            ActionResult with split parts.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', ' ')
        maxsplit = params.get('maxsplit', 0)
        output_var = params.get('output_var', 'string_parts')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_delim = context.resolve_value(delimiter)
            resolved_maxsplit = context.resolve_value(maxsplit)

            parts = resolved_text.split(resolved_delim, resolved_maxsplit)
            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"已分割: {len(parts)} 部分",
                data={'parts': parts, 'count': len(parts), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串分割失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text', 'delimiter']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'maxsplit': 0, 'output_var': 'string_parts'}


class StringJoinAction(BaseAction):
    """Join strings."""
    action_type = "string_join"
    display_name = "字符串连接"
    description = "连接字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join.

        Args:
            context: Execution context.
            params: Dict with strings, delimiter, output_var.

        Returns:
            ActionResult with joined string.
        """
        strings = params.get('strings', [])
        delimiter = params.get('delimiter', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(strings, list, 'strings')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_strings = context.resolve_value(strings)
            resolved_delim = context.resolve_value(delimiter)

            result = resolved_delim.join(str(s) for s in resolved_strings)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已连接: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串连接失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['strings', 'delimiter']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}


class StringContainsAction(BaseAction):
    """Check substring."""
    action_type = "string_contains"
    display_name = "字符串包含"
    description = "检查是否包含子串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with text, substring, output_var.

        Returns:
            ActionResult with contains flag.
        """
        text = params.get('text', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'string_contains')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_sub = context.resolve_value(substring)

            result = resolved_sub in resolved_text
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"{'包含' if result else '不包含'}: {resolved_sub}",
                data={'contains': result, 'substring': resolved_sub, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串包含检查失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_contains'}


class StringLengthAction(BaseAction):
    """Get string length."""
    action_type = "string_length"
    display_name = "字符串长度"
    description = "获取字符串长度"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute length.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with length.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'string_length')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            length = len(resolved_text)

            context.set(output_var, length)

            return ActionResult(
                success=True,
                message=f"长度: {length}",
                data={'length': length, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取字符串长度失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_length'}


class StringReverseAction(BaseAction):
    """Reverse string."""
    action_type = "string_reverse"
    display_name = "字符串反转"
    description = "反转字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with reversed string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'string_result')

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            result = resolved_text[::-1]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字符串反转失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_result'}
