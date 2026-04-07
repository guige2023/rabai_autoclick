"""String action module for RabAI AutoClick.

Provides string manipulation operations:
- StringUpperAction: Convert to uppercase
- StringLowerAction: Convert to lowercase
- StringStripAction: Strip whitespace
- StringSplitAction: Split string
- StringJoinAction: Join strings
- StringReplaceAction: Replace substring
- StringContainsAction: Check if contains substring
- StringStartsWithAction: Check if starts with substring
- StringEndsWithAction: Check if ends with substring
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StringUpperAction(BaseAction):
    """Convert string to uppercase."""
    action_type = "string_upper"
    display_name = "转大写"
    description = "将字符串转换为大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upper conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with uppercase string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).upper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为大写: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class StringLowerAction(BaseAction):
    """Convert string to lowercase."""
    action_type = "string_lower"
    display_name = "转小写"
    description = "将字符串转换为小写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lower conversion.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with lowercase string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为小写: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result'}


class StringStripAction(BaseAction):
    """Strip whitespace from string."""
    action_type = "string_strip"
    display_name = "去除空格"
    description = "去除字符串首尾空格"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute strip operation.

        Args:
            context: Execution context.
            params: Dict with value, chars, output_var.

        Returns:
            ActionResult with stripped string.
        """
        value = params.get('value', '')
        chars = params.get('chars', None)
        output_var = params.get('output_var', 'result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = str(resolved).strip(chars)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"去除空格完成",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"去除空格失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'chars': None, 'output_var': 'result'}


class StringSplitAction(BaseAction):
    """Split string into list."""
    action_type = "string_split"
    display_name = "字符串分割"
    description = "将字符串分割为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split operation.

        Args:
            context: Execution context.
            params: Dict with value, separator, maxsplit, output_var.

        Returns:
            ActionResult with split list.
        """
        value = params.get('value', '')
        separator = params.get('separator', None)
        maxsplit = params.get('maxsplit', -1)
        output_var = params.get('output_var', 'split_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)

            if separator is not None:
                separator = context.resolve_value(separator)

            if maxsplit == -1:
                result = str(resolved).split(separator)
            else:
                result = str(resolved).split(separator, maxsplit)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"分割完成: {len(result)} 部分",
                data={
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': None, 'maxsplit': -1, 'output_var': 'split_result'}


class StringJoinAction(BaseAction):
    """Join strings with separator."""
    action_type = "string_join"
    display_name = "字符串连接"
    description = "使用分隔符连接字符串列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join operation.

        Args:
            context: Execution context.
            params: Dict with items, separator, output_var.

        Returns:
            ActionResult with joined string.
        """
        items = params.get('items', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'joined')

        valid, msg = self.validate_type(items, (list, tuple, str), 'items')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(separator, str, 'separator')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            if isinstance(items, str):
                items = [items]

            resolved_separator = context.resolve_value(separator)
            resolved_items = [context.resolve_value(item) for item in items]

            result = str(resolved_separator).join(str(x) for x in resolved_items)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接完成: {len(result)} 字符",
                data={
                    'result': result,
                    'length': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items', 'separator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'joined'}


class StringReplaceAction(BaseAction):
    """Replace substring in string."""
    action_type = "string_replace"
    display_name = "字符串替换"
    description = "替换字符串中的子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace operation.

        Args:
            context: Execution context.
            params: Dict with value, old, new, count, output_var.

        Returns:
            ActionResult with replaced string.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        count = params.get('count', -1)
        output_var = params.get('output_var', 'result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(old, str, 'old')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(new, str, 'new')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_old = context.resolve_value(old)
            resolved_new = context.resolve_value(new)

            if count == -1:
                result = str(resolved_value).replace(resolved_old, resolved_new)
            else:
                result = str(resolved_value).replace(resolved_old, resolved_new, count)

            context.set(output_var, result)

            # Count replacements
            replace_count = str(resolved_value).count(resolved_old)
            if count != -1:
                replace_count = min(replace_count, count)

            return ActionResult(
                success=True,
                message=f"替换完成: {replace_count} 处",
                data={
                    'result': result,
                    'replaced_count': replace_count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串替换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': -1, 'output_var': 'result'}


class StringContainsAction(BaseAction):
    """Check if string contains substring."""
    action_type = "string_contains"
    display_name = "字符串包含"
    description = "检查字符串是否包含子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains check.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with contains result.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'contains_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_substring = context.resolve_value(substring)

            result = resolved_substring in resolved_value
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含检查: {'是' if result else '否'}",
                data={
                    'contains': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串包含检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class StringStartsWithAction(BaseAction):
    """Check if string starts with prefix."""
    action_type = "string_starts_with"
    display_name = "字符串开头"
    description = "检查字符串是否以指定前缀开头"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute starts with check.

        Args:
            context: Execution context.
            params: Dict with value, prefix, output_var.

        Returns:
            ActionResult with starts with result.
        """
        value = params.get('value', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'starts_with_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(prefix, str, 'prefix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_prefix = context.resolve_value(prefix)

            result = str(resolved_value).startswith(resolved_prefix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"开头检查: {'是' if result else '否'}",
                data={
                    'starts_with': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串开头检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'prefix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'starts_with_result'}


class StringEndsWithAction(BaseAction):
    """Check if string ends with suffix."""
    action_type = "string_ends_with"
    display_name = "字符串结尾"
    description = "检查字符串是否以指定后缀结尾"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ends with check.

        Args:
            context: Execution context.
            params: Dict with value, suffix, output_var.

        Returns:
            ActionResult with ends with result.
        """
        value = params.get('value', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'ends_with_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(suffix, str, 'suffix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_suffix = context.resolve_value(suffix)

            result = str(resolved_value).endswith(resolved_suffix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"结尾检查: {'是' if result else '否'}",
                data={
                    'ends_with': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串结尾检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'suffix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'ends_with_result'}