"""Text8 action module for RabAI AutoClick.

Provides additional text operations:
- TextLengthAction: Get text length
- TextContainsAction: Check if text contains substring
- TextStartsWithAction: Check if text starts with prefix
- TextEndsWithAction: Check if text ends with suffix
- TextSplitAction: Split text
- TextJoinAction: Join text
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextLengthAction(BaseAction):
    """Get text length."""
    action_type = "text8_length"
    display_name = "文本长度"
    description = "获取文本长度"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text length.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with text length.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'text_length')

        try:
            resolved = context.resolve_value(text)

            if isinstance(resolved, str):
                result = len(resolved)
            elif isinstance(resolved, (list, tuple)):
                result = len(resolved)
            else:
                result = len(str(resolved))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本长度: {result}",
                data={
                    'text': resolved,
                    'length': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取文本长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_length'}


class TextContainsAction(BaseAction):
    """Check if text contains substring."""
    action_type = "text8_contains"
    display_name = "文本包含"
    description = "检查文本是否包含子串"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text contains.

        Args:
            context: Execution context.
            params: Dict with text, substring, output_var.

        Returns:
            ActionResult with contains result.
        """
        text = params.get('text', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'contains_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_substring = context.resolve_value(substring)

            result = resolved_substring in resolved_text
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本包含: {'是' if result else '否'}",
                data={
                    'text': resolved_text,
                    'substring': resolved_substring,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文本包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}


class TextStartsWithAction(BaseAction):
    """Check if text starts with prefix."""
    action_type = "text8_startswith"
    display_name = "文本开头"
    description = "检查文本是否以子串开头"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text starts with.

        Args:
            context: Execution context.
            params: Dict with text, prefix, output_var.

        Returns:
            ActionResult with starts with result.
        """
        text = params.get('text', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'startswith_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_prefix = context.resolve_value(prefix)

            result = resolved_text.startswith(resolved_prefix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本开头: {'是' if result else '否'}",
                data={
                    'text': resolved_text,
                    'prefix': resolved_prefix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文本开头失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'prefix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'startswith_result'}


class TextEndsWithAction(BaseAction):
    """Check if text ends with suffix."""
    action_type = "text8_endswith"
    display_name = "文本结尾"
    description = "检查文本是否以子串结尾"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text ends with.

        Args:
            context: Execution context.
            params: Dict with text, suffix, output_var.

        Returns:
            ActionResult with ends with result.
        """
        text = params.get('text', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'endswith_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_suffix = context.resolve_value(suffix)

            result = resolved_text.endswith(resolved_suffix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本结尾: {'是' if result else '否'}",
                data={
                    'text': resolved_text,
                    'suffix': resolved_suffix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查文本结尾失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text', 'suffix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'endswith_result'}


class TextSplitAction(BaseAction):
    """Split text."""
    action_type = "text8_split"
    display_name = "文本分割"
    description = "分割文本"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text split.

        Args:
            context: Execution context.
            params: Dict with text, delimiter, maxsplit, output_var.

        Returns:
            ActionResult with split result.
        """
        text = params.get('text', '')
        delimiter = params.get('delimiter', None)
        maxsplit = params.get('maxsplit', -1)
        output_var = params.get('output_var', 'split_result')

        try:
            resolved_text = context.resolve_value(text)
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else None
            resolved_maxsplit = int(context.resolve_value(maxsplit)) if maxsplit else -1

            if resolved_delimiter is None:
                result = resolved_text.split(maxsplit=resolved_maxsplit)
            else:
                result = resolved_text.split(resolved_delimiter, maxsplit=resolved_maxsplit)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本分割: {len(result)}部分",
                data={
                    'text': resolved_text,
                    'delimiter': resolved_delimiter,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"分割文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': None, 'maxsplit': -1, 'output_var': 'split_result'}


class TextJoinAction(BaseAction):
    """Join text."""
    action_type = "text8_join"
    display_name = "文本连接"
    description = "连接文本"
    version = "8.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute text join.

        Args:
            context: Execution context.
            params: Dict with list, separator, output_var.

        Returns:
            ActionResult with joined text.
        """
        list_param = params.get('list', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'joined_text')

        try:
            resolved_list = context.resolve_value(list_param)
            resolved_separator = context.resolve_value(separator) if separator else ''

            if not isinstance(resolved_list, (list, tuple)):
                resolved_list = [resolved_list]

            result = resolved_separator.join(str(x) for x in resolved_list)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"文本连接: {len(result)}字符",
                data={
                    'list': resolved_list,
                    'separator': resolved_separator,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接文本失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'joined_text'}