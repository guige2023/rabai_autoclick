"""Text action module for RabAI AutoClick.

Provides text operations:
- TextReverseAction: Reverse string
- TextCountAction: Count substring occurrences
- TextFindAction: Find substring position
- TextReplaceAllAction: Replace all occurrences
- TextWordCountAction: Count words
- TextLineCountAction: Count lines
- TextContainsAction: Check if contains substring
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TextReverseAction(BaseAction):
    """Reverse string."""
    action_type = "text_reverse"
    display_name = "反转字符串"
    description = "反转字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute reverse.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with reversed string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = resolved[::-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"反转完成: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_result'}


class TextCountAction(BaseAction):
    """Count substring occurrences."""
    action_type = "text_count"
    display_name = "计数子串"
    description = "统计子串出现次数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with count.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)
            result = resolved_value.count(resolved_sub)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"计数: {result}",
                data={
                    'count': result,
                    'substring': resolved_sub,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_result'}


class TextFindAction(BaseAction):
    """Find substring position."""
    action_type = "text_find"
    display_name = "查找子串位置"
    description = "查找子串位置"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find.

        Args:
            context: Execution context.
            params: Dict with value, substring, start, end, output_var.

        Returns:
            ActionResult with position.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        start = params.get('start', 0)
        end = params.get('end', None)
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)
            resolved_start = context.resolve_value(start)
            resolved_end = context.resolve_value(end) if end else None

            pos = resolved_value.find(resolved_sub, int(resolved_start), resolved_end if resolved_end is None else int(resolved_end))
            found = pos != -1
            context.set(output_var, pos)

            return ActionResult(
                success=True,
                message=f"查找: {'找到' if found else '未找到'} at {pos}",
                data={
                    'position': pos,
                    'found': found,
                    'substring': resolved_sub,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查找子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': 0, 'end': None, 'output_var': 'text_result'}


class TextReplaceAllAction(BaseAction):
    """Replace all occurrences."""
    action_type = "text_replace_all"
    display_name = "替换所有"
    description = "替换所有子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace all.

        Args:
            context: Execution context.
            params: Dict with value, old, new, output_var.

        Returns:
            ActionResult with replaced string.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        output_var = params.get('output_var', 'text_result')

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
            result = resolved_value.replace(resolved_old, resolved_new)
            count = resolved_value.count(resolved_old)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换完成: {count} 处",
                data={
                    'result': result,
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"替换所有失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_result'}


class TextWordCountAction(BaseAction):
    """Count words."""
    action_type = "text_word_count"
    display_name = "计数单词"
    description = "统计单词数量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute word count.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with word count.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            words = resolved.split()
            count = len(words)
            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"单词数: {count}",
                data={
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数单词失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_result'}


class TextLineCountAction(BaseAction):
    """Count lines."""
    action_type = "text_line_count"
    display_name = "计数行数"
    description = "统计行数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute line count.

        Args:
            context: Execution context.
            params: Dict with value, keep_empty, output_var.

        Returns:
            ActionResult with line count.
        """
        value = params.get('value', '')
        keep_empty = params.get('keep_empty', False)
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            lines = resolved.splitlines()
            if not keep_empty:
                lines = [l for l in lines if l.strip()]
            count = len(lines)
            context.set(output_var, count)

            return ActionResult(
                success=True,
                message=f"行数: {count}",
                data={
                    'count': count,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数行数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keep_empty': False, 'output_var': 'text_result'}


class TextContainsAction(BaseAction):
    """Check if contains substring."""
    action_type = "text_contains"
    display_name = "检查包含"
    description = "检查是否包含子串"

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
        output_var = params.get('output_var', 'text_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_sub = context.resolve_value(substring)
            result = resolved_sub in resolved_value
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
                message=f"检查包含失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'text_result'}