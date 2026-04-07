"""Regex5 action module for RabAI AutoClick.

Provides additional regex operations:
- RegexMatchAction: Check if pattern matches
- RegexSearchAction: Search for pattern
- RegexFindAllAction: Find all matches
- RegexSplitAction: Split by pattern
- RegexReplaceAction: Replace pattern
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Check if pattern matches."""
    action_type = "regex5_match"
    display_name = "正则匹配"
    description = "检查正则表达式是否匹配"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex match.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with match result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'match_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            result = re.match(resolved_pattern, resolved_text) is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则匹配: {'匹配' if result else '不匹配'}",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'matched': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则匹配失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'match_result'}


class RegexSearchAction(BaseAction):
    """Search for pattern."""
    action_type = "regex5_search"
    display_name = "正则搜索"
    description = "搜索正则表达式"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex search.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with search result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'search_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            match = re.search(resolved_pattern, resolved_text)

            if match:
                result = {
                    'found': True,
                    'match': match.group(),
                    'start': match.start(),
                    'end': match.end()
                }
            else:
                result = {'found': False, 'match': None, 'start': None, 'end': None}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则搜索: {'找到' if result['found'] else '未找到'}",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则搜索失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'search_result'}


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex5_find_all"
    display_name = "正则查找全部"
    description = "查找所有匹配项"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex find all.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with all matches.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'find_all_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            result = re.findall(resolved_pattern, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则查找全部: 找到{len(result)}个",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'matches': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则查找全部失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'find_all_result'}


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex5_split"
    display_name = "正则分割"
    description = "按正则表达式分割"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex split.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with split result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'split_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            result = re.split(resolved_pattern, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则分割: {len(result)}个部分",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'parts': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'split_result'}


class RegexReplaceAction(BaseAction):
    """Replace pattern."""
    action_type = "regex5_replace"
    display_name = "正则替换"
    description = "替换匹配的文本"
    version = "5.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex replace.

        Args:
            context: Execution context.
            params: Dict with pattern, text, replacement, output_var.

        Returns:
            ActionResult with replaced text.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        output_var = params.get('output_var', 'replace_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_replacement = context.resolve_value(replacement)

            result = re.sub(resolved_pattern, resolved_replacement, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则替换: 完成",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'replacement': resolved_replacement,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则替换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text', 'replacement']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'replace_result'}