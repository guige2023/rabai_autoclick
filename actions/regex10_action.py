"""Regex10 action module for RabAI AutoClick.

Provides additional regex operations:
- RegexMatchAction: Match regex pattern
- RegexSearchAction: Search regex pattern
- RegexFindAllAction: Find all matches
- RegexReplaceAction: Replace pattern
- RegexSplitAction: Split by pattern
- RegexGroupsAction: Get regex groups
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match regex pattern."""
    action_type = "regex10_match"
    display_name = "正则匹配"
    description = "匹配正则表达式"
    version = "10.0"

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

            result = re.match(resolved_pattern, resolved_text)

            context.set(output_var, result is not None)

            return ActionResult(
                success=True,
                message=f"正则匹配: {'成功' if result else '失败'}",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'matched': result is not None,
                    'match': result.group() if result else None,
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
    """Search regex pattern."""
    action_type = "regex10_search"
    display_name = "正则搜索"
    description = "搜索正则表达式"
    version = "10.0"

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

            result = re.search(resolved_pattern, resolved_text)

            context.set(output_var, result is not None)

            return ActionResult(
                success=True,
                message=f"正则搜索: {'找到' if result else '未找到'}",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'found': result is not None,
                    'match': result.group() if result else None,
                    'span': result.span() if result else None,
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
    action_type = "regex10_findall"
    display_name = "正则查找全部"
    description = "查找所有匹配"
    version = "10.0"

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
        output_var = params.get('output_var', 'findall_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            results = re.findall(resolved_pattern, resolved_text)

            context.set(output_var, results)

            return ActionResult(
                success=True,
                message=f"正则查找全部: {len(results)}个",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'matches': results,
                    'count': len(results),
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
        return {'output_var': 'findall_result'}


class RegexReplaceAction(BaseAction):
    """Replace pattern."""
    action_type = "regex10_replace"
    display_name = "正则替换"
    description = "替换正则匹配"
    version = "10.0"

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
                message=f"正则替换: {result[:50]}...",
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


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex10_split"
    display_name = "正则分割"
    description = "按正则分割"
    version = "10.0"

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
                message=f"正则分割: {len(result)}部分",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'result': result,
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


class RegexGroupsAction(BaseAction):
    """Get regex groups."""
    action_type = "regex10_groups"
    display_name = "正则分组"
    description = "获取正则分组"
    version = "10.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex groups.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with groups.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'groups_result')

        try:
            import re

            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            result = re.search(resolved_pattern, resolved_text)

            if result:
                groups = result.groups()
                context.set(output_var, groups)
                return ActionResult(
                    success=True,
                    message=f"正则分组: {len(groups)}个",
                    data={
                        'pattern': resolved_pattern,
                        'text': resolved_text,
                        'groups': groups,
                        'count': len(groups),
                        'output_var': output_var
                    }
                )
            else:
                context.set(output_var, [])
                return ActionResult(
                    success=False,
                    message=f"正则分组失败: 无匹配"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则分组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'groups_result'}