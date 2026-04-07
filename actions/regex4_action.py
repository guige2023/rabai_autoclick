"""Regex4 action module for RabAI AutoClick.

Provides additional regex operations:
- RegexMatchAction: Check if pattern matches
- RegexSearchAction: Search for pattern
- RegexFindAllAction: Find all matches
- RegexReplaceAction: Replace pattern
- RegexSplitAction: Split by pattern
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Check if pattern matches."""
    action_type = "regex4_match"
    display_name = "正则匹配"
    description = "检查正则表达式是否匹配"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex match.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with match result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'match_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            match = regex.match(resolved_text)
            result = match is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则匹配: {'是' if result else '否'}",
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
                message=f"正则匹配失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'match_result'}


class RegexSearchAction(BaseAction):
    """Search for pattern."""
    action_type = "regex4_search"
    display_name = "正则搜索"
    description = "搜索正则表达式匹配项"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex search.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with search result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'search_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            match = regex.search(resolved_text)

            if match:
                result = {
                    'found': True,
                    'match': match.group(),
                    'start': match.start(),
                    'end': match.end(),
                    'groups': match.groups()
                }
            else:
                result = {'found': False}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则搜索: {'找到' if result.get('found') else '未找到'}",
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
        return {'flags': 0, 'output_var': 'search_result'}


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex4_find_all"
    display_name = "正则查找全部"
    description = "查找所有正则表达式匹配项"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex find all.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with all matches.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'findall_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            matches = regex.findall(resolved_text)

            context.set(output_var, matches)

            return ActionResult(
                success=True,
                message=f"正则查找全部: 找到 {len(matches)} 个匹配",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'matches': matches,
                    'count': len(matches),
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
        return {'flags': 0, 'output_var': 'findall_result'}


class RegexReplaceAction(BaseAction):
    """Replace pattern."""
    action_type = "regex4_replace"
    display_name = "正则替换"
    description = "替换正则表达式匹配项"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex replace.

        Args:
            context: Execution context.
            params: Dict with pattern, text, replacement, flags, output_var.

        Returns:
            ActionResult with replaced text.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'replace_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_replacement = context.resolve_value(replacement)
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            result = regex.sub(resolved_replacement, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则替换完成",
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
        return {'flags': 0, 'output_var': 'replace_result'}


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex4_split"
    display_name = "正则分割"
    description = "按正则表达式分割字符串"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex split.

        Args:
            context: Execution context.
            params: Dict with pattern, text, maxsplit, flags, output_var.

        Returns:
            ActionResult with split result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        maxsplit = params.get('maxsplit', 0)
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'split_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_maxsplit = int(context.resolve_value(maxsplit)) if maxsplit else 0
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            result = regex.split(resolved_text, maxsplit=resolved_maxsplit)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则分割完成: {len(result)} 部分",
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
        return {'maxsplit': 0, 'flags': 0, 'output_var': 'split_result'}