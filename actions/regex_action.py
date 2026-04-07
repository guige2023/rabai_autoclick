"""Regex action module for RabAI AutoClick.

Provides regex operations:
- RegexMatchAction: Match pattern
- RegexSearchAction: Search pattern
- RegexFindAllAction: Find all matches
- RegexReplaceAction: Replace pattern
- RegexSplitAction: Split by pattern
- RegexGroupsAction: Extract groups
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match pattern at start."""
    action_type = "regex_match"
    display_name = "正则匹配"
    description = "从头匹配正则"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute match.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with match result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_match')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_flags = context.resolve_value(flags)

            flags_val = self._parse_flags(resolved_flags)
            match = re.match(resolved_pattern, resolved_text, flags_val)

            if match:
                result = {
                    'matched': True,
                    'group': match.group(),
                    'span': match.span(),
                }
                context.set(output_var, result)
                return ActionResult(
                    success=True,
                    message=f"匹配成功: {match.group()}",
                    data={'match': result, 'output_var': output_var}
                )
            else:
                context.set(output_var, {'matched': False})
                return ActionResult(
                    success=True,
                    message="未匹配",
                    data={'match': {'matched': False}, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"正则匹配失败: {str(e)}")

    def _parse_flags(self, flags: int) -> int:
        return flags

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_match'}


class RegexSearchAction(BaseAction):
    """Search pattern in text."""
    action_type = "regex_search"
    display_name = "正则搜索"
    description = "搜索正则"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute search.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with search result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_search')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            match = re.search(resolved_pattern, resolved_text)

            if match:
                result = {
                    'matched': True,
                    'group': match.group(),
                    'span': match.span(),
                }
                context.set(output_var, result)
                return ActionResult(
                    success=True,
                    message=f"找到: {match.group()}",
                    data={'match': result, 'output_var': output_var}
                )
            else:
                context.set(output_var, {'matched': False})
                return ActionResult(
                    success=True,
                    message="未找到",
                    data={'match': {'matched': False}, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"正则搜索失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_search'}


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex_findall"
    display_name = "正则查找全部"
    description = "查找所有匹配"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute find all.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with all matches.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_matches')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            matches = re.findall(resolved_pattern, resolved_text)

            context.set(output_var, matches)

            return ActionResult(
                success=True,
                message=f"找到 {len(matches)} 个匹配",
                data={'matches': matches, 'count': len(matches), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"正则查找全部失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_matches'}


class RegexReplaceAction(BaseAction):
    """Replace pattern."""
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "替换正则匹配"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace.

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
        output_var = params.get('output_var', 'regex_replaced')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(replacement, str, 'replacement')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_replacement = context.resolve_value(replacement)

            result = re.sub(resolved_pattern, resolved_replacement, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已替换: {result[:50]}...",
                data={'replaced': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"正则替换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text', 'replacement']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_replaced'}


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex_split"
    display_name = "正则分割"
    description = "按正则分割"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute split.

        Args:
            context: Execution context.
            params: Dict with pattern, text, maxsplit, output_var.

        Returns:
            ActionResult with split parts.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        maxsplit = params.get('maxsplit', 0)
        output_var = params.get('output_var', 'regex_split')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)
            resolved_maxsplit = context.resolve_value(maxsplit)

            parts = re.split(resolved_pattern, resolved_text, maxsplit=resolved_maxsplit)

            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"已分割为 {len(parts)} 部分",
                data={'parts': parts, 'count': len(parts), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"正则分割失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'maxsplit': 0, 'output_var': 'regex_split'}


class RegexGroupsAction(BaseAction):
    """Extract regex groups."""
    action_type = "regex_groups"
    display_name = "正则分组"
    description = "提取正则分组"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute groups.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with groups.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_groups')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            match = re.search(resolved_pattern, resolved_text)

            if match:
                groups = match.groups()
                result = {
                    'matched': True,
                    'groups': groups,
                    'group_dict': match.groupdict() if match.groupdict() else None,
                }
                context.set(output_var, result)
                return ActionResult(
                    success=True,
                    message=f"分组: {len(groups)} 个",
                    data={'groups': result, 'output_var': output_var}
                )
            else:
                context.set(output_var, {'matched': False})
                return ActionResult(
                    success=True,
                    message="未匹配",
                    data={'groups': {'matched': False}, 'output_var': output_var}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"正则分组失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_groups'}
