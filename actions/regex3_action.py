"""Regex3 action module for RabAI AutoClick.

Provides additional regex operations:
- RegexMatchAction: Check if regex matches
- RegexSearchAction: Search pattern in text
- RegexFullMatchAction: Full regex match
- RegexSplitAction: Split by regex
- RegexSubAction: Substitute by regex
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Check if regex matches."""
    action_type = "regex3_match"
    display_name = "正则匹配"
    description = "检查正则表达式是否匹配"

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

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            match = re.match(resolved_pattern, resolved_text)
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
        return {'output_var': 'match_result'}


class RegexSearchAction(BaseAction):
    """Search pattern in text."""
    action_type = "regex3_search"
    display_name = "正则搜索"
    description = "在文本中搜索正则表达式"

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
            result = match.group(0) if match else None
            found = match is not None

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则搜索: {'找到' if found else '未找到'}",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'result': result,
                    'found': found,
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


class RegexFullMatchAction(BaseAction):
    """Full regex match."""
    action_type = "regex3_fullmatch"
    display_name = "完全正则匹配"
    description = "检查正则表达式是否完全匹配"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute full match.

        Args:
            context: Execution context.
            params: Dict with pattern, text, output_var.

        Returns:
            ActionResult with full match result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'fullmatch_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            match = re.fullmatch(resolved_pattern, resolved_text)
            result = match is not None
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"完全匹配: {'是' if result else '否'}",
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
                message=f"完全匹配失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'fullmatch_result'}


class RegexSplitAction(BaseAction):
    """Split by regex."""
    action_type = "regex3_regex_split"
    display_name = "正则分割"
    description = "用正则表达式分割文本"

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

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_text = context.resolve_value(text)

            parts = re.split(resolved_pattern, resolved_text)
            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"正则分割: {len(parts)} 部分",
                data={
                    'pattern': resolved_pattern,
                    'text': resolved_text,
                    'result': parts,
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


class RegexSubAction(BaseAction):
    """Substitute by regex."""
    action_type = "regex3_sub"
    display_name = "正则替换"
    description = "用正则表达式替换文本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex sub.

        Args:
            context: Execution context.
            params: Dict with pattern, replacement, text, output_var.

        Returns:
            ActionResult with substituted text.
        """
        pattern = params.get('pattern', '')
        replacement = params.get('replacement', '')
        text = params.get('text', '')
        output_var = params.get('output_var', 'sub_result')

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(replacement, str, 'replacement')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_pattern = context.resolve_value(pattern)
            resolved_replacement = context.resolve_value(replacement)
            resolved_text = context.resolve_value(text)

            result = re.sub(resolved_pattern, resolved_replacement, resolved_text)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则替换完成",
                data={
                    'pattern': resolved_pattern,
                    'replacement': resolved_replacement,
                    'text': resolved_text,
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
        return ['pattern', 'replacement', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sub_result'}
