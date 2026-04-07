"""Regex2 action module for RabAI AutoClick.

Provides additional regex operations:
- RegexFindAllAction: Find all matches
- RegexFindOneAction: Find first match
- RegexSplitAction: Split by pattern
- RegexReplaceAction: Replace by pattern
- RegexGroupsAction: Extract groups from match
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex_find_all"
    display_name = "正则查找全部"
    description = "查找所有匹配的子串"

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
            ActionResult with matches.
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
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            matches = regex.findall(resolved_text)

            context.set(output_var, matches)

            return ActionResult(
                success=True,
                message=f"正则查找: {len(matches)} 个匹配",
                data={
                    'pattern': resolved_pattern,
                    'matches': matches,
                    'count': len(matches),
                    'output_var': output_var
                }
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则查找失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_matches'}


class RegexFindOneAction(BaseAction):
    """Find first match."""
    action_type = "regex_find_one"
    display_name = "正则查找一个"
    description = "查找第一个匹配的子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex find one.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with first match.
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
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            match = regex.search(resolved_text)

            if match:
                result = match.group(0)
                context.set(output_var, result)

                return ActionResult(
                    success=True,
                    message=f"正则查找: {result}",
                    data={
                        'pattern': resolved_pattern,
                        'match': result,
                        'found': True,
                        'output_var': output_var
                    }
                )
            else:
                context.set(output_var, None)

                return ActionResult(
                    success=True,
                    message="正则查找: 未找到匹配",
                    data={
                        'pattern': resolved_pattern,
                        'match': None,
                        'found': False,
                        'output_var': output_var
                    }
                )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则查找失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_match'}


class RegexSplitAction(BaseAction):
    """Split by pattern."""
    action_type = "regex_split"
    display_name = "正则分割"
    description = "按正则表达式分割字符串"

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
        output_var = params.get('output_var', 'regex_split_result')

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
            parts = regex.split(resolved_text, maxsplit=resolved_maxsplit)

            context.set(output_var, parts)

            return ActionResult(
                success=True,
                message=f"正则分割: {len(parts)} 部分",
                data={
                    'pattern': resolved_pattern,
                    'parts': parts,
                    'count': len(parts),
                    'output_var': output_var
                }
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则分割失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'maxsplit': 0, 'flags': 0, 'output_var': 'regex_split_result'}


class RegexReplaceAction(BaseAction):
    """Replace by pattern."""
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "按正则表达式替换字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex replace.

        Args:
            context: Execution context.
            params: Dict with pattern, text, replacement, count, flags, output_var.

        Returns:
            ActionResult with replaced string.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        count = params.get('count', 0)
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_replace_result')

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
            resolved_count = int(context.resolve_value(count)) if count else 0
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)

            if resolved_count > 0:
                result = regex.sub(resolved_replacement, resolved_text, count=resolved_count)
            else:
                result = regex.sub(resolved_replacement, resolved_text)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则替换完成",
                data={
                    'pattern': resolved_pattern,
                    'replacement': resolved_replacement,
                    'result': result,
                    'output_var': output_var
                }
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则替换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text', 'replacement']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 0, 'flags': 0, 'output_var': 'regex_replace_result'}


class RegexGroupsAction(BaseAction):
    """Extract groups from match."""
    action_type = "regex_groups"
    display_name = "正则提取组"
    description = "从匹配中提取捕获组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex groups.

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
            resolved_flags = int(context.resolve_value(flags)) if flags else 0

            regex = re.compile(resolved_pattern, resolved_flags)
            match = regex.search(resolved_text)

            if match:
                groups = match.groups()
                context.set(output_var, groups)

                return ActionResult(
                    success=True,
                    message=f"正则提取组: {len(groups)} 个",
                    data={
                        'pattern': resolved_pattern,
                        'groups': groups,
                        'count': len(groups),
                        'output_var': output_var
                    }
                )
            else:
                context.set(output_var, None)

                return ActionResult(
                    success=True,
                    message="正则提取组: 未找到匹配",
                    data={
                        'pattern': resolved_pattern,
                        'groups': None,
                        'found': False,
                        'output_var': output_var
                    }
                )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则提取组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_groups'}