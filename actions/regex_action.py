"""Regex action module for RabAI AutoClick.

Provides regex operations:
- RegexMatchAction: Match pattern in text
- RegexSearchAction: Search for pattern
- RegexReplaceAction: Replace pattern matches
- RegexGroupsAction: Extract regex groups
"""

import re
from typing import Any, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match a regex pattern exactly."""
    action_type = "regex_match"
    display_name = "正则匹配"
    description = "检查文本是否完全匹配正则表达式"

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
            ActionResult with match status.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_match')

        # Validate pattern
        if not pattern:
            return ActionResult(
                success=False,
                message="未指定正则表达式"
            )
        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_pattern = context.resolve_value(pattern)

            regex = re.compile(resolved_pattern, flags)
            match = regex.fullmatch(resolved_text)
            matched = match is not None

            # Store in context
            context.set(output_var, matched)
            if match:
                context.set(f'{output_var}_group', match.group(0))

            return ActionResult(
                success=True,
                message=f"正则匹配: {'成功' if matched else '失败'}",
                data={
                    'matched': matched,
                    'pattern': resolved_pattern,
                    'text': resolved_text[:100],
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
                message=f"正则匹配失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'flags': 0,
            'output_var': 'regex_match'
        }


class RegexSearchAction(BaseAction):
    """Search for pattern in text."""
    action_type = "regex_search"
    display_name = "正则搜索"
    description = "在文本中搜索正则表达式匹配"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex search.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, find_all, output_var.

        Returns:
            ActionResult with search results.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        find_all = params.get('find_all', False)
        output_var = params.get('output_var', 'regex_search')

        # Validate inputs
        if not pattern:
            return ActionResult(
                success=False,
                message="未指定正则表达式"
            )
        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(find_all, bool, 'find_all')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_pattern = context.resolve_value(pattern)

            regex = re.compile(resolved_pattern, flags)

            if find_all:
                matches = regex.findall(resolved_text)
                context.set(output_var, matches)
                result_data = {
                    'matches': matches,
                    'count': len(matches),
                    'pattern': resolved_pattern,
                    'output_var': output_var
                }
            else:
                match = regex.search(resolved_text)
                if match:
                    result_data = {
                        'found': True,
                        'match': match.group(0),
                        'start': match.start(),
                        'end': match.end(),
                        'pattern': resolved_pattern,
                        'output_var': output_var
                    }
                    context.set(output_var, match.group(0))
                    context.set(f'{output_var}_start', match.start())
                    context.set(f'{output_var}_end', match.end())
                else:
                    result_data = {
                        'found': False,
                        'pattern': resolved_pattern,
                        'output_var': output_var
                    }
                    context.set(output_var, None)

            return ActionResult(
                success=True,
                message=f"正则搜索: {'找到' if result_data.get('found', len(matches := result_data.get('matches', []))) > 0 else '未找到'} {result_data.get('count', 1)} 个匹配",
                data=result_data
            )
        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则表达式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"正则搜索失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'flags': 0,
            'find_all': False,
            'output_var': 'regex_search'
        }


class RegexReplaceAction(BaseAction):
    """Replace pattern matches."""
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "替换文本中的正则表达式匹配"

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
        output_var = params.get('output_var', 'regex_replace')

        # Validate inputs
        if not pattern:
            return ActionResult(
                success=False,
                message="未指定正则表达式"
            )
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
            resolved_text = context.resolve_value(text)
            resolved_pattern = context.resolve_value(pattern)
            resolved_replacement = context.resolve_value(replacement)

            result = re.sub(resolved_pattern, resolved_replacement, resolved_text)

            # Store in context
            context.set(output_var, result)

            # Count replacements
            count = len(re.findall(resolved_pattern, resolved_text))

            return ActionResult(
                success=True,
                message=f"正则替换: 替换了 {count} 处",
                data={
                    'result': result,
                    'count': count,
                    'pattern': resolved_pattern,
                    'replacement': resolved_replacement,
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
        return {'output_var': 'regex_replace'}


class RegexGroupsAction(BaseAction):
    """Extract regex group matches."""
    action_type = "regex_groups"
    display_name = "正则分组"
    description = "提取正则表达式的分组匹配"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex groups extraction.

        Args:
            context: Execution context.
            params: Dict with pattern, text, flags, output_var.

        Returns:
            ActionResult with group matches.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_groups')

        # Validate inputs
        if not pattern:
            return ActionResult(
                success=False,
                message="未指定正则表达式"
            )
        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(text, str, 'text')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_text = context.resolve_value(text)
            resolved_pattern = context.resolve_value(pattern)

            regex = re.compile(resolved_pattern, flags)
            match = regex.search(resolved_text)

            if not match:
                return ActionResult(
                    success=True,
                    message="未找到匹配",
                    data={
                        'found': False,
                        'groups': [],
                        'output_var': output_var
                    }
                )

            groups = match.groups()
            group_dict = {}
            for i, group in enumerate(groups, 1):
                group_dict[f'group_{i}'] = group

            # Store in context
            context.set(output_var, groups)
            for key, value in group_dict.items():
                context.set(f'{output_var}_{key}', value)

            return ActionResult(
                success=True,
                message=f"提取了 {len(groups)} 个分组",
                data={
                    'found': True,
                    'groups': groups,
                    'group_dict': group_dict,
                    'match': match.group(0),
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
                message=f"分组提取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'flags': 0,
            'output_var': 'regex_groups'
        }