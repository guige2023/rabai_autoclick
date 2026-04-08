"""Regex utilities action module for RabAI AutoClick.

Provides regex pattern matching, extraction,
and replacement operations.
"""

import re
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Match regex pattern against string.
    
    Supports full match, search, and find all operations.
    """
    action_type = "regex_match"
    display_name = "正则匹配"
    description = "正则表达式匹配"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Match regex pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, mode,
                   flags, group_index, save_to_var.
        
        Returns:
            ActionResult with match result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        mode = params.get('mode', 'search')  # search, match, findall, finditer
        flags = params.get('flags', 0)
        group_index = params.get('group_index', 0)
        save_to_var = params.get('save_to_var', None)

        if not pattern:
            return ActionResult(success=False, message="Pattern is required")

        if not text:
            return ActionResult(success=False, message="Text is required")

        # Parse flags
        flag_val = 0
        if isinstance(flags, str):
            for f in flags.upper().split(','):
                if f == 'IGNORECASE':
                    flag_val |= re.IGNORECASE
                elif f == 'MULTILINE':
                    flag_val |= re.MULTILINE
                elif f == 'DOTALL':
                    flag_val |= re.DOTALL

        try:
            regex = re.compile(pattern, flag_val)

            if mode == 'search':
                match = regex.search(text)
                if match:
                    result_data = {
                        'matched': True,
                        'match': match.group(group_index) if match.groups() or group_index else match.group(),
                        'start': match.start(),
                        'end': match.end(),
                        'groups': match.groups()
                    }
                else:
                    result_data = {'matched': False}

            elif mode == 'match':
                match = regex.match(text)
                if match:
                    result_data = {
                        'matched': True,
                        'match': match.group(group_index) if match.groups() or group_index else match.group(),
                        'start': match.start(),
                        'end': match.end()
                    }
                else:
                    result_data = {'matched': False}

            elif mode == 'findall':
                matches = regex.findall(text)
                result_data = {
                    'matched': len(matches) > 0,
                    'matches': matches,
                    'count': len(matches)
                }

            elif mode == 'finditer':
                iter_matches = list(regex.finditer(text))
                result_data = {
                    'matched': len(iter_matches) > 0,
                    'count': len(iter_matches),
                    'matches': [{
                        'match': m.group(group_index) if m.groups() or group_index else m.group(),
                        'start': m.start(),
                        'end': m.end()
                    } for m in iter_matches]
                }
            else:
                return ActionResult(
                    success=False,
                    message=f"Invalid mode: {mode}"
                )

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"正则匹配: {'成功' if result_data.get('matched') else '失败'}",
                data=result_data
            )

        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则错误: {e}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'mode': 'search',
            'flags': 0,
            'group_index': 0,
            'save_to_var': None
        }


class RegexReplaceAction(BaseAction):
    """Replace text using regex pattern.
    
    Supports backreferences and count limit.
    """
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "使用正则表达式替换文本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Replace using regex.
        
        Args:
            context: Execution context.
            params: Dict with keys: pattern, text, replacement,
                   count, flags, save_to_var.
        
        Returns:
            ActionResult with replacement result.
        """
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        count = params.get('count', 0)
        flags = params.get('flags', 0)
        save_to_var = params.get('save_to_var', None)

        if not pattern:
            return ActionResult(success=False, message="Pattern is required")

        # Parse flags
        flag_val = 0
        if isinstance(flags, str):
            for f in flags.upper().split(','):
                if f == 'IGNORECASE':
                    flag_val |= re.IGNORECASE
                elif f == 'MULTILINE':
                    flag_val |= re.MULTILINE

        try:
            regex = re.compile(pattern, flag_val)
            result = regex.sub(replacement, text, count=count if count else 0)

            replacements = len(regex.findall(text))

            result_data = {
                'result': result,
                'original': text,
                'replacements': replacements,
                'pattern': pattern,
                'replacement': replacement
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"替换完成: {replacements} 处",
                data=result_data
            )

        except re.error as e:
            return ActionResult(
                success=False,
                message=f"正则错误: {e}"
            )

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text', 'replacement']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'count': 0,
            'flags': 0,
            'save_to_var': None
        }
