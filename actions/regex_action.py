"""Regular expression action module for RabAI AutoClick.

Provides regex operations:
- RegexMatchAction: Check if pattern matches
- RegexSearchAction: Search for pattern
- RegexFindAllAction: Find all matches
- RegexReplaceAction: Replace matches
- RegexSplitAction: Split by pattern
- RegexGroupsAction: Extract groups
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RegexMatchAction(BaseAction):
    """Check if pattern matches."""
    action_type = "regex_match"
    display_name = "正则匹配"
    description = "检查正则是否匹配"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex match."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_match_result')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            compiled = re_module.compile(resolved_pattern, flags_int)
            match = compiled.search(resolved_text)
            matched = match is not None

            result = {'matched': matched, 'pattern': resolved_pattern}
            if match:
                result['match'] = match.group(0)
                result['span'] = match.span()

            if context:
                context.set(output_var, matched)
            return ActionResult(success=matched, message=f"Pattern {'matched' if matched else 'not matched'}", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex match error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_match_result'}


class RegexSearchAction(BaseAction):
    """Search for pattern."""
    action_type = "regex_search"
    display_name = "正则搜索"
    description = "搜索正则匹配"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex search."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_search_result')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            compiled = re_module.compile(resolved_pattern, flags_int)
            match = compiled.search(resolved_text)

            if match:
                result = {
                    'found': True,
                    'match': match.group(0),
                    'span': match.span(),
                    'start': match.start(),
                    'end': match.end(),
                    'groups': match.groups(),
                }
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Found: {match.group(0)[:50]}", data=result)
            else:
                result = {'found': False}
                if context:
                    context.set(output_var, None)
                return ActionResult(success=False, message="Pattern not found", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex search error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_search_result'}


class RegexFindAllAction(BaseAction):
    """Find all matches."""
    action_type = "regex_findall"
    display_name = "正则查找全部"
    description = "查找所有匹配"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex findall."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        limit = params.get('limit', None)
        output_var = params.get('output_var', 'regex_matches')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_flags = context.resolve_value(flags) if context else flags
            resolved_limit = context.resolve_value(limit) if context else limit

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            compiled = re_module.compile(resolved_pattern, flags_int)
            matches = compiled.findall(resolved_text)

            if resolved_limit:
                matches = matches[:resolved_limit]

            result = {'matches': matches, 'count': len(matches)}
            if context:
                context.set(output_var, matches)
            return ActionResult(success=True, message=f"Found {len(matches)} matches", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex findall error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'limit': None, 'output_var': 'regex_matches'}


class RegexReplaceAction(BaseAction):
    """Replace pattern matches."""
    action_type = "regex_replace"
    display_name = "正则替换"
    description = "正则替换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex replace."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        replacement = params.get('replacement', '')
        count = params.get('count', 0)  # 0 = all
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_replace_result')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_replacement = context.resolve_value(replacement) if context else replacement
            resolved_count = context.resolve_value(count) if context else count
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            result = re_module.sub(resolved_pattern, resolved_replacement, resolved_text, count=int(resolved_count), flags=flags_int)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Replaced text", data={'result': result})
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex replace error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'replacement': '', 'count': 0, 'flags': 0, 'output_var': 'regex_replace_result'}


class RegexSplitAction(BaseAction):
    """Split text by pattern."""
    action_type = "regex_split"
    display_name = "正则分割"
    description = "按正则分割"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex split."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        maxsplit = params.get('maxsplit', 0)
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_split_result')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_max = context.resolve_value(maxsplit) if context else maxsplit
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            parts = re_module.split(resolved_pattern, resolved_text, maxsplit=int(resolved_max), flags=flags_int)

            result = {'parts': parts, 'count': len(parts)}
            if context:
                context.set(output_var, parts)
            return ActionResult(success=True, message=f"Split into {len(parts)} parts", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex split error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'maxsplit': 0, 'flags': 0, 'output_var': 'regex_split_result'}


class RegexGroupsAction(BaseAction):
    """Extract capture groups."""
    action_type = "regex_groups"
    display_name = "正则提取组"
    description = "提取捕获组"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute regex groups."""
        pattern = params.get('pattern', '')
        text = params.get('text', '')
        flags = params.get('flags', 0)
        output_var = params.get('output_var', 'regex_groups')

        if not pattern or not text:
            return ActionResult(success=False, message="pattern and text are required")

        try:
            import re as re_module

            resolved_pattern = context.resolve_value(pattern) if context else pattern
            resolved_text = context.resolve_value(text) if context else text
            resolved_flags = context.resolve_value(flags) if context else flags

            flags_int = 0
            if resolved_flags == 1:
                flags_int = re_module.IGNORECASE
            elif resolved_flags == 2:
                flags_int = re_module.MULTILINE

            compiled = re_module.compile(resolved_pattern, flags_int)
            match = compiled.search(resolved_text)

            if match:
                groups = match.groups()
                named = match.groupdict()
                result = {
                    'groups': groups,
                    'named': named,
                    'count': len(groups),
                    'match': match.group(0),
                }
                if context:
                    context.set(output_var, result)
                return ActionResult(success=True, message=f"Found {len(groups)} groups", data=result)
            else:
                result = {'groups': [], 'count': 0}
                if context:
                    context.set(output_var, result)
                return ActionResult(success=False, message="No match found", data=result)
        except re_module.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {str(e)}")
        except Exception as e:
            return ActionResult(success=False, message=f"Regex groups error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['pattern', 'text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'flags': 0, 'output_var': 'regex_groups'}
