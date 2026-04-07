"""String5 action module for RabAI AutoClick.

Provides additional string operations:
- StringReplaceAction: Replace substring
- StringReplaceAllAction: Replace all occurrences
- StringRemoveAction: Remove substring
- StringContainsAnyAction: Check if contains any of multiple substrings
- StringContainsAllAction: Check if contains all of multiple substrings
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringReplaceAction(BaseAction):
    """Replace substring."""
    action_type = "string5_replace"
    display_name = "替换字符串"
    description = "替换字符串中的子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute replace.

        Args:
            context: Execution context.
            params: Dict with value, old, new, count, output_var.

        Returns:
            ActionResult with replaced string.
        """
        value = params.get('value', '')
        old = params.get('old', '')
        new = params.get('new', '')
        count = params.get('count', None)
        output_var = params.get('output_var', 'replaced_string')

        try:
            resolved_value = str(context.resolve_value(value))
            resolved_old = str(context.resolve_value(old))
            resolved_new = str(context.resolve_value(new))
            resolved_count = int(context.resolve_value(count)) if count is not None else None

            if resolved_count is not None:
                result = resolved_value.replace(resolved_old, resolved_new, resolved_count)
            else:
                result = resolved_value.replace(resolved_old, resolved_new)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换: {resolved_old} -> {resolved_new}",
                data={
                    'original': resolved_value,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"替换字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'old', 'new']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': None, 'output_var': 'replaced_string'}


class StringReplaceAllAction(BaseAction):
    """Replace all occurrences."""
    action_type = "string5_replace_all"
    display_name = "替换所有"
    description = "替换所有出现的子串"

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
        output_var = params.get('output_var', 'replaced_string')

        try:
            resolved_value = str(context.resolve_value(value))
            resolved_old = str(context.resolve_value(old))
            resolved_new = str(context.resolve_value(new))

            count = resolved_value.count(resolved_old)
            result = resolved_value.replace(resolved_old, resolved_new)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"替换所有: {count} 处",
                data={
                    'original': resolved_value,
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
        return {'output_var': 'replaced_string'}


class StringRemoveAction(BaseAction):
    """Remove substring."""
    action_type = "string5_remove"
    display_name = "移除子串"
    description = "从字符串中移除子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute remove.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with string after removal.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'result_string')

        try:
            resolved_value = str(context.resolve_value(value))
            resolved_sub = str(context.resolve_value(substring))

            result = resolved_value.replace(resolved_sub, '')
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"移除: {resolved_sub}",
                data={
                    'original': resolved_value,
                    'substring': resolved_sub,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"移除子串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'result_string'}


class StringContainsAnyAction(BaseAction):
    """Check if contains any of multiple substrings."""
    action_type = "string5_contains_any"
    display_name = "包含任意"
    description = "检查字符串是否包含任意给定子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains any.

        Args:
            context: Execution context.
            params: Dict with value, substrings, case_sensitive, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', '')
        substrings = params.get('substrings', [])
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'contains_any_result')

        try:
            resolved_value = str(context.resolve_value(value))
            resolved_substrings = context.resolve_value(substrings)
            resolved_case = context.resolve_value(case_sensitive) if case_sensitive else True

            if not isinstance(resolved_substrings, list):
                resolved_substrings = [resolved_substrings]

            check_value = resolved_value if resolved_case else resolved_value.lower()
            found = []
            for s in resolved_substrings:
                check_s = s if resolved_case else s.lower()
                if check_s in check_value:
                    found.append(s)

            result = len(found) > 0
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含任意: {'是' if result else '否'} ({len(found)} 个)",
                data={
                    'value': resolved_value,
                    'substrings': resolved_substrings,
                    'found': found,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"包含任意检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substrings']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'case_sensitive': True, 'output_var': 'contains_any_result'}


class StringContainsAllAction(BaseAction):
    """Check if contains all of multiple substrings."""
    action_type = "string5_contains_all"
    display_name = "包含全部"
    description = "检查字符串是否包含所有给定子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains all.

        Args:
            context: Execution context.
            params: Dict with value, substrings, case_sensitive, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', '')
        substrings = params.get('substrings', [])
        case_sensitive = params.get('case_sensitive', True)
        output_var = params.get('output_var', 'contains_all_result')

        try:
            resolved_value = str(context.resolve_value(value))
            resolved_substrings = context.resolve_value(substrings)
            resolved_case = context.resolve_value(case_sensitive) if case_sensitive else True

            if not isinstance(resolved_substrings, list):
                resolved_substrings = [resolved_substrings]

            check_value = resolved_value if resolved_case else resolved_value.lower()
            found = []
            for s in resolved_substrings:
                check_s = s if resolved_case else s.lower()
                if check_s in check_value:
                    found.append(s)

            result = len(found) == len(resolved_substrings)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含全部: {'是' if result else '否'} ({len(found)}/{len(resolved_substrings)})",
                data={
                    'value': resolved_value,
                    'substrings': resolved_substrings,
                    'found': found,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"包含全部检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substrings']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'case_sensitive': True, 'output_var': 'contains_all_result'}