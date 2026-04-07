"""Compare3 action module for RabAI AutoClick.

Provides additional comparison operations:
- CompareStartsWithAction: Compare starts with
- CompareEndsWithAction: Compare ends with
- CompareContainsAction: Compare contains
- CompareMatchesRegexAction: Compare matches regex
- CompareIsDigitAction: Check if digit
"""

import re
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CompareStartsWithAction(BaseAction):
    """Compare starts with."""
    action_type = "compare3_starts_with"
    display_name = "比较开头"
    description = "比较字符串是否以指定前缀开头"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute starts with compare.

        Args:
            context: Execution context.
            params: Dict with value, prefix, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', '')
        prefix = params.get('prefix', '')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(prefix, str, 'prefix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_prefix = context.resolve_value(prefix)

            result = resolved_value.startswith(resolved_prefix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"开头比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'prefix': resolved_prefix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"开头比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'prefix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareEndsWithAction(BaseAction):
    """Compare ends with."""
    action_type = "compare3_ends_with"
    display_name = "比较结尾"
    description = "比较字符串是否以指定后缀结尾"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute ends with compare.

        Args:
            context: Execution context.
            params: Dict with value, suffix, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', '')
        suffix = params.get('suffix', '')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(suffix, str, 'suffix')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_suffix = context.resolve_value(suffix)

            result = resolved_value.endswith(resolved_suffix)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"结尾比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'suffix': resolved_suffix,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"结尾比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'suffix']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareContainsAction(BaseAction):
    """Compare contains."""
    action_type = "compare3_contains"
    display_name = "比较包含"
    description = "比较字符串是否包含指定子串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains compare.

        Args:
            context: Execution context.
            params: Dict with value, substring, output_var.

        Returns:
            ActionResult with comparison result.
        """
        value = params.get('value', '')
        substring = params.get('substring', '')
        output_var = params.get('output_var', 'compare_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(substring, str, 'substring')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_substring = context.resolve_value(substring)

            result = resolved_substring in resolved_value
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"包含比较: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'substring': resolved_substring,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"包含比较失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'substring']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'compare_result'}


class CompareMatchesRegexAction(BaseAction):
    """Compare matches regex."""
    action_type = "compare3_matches_regex"
    display_name = "正则匹配"
    description = "比较字符串是否匹配正则表达式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute regex match compare.

        Args:
            context: Execution context.
            params: Dict with value, pattern, output_var.

        Returns:
            ActionResult with regex match result.
        """
        value = params.get('value', '')
        pattern = params.get('pattern', '')
        output_var = params.get('output_var', 'regex_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(pattern, str, 'pattern')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_pattern = context.resolve_value(pattern)

            match = re.match(resolved_pattern, resolved_value)
            result = match is not None
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"正则匹配: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'pattern': resolved_pattern,
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
        return ['value', 'pattern']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'regex_result'}


class CompareIsDigitAction(BaseAction):
    """Check if digit."""
    action_type = "compare3_is_digit"
    display_name = "判断数字"
    description = "判断字符串是否全是数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is digit check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with digit check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_digit_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isdigit()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"数字判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_digit_result'}
