"""Validate3 action module for RabAI AutoClick.

Provides additional validation operations:
- ValidateIsAlphaAction: Check if alphabetic
- ValidateIsAlnumAction: Check if alphanumeric
- ValidateIsSpaceAction: Check if whitespace
- ValidateIsLowerAction: Check if lowercase
- ValidateIsUpperAction: Check if uppercase
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateIsAlphaAction(BaseAction):
    """Check if alphabetic."""
    action_type = "validate3_is_alpha"
    display_name = "判断字母"
    description = "检查字符串是否全是字母"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is alpha check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with alpha check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_alpha_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isalpha()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字母判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断字母失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_alpha_result'}


class ValidateIsAlnumAction(BaseAction):
    """Check if alphanumeric."""
    action_type = "validate3_is_alnum"
    display_name = "判断字母数字"
    description = "检查字符串是否全是字母和数字"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is alnum check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with alnum check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_alnum_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isalnum()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字母数字判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断字母数字失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_alnum_result'}


class ValidateIsSpaceAction(BaseAction):
    """Check if whitespace."""
    action_type = "validate3_is_space"
    display_name = "判断空白"
    description = "检查字符串是否全是空白字符"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is space check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with space check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_space_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isspace()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"空白判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断空白失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_space_result'}


class ValidateIsLowerAction(BaseAction):
    """Check if lowercase."""
    action_type = "validate3_is_lower"
    display_name = "判断小写"
    description = "检查字符串是否全是小写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is lower check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with lower check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_lower_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.islower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"小写判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_lower_result'}


class ValidateIsUpperAction(BaseAction):
    """Check if uppercase."""
    action_type = "validate3_is_upper"
    display_name = "判断大写"
    description = "检查字符串是否全是大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is upper check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with upper check result.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'is_upper_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.isupper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大写判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_upper_result'}
