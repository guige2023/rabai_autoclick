"""String6 action module for RabAI AutoClick.

Provides additional string operations:
- StringUpperAction: Convert to uppercase
- StringLowerAction: Convert to lowercase
- StringTitleAction: Convert to title case
- StringSwapcaseAction: Swap case
- StringCapitalizeAction: Capitalize first letter
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StringUpperAction(BaseAction):
    """Convert to uppercase."""
    action_type = "string6_upper"
    display_name = "转大写"
    description = "将字符串转换为大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute upper.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with uppercase string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'upper_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.upper()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转大写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'upper_result'}


class StringLowerAction(BaseAction):
    """Convert to lowercase."""
    action_type = "string6_lower"
    display_name = "转小写"
    description = "将字符串转换为小写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute lower.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with lowercase string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'lower_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.lower()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转小写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转小写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'lower_result'}


class StringTitleAction(BaseAction):
    """Convert to title case."""
    action_type = "string6_title"
    display_name = "转标题格式"
    description = "将字符串转换为标题格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute title.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with title cased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'title_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.title()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转标题格式: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转标题格式失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'title_result'}


class StringSwapcaseAction(BaseAction):
    """Swap case."""
    action_type = "string6_swapcase"
    display_name = "大小写翻转"
    description = "将字符串大小写翻转"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute swapcase.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with swapcased string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'swapcase_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.swapcase()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"大小写翻转: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"大小写翻转失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'swapcase_result'}


class StringCapitalizeAction(BaseAction):
    """Capitalize first letter."""
    action_type = "string6_capitalize"
    display_name = "首字母大写"
    description = "将字符串首字母大写"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute capitalize.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with capitalized string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'capitalize_result')

        try:
            resolved = str(context.resolve_value(value))
            result = resolved.capitalize()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"首字母大写: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"首字母大写失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'capitalize_result'}
