"""Type2 action module for RabAI AutoClick.

Provides additional type checking operations:
- TypeIsNumberAction: Check if number
- TypeIsStringAction: Check if string
- TypeIsBoolAction: Check if boolean
- TypeIsListAction: Check if list
- TypeIsDictAction: Check if dictionary
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TypeIsNumberAction(BaseAction):
    """Check if number."""
    action_type = "type2_is_number"
    display_name = "类型判断-数字"
    description = "判断值是否为数字类型"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is number check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_number_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, (int, float)) and not isinstance(resolved, bool)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型判断-数字: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_number_result'}


class TypeIsStringAction(BaseAction):
    """Check if string."""
    action_type = "type2_is_string"
    display_name = "类型判断-字符串"
    description = "判断值是否为字符串类型"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is string check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_string_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, str)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型判断-字符串: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_string_result'}


class TypeIsBoolAction(BaseAction):
    """Check if boolean."""
    action_type = "type2_is_bool"
    display_name = "类型判断-布尔值"
    description = "判断值是否为布尔值类型"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is bool check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_bool_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, bool)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型判断-布尔值: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_bool_result'}


class TypeIsListAction(BaseAction):
    """Check if list."""
    action_type = "type2_is_list"
    display_name = "类型判断-列表"
    description = "判断值是否为列表类型"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is list check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_list_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, list)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型判断-列表: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_list_result'}


class TypeIsDictAction(BaseAction):
    """Check if dictionary."""
    action_type = "type2_is_dict"
    display_name = "类型判断-字典"
    description = "判断值是否为字典类型"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is dict check.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_dict_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, dict)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型判断-字典: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型判断失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_dict_result'}