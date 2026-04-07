"""Validate4 action module for RabAI AutoClick.

Provides additional validation operations:
- ValidateIsBoolAction: Check if boolean
- ValidateIsListAction: Check if list
- ValidateIsDictAction: Check if dictionary
- ValidateIsTupleAction: Check if tuple
- ValidateIsSetAction: Check if set
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateIsBoolAction(BaseAction):
    """Check if boolean."""
    action_type = "validate4_is_bool"
    display_name = "判断布尔值"
    description = "检查值是否为布尔类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is bool.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with bool check.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_bool_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, bool)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"布尔值判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断布尔值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_bool_result'}


class ValidateIsListAction(BaseAction):
    """Check if list."""
    action_type = "validate4_is_list"
    display_name = "判断列表"
    description = "检查值是否为列表类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is list.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list check.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_list_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, list)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_list_result'}


class ValidateIsDictAction(BaseAction):
    """Check if dictionary."""
    action_type = "validate4_is_dict"
    display_name = "判断字典"
    description = "检查值是否为字典类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is dict.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with dict check.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_dict_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, dict)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_dict_result'}


class ValidateIsTupleAction(BaseAction):
    """Check if tuple."""
    action_type = "validate4_is_tuple"
    display_name = "判断元组"
    description = "检查值是否为元组类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is tuple.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with tuple check.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_tuple_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, tuple)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_tuple_result'}


class ValidateIsSetAction(BaseAction):
    """Check if set."""
    action_type = "validate4_is_set"
    display_name = "判断集合"
    description = "检查值是否为集合类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is set.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with set check.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_set_result')

        try:
            resolved = context.resolve_value(value)
            result = isinstance(resolved, (set, frozenset))
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"集合判断: {'是' if result else '否'}",
                data={
                    'value': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_set_result'}
