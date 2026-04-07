"""Cast action module for RabAI AutoClick.

Provides type casting operations:
- CastToIntAction: Cast to integer
- CastToFloatAction: Cast to float
- CastToStringAction: Cast to string
- CastToBoolAction: Cast to boolean
- CastToListAction: Cast to list
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CastToIntAction(BaseAction):
    """Cast to integer."""
    action_type = "cast_to_int"
    display_name = "转换为整数"
    description = "将值转换为整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cast to int.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with integer.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'int_value')

        try:
            resolved = context.resolve_value(value)
            result = int(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为整数: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'int_value'}


class CastToFloatAction(BaseAction):
    """Cast to float."""
    action_type = "cast_to_float"
    display_name = "转换为浮点数"
    description = "将值转换为浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cast to float.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with float.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'float_value')

        try:
            resolved = context.resolve_value(value)
            result = float(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为浮点数: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为浮点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'float_value'}


class CastToStringAction(BaseAction):
    """Cast to string."""
    action_type = "cast_to_string"
    display_name = "转换为字符串"
    description = "将值转换为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cast to string.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with string.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'string_value')

        try:
            resolved = context.resolve_value(value) if value is not None else None
            result = str(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字符串: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_value'}


class CastToBoolAction(BaseAction):
    """Cast to boolean."""
    action_type = "cast_to_bool"
    display_name = "转换为布尔值"
    description = "将值转换为布尔值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cast to bool.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with boolean.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'bool_value')

        try:
            resolved = context.resolve_value(value)
            result = bool(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为布尔值: {result}",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为布尔值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bool_value'}


class CastToListAction(BaseAction):
    """Cast to list."""
    action_type = "cast_to_list"
    display_name = "转换为列表"
    description = "将值转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute cast to list.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'list_value')

        try:
            resolved = context.resolve_value(value) if value is not None else None

            if isinstance(resolved, list):
                result = resolved
            elif isinstance(resolved, tuple):
                result = list(resolved)
            elif isinstance(resolved, set):
                result = list(resolved)
            elif isinstance(resolved, dict):
                result = list(resolved.items())
            else:
                result = [resolved]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为列表: {len(result)} 项",
                data={
                    'original': resolved,
                    'result': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换为列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_value'}
