"""Convert action module for RabAI AutoClick.

Provides type conversion operations:
- ConvertToIntAction: Convert to integer
- ConvertToFloatAction: Convert to float
- ConvertToStringAction: Convert to string
- ConvertToBoolAction: Convert to boolean
- ConvertToListAction: Convert to list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConvertToIntAction(BaseAction):
    """Convert to integer."""
    action_type = "convert_to_int"
    display_name = "转换为整数"
    description = "将值转换为整数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert to int.

        Args:
            context: Execution context.
            params: Dict with value, base, output_var.

        Returns:
            ActionResult with integer.
        """
        value = params.get('value', 0)
        base = params.get('base', 10)
        output_var = params.get('output_var', 'int_result')

        try:
            resolved = context.resolve_value(value)
            resolved_base = int(context.resolve_value(base))

            if resolved_base == 10:
                result = int(float(resolved))
            else:
                result = int(str(resolved), resolved_base)

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
        return {'base': 10, 'output_var': 'int_result'}


class ConvertToFloatAction(BaseAction):
    """Convert to float."""
    action_type = "convert_to_float"
    display_name = "转换为浮点数"
    description = "将值转换为浮点数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert to float.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with float.
        """
        value = params.get('value', 0)
        output_var = params.get('output_var', 'float_result')

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
        return {'output_var': 'float_result'}


class ConvertToStringAction(BaseAction):
    """Convert to string."""
    action_type = "convert_to_string"
    display_name = "转换为字符串"
    description = "将值转换为字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert to string.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'string_result')

        try:
            resolved = context.resolve_value(value)
            result = str(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字符串: {result}",
                data={
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
        return {'output_var': 'string_result'}


class ConvertToBoolAction(BaseAction):
    """Convert to boolean."""
    action_type = "convert_to_bool"
    display_name = "转换为布尔值"
    description = "将值转换为布尔值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert to bool.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with boolean.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'bool_result')

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
        return {'output_var': 'bool_result'}


class ConvertToListAction(BaseAction):
    """Convert to list."""
    action_type = "convert_to_list"
    display_name = "转换为列表"
    description = "将值转换为列表"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert to list.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'list_result')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, list):
                result = resolved
            elif isinstance(resolved, tuple):
                result = list(resolved)
            elif isinstance(resolved, dict):
                result = list(resolved.items())
            elif isinstance(resolved, str):
                result = list(resolved)
            else:
                result = [resolved]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为列表: {len(result)} 个元素",
                data={
                    'original': resolved,
                    'result': result,
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
        return {'output_var': 'list_result'}