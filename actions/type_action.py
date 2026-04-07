"""Type2 action module for RabAI AutoClick.

Provides additional type operations:
- TypeCheckIntAction: Check if integer
- TypeCheckFloatAction: Check if float
- TypeCheckStringAction: Check if string
- TypeCheckListAction: Check if list
- TypeCheckDictAction: Check if dict
- TypeConvertAction: Type conversion
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TypeCheckIntAction(BaseAction):
    """Check if integer."""
    action_type = "type_check_int"
    display_name = "检查整数"
    description = "检查值是否为整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check int.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_int_result')

        try:
            resolved_value = context.resolve_value(value)

            result = isinstance(resolved_value, int) and not isinstance(resolved_value, bool)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"整数检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_int': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查整数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_int_result'}


class TypeCheckFloatAction(BaseAction):
    """Check if float."""
    action_type = "type_check_float"
    display_name = "检查浮点数"
    description = "检查值是否为浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check float.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_float_result')

        try:
            resolved_value = context.resolve_value(value)

            result = isinstance(resolved_value, float)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"浮点数检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_float': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查浮点数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_float_result'}


class TypeCheckStringAction(BaseAction):
    """Check if string."""
    action_type = "type_check_string"
    display_name = "检查字符串"
    description = "检查值是否为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check string.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_string_result')

        try:
            resolved_value = context.resolve_value(value)

            result = isinstance(resolved_value, str)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_string': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_string_result'}


class TypeCheckListAction(BaseAction):
    """Check if list."""
    action_type = "type_check_list"
    display_name = "检查列表"
    description = "检查值是否为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check list.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_list_result')

        try:
            resolved_value = context.resolve_value(value)

            result = isinstance(resolved_value, list)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_list': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_list_result'}


class TypeCheckDictAction(BaseAction):
    """Check if dict."""
    action_type = "type_check_dict"
    display_name = "检查字典"
    description = "检查值是否为字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check dict.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'is_dict_result')

        try:
            resolved_value = context.resolve_value(value)

            result = isinstance(resolved_value, dict)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典检查: {'是' if result else '否'}",
                data={
                    'value': resolved_value,
                    'is_dict': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_dict_result'}


class TypeConvertAction(BaseAction):
    """Type conversion."""
    action_type = "type_convert"
    display_name = "类型转换"
    description = "转换值的类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, target_type, output_var.

        Returns:
            ActionResult with converted value.
        """
        value = params.get('value', None)
        target_type = params.get('target_type', 'str')
        output_var = params.get('output_var', 'converted_value')

        valid, msg = self.validate_type(target_type, str, 'target_type')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_value = context.resolve_value(value)
            resolved_type = context.resolve_value(target_type)

            if resolved_type == 'int':
                result = int(resolved_value)
            elif resolved_type == 'float':
                result = float(resolved_value)
            elif resolved_type == 'str':
                result = str(resolved_value)
            elif resolved_type == 'bool':
                result = bool(resolved_value)
            elif resolved_type == 'list':
                result = list(resolved_value) if isinstance(resolved_value, (list, tuple, set)) else [resolved_value]
            elif resolved_type == 'dict':
                result = dict(resolved_value) if isinstance(resolved_value, dict) else {}
            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的目标类型: {resolved_type}"
                )

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"类型转换: {type(resolved_value).__name__} -> {resolved_type}",
                data={
                    'original_value': resolved_value,
                    'original_type': type(resolved_value).__name__,
                    'target_type': resolved_type,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"类型转换失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value', 'target_type']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_value'}
