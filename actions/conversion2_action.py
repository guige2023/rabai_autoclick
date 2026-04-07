"""Conversion2 action module for RabAI AutoClick.

Provides additional conversion operations:
- ConversionToIntAction: Convert to integer
- ConversionToFloatAction: Convert to float
- ConversionToStringAction: Convert to string
- ConversionToBoolAction: Convert to boolean
- ConversionToListAction: Convert to list
- ConversionToDictAction: Convert to dict
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConversionToIntAction(BaseAction):
    """Convert to integer."""
    action_type = "conversion_to_int"
    display_name = "转换为整数"
    description = "将值转换为整数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to int.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with integer.
        """
        value = params.get('value', 0)
        default = params.get('default', 0)
        output_var = params.get('output_var', 'int_value')

        try:
            resolved = context.resolve_value(value)

            try:
                result = int(resolved)
            except (ValueError, TypeError):
                try:
                    result = int(float(resolved))
                except:
                    result = default

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
        return {'default': 0, 'output_var': 'int_value'}


class ConversionToFloatAction(BaseAction):
    """Convert to float."""
    action_type = "conversion_to_float"
    display_name = "转换为浮点数"
    description = "将值转换为浮点数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to float.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with float.
        """
        value = params.get('value', 0.0)
        default = params.get('default', 0.0)
        output_var = params.get('output_var', 'float_value')

        try:
            resolved = context.resolve_value(value)

            try:
                result = float(resolved)
            except (ValueError, TypeError):
                result = default

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
        return {'default': 0.0, 'output_var': 'float_value'}


class ConversionToStringAction(BaseAction):
    """Convert to string."""
    action_type = "conversion_to_string"
    display_name = "转换为字符串"
    description = "将值转换为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to string.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with string.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'string_value')

        try:
            resolved = context.resolve_value(value)
            result = str(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字符串: {result[:50]}...",
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


class ConversionToBoolAction(BaseAction):
    """Convert to boolean."""
    action_type = "conversion_to_bool"
    display_name = "转换为布尔值"
    description = "将值转换为布尔值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to bool.

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


class ConversionToListAction(BaseAction):
    """Convert to list."""
    action_type = "conversion_to_list"
    display_name = "转换为列表"
    description = "将值转换为列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to list.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'list_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, list):
                result = resolved
            elif isinstance(resolved, tuple):
                result = list(resolved)
            elif isinstance(resolved, set):
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


class ConversionToDictAction(BaseAction):
    """Convert to dict."""
    action_type = "conversion_to_dict"
    display_name = "转换为字典"
    description = "将值转换为字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to dict.

        Args:
            context: Execution context.
            params: Dict with value, keys, output_var.

        Returns:
            ActionResult with dict.
        """
        value = params.get('value', None)
        keys = params.get('keys', None)
        output_var = params.get('output_var', 'dict_value')

        try:
            resolved = context.resolve_value(value)

            if isinstance(resolved, dict):
                result = resolved
            elif isinstance(resolved, (list, tuple)) and keys is not None:
                resolved_keys = context.resolve_value(keys)
                if isinstance(resolved_keys, list):
                    result = dict(zip(resolved_keys, resolved))
                else:
                    result = {}
            elif isinstance(resolved, list):
                result = {i: v for i, v in enumerate(resolved)}
            else:
                result = {}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换为字典: {len(result)} 项",
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
                message=f"转换为字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'keys': None, 'output_var': 'dict_value'}