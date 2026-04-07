"""Type action module for RabAI AutoClick.

Provides type checking/conversion operations:
- TypeCheckAction: Check value type
- TypeToIntAction: Convert to int
- TypeToFloatAction: Convert to float
- TypeToStringAction: Convert to string
- TypeToBoolAction: Convert to bool
- TypeToListAction: Convert to list
- TypeToDictAction: Convert to dict
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TypeCheckAction(BaseAction):
    """Check value type."""
    action_type = "type_check"
    display_name = "类型检查"
    description = "检查值类型"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check.

        Args:
            context: Execution context.
            params: Dict with value, expected_type, output_var.

        Returns:
            ActionResult with check result.
        """
        value = params.get('value', None)
        expected_type = params.get('expected_type', 'string')
        output_var = params.get('output_var', 'type_check_result')

        try:
            resolved_value = context.resolve_value(value)
            resolved_type = context.resolve_value(expected_type)

            type_map = {
                'string': str, 'str': str,
                'int': int, 'integer': int,
                'float': float, 'number': float,
                'bool': bool, 'boolean': bool,
                'list': list, 'array': list,
                'dict': dict, 'dictionary': dict,
                'tuple': tuple,
                'set': set,
            }

            expected = type_map.get(resolved_type.lower(), str)
            is_match = isinstance(resolved_value, expected)

            context.set(output_var, is_match)

            return ActionResult(
                success=True,
                message=f"类型检查: {'匹配' if is_match else '不匹配'} (期望 {resolved_type}, 实际 {type(resolved_value).__name__})",
                data={'match': is_match, 'expected': resolved_type, 'actual': type(resolved_value).__name__, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"类型检查失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value', 'expected_type']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'type_check_result'}


class TypeToIntAction(BaseAction):
    """Convert to int."""
    action_type = "type_to_int"
    display_name = "转整数"
    description = "转换为整数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with int value.
        """
        value = params.get('value', 0)
        default = params.get('default', 0)
        output_var = params.get('output_var', 'int_value')

        try:
            resolved_value = context.resolve_value(value)
            resolved_default = context.resolve_value(default)

            try:
                result = int(resolved_value)
            except (ValueError, TypeError):
                try:
                    result = int(float(resolved_value))
                except:
                    result = resolved_default

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换整数: {result}",
                data={'value': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转整数失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': 0, 'output_var': 'int_value'}


class TypeToFloatAction(BaseAction):
    """Convert to float."""
    action_type = "type_to_float"
    display_name = "转浮点数"
    description = "转换为浮点数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, default, output_var.

        Returns:
            ActionResult with float value.
        """
        value = params.get('value', 0.0)
        default = params.get('default', 0.0)
        output_var = params.get('output_var', 'float_value')

        try:
            resolved_value = context.resolve_value(value)
            resolved_default = context.resolve_value(default)

            try:
                result = float(resolved_value)
            except (ValueError, TypeError):
                result = resolved_default

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换浮点数: {result}",
                data={'value': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转浮点数失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': 0.0, 'output_var': 'float_value'}


class TypeToStringAction(BaseAction):
    """Convert to string."""
    action_type = "type_to_string"
    display_name = "转字符串"
    description = "转换为字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with string value.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'string_value')

        try:
            resolved_value = context.resolve_value(value)
            result = str(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换字符串: {result[:50]}...",
                data={'value': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转字符串失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'string_value'}


class TypeToBoolAction(BaseAction):
    """Convert to bool."""
    action_type = "type_to_bool"
    display_name = "转布尔值"
    description = "转换为布尔值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with bool value.
        """
        value = params.get('value', False)
        output_var = params.get('output_var', 'bool_value')

        try:
            resolved_value = context.resolve_value(value)
            result = bool(resolved_value)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换布尔值: {result}",
                data={'value': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转布尔值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'bool_value'}


class TypeToListAction(BaseAction):
    """Convert to list."""
    action_type = "type_to_list"
    display_name = "转列表"
    description = "转换为列表"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with list value.
        """
        value = params.get('value', [])
        output_var = params.get('output_var', 'list_value')

        try:
            resolved_value = context.resolve_value(value)

            if isinstance(resolved_value, list):
                result = resolved_value
            elif isinstance(resolved_value, (tuple, set)):
                result = list(resolved_value)
            elif isinstance(resolved_value, str):
                result = [resolved_value]
            elif isinstance(resolved_value, dict):
                result = list(resolved_value.items())
            else:
                result = [resolved_value]

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换列表: {len(result)} 个元素",
                data={'value': result, 'count': len(result), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转列表失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'list_value'}


class TypeToDictAction(BaseAction):
    """Convert to dict."""
    action_type = "type_to_dict"
    display_name = "转字典"
    description = "转换为字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with dict value.
        """
        value = params.get('value', {})
        output_var = params.get('output_var', 'dict_value')

        try:
            resolved_value = context.resolve_value(value)

            if isinstance(resolved_value, dict):
                result = resolved_value
            elif isinstance(resolved_value, list):
                result = {f'key_{i}': v for i, v in enumerate(resolved_value)}
            elif hasattr(resolved_value, '__dict__'):
                result = vars(resolved_value)
            else:
                result = {'value': resolved_value}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"转换字典: {len(result)} 个键",
                data={'value': result, 'count': len(result), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"转字典失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_value'}
