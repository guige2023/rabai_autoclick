"""Inspect action module for RabAI AutoClick.

Provides inspection operations:
- InspectTypeAction: Inspect type of value
- InspectLengthAction: Inspect length
- InspectKeysAction: Inspect dictionary keys
- InspectValuesAction: Inspect dictionary values
- InspectHasKeyAction: Check if key exists
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class InspectTypeAction(BaseAction):
    """Inspect type of value."""
    action_type = "inspect_type"
    display_name = "检查类型"
    description = "检查值的类型"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inspect type.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with type info.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'type_result')

        try:
            resolved = context.resolve_value(value) if value is not None else None
            type_name = type(resolved).__name__
            context.set(output_var, type_name)

            return ActionResult(
                success=True,
                message=f"类型: {type_name}",
                data={
                    'value': resolved,
                    'type': type_name,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查类型失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'type_result'}


class InspectLengthAction(BaseAction):
    """Inspect length."""
    action_type = "inspect_length"
    display_name = "检查长度"
    description = "检查值的长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inspect length.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with length info.
        """
        value = params.get('value', None)
        output_var = params.get('output_var', 'length_result')

        try:
            resolved = context.resolve_value(value) if value is not None else None

            try:
                length = len(resolved)
            except TypeError:
                return ActionResult(
                    success=False,
                    message=f"值没有长度属性: {type(resolved).__name__}"
                )

            context.set(output_var, length)

            return ActionResult(
                success=True,
                message=f"长度: {length}",
                data={
                    'value': resolved,
                    'length': length,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'length_result'}


class InspectKeysAction(BaseAction):
    """Inspect dictionary keys."""
    action_type = "inspect_keys"
    display_name = "检查字典键"
    description = "检查字典的所有键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inspect keys.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with keys info.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'keys_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(dict_var)
            d = context.get(resolved) if isinstance(resolved, str) else resolved

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved} 不是字典"
                )

            keys = list(d.keys())
            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"字典键: {len(keys)} 个",
                data={
                    'keys': keys,
                    'count': len(keys),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字典键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'keys_result'}


class InspectValuesAction(BaseAction):
    """Inspect dictionary values."""
    action_type = "inspect_values"
    display_name = "检查字典值"
    description = "检查字典的所有值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute inspect values.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with values info.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'values_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(dict_var)
            d = context.get(resolved) if isinstance(resolved, str) else resolved

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved} 不是字典"
                )

            values = list(d.values())
            context.set(output_var, values)

            return ActionResult(
                success=True,
                message=f"字典值: {len(values)} 个",
                data={
                    'values': values,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'values_result'}


class InspectHasKeyAction(BaseAction):
    """Check if key exists."""
    action_type = "inspect_has_key"
    display_name = "检查键存在"
    description = "检查字典中键是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute check has key.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, output_var.

        Returns:
            ActionResult with check result.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'has_key_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)
            resolved_key = context.resolve_value(key)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_dict} 不是字典"
                )

            result = resolved_key in d
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"键存在: {'是' if result else '否'}",
                data={
                    'key': resolved_key,
                    'exists': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查键存在失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_key_result'}
