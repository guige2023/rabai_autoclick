"""Conversion4 action module for RabAI AutoClick.

Provides additional conversion operations:
- ConversionListToDictAction: Convert list to dictionary
- ConversionDictToListAction: Convert dictionary to list
- ConversionTupleToListAction: Convert tuple to list
- ConversionListToTupleAction: Convert list to tuple
- ConversionStringToListAction: Convert string to list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConversionListToDictAction(BaseAction):
    """Convert list to dictionary."""
    action_type = "conversion4_list_to_dict"
    display_name = "列表转字典"
    description = "将列表转换为字典"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list to dict.

        Args:
            context: Execution context.
            params: Dict with list, key_prefix, output_var.

        Returns:
            ActionResult with dictionary.
        """
        input_list = params.get('list', [])
        key_prefix = params.get('key_prefix', 'key')
        output_var = params.get('output_var', 'converted_dict')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            resolved_prefix = context.resolve_value(key_prefix) if key_prefix else 'key'

            result = {f'{resolved_prefix}_{i}': v for i, v in enumerate(resolved)}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转字典: {len(result)}个键值对",
                data={
                    'original': resolved,
                    'converted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key_prefix': 'key', 'output_var': 'converted_dict'}


class ConversionDictToListAction(BaseAction):
    """Convert dictionary to list."""
    action_type = "conversion4_dict_to_list"
    display_name = "字典转列表"
    description = "将字典转换为列表"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict to list.

        Args:
            context: Execution context.
            params: Dict with dict, output_var.

        Returns:
            ActionResult with list.
        """
        input_dict = params.get('dict', {})
        output_var = params.get('output_var', 'converted_list')

        try:
            resolved = context.resolve_value(input_dict)

            if not isinstance(resolved, dict):
                return ActionResult(
                    success=False,
                    message="字典转列表失败: 输入不是字典"
                )

            result = list(resolved.items())

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典转列表: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_list'}


class ConversionTupleToListAction(BaseAction):
    """Convert tuple to list."""
    action_type = "conversion4_tuple_to_list"
    display_name = "元组转列表"
    description = "将元组转换为列表"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tuple to list.

        Args:
            context: Execution context.
            params: Dict with tuple, output_var.

        Returns:
            ActionResult with list.
        """
        input_tuple = params.get('tuple', ())
        output_var = params.get('output_var', 'converted_list')

        try:
            resolved = context.resolve_value(input_tuple)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组转列表: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_list'}


class ConversionListToTupleAction(BaseAction):
    """Convert list to tuple."""
    action_type = "conversion4_list_to_tuple"
    display_name = "列表转元组"
    description = "将列表转换为元组"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute list to tuple.

        Args:
            context: Execution context.
            params: Dict with list, output_var.

        Returns:
            ActionResult with tuple.
        """
        input_list = params.get('list', [])
        output_var = params.get('output_var', 'converted_tuple')

        try:
            resolved = context.resolve_value(input_list)

            if not isinstance(resolved, (list, tuple)):
                resolved = [resolved]

            result = tuple(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转元组: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_tuple'}


class ConversionStringToListAction(BaseAction):
    """Convert string to list."""
    action_type = "conversion4_string_to_list"
    display_name = "字符串转列表"
    description = "将字符串转换为列表"
    version = "4.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute string to list.

        Args:
            context: Execution context.
            params: Dict with string, delimiter, output_var.

        Returns:
            ActionResult with list.
        """
        input_string = params.get('string', '')
        delimiter = params.get('delimiter', '')
        output_var = params.get('output_var', 'converted_list')

        try:
            resolved = context.resolve_value(input_string)
            resolved_delimiter = context.resolve_value(delimiter) if delimiter else ''

            if resolved_delimiter:
                result = resolved.split(resolved_delimiter)
            else:
                result = list(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字符串转列表: {len(result)}个元素",
                data={
                    'original': resolved,
                    'converted': result,
                    'delimiter': resolved_delimiter or 'char',
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字符串转列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'delimiter': '', 'output_var': 'converted_list'}