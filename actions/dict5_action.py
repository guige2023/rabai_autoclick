"""Dict5 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictLengthAction: Get dictionary length
- DictIsEmptyAction: Check if dictionary is empty
- DictCopyAction: Copy dictionary
- DictKeysAction: Get all keys
- DictValuesAction: Get all values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictLengthAction(BaseAction):
    """Get dictionary length."""
    action_type = "dict5_length"
    display_name = "字典长度"
    description = "获取字典键值对数量"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute length.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with length.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'length_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            result = len(d)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典长度: {result}",
                data={
                    'dict': d,
                    'length': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'length_result'}


class DictIsEmptyAction(BaseAction):
    """Check if dictionary is empty."""
    action_type = "dict5_is_empty"
    display_name = "字典是否为空"
    description = "检查字典是否为空"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is empty.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with empty check.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'is_empty_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            result = len(d) == 0
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典为空: {'是' if result else '否'}",
                data={
                    'dict': d,
                    'is_empty': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查字典是否为空失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'is_empty_result'}


class DictCopyAction(BaseAction):
    """Copy dictionary."""
    action_type = "dict5_copy"
    display_name = "复制字典"
    description = "复制字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute copy.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with copied dict.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'copied_dict')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            result = d.copy()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"复制字典: {len(result)} 个键值对",
                data={
                    'original': d,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"复制字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'copied_dict'}


class DictKeysAction(BaseAction):
    """Get all keys."""
    action_type = "dict5_keys"
    display_name = "获取所有键"
    description = "获取字典所有键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keys.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with keys list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'keys_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            result = list(d.keys())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取键: {len(result)} 个",
                data={
                    'dict': d,
                    'keys': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取所有键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'keys_result'}


class DictValuesAction(BaseAction):
    """Get all values."""
    action_type = "dict5_values"
    display_name = "获取所有值"
    description = "获取字典所有值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute values.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with values list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'values_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            result = list(d.values())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取值: {len(result)} 个",
                data={
                    'dict': d,
                    'values': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取所有值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'values_result'}
