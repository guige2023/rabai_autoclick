"""Dict3 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictGetAction: Get value from dict
- DictSetAction: Set value in dict
- DictDeleteAction: Delete key from dict
- DictKeysAction: Get all keys
- DictValuesAction: Get all values
- DictMergeAction: Merge two dicts
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictGetAction(BaseAction):
    """Get value from dict."""
    action_type = "dict3_get"
    display_name = "获取字典值"
    description = "从字典中获取指定键的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict get.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'dict_value')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_dict} 不是字典"
                )

            result = d.get(resolved_key, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取字典值: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'found': resolved_key in d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'dict_value'}


class DictSetAction(BaseAction):
    """Set value in dict."""
    action_type = "dict3_set"
    display_name = "设置字典值"
    description = "在字典中设置指定键的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict set.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, value, output_var.

        Returns:
            ActionResult with updated dict.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'dict_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value) if value is not None else None

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_dict} 不是字典"
                )

            d[resolved_key] = resolved_value
            context.set(output_var, d)

            return ActionResult(
                success=True,
                message=f"设置字典值: {resolved_key}",
                data={
                    'key': resolved_key,
                    'value': resolved_value,
                    'result': d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_result'}


class DictDeleteAction(BaseAction):
    """Delete key from dict."""
    action_type = "dict3_delete"
    display_name = "删除字典键"
    description = "从字典中删除指定键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict delete.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, output_var.

        Returns:
            ActionResult with updated dict.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        output_var = params.get('output_var', 'dict_result')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
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

            deleted = resolved_key in d
            if deleted:
                del d[resolved_key]

            context.set(output_var, d)

            return ActionResult(
                success=True,
                message=f"删除字典键: {'成功' if deleted else '键不存在'}",
                data={
                    'key': resolved_key,
                    'deleted': deleted,
                    'result': d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"删除字典键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_result'}


class DictKeysAction(BaseAction):
    """Get all keys."""
    action_type = "dict3_keys"
    display_name = "获取字典键"
    description = "获取字典的所有键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict keys.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with keys.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_keys')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_dict} 不是字典"
                )

            keys = list(d.keys())
            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"获取字典键: {len(keys)} 个",
                data={
                    'keys': keys,
                    'count': len(keys),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_keys'}


class DictValuesAction(BaseAction):
    """Get all values."""
    action_type = "dict3_values"
    display_name = "获取字典值"
    description = "获取字典的所有值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict values.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with values.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_values')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dict_var)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_dict} 不是字典"
                )

            values = list(d.values())
            context.set(output_var, values)

            return ActionResult(
                success=True,
                message=f"获取字典值: {len(values)} 个",
                data={
                    'values': values,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_values'}


class DictMergeAction(BaseAction):
    """Merge two dicts."""
    action_type = "dict3_merge"
    display_name = "合并字典"
    description = "合并两个字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict merge.

        Args:
            context: Execution context.
            params: Dict with dict1, dict2, output_var.

        Returns:
            ActionResult with merged dict.
        """
        dict1_var = params.get('dict1', '')
        dict2_var = params.get('dict2', '')
        output_var = params.get('output_var', 'merged_dict')

        valid, msg = self.validate_type(dict1_var, str, 'dict1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dict2_var, str, 'dict2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict1 = context.resolve_value(dict1_var)
            resolved_dict2 = context.resolve_value(dict2_var)

            d1 = context.get(resolved_dict1) if isinstance(resolved_dict1, str) else resolved_dict1
            d2 = context.get(resolved_dict2) if isinstance(resolved_dict2, str) else resolved_dict2

            if not isinstance(d1, dict) or not isinstance(d2, dict):
                return ActionResult(
                    success=False,
                    message="合并需要两个字典对象"
                )

            merged = {**d1, **d2}
            context.set(output_var, merged)

            return ActionResult(
                success=True,
                message=f"合并字典: {len(merged)} 个键",
                data={
                    'dict1': d1,
                    'dict2': d2,
                    'merged': merged,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict1', 'dict2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_dict'}