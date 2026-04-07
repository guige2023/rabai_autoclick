"""Dictionary action module for RabAI AutoClick.

Provides dictionary operations:
- DictCreateAction: Create a new dictionary
- DictGetAction: Get value from dictionary
- DictSetAction: Set value in dictionary
- DictDeleteAction: Delete key from dictionary
- DictKeysAction: Get all keys
- DictValuesAction: Get all values
- DictItemsAction: Get all items
- DictMergeAction: Merge two dictionaries
"""

from typing import Any, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DictCreateAction(BaseAction):
    """Create a new dictionary."""
    action_type = "dict_create"
    display_name = "创建字典"
    description = "创建一个新的字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute creating a dictionary.

        Args:
            context: Execution context.
            params: Dict with dict_var, pairs.

        Returns:
            ActionResult indicating success.
        """
        dict_var = params.get('dict_var', 'data')
        pairs = params.get('pairs', {})

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(pairs, dict, 'pairs')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            result_dict = dict(pairs)
            context.set(dict_var, result_dict)

            return ActionResult(
                success=True,
                message=f"已创建字典 {dict_var}: {len(result_dict)} 项",
                data={
                    'dict': result_dict,
                    'count': len(result_dict),
                    'output_var': dict_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'pairs': {}}


class DictGetAction(BaseAction):
    """Get value from dictionary."""
    action_type = "dict_get"
    display_name = "字典获取"
    description = "从字典获取值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting from dictionary.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        dict_var = params.get('dict_var', 'data')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'dict_value')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                return ActionResult(
                    success=False,
                    message=f"变量 {dict_var} 不是字典"
                )

            if key not in current_dict:
                if default is not None:
                    context.set(output_var, default)
                    return ActionResult(
                        success=True,
                        message=f"键 {key} 不存在，返回默认值",
                        data={
                            'key': key,
                            'value': default,
                            'found': False,
                            'output_var': output_var
                        }
                    )
                return ActionResult(
                    success=False,
                    message=f"键 {key} 不存在于字典中"
                )

            value = current_dict[key]
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取键 {key}",
                data={
                    'key': key,
                    'value': value,
                    'found': True,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典获取失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'dict_value'}


class DictSetAction(BaseAction):
    """Set value in dictionary."""
    action_type = "dict_set"
    display_name = "字典设置"
    description = "在字典中设置值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute setting in dictionary.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, value.

        Returns:
            ActionResult indicating success.
        """
        dict_var = params.get('dict_var', 'data')
        key = params.get('key', '')
        value = params.get('value', None)

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_string_not_empty(key, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                current_dict = {}

            current_dict[key] = value
            context.set(dict_var, current_dict)

            return ActionResult(
                success=True,
                message=f"已设置键 {key}",
                data={
                    'dict': current_dict,
                    'count': len(current_dict),
                    'key': key,
                    'value': value
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典设置失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DictDeleteAction(BaseAction):
    """Delete key from dictionary."""
    action_type = "dict_delete"
    display_name = "字典删除"
    description = "从字典删除键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deleting from dictionary.

        Args:
            context: Execution context.
            params: Dict with dict_var, key.

        Returns:
            ActionResult indicating success.
        """
        dict_var = params.get('dict_var', 'data')
        key = params.get('key', '')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                return ActionResult(
                    success=False,
                    message=f"变量 {dict_var} 不是字典"
                )

            if key not in current_dict:
                return ActionResult(
                    success=False,
                    message=f"键 {key} 不存在于字典中"
                )

            del current_dict[key]
            context.set(dict_var, current_dict)

            return ActionResult(
                success=True,
                message=f"已删除键 {key}",
                data={
                    'dict': current_dict,
                    'count': len(current_dict),
                    'deleted_key': key
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典删除失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {}


class DictKeysAction(BaseAction):
    """Get all keys from dictionary."""
    action_type = "dict_keys"
    display_name = "字典键列表"
    description = "获取字典所有键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting keys.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with keys list.
        """
        dict_var = params.get('dict_var', 'data')
        output_var = params.get('output_var', 'dict_keys')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                return ActionResult(
                    success=False,
                    message=f"变量 {dict_var} 不是字典"
                )

            keys = list(current_dict.keys())
            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"获取键列表: {len(keys)} 项",
                data={
                    'keys': keys,
                    'count': len(keys),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取键列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_keys'}


class DictValuesAction(BaseAction):
    """Get all values from dictionary."""
    action_type = "dict_values"
    display_name = "字典值列表"
    description = "获取字典所有值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting values.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with values list.
        """
        dict_var = params.get('dict_var', 'data')
        output_var = params.get('output_var', 'dict_values')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                return ActionResult(
                    success=False,
                    message=f"变量 {dict_var} 不是字典"
                )

            values = list(current_dict.values())
            context.set(output_var, values)

            return ActionResult(
                success=True,
                message=f"获取值列表: {len(values)} 项",
                data={
                    'values': values,
                    'count': len(values),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取值列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_values'}


class DictItemsAction(BaseAction):
    """Get all items from dictionary."""
    action_type = "dict_items"
    display_name = "字典项列表"
    description = "获取字典所有键值对"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute getting items.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with items list.
        """
        dict_var = params.get('dict_var', 'data')
        output_var = params.get('output_var', 'dict_items')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            current_dict = context.get(dict_var, {})

            if not isinstance(current_dict, dict):
                return ActionResult(
                    success=False,
                    message=f"变量 {dict_var} 不是字典"
                )

            items = list(current_dict.items())
            context.set(output_var, items)

            return ActionResult(
                success=True,
                message=f"获取项列表: {len(items)} 项",
                data={
                    'items': items,
                    'count': len(items),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取项列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_items'}


class DictMergeAction(BaseAction):
    """Merge two dictionaries."""
    action_type = "dict_merge"
    display_name = "字典合并"
    description = "合并两个字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merging dictionaries.

        Args:
            context: Execution context.
            params: Dict with dict1_var, dict2_var, output_var, overwrite.

        Returns:
            ActionResult with merged dictionary.
        """
        dict1_var = params.get('dict1_var', 'dict1')
        dict2_var = params.get('dict2_var', 'dict2')
        output_var = params.get('output_var', 'merged_dict')
        overwrite = params.get('overwrite', True)

        valid, msg = self.validate_type(dict1_var, str, 'dict1_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(dict2_var, str, 'dict2_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(overwrite, bool, 'overwrite')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            dict1 = context.get(dict1_var, {})
            dict2 = context.get(dict2_var, {})

            if not isinstance(dict1, dict):
                dict1 = {}
            if not isinstance(dict2, dict):
                dict2 = {}

            if overwrite:
                result = {**dict1, **dict2}
            else:
                result = {**dict2, **dict1}

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典合并完成: {len(result)} 项",
                data={
                    'merged': result,
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典合并失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict1_var', 'dict2_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_dict', 'overwrite': True}