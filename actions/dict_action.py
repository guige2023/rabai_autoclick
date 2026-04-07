"""Dict action module for RabAI AutoClick.

Provides dictionary operations:
- DictGetAction: Get value from dict
- DictSetAction: Set value in dict
- DictDeleteAction: Delete key from dict
- DictKeysAction: Get all keys
- DictValuesAction: Get all values
- DictItemsAction: Get all items
- DictMergeAction: Merge dictionaries
- DictHasKeyAction: Check if key exists
- DictSizeAction: Get dict size
- DictFlattenAction: Flatten nested dict
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictGetAction(BaseAction):
    """Get value from dict."""
    action_type = "dict_get"
    display_name = "字典取值"
    description = "获取字典值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get.

        Args:
            context: Execution context.
            params: Dict with dictionary, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        dictionary = params.get('dictionary', {})
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'dict_value')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_key = context.resolve_value(key)
            resolved_default = context.resolve_value(default) if default is not None else None

            value = resolved_dict.get(resolved_key, resolved_default)
            context.set(output_var, value)

            return ActionResult(
                success=True,
                message=f"获取: {resolved_key} = {value}",
                data={'value': value, 'key': resolved_key, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典取值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'dict_value'}


class DictSetAction(BaseAction):
    """Set value in dict."""
    action_type = "dict_set"
    display_name = "字典赋值"
    description = "设置字典值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute set.

        Args:
            context: Execution context.
            params: Dict with dictionary, key, value, output_var.

        Returns:
            ActionResult indicating success.
        """
        dictionary = params.get('dictionary', {})
        key = params.get('key', '')
        value = params.get('value', None)
        output_var = params.get('output_var', 'dict_result')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_key = context.resolve_value(key)
            resolved_value = context.resolve_value(value)

            resolved_dict[resolved_key] = resolved_value
            context.set(output_var, resolved_dict)

            return ActionResult(
                success=True,
                message=f"已设置: {resolved_key}",
                data={'dictionary': resolved_dict, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典赋值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'key', 'value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_result'}


class DictDeleteAction(BaseAction):
    """Delete key from dict."""
    action_type = "dict_delete"
    display_name = "字典删除"
    description = "删除字典键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute delete.

        Args:
            context: Execution context.
            params: Dict with dictionary, key, output_var.

        Returns:
            ActionResult indicating success.
        """
        dictionary = params.get('dictionary', {})
        key = params.get('key', '')
        output_var = params.get('output_var', 'dict_result')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_key = context.resolve_value(key)

            if resolved_key in resolved_dict:
                del resolved_dict[resolved_key]

            context.set(output_var, resolved_dict)

            return ActionResult(
                success=True,
                message=f"已删除: {resolved_key}",
                data={'dictionary': resolved_dict, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典删除失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_result'}


class DictKeysAction(BaseAction):
    """Get all keys."""
    action_type = "dict_keys"
    display_name = "字典键列表"
    description = "获取字典所有键"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute keys.

        Args:
            context: Execution context.
            params: Dict with dictionary, output_var.

        Returns:
            ActionResult with keys list.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'dict_keys')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            keys = list(resolved_dict.keys())

            context.set(output_var, keys)

            return ActionResult(
                success=True,
                message=f"键列表: {len(keys)} 个",
                data={'keys': keys, 'count': len(keys), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取字典键失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_keys'}


class DictValuesAction(BaseAction):
    """Get all values."""
    action_type = "dict_values"
    display_name = "字典值列表"
    description = "获取字典所有值"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute values.

        Args:
            context: Execution context.
            params: Dict with dictionary, output_var.

        Returns:
            ActionResult with values list.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'dict_values')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            values = list(resolved_dict.values())

            context.set(output_var, values)

            return ActionResult(
                success=True,
                message=f"值列表: {len(values)} 个",
                data={'values': values, 'count': len(values), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取字典值失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_values'}


class DictItemsAction(BaseAction):
    """Get all items."""
    action_type = "dict_items"
    display_name = "字典项列表"
    description = "获取字典所有键值对"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute items.

        Args:
            context: Execution context.
            params: Dict with dictionary, output_var.

        Returns:
            ActionResult with items list.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'dict_items')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            items = list(resolved_dict.items())

            context.set(output_var, items)

            return ActionResult(
                success=True,
                message=f"项列表: {len(items)} 个",
                data={'items': items, 'count': len(items), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取字典项失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_items'}


class DictMergeAction(BaseAction):
    """Merge dictionaries."""
    action_type = "dict_merge"
    display_name = "字典合并"
    description = "合并字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with dictionaries, output_var.

        Returns:
            ActionResult with merged dict.
        """
        dictionaries = params.get('dictionaries', [])
        output_var = params.get('output_var', 'dict_merged')

        valid, msg = self.validate_type(dictionaries, list, 'dictionaries')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dicts = context.resolve_value(dictionaries)

            merged = {}
            for d in resolved_dicts:
                if isinstance(d, dict):
                    merged.update(d)

            context.set(output_var, merged)

            return ActionResult(
                success=True,
                message=f"已合并: {len(merged)} 个键",
                data={'dictionary': merged, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典合并失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionaries']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_merged'}


class DictHasKeyAction(BaseAction):
    """Check if key exists."""
    action_type = "dict_has_key"
    display_name = "字典键检查"
    description = "检查字典键是否存在"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute has_key.

        Args:
            context: Execution context.
            params: Dict with dictionary, key, output_var.

        Returns:
            ActionResult with exists flag.
        """
        dictionary = params.get('dictionary', {})
        key = params.get('key', '')
        output_var = params.get('output_var', 'dict_has_key')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(key, str, 'key')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_key = context.resolve_value(key)

            exists = resolved_key in resolved_dict
            context.set(output_var, exists)

            return ActionResult(
                success=True,
                message=f"键{'存在' if exists else '不存在'}: {resolved_key}",
                data={'exists': exists, 'key': resolved_key, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典键检查失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_has_key'}


class DictSizeAction(BaseAction):
    """Get dict size."""
    action_type = "dict_size"
    display_name = "字典大小"
    description = "获取字典大小"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute size.

        Args:
            context: Execution context.
            params: Dict with dictionary, output_var.

        Returns:
            ActionResult with size.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'dict_size')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            size = len(resolved_dict)

            context.set(output_var, size)

            return ActionResult(
                success=True,
                message=f"字典大小: {size}",
                data={'size': size, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取字典大小失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_size'}


class DictFlattenAction(BaseAction):
    """Flatten nested dict."""
    action_type = "dict_flatten"
    display_name = "字典扁平化"
    description = "扁平化嵌套字典"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute flatten.

        Args:
            context: Execution context.
            params: Dict with dictionary, separator, output_var.

        Returns:
            ActionResult with flattened dict.
        """
        dictionary = params.get('dictionary', {})
        separator = params.get('separator', '.')
        output_var = params.get('output_var', 'dict_flattened')

        valid, msg = self.validate_type(dictionary, dict, 'dictionary')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_sep = context.resolve_value(separator)

            flattened = {}
            self._flatten(resolved_dict, '', flattened, resolved_sep)

            context.set(output_var, flattened)

            return ActionResult(
                success=True,
                message=f"已扁平化: {len(flattened)} 个键",
                data={'dictionary': flattened, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"字典扁平化失败: {str(e)}")

    def _flatten(self, d: dict, prefix: str, result: dict, sep: str):
        for k, v in d.items():
            new_key = f"{prefix}{sep}{k}" if prefix else k
            if isinstance(v, dict):
                self._flatten(v, new_key, result, sep)
            else:
                result[new_key] = v

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '.', 'output_var': 'dict_flattened'}
