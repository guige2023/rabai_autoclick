"""Dict9 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictGetAction: Get value from dict
- DictKeysAction: Get dict keys
- DictValuesAction: Get dict values
- DictItemsAction: Get dict items
- DictUpdateAction: Update dict
- DictMergeAction: Merge dicts
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictGetAction(BaseAction):
    """Get value from dict."""
    action_type = "dict9_get"
    display_name = "字典取值"
    description = "从字典获取值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict get.

        Args:
            context: Execution context.
            params: Dict with dict, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        dict_param = params.get('dict', {})
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'dict_value')

        try:
            resolved_dict = context.resolve_value(dict_param)
            resolved_key = context.resolve_value(key) if key else ''
            resolved_default = context.resolve_value(default) if default is not None else None

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}

            result = resolved_dict.get(resolved_key, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典取值: {resolved_key}",
                data={
                    'dict': resolved_dict,
                    'key': resolved_key,
                    'value': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典取值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'dict_value'}


class DictKeysAction(BaseAction):
    """Get dict keys."""
    action_type = "dict9_keys"
    display_name = "字典键"
    description = "获取字典所有键"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict keys.

        Args:
            context: Execution context.
            params: Dict with dict, output_var.

        Returns:
            ActionResult with keys.
        """
        dict_param = params.get('dict', {})
        output_var = params.get('output_var', 'dict_keys')

        try:
            resolved_dict = context.resolve_value(dict_param)

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}

            result = list(resolved_dict.keys())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典键: {len(result)}个",
                data={
                    'dict': resolved_dict,
                    'keys': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_keys'}


class DictValuesAction(BaseAction):
    """Get dict values."""
    action_type = "dict9_values"
    display_name = "字典值"
    description = "获取字典所有值"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict values.

        Args:
            context: Execution context.
            params: Dict with dict, output_var.

        Returns:
            ActionResult with values.
        """
        dict_param = params.get('dict', {})
        output_var = params.get('output_var', 'dict_values')

        try:
            resolved_dict = context.resolve_value(dict_param)

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}

            result = list(resolved_dict.values())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典值: {len(result)}个",
                data={
                    'dict': resolved_dict,
                    'values': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_values'}


class DictItemsAction(BaseAction):
    """Get dict items."""
    action_type = "dict9_items"
    display_name = "字典项"
    description = "获取字典所有项"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict items.

        Args:
            context: Execution context.
            params: Dict with dict, output_var.

        Returns:
            ActionResult with items.
        """
        dict_param = params.get('dict', {})
        output_var = params.get('output_var', 'dict_items')

        try:
            resolved_dict = context.resolve_value(dict_param)

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}

            result = list(resolved_dict.items())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典项: {len(result)}个",
                data={
                    'dict': resolved_dict,
                    'items': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_items'}


class DictUpdateAction(BaseAction):
    """Update dict."""
    action_type = "dict9_update"
    display_name = "字典更新"
    description = "更新字典"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict update.

        Args:
            context: Execution context.
            params: Dict with dict, updates, output_var.

        Returns:
            ActionResult with updated dict.
        """
        dict_param = params.get('dict', {})
        updates = params.get('updates', {})
        output_var = params.get('output_var', 'updated_dict')

        try:
            resolved_dict = context.resolve_value(dict_param)
            resolved_updates = context.resolve_value(updates)

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}
            if not isinstance(resolved_updates, dict):
                resolved_updates = {}

            result = {**resolved_dict, **resolved_updates}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典更新: {len(resolved_updates)}项",
                data={
                    'original': resolved_dict,
                    'updates': resolved_updates,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"更新字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict', 'updates']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_dict'}


class DictMergeAction(BaseAction):
    """Merge dicts."""
    action_type = "dict9_merge"
    display_name = "字典合并"
    description = "合并多个字典"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict merge.

        Args:
            context: Execution context.
            params: Dict with dicts, output_var.

        Returns:
            ActionResult with merged dict.
        """
        dicts = params.get('dicts', [])
        output_var = params.get('output_var', 'merged_dict')

        try:
            resolved_dicts = context.resolve_value(dicts)

            if not isinstance(resolved_dicts, (list, tuple)):
                resolved_dicts = [resolved_dicts]

            result = {}
            for d in resolved_dicts:
                if isinstance(d, dict):
                    result.update(d)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典合并: {len(result)}个键",
                data={
                    'dicts': resolved_dicts,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"合并字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dicts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'merged_dict'}