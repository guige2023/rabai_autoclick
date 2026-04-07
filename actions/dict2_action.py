"""Dict2 action module for RabAI AutoClick.

Provides additional dict operations:
- DictGetKeysAction: Get dictionary keys
- DictGetValuesAction: Get dictionary values
- DictGetItemsAction: Get dictionary items
- DictUpdateAction: Update dictionary
- DictMergeAction: Merge dictionaries
- DictInvertAction: Invert dictionary
"""

from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictGetKeysAction(BaseAction):
    """Get dictionary keys."""
    action_type = "dict_get_keys"
    display_name = "获取字典键"
    description = "获取字典的所有键"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get keys.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with keys list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_keys')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            result = list(d.keys())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取键: {len(result)} 个",
                data={
                    'count': len(result),
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
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_keys'}


class DictGetValuesAction(BaseAction):
    """Get dictionary values."""
    action_type = "dict_get_values"
    display_name = "获取字典值"
    description = "获取字典的所有值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get values.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with values list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_values')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            result = list(d.values())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取值: {len(result)} 个",
                data={
                    'count': len(result),
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


class DictGetItemsAction(BaseAction):
    """Get dictionary items."""
    action_type = "dict_get_items"
    display_name = "获取字典项"
    description = "获取字典的所有键值对"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get items.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with items list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_items')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            result = [(k, v) for k, v in d.items()]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取字典项: {len(result)} 个",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取字典项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_items'}


class DictUpdateAction(BaseAction):
    """Update dictionary."""
    action_type = "dict_update"
    display_name = "更新字典"
    description = "更新字典内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute update.

        Args:
            context: Execution context.
            params: Dict with dict_var, updates, output_var.

        Returns:
            ActionResult with updated dict.
        """
        dict_var = params.get('dict_var', '')
        updates = params.get('updates', {})
        output_var = params.get('output_var', 'updated_dict')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)
            resolved_updates = context.resolve_value(updates)

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            if isinstance(resolved_updates, dict):
                d.update(resolved_updates)
            elif isinstance(resolved_updates, (list, tuple)):
                for item in resolved_updates:
                    if isinstance(item, (list, tuple)) and len(item) == 2:
                        d[item[0]] = item[1]

            context.set(resolved_var, d)
            context.set(output_var, d)

            return ActionResult(
                success=True,
                message=f"字典更新: {len(d)} 项",
                data={
                    'count': len(d),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"更新字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'updates']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_dict'}


class DictMergeAction(BaseAction):
    """Merge dictionaries."""
    action_type = "dict_merge"
    display_name = "合并字典"
    description = "合并多个字典"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge.

        Args:
            context: Execution context.
            params: Dict with dicts, output_var.

        Returns:
            ActionResult with merged dict.
        """
        dicts = params.get('dicts', [])
        output_var = params.get('output_var', 'merged_dict')

        try:
            resolved_dicts = [context.resolve_value(d) for d in dicts]

            result = {}
            for d in resolved_dicts:
                if isinstance(d, dict):
                    result.update(d)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典合并: {len(result)} 项",
                data={
                    'count': len(result),
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


class DictInvertAction(BaseAction):
    """Invert dictionary."""
    action_type = "dict_invert"
    display_name = "反转字典"
    description = "将字典的键值互换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute invert.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with inverted dict.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'inverted_dict')

        valid, msg = self.validate_type(dict_var, str, 'dict_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_var = context.resolve_value(dict_var)

            d = context.get(resolved_var)
            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message=f"{resolved_var} 不是字典"
                )

            result = {v: k for k, v in d.items()}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典反转: {len(result)} 项",
                data={
                    'count': len(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"反转字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'inverted_dict'}
