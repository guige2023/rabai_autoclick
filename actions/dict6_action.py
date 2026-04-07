"""Dict6 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictItemsAction: Get all items
- DictUpdateAction: Update dictionary
- DictPopAction: Pop item
- DictPopitemAction: Pop last item
- DictSetdefaultAction: Set default value
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictItemsAction(BaseAction):
    """Get all items."""
    action_type = "dict6_items"
    display_name = "获取所有键值对"
    description = "获取字典所有键值对"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute items.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with items list.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'items_result')

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

            result = list(d.items())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取键值对: {len(result)} 个",
                data={
                    'dict': d,
                    'items': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取所有键值对失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'items_result'}


class DictUpdateAction(BaseAction):
    """Update dictionary."""
    action_type = "dict6_update"
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
            resolved_dict = context.resolve_value(dict_var)
            resolved_updates = context.resolve_value(updates)

            d = context.get(resolved_dict) if isinstance(resolved_dict, str) else resolved_dict

            if not isinstance(d, dict):
                return ActionResult(
                    success=False,
                    message="dict_var 必须是字典"
                )

            if not isinstance(resolved_updates, dict):
                return ActionResult(
                    success=False,
                    message="updates 必须是字典"
                )

            d.update(resolved_updates)
            context.set(output_var, d)

            return ActionResult(
                success=True,
                message=f"更新字典: +{len(resolved_updates)} 个键",
                data={
                    'updates': resolved_updates,
                    'result': d,
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


class DictPopAction(BaseAction):
    """Pop item."""
    action_type = "dict6_pop"
    display_name = "弹出键值对"
    description = "弹出并返回指定键的值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute pop.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, default, output_var.

        Returns:
            ActionResult with popped value.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'pop_result')

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
                    message="dict_var 必须是字典"
                )

            if resolved_key in d:
                result = d.pop(resolved_key)
                found = True
            else:
                result = resolved_default
                found = False

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"弹出键值对: {'成功' if found else '键不存在'}",
                data={
                    'key': resolved_key,
                    'value': result,
                    'found': found,
                    'result': d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"弹出键值对失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'pop_result'}


class DictPopitemAction(BaseAction):
    """Pop last item."""
    action_type = "dict6_popitem"
    display_name = "弹出最后键值对"
    description = "弹出并返回最后的键值对"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute popitem.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with popped item.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'popitem_result')

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

            if len(d) == 0:
                return ActionResult(
                    success=False,
                    message="字典为空"
                )

            result = d.popitem()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"弹出最后键值对: {result[0]}",
                data={
                    'item': result,
                    'result': d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"弹出最后键值对失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'popitem_result'}


class DictSetdefaultAction(BaseAction):
    """Set default value."""
    action_type = "dict6_setdefault"
    display_name = "设置默认值"
    description = "设置键的默认值（如果键不存在）"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute setdefault.

        Args:
            context: Execution context.
            params: Dict with dict_var, key, default, output_var.

        Returns:
            ActionResult with value.
        """
        dict_var = params.get('dict_var', '')
        key = params.get('key', '')
        default = params.get('default', None)
        output_var = params.get('output_var', 'setdefault_result')

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
                    message="dict_var 必须是字典"
                )

            result = d.setdefault(resolved_key, resolved_default)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"设置默认值: {result}",
                data={
                    'key': resolved_key,
                    'default': resolved_default,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"设置默认值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'setdefault_result'}
