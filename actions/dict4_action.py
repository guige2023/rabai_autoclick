"""Dict4 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictHasKeyAction: Check if key exists
- DictUpdateAction: Update dictionary
- DictPopAction: Pop item
- DictClearAction: Clear dictionary
- DictItemsAction: Get items
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictHasKeyAction(BaseAction):
    """Check if key exists."""
    action_type = "dict4_has_key"
    display_name = "检查字典键"
    description = "检查字典中键是否存在"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict has key.

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
                message=f"检查字典键失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'has_key_result'}


class DictUpdateAction(BaseAction):
    """Update dictionary."""
    action_type = "dict4_update"
    display_name = "更新字典"
    description = "更新字典内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict update.

        Args:
            context: Execution context.
            params: Dict with dict_var, updates, output_var.

        Returns:
            ActionResult with updated dict.
        """
        dict_var = params.get('dict_var', '')
        updates = params.get('updates', {})
        output_var = params.get('output_var', 'dict_result')

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
                    message=f"{resolved_dict} 不是字典"
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
                message=f"字典已更新: +{len(resolved_updates)} 个键",
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
        return {'output_var': 'dict_result'}


class DictPopAction(BaseAction):
    """Pop item."""
    action_type = "dict4_pop"
    display_name = "弹出字典项"
    description = "弹出并返回字典中的项"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict pop.

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
                    message=f"{resolved_dict} 不是字典"
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
                message=f"弹出: {'成功' if found else '键不存在'}",
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
                message=f"弹出字典项失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'default': None, 'output_var': 'pop_result'}


class DictClearAction(BaseAction):
    """Clear dictionary."""
    action_type = "dict4_clear"
    display_name = "清空字典"
    description = "清空字典所有内容"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict clear.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with cleared dict.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_result')

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

            d.clear()
            context.set(output_var, d)

            return ActionResult(
                success=True,
                message="字典已清空",
                data={
                    'result': d,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空字典失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dict_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'dict_result'}


class DictItemsAction(BaseAction):
    """Get items."""
    action_type = "dict4_items"
    display_name = "获取字典项"
    description = "获取字典所有键值对"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute dict items.

        Args:
            context: Execution context.
            params: Dict with dict_var, output_var.

        Returns:
            ActionResult with items.
        """
        dict_var = params.get('dict_var', '')
        output_var = params.get('output_var', 'dict_items')

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

            items = list(d.items())
            context.set(output_var, items)

            return ActionResult(
                success=True,
                message=f"字典项: {len(items)} 个",
                data={
                    'items': items,
                    'count': len(items),
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