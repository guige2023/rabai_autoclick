"""Dict7 action module for RabAI AutoClick.

Provides additional dictionary operations:
- DictKeysAction: Get dictionary keys
- DictValuesAction: Get dictionary values
- DictItemsAction: Get dictionary items
- DictUpdateAction: Update dictionary
- DictClearAction: Clear dictionary
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DictKeysAction(BaseAction):
    """Get dictionary keys."""
    action_type = "dict7_keys"
    display_name = "字典键"
    description = "获取字典的所有键"
    version = "7.0"

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
            ActionResult with keys.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'keys_result')

        try:
            resolved = context.resolve_value(dictionary)

            if not isinstance(resolved, dict):
                return ActionResult(
                    success=False,
                    message=f"获取字典键失败: 输入不是字典"
                )

            result = list(resolved.keys())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取字典键: {len(result)} 个",
                data={
                    'dictionary': resolved,
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
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'keys_result'}


class DictValuesAction(BaseAction):
    """Get dictionary values."""
    action_type = "dict7_values"
    display_name = "字典值"
    description = "获取字典的所有值"
    version = "7.0"

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
            ActionResult with values.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'values_result')

        try:
            resolved = context.resolve_value(dictionary)

            if not isinstance(resolved, dict):
                return ActionResult(
                    success=False,
                    message=f"获取字典值失败: 输入不是字典"
                )

            result = list(resolved.values())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取字典值: {len(result)} 个",
                data={
                    'dictionary': resolved,
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
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'values_result'}


class DictItemsAction(BaseAction):
    """Get dictionary items."""
    action_type = "dict7_items"
    display_name = "字典项"
    description = "获取字典的所有键值对"
    version = "7.0"

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
            ActionResult with items.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'items_result')

        try:
            resolved = context.resolve_value(dictionary)

            if not isinstance(resolved, dict):
                return ActionResult(
                    success=False,
                    message=f"获取字典项失败: 输入不是字典"
                )

            result = list(resolved.items())
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取字典项: {len(result)} 个",
                data={
                    'dictionary': resolved,
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
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'items_result'}


class DictUpdateAction(BaseAction):
    """Update dictionary."""
    action_type = "dict7_update"
    display_name = "字典更新"
    description = "更新字典内容"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute update.

        Args:
            context: Execution context.
            params: Dict with dictionary, updates, output_var.

        Returns:
            ActionResult with updated dictionary.
        """
        dictionary = params.get('dictionary', {})
        updates = params.get('updates', {})
        output_var = params.get('output_var', 'updated_dict')

        try:
            resolved_dict = context.resolve_value(dictionary)
            resolved_updates = context.resolve_value(updates)

            if not isinstance(resolved_dict, dict):
                resolved_dict = {}
            if not isinstance(resolved_updates, dict):
                return ActionResult(
                    success=False,
                    message=f"字典更新失败: 更新内容不是字典"
                )

            result = {**resolved_dict, **resolved_updates}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典更新完成",
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
                message=f"字典更新失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dictionary', 'updates']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'updated_dict'}


class DictClearAction(BaseAction):
    """Clear dictionary."""
    action_type = "dict7_clear"
    display_name = "字典清空"
    description = "清空字典内容"
    version = "7.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with dictionary, output_var.

        Returns:
            ActionResult with empty dictionary.
        """
        dictionary = params.get('dictionary', {})
        output_var = params.get('output_var', 'cleared_dict')

        try:
            resolved = context.resolve_value(dictionary)

            if not isinstance(resolved, dict):
                return ActionResult(
                    success=False,
                    message=f"字典清空失败: 输入不是字典"
                )

            result = {}
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"字典清空完成",
                data={
                    'original': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"字典清空失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['dictionary']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cleared_dict'}