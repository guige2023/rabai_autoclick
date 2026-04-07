"""List7 action module for RabAI AutoClick.

Provides additional list operations:
- ListAllAction: All elements are truthy
- ListJoinAction: Join list to string
- ListToSetAction: Convert list to set
- ListToTupleAction: Convert list to tuple
- ListClearAction: Clear list
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListAllAction(BaseAction):
    """All elements are truthy."""
    action_type = "list7_all"
    display_name = "全部为真"
    description = "检查列表所有元素是否都为真"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute all.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with all result.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'all_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            result = all(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"全部为真: {'是' if result else '否'}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查全部为真失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'all_result'}


class ListJoinAction(BaseAction):
    """Join list to string."""
    action_type = "list7_join"
    display_name = "连接列表为字符串"
    description = "使用分隔符将列表连接为字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join.

        Args:
            context: Execution context.
            params: Dict with list_var, separator, output_var.

        Returns:
            ActionResult with joined string.
        """
        list_var = params.get('list_var', '')
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'joined_string')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)
            resolved_sep = context.resolve_value(separator) if separator else ''

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            result = resolved_sep.join(str(x) for x in lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接为字符串: {len(result)} 字符",
                data={
                    'list': lst,
                    'separator': resolved_sep,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接列表为字符串失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'joined_string'}


class ListToSetAction(BaseAction):
    """Convert list to set."""
    action_type: "list7_to_set"
    display_name = "列表转集合"
    description = "将列表转换为集合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to set.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with set.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'set_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple, set)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            result = set(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转集合: {len(result)} 个元素",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转集合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'set_result'}


class ListToTupleAction(BaseAction):
    """Convert list to tuple."""
    action_type = "list7_to_tuple"
    display_name = "列表转元组"
    description = "将列表转换为元组"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute to tuple.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with tuple.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'tuple_result')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            result = tuple(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"列表转元组: {len(result)} 个元素",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"列表转元组失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'tuple_result'}


class ListClearAction(BaseAction):
    """Clear list."""
    action_type = "list7_clear"
    display_name = "清空列表"
    description = "清空列表所有元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute clear.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with cleared list.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'cleared_list')

        valid, msg = self.validate_type(list_var, str, 'list_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_list = context.resolve_value(list_var)

            lst = context.get(resolved_list) if isinstance(resolved_list, str) else resolved_list

            if not isinstance(lst, list):
                return ActionResult(
                    success=False,
                    message="list_var 必须是列表"
                )

            lst.clear()
            context.set(output_var, lst)

            return ActionResult(
                success=True,
                message=f"清空列表: {len(lst)} 个元素",
                data={
                    'result': lst,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"清空列表失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'cleared_list'}
