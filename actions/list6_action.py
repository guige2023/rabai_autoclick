"""List6 action module for RabAI AutoClick.

Provides additional list operations:
- ListMinAction: Get minimum element
- ListMaxAction: Get maximum element
- ListSumAction: Sum elements
- ListAvgAction: Average elements
- ListAnyAction: Any element is truthy
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ListMinAction(BaseAction):
    """Get minimum element."""
    action_type = "list6_min"
    display_name = "最小元素"
    description = "获取列表最小元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with minimum.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'min_result')

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

            if len(lst) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = min(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小元素: {result}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最小元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'min_result'}


class ListMaxAction(BaseAction):
    """Get maximum element."""
    action_type = "list6_max"
    display_name = "最大元素"
    description = "获取列表最大元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with maximum.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'max_result')

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

            if len(lst) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = max(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大元素: {result}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最大元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'max_result'}


class ListSumAction(BaseAction):
    """Sum elements."""
    action_type = "list6_sum"
    display_name = "求和"
    description = "对列表元素求和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with sum.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'sum_result')

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

            if len(lst) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = sum(float(x) for x in lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"求和: {result}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_result'}


class ListAvgAction(BaseAction):
    """Average elements."""
    action_type = "list6_avg"
    display_name = "平均值"
    description = "计算列表元素的平均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with average.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'avg_result')

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

            if len(lst) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = sum(float(x) for x in lst) / len(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平均值: {result}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'avg_result'}


class ListAnyAction(BaseAction):
    """Any element is truthy."""
    action_type = "list6_any"
    display_name = "任意为真"
    description = "检查列表是否有任意元素为真"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute any.

        Args:
            context: Execution context.
            params: Dict with list_var, output_var.

        Returns:
            ActionResult with any result.
        """
        list_var = params.get('list_var', '')
        output_var = params.get('output_var', 'any_result')

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

            result = any(lst)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"任意为真: {'是' if result else '否'}",
                data={
                    'list': lst,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查任意为真失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['list_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'any_result'}
