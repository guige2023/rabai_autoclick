"""Tuple4 action module for RabAI AutoClick.

Provides additional tuple operations:
- TupleMaxAction: Get maximum element
- TupleSumAction: Sum elements
- TupleAvgAction: Average elements
- TupleLengthAction: Get tuple length
- TupleContainsAction: Check if contains element
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TupleMaxAction(BaseAction):
    """Get maximum element."""
    action_type = "tuple4_max"
    display_name = "元组最大值"
    description = "获取元组最大元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with maximum.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'max_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            if len(t) == 0:
                return ActionResult(
                    success=False,
                    message="元组不能为空"
                )

            result = max(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组最大值: {result}",
                data={
                    'tuple': t,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取元组最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'max_result'}


class TupleSumAction(BaseAction):
    """Sum elements."""
    action_type = "tuple4_sum"
    display_name = "元组求和"
    description = "对元组元素求和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with sum.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'sum_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            if len(t) == 0:
                return ActionResult(
                    success=False,
                    message="元组不能为空"
                )

            result = sum(float(x) for x in t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组求和: {result}",
                data={
                    'tuple': t,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组求和失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_result'}


class TupleAvgAction(BaseAction):
    """Average elements."""
    action_type = "tuple4_avg"
    display_name = "元组平均值"
    description = "计算元组元素的平均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with average.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'avg_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            if len(t) == 0:
                return ActionResult(
                    success=False,
                    message="元组不能为空"
                )

            result = sum(float(x) for x in t) / len(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组平均值: {result}",
                data={
                    'tuple': t,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算元组平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'avg_result'}


class TupleLengthAction(BaseAction):
    """Get tuple length."""
    action_type = "tuple4_length"
    display_name = "元组长度"
    description = "获取元组长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute length.

        Args:
            context: Execution context.
            params: Dict with tuple_var, output_var.

        Returns:
            ActionResult with length.
        """
        tuple_var = params.get('tuple_var', '')
        output_var = params.get('output_var', 'length_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            result = len(t)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组长度: {result}",
                data={
                    'tuple': t,
                    'length': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取元组长度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'length_result'}


class TupleContainsAction(BaseAction):
    """Check if contains element."""
    action_type = "tuple4_contains"
    display_name = "元组包含检查"
    description = "检查元组是否包含元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute contains.

        Args:
            context: Execution context.
            params: Dict with tuple_var, item, output_var.

        Returns:
            ActionResult with contains check.
        """
        tuple_var = params.get('tuple_var', '')
        item = params.get('item', None)
        output_var = params.get('output_var', 'contains_result')

        valid, msg = self.validate_type(tuple_var, str, 'tuple_var')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_tuple = context.resolve_value(tuple_var)
            resolved_item = context.resolve_value(item) if item is not None else None

            t = context.get(resolved_tuple) if isinstance(resolved_tuple, str) else resolved_tuple

            if not isinstance(t, (tuple, list)):
                return ActionResult(
                    success=False,
                    message="tuple_var 必须是元组或列表"
                )

            result = resolved_item in t
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"元组包含检查: {'是' if result else '否'}",
                data={
                    'tuple': t,
                    'item': resolved_item,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"元组包含检查失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['tuple_var', 'item']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'contains_result'}
