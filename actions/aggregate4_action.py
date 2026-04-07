"""Aggregate4 action module for RabAI AutoClick.

Provides additional aggregate operations:
- AggregateMedianAction: Calculate median
- AggregateStdevAction: Calculate standard deviation
- AggregateVarianceAction: Calculate variance
- AggregateProductAction: Calculate product
- AggregateFirstAction: Get first element
"""

import statistics
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateMedianAction(BaseAction):
    """Calculate median."""
    action_type = "aggregate4_median"
    display_name = "中位数"
    description = "计算数值列表的中位数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute median.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with median.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'median_result')

        valid, msg = self.validate_type(numbers, (list, tuple), 'numbers')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(numbers)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="numbers 必须是列表"
                )

            if len(resolved) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            nums = [float(n) for n in resolved]
            result = statistics.median(nums)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"中位数: {result}",
                data={
                    'numbers': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算中位数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'median_result'}


class AggregateStdevAction(BaseAction):
    """Calculate standard deviation."""
    action_type = "aggregate4_stdev"
    display_name = "标准差"
    description = "计算数值列表的标准差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute stdev.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with standard deviation.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'stdev_result')

        valid, msg = self.validate_type(numbers, (list, tuple), 'numbers')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(numbers)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="numbers 必须是列表"
                )

            if len(resolved) < 2:
                return ActionResult(
                    success=False,
                    message="列表至少需要2个元素"
                )

            nums = [float(n) for n in resolved]
            result = statistics.stdev(nums)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"标准差: {result}",
                data={
                    'numbers': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算标准差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'stdev_result'}


class AggregateVarianceAction(BaseAction):
    """Calculate variance."""
    action_type = "aggregate4_variance"
    display_name = "方差"
    description = "计算数值列表的方差"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute variance.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with variance.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'variance_result')

        valid, msg = self.validate_type(numbers, (list, tuple), 'numbers')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(numbers)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="numbers 必须是列表"
                )

            if len(resolved) < 2:
                return ActionResult(
                    success=False,
                    message="列表至少需要2个元素"
                )

            nums = [float(n) for n in resolved]
            result = statistics.variance(nums)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"方差: {result}",
                data={
                    'numbers': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算方差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'variance_result'}


class AggregateProductAction(BaseAction):
    """Calculate product."""
    action_type = "aggregate4_product"
    display_name = "乘积"
    description = "计算数值列表的乘积"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute product.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with product.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'product_result')

        valid, msg = self.validate_type(numbers, (list, tuple), 'numbers')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(numbers)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="numbers 必须是列表"
                )

            if len(resolved) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = 1
            for n in resolved:
                result *= float(n)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"乘积: {result}",
                data={
                    'numbers': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算乘积失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'product_result'}


class AggregateFirstAction(BaseAction):
    """Get first element."""
    action_type = "aggregate4_first"
    display_name = "第一个元素"
    description = "获取列表的第一个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute first.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with first element.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'first_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="items 必须是列表"
                )

            if len(resolved) == 0:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = resolved[0]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"第一个元素: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取第一个元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'first_result'}
