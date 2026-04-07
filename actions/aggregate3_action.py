"""Aggregate3 action module for RabAI AutoClick.

Provides additional aggregate operations:
- AggregateSumAction: Sum values
- AggregateAvgAction: Average values
- AggregateMinAction: Minimum value
- AggregateMaxAction: Maximum value
- AggregateCountAction: Count items
- AggregateConcatAction: Concatenate values
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateSumAction(BaseAction):
    """Sum values."""
    action_type = "aggregate3_sum"
    display_name = "求和"
    description = "计算数值总和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sum.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with sum.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'sum_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="求和需要列表或元组"
                )

            result = sum(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"求和: {result}",
                data={
                    'items': resolved,
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
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_result'}


class AggregateAvgAction(BaseAction):
    """Average values."""
    action_type = "aggregate3_avg"
    display_name = "平均值"
    description = "计算数值平均值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute average.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with average.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'avg_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="平均值需要列表或元组"
                )

            if not resolved:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = sum(resolved) / len(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"平均值: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'count': len(resolved),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"平均值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'avg_result'}


class AggregateMinAction(BaseAction):
    """Minimum value."""
    action_type = "aggregate3_min"
    display_name = "最小值"
    description = "获取最小值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute min.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with min.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'min_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="最小值需要列表或元组"
                )

            if not resolved:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = min(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最小值: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最小值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'min_result'}


class AggregateMaxAction(BaseAction):
    """Maximum value."""
    action_type = "aggregate3_max"
    display_name = "最大值"
    description = "获取最大值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute max.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with max.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'max_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="最大值需要列表或元组"
                )

            if not resolved:
                return ActionResult(
                    success=False,
                    message="列表不能为空"
                )

            result = max(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最大值: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"最大值失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'max_result'}


class AggregateCountAction(BaseAction):
    """Count items."""
    action_type = "aggregate3_count"
    display_name = "计数"
    description = "计算列表长度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute count.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with count.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'count_result')

        try:
            resolved = context.resolve_value(items)

            if isinstance(resolved, (list, tuple, str)):
                result = len(resolved)
            elif isinstance(resolved, dict):
                result = len(resolved.keys())
            else:
                result = 1

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"计数: {result}",
                data={
                    'items': resolved,
                    'count': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'count_result'}


class AggregateConcatAction(BaseAction):
    """Concatenate values."""
    action_type = "aggregate3_concat"
    display_name = "连接"
    description = "连接列表或字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute concat.

        Args:
            context: Execution context.
            params: Dict with items, separator, output_var.

        Returns:
            ActionResult with concatenated string.
        """
        items = params.get('items', [])
        separator = params.get('separator', '')
        output_var = params.get('output_var', 'concat_result')

        try:
            resolved = context.resolve_value(items)
            resolved_sep = str(context.resolve_value(separator)) if separator else ''

            if isinstance(resolved, (list, tuple)):
                result = resolved_sep.join(str(item) for item in resolved)
            elif isinstance(resolved, str):
                result = resolved
            else:
                result = str(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"连接: {result[:50]}...",
                data={
                    'items': resolved,
                    'separator': resolved_sep,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'separator': '', 'output_var': 'concat_result'}