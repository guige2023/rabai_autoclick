"""Aggregate5 action module for RabAI AutoClick.

Provides additional aggregate operations:
- AggregateLastAction: Get last element
- AggregateSecondAction: Get second element
- AggregateThirdAction: Get third element
- AggregateModeAction: Calculate mode
- AggregateRangeAction: Calculate range
"""

import statistics
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AggregateLastAction(BaseAction):
    """Get last element."""
    action_type = "aggregate5_last"
    display_name = "最后一个元素"
    description = "获取列表的最后一个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute last.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with last element.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'last_result')

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

            result = resolved[-1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"最后一个元素: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取最后一个元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'last_result'}


class AggregateSecondAction(BaseAction):
    """Get second element."""
    action_type = "aggregate5_second"
    display_name = "第二个元素"
    description = "获取列表的第二个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute second.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with second element.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'second_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="items 必须是列表"
                )

            if len(resolved) < 2:
                return ActionResult(
                    success=False,
                    message="列表至少需要2个元素"
                )

            result = resolved[1]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"第二个元素: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取第二个元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'second_result'}


class AggregateThirdAction(BaseAction):
    """Get third element."""
    action_type = "aggregate5_third"
    display_name = "第三个元素"
    description = "获取列表的第三个元素"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute third.

        Args:
            context: Execution context.
            params: Dict with items, output_var.

        Returns:
            ActionResult with third element.
        """
        items = params.get('items', [])
        output_var = params.get('output_var', 'third_result')

        try:
            resolved = context.resolve_value(items)

            if not isinstance(resolved, (list, tuple)):
                return ActionResult(
                    success=False,
                    message="items 必须是列表"
                )

            if len(resolved) < 3:
                return ActionResult(
                    success=False,
                    message="列表至少需要3个元素"
                )

            result = resolved[2]
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"第三个元素: {result}",
                data={
                    'items': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取第三个元素失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'third_result'}


class AggregateModeAction(BaseAction):
    """Calculate mode."""
    action_type = "aggregate5_mode"
    display_name = "众数"
    description = "计算数值列表的众数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mode.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with mode.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'mode_result')

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

            result = statistics.mode(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"众数: {result}",
                data={
                    'numbers': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算众数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'mode_result'}


class AggregateRangeAction(BaseAction):
    """Calculate range."""
    action_type = "aggregate5_range"
    display_name = "范围"
    description = "计算数值列表的范围（最大值-最小值）"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute range.

        Args:
            context: Execution context.
            params: Dict with numbers, output_var.

        Returns:
            ActionResult with range.
        """
        numbers = params.get('numbers', [])
        output_var = params.get('output_var', 'range_result')

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
            result = max(nums) - min(nums)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"范围: {result}",
                data={
                    'numbers': resolved,
                    'min': min(nums),
                    'max': max(nums),
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算范围失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['numbers']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'range_result'}
