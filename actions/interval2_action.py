"""Interval2 action module for RabAI AutoClick.

Provides additional interval operations:
- IntervalCreateAction: Create time interval
- IntervalAddAction: Add to interval
- IntervalSubtractAction: Subtract from interval
- IntervalMultiplyAction: Multiply interval
- IntervalDivideAction: Divide interval
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class IntervalCreateAction(BaseAction):
    """Create time interval."""
    action_type = "interval2_create"
    display_name = "创建时间间隔"
    description = "创建时间间隔"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create interval.

        Args:
            context: Execution context.
            params: Dict with days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with interval in seconds.
        """
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'interval_seconds')

        try:
            from datetime import timedelta

            resolved_days = int(context.resolve_value(days)) if days else 0
            resolved_hours = int(context.resolve_value(hours)) if hours else 0
            resolved_minutes = int(context.resolve_value(minutes)) if minutes else 0
            resolved_seconds = int(context.resolve_value(seconds)) if seconds else 0

            delta = timedelta(
                days=resolved_days,
                hours=resolved_hours,
                minutes=resolved_minutes,
                seconds=resolved_seconds
            )

            total_seconds = int(delta.total_seconds())

            context.set(output_var, total_seconds)

            return ActionResult(
                success=True,
                message=f"时间间隔创建: {total_seconds}秒",
                data={
                    'days': resolved_days,
                    'hours': resolved_hours,
                    'minutes': resolved_minutes,
                    'seconds': resolved_seconds,
                    'total_seconds': total_seconds,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建时间间隔失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'interval_seconds'}


class IntervalAddAction(BaseAction):
    """Add to interval."""
    action_type = "interval2_add"
    display_name = "间隔相加"
    description = "将两个时间间隔相加"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add intervals.

        Args:
            context: Execution context.
            params: Dict with interval1, interval2, output_var.

        Returns:
            ActionResult with sum of intervals.
        """
        interval1 = params.get('interval1', 0)
        interval2 = params.get('interval2', 0)
        output_var = params.get('output_var', 'sum_interval')

        try:
            resolved1 = int(context.resolve_value(interval1)) if interval1 else 0
            resolved2 = int(context.resolve_value(interval2)) if interval2 else 0

            result = resolved1 + resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"间隔相加: {result}秒",
                data={
                    'interval1': resolved1,
                    'interval2': resolved2,
                    'sum': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"间隔相加失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval1', 'interval2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'sum_interval'}


class IntervalSubtractAction(BaseAction):
    """Subtract from interval."""
    action_type = "interval2_subtract"
    display_name = "间隔相减"
    description = "将两个时间间隔相减"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract intervals.

        Args:
            context: Execution context.
            params: Dict with interval1, interval2, output_var.

        Returns:
            ActionResult with difference of intervals.
        """
        interval1 = params.get('interval1', 0)
        interval2 = params.get('interval2', 0)
        output_var = params.get('output_var', 'difference_interval')

        try:
            resolved1 = int(context.resolve_value(interval1)) if interval1 else 0
            resolved2 = int(context.resolve_value(interval2)) if interval2 else 0

            result = resolved1 - resolved2

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"间隔相减: {result}秒",
                data={
                    'interval1': resolved1,
                    'interval2': resolved2,
                    'difference': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"间隔相减失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval1', 'interval2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'difference_interval'}


class IntervalMultiplyAction(BaseAction):
    """Multiply interval."""
    action_type = "interval2_multiply"
    display_name = "间隔乘法"
    description = "将时间间隔乘以倍数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute multiply interval.

        Args:
            context: Execution context.
            params: Dict with interval, multiplier, output_var.

        Returns:
            ActionResult with multiplied interval.
        """
        interval = params.get('interval', 0)
        multiplier = params.get('multiplier', 1)
        output_var = params.get('output_var', 'multiplied_interval')

        try:
            resolved = int(context.resolve_value(interval)) if interval else 0
            resolved_mult = float(context.resolve_value(multiplier)) if multiplier else 1

            result = int(resolved * resolved_mult)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"间隔乘法: {result}秒",
                data={
                    'interval': resolved,
                    'multiplier': resolved_mult,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"间隔乘法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval', 'multiplier']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'multiplied_interval'}


class IntervalDivideAction(BaseAction):
    """Divide interval."""
    action_type = "interval2_divide"
    display_name = "间隔除法"
    description = "将时间间隔除以倍数"
    version = "2.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute divide interval.

        Args:
            context: Execution context.
            params: Dict with interval, divisor, output_var.

        Returns:
            ActionResult with divided interval.
        """
        interval = params.get('interval', 0)
        divisor = params.get('divisor', 1)
        output_var = params.get('output_var', 'divided_interval')

        try:
            resolved = int(context.resolve_value(interval)) if interval else 0
            resolved_div = float(context.resolve_value(divisor)) if divisor else 1

            if resolved_div == 0:
                return ActionResult(
                    success=False,
                    message="除数不能为零"
                )

            result = int(resolved / resolved_div)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"间隔除法: {result}秒",
                data={
                    'interval': resolved,
                    'divisor': resolved_div,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"间隔除法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['interval', 'divisor']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'divided_interval'}