"""Datetime4 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeWeekdayAction: Get day of week
- DatetimeWeekAction: Get week number
- DatetimeQuarterAction: Get quarter
- DatetimeIsWeekendAction: Check if weekend
- DatetimeDaysInMonthAction: Get days in month
"""

from datetime import datetime
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeWeekdayAction(BaseAction):
    """Get day of week."""
    action_type = "datetime4_weekday"
    display_name = "星期几"
    description = "获取日期是星期几"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute weekday.

        Args:
            context: Execution context.
            params: Dict with datetime_str, output_var.

        Returns:
            ActionResult with weekday.
        """
        datetime_str = params.get('datetime_str', None)
        output_var = params.get('output_var', 'weekday_result')

        try:
            if datetime_str is None:
                dt = datetime.now()
            else:
                resolved = context.resolve_value(datetime_str)
                if isinstance(resolved, str):
                    dt = datetime.fromisoformat(resolved)
                elif isinstance(resolved, datetime):
                    dt = resolved
                else:
                    dt = datetime.fromtimestamp(resolved)

            weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            result = {
                'number': dt.weekday(),
                'name': weekdays[dt.weekday()],
                'is_weekend': dt.weekday() >= 5
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"星期: {result['name']}",
                data={
                    'weekday': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取星期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_str': None, 'output_var': 'weekday_result'}


class DatetimeWeekAction(BaseAction):
    """Get week number."""
    action_type = "datetime4_week"
    display_name = "第几周"
    description = "获取日期是第几周"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute week.

        Args:
            context: Execution context.
            params: Dict with datetime_str, output_var.

        Returns:
            ActionResult with week number.
        """
        datetime_str = params.get('datetime_str', None)
        output_var = params.get('output_var', 'week_result')

        try:
            if datetime_str is None:
                dt = datetime.now()
            else:
                resolved = context.resolve_value(datetime_str)
                if isinstance(resolved, str):
                    dt = datetime.fromisoformat(resolved)
                elif isinstance(resolved, datetime):
                    dt = resolved
                else:
                    dt = datetime.fromtimestamp(resolved)

            week_num = dt.isocalendar()[1]
            context.set(output_var, week_num)

            return ActionResult(
                success=True,
                message=f"第{week_num}周",
                data={
                    'week': week_num,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取周数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_str': None, 'output_var': 'week_result'}


class DatetimeQuarterAction(BaseAction):
    """Get quarter."""
    action_type = "datetime4_quarter"
    display_name = "第几季度"
    description = "获取日期是第几季度"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute quarter.

        Args:
            context: Execution context.
            params: Dict with datetime_str, output_var.

        Returns:
            ActionResult with quarter.
        """
        datetime_str = params.get('datetime_str', None)
        output_var = params.get('output_var', 'quarter_result')

        try:
            if datetime_str is None:
                dt = datetime.now()
            else:
                resolved = context.resolve_value(datetime_str)
                if isinstance(resolved, str):
                    dt = datetime.fromisoformat(resolved)
                elif isinstance(resolved, datetime):
                    dt = resolved
                else:
                    dt = datetime.fromtimestamp(resolved)

            quarter = (dt.month - 1) // 3 + 1
            context.set(output_var, quarter)

            return ActionResult(
                success=True,
                message=f"第{quarter}季度",
                data={
                    'quarter': quarter,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取季度失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_str': None, 'output_var': 'quarter_result'}


class DatetimeIsWeekendAction(BaseAction):
    """Check if weekend."""
    action_type = "datetime4_is_weekend"
    display_name = "是否周末"
    description = "检查日期是否是周末"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is weekend.

        Args:
            context: Execution context.
            params: Dict with datetime_str, output_var.

        Returns:
            ActionResult with check result.
        """
        datetime_str = params.get('datetime_str', None)
        output_var = params.get('output_var', 'is_weekend_result')

        try:
            if datetime_str is None:
                dt = datetime.now()
            else:
                resolved = context.resolve_value(datetime_str)
                if isinstance(resolved, str):
                    dt = datetime.fromisoformat(resolved)
                elif isinstance(resolved, datetime):
                    dt = resolved
                else:
                    dt = datetime.fromtimestamp(resolved)

            is_weekend = dt.weekday() >= 5
            context.set(output_var, is_weekend)

            return ActionResult(
                success=True,
                message=f"{'是周末' if is_weekend else '不是周末'}",
                data={
                    'is_weekend': is_weekend,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"检查周末失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_str': None, 'output_var': 'is_weekend_result'}


class DatetimeDaysInMonthAction(BaseAction):
    """Get days in month."""
    action_type = "datetime4_days_in_month"
    display_name = "月份天数"
    description = "获取指定月份的天数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute days in month.

        Args:
            context: Execution context.
            params: Dict with year, month, output_var.

        Returns:
            ActionResult with days count.
        """
        year = params.get('year', None)
        month = params.get('month', None)
        output_var = params.get('output_var', 'days_result')

        try:
            if year is None or month is None:
                dt = datetime.now()
                y, m = dt.year, dt.month
            else:
                y = int(context.resolve_value(year))
                m = int(context.resolve_value(month))

            import calendar
            days = calendar.monthrange(y, m)[1]
            context.set(output_var, days)

            return ActionResult(
                success=True,
                message=f"{y}年{m}月有{days}天",
                data={
                    'year': y,
                    'month': m,
                    'days': days,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取月份天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'year': None, 'month': None, 'output_var': 'days_result'}