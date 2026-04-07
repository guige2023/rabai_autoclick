"""Date5 action module for RabAI AutoClick.

Provides additional date operations:
- DateAddMonthsAction: Add months to date
- DateSubtractMonthsAction: Subtract months from date
- DateIsWeekendAction: Check if weekend
- DateIsWeekdayAction: Check if weekday
- DateNextWeekdayAction: Get next weekday
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateAddMonthsAction(BaseAction):
    """Add months to date."""
    action_type = "date5_add_months"
    display_name = "日期加月数"
    description = "向日期添加月数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add months.

        Args:
            context: Execution context.
            params: Dict with date_str, months, format, output_var.

        Returns:
            ActionResult with new date.
        """
        date_str = params.get('date_str', '')
        months = params.get('months', 0)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'new_date')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_months = int(context.resolve_value(months))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            month = dt.month - 1 + resolved_months
            year = dt.year + month // 12
            month = month % 12 + 1
            day = min(dt.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])

            new_date = datetime(year, month, day)
            result = new_date.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期加月数: {result}",
                data={
                    'original': resolved_date,
                    'months_added': resolved_months,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期加月数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str', 'months']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'new_date'}


class DateSubtractMonthsAction(BaseAction):
    """Subtract months from date."""
    action_type = "date5_subtract_months"
    display_name = "日期减月数"
    description = "从日期减去月数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract months.

        Args:
            context: Execution context.
            params: Dict with date_str, months, format, output_var.

        Returns:
            ActionResult with new date.
        """
        date_str = params.get('date_str', '')
        months = params.get('months', 0)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'new_date')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_months = int(context.resolve_value(months))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            month = dt.month - 1 - resolved_months
            year = dt.year + month // 12
            month = month % 12 + 1
            day = min(dt.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])

            new_date = datetime(year, month, day)
            result = new_date.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期减月数: {result}",
                data={
                    'original': resolved_date,
                    'months_subtracted': resolved_months,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期减月数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str', 'months']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'new_date'}


class DateIsWeekendAction(BaseAction):
    """Check if weekend."""
    action_type = "date5_is_weekend"
    display_name = "判断周末"
    description = "判断日期是否为周末"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is weekend.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with weekend check.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'is_weekend')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            result = dt.weekday() >= 5
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"周末判断: {'是' if result else '否'}",
                data={
                    'date': resolved_date,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断周末失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'is_weekend'}


class DateIsWeekdayAction(BaseAction):
    """Check if weekday."""
    action_type = "date5_is_weekday"
    display_name = "判断工作日"
    description = "判断日期是否为工作日"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute is weekday.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with weekday check.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'is_weekday')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            result = dt.weekday() < 5
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"工作日判断: {'是' if result else '否'}",
                data={
                    'date': resolved_date,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"判断工作日失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'is_weekday'}


class DateNextWeekdayAction(BaseAction):
    """Get next weekday."""
    action_type = "date5_next_weekday"
    display_name = "下一个工作日"
    description = "获取下一个工作日"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute next weekday.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with next weekday.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'next_weekday')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt = datetime.strptime(resolved_date, resolved_format)
            days_ahead = 1
            while True:
                next_day = dt + timedelta(days=days_ahead)
                if next_day.weekday() < 5:
                    result = next_day.strftime(resolved_format)
                    break
                days_ahead += 1

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"下一个工作日: {result}",
                data={
                    'original': resolved_date,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取下一个工作日失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'next_weekday'}
