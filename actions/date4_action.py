"""Date4 action module for RabAI AutoClick.

Provides additional date operations:
- DateTomorrowAction: Get tomorrow's date
- DateYesterdayAction: Get yesterday's date
- DateStartOfMonthAction: Get start of month
- DateEndOfMonthAction: Get end of month
- DateDaysBetweenAction: Get days between dates
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateTomorrowAction(BaseAction):
    """Get tomorrow's date."""
    action_type = "date4_tomorrow"
    display_name = "明日日期"
    description = "获取明天的日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute tomorrow.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with tomorrow's date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'tomorrow_date')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'
            tomorrow = (datetime.now() + timedelta(days=1)).strftime(resolved_format)
            context.set(output_var, tomorrow)

            return ActionResult(
                success=True,
                message=f"明日日期: {tomorrow}",
                data={
                    'date': tomorrow,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取明日日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'tomorrow_date'}


class DateYesterdayAction(BaseAction):
    """Get yesterday's date."""
    action_type = "date4_yesterday"
    display_name = "昨日日期"
    description = "获取昨天的日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute yesterday.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with yesterday's date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'yesterday_date')

        try:
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'
            yesterday = (datetime.now() - timedelta(days=1)).strftime(resolved_format)
            context.set(output_var, yesterday)

            return ActionResult(
                success=True,
                message=f"昨日日期: {yesterday}",
                data={
                    'date': yesterday,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取昨日日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'yesterday_date'}


class DateStartOfMonthAction(BaseAction):
    """Get start of month."""
    action_type = "date4_start_of_month"
    display_name = "月初日期"
    description = "获取本月初日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute start of month.

        Args:
            context: Execution context.
            params: Dict with year, month, format, output_var.

        Returns:
            ActionResult with start of month date.
        """
        year = params.get('year', None)
        month = params.get('month', None)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'start_of_month')

        try:
            now = datetime.now()
            resolved_year = int(context.resolve_value(year)) if year else now.year
            resolved_month = int(context.resolve_value(month)) if month else now.month
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            start = datetime(resolved_year, resolved_month, 1)
            result = start.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"月初日期: {result}",
                data={
                    'year': resolved_year,
                    'month': resolved_month,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取月初日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'year': None, 'month': None, 'format': '%Y-%m-%d', 'output_var': 'start_of_month'}


class DateEndOfMonthAction(BaseAction):
    """Get end of month."""
    action_type = "date4_end_of_month"
    display_name = "月末日期"
    description = "获取本月末日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute end of month.

        Args:
            context: Execution context.
            params: Dict with year, month, format, output_var.

        Returns:
            ActionResult with end of month date.
        """
        year = params.get('year', None)
        month = params.get('month', None)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'end_of_month')

        try:
            now = datetime.now()
            resolved_year = int(context.resolve_value(year)) if year else now.year
            resolved_month = int(context.resolve_value(month)) if month else now.month
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            if resolved_month == 12:
                end = datetime(resolved_year + 1, 1, 1) - timedelta(days=1)
            else:
                end = datetime(resolved_year, resolved_month + 1, 1) - timedelta(days=1)

            result = end.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"月末日期: {result}",
                data={
                    'year': resolved_year,
                    'month': resolved_month,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取月末日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'year': None, 'month': None, 'format': '%Y-%m-%d', 'output_var': 'end_of_month'}


class DateDaysBetweenAction(BaseAction):
    """Get days between dates."""
    action_type = "date4_days_between"
    display_name = "日期间天数"
    description = "计算两个日期之间的天数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute days between.

        Args:
            context: Execution context.
            params: Dict with date1, date2, format, output_var.

        Returns:
            ActionResult with days between.
        """
        date1 = params.get('date1', '')
        date2 = params.get('date2', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'days_between')

        valid, msg = self.validate_type(date1, str, 'date1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(date2, str, 'date2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date1 = context.resolve_value(date1)
            resolved_date2 = context.resolve_value(date2)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            dt1 = datetime.strptime(resolved_date1, resolved_format)
            dt2 = datetime.strptime(resolved_date2, resolved_format)

            diff = abs((dt1 - dt2).days)
            context.set(output_var, diff)

            return ActionResult(
                success=True,
                message=f"日期间天数: {diff} 天",
                data={
                    'date1': resolved_date1,
                    'date2': resolved_date2,
                    'days': diff,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算日期间天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date1', 'date2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'days_between'}
