"""Date2 action module for RabAI AutoClick.

Provides additional date operations:
- DateTodayAction: Get today's date
- DateCreateAction: Create date from year, month, day
- DateFromTimestampAction: Create date from timestamp
- DateToTimestampAction: Convert date to timestamp
- DateAddDaysAction: Add days to date
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateTodayAction(BaseAction):
    """Get today's date."""
    action_type = "date2_today"
    display_name = "获取今日日期"
    description = "获取当前日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute get today date.

        Args:
            context: Execution context.
            params: Dict with format, output_var.

        Returns:
            ActionResult with today's date.
        """
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'today_date')

        try:
            from datetime import datetime
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'
            today = datetime.now().strftime(resolved_format)
            context.set(output_var, today)

            return ActionResult(
                success=True,
                message=f"今日日期: {today}",
                data={
                    'date': today,
                    'format': resolved_format,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取今日日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'today_date'}


class DateCreateAction(BaseAction):
    """Create date from year, month, day."""
    action_type = "date2_create"
    display_name = "创建日期"
    description = "从年月日创建日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute create date.

        Args:
            context: Execution context.
            params: Dict with year, month, day, output_var.

        Returns:
            ActionResult with created date.
        """
        year = params.get('year', 2024)
        month = params.get('month', 1)
        day = params.get('day', 1)
        output_var = params.get('output_var', 'created_date')

        try:
            resolved_year = int(context.resolve_value(year))
            resolved_month = int(context.resolve_value(month))
            resolved_day = int(context.resolve_value(day))

            from datetime import date
            result = date(resolved_year, resolved_month, resolved_day)
            context.set(output_var, str(result))

            return ActionResult(
                success=True,
                message=f"创建日期: {result}",
                data={
                    'year': resolved_year,
                    'month': resolved_month,
                    'day': resolved_day,
                    'result': str(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"创建日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['year', 'month', 'day']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'created_date'}


class DateFromTimestampAction(BaseAction):
    """Create date from timestamp."""
    action_type = "date2_from_timestamp"
    display_name = "时间戳转日期"
    description = "从时间戳创建日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date from timestamp.

        Args:
            context: Execution context.
            params: Dict with timestamp, output_var.

        Returns:
            ActionResult with date from timestamp.
        """
        timestamp = params.get('timestamp', 0)
        output_var = params.get('output_var', 'date_from_timestamp')

        valid, msg = self.validate_type(timestamp, (int, float), 'timestamp')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_timestamp = float(context.resolve_value(timestamp))

            from datetime import datetime
            result = datetime.fromtimestamp(resolved_timestamp)
            context.set(output_var, str(result))

            return ActionResult(
                success=True,
                message=f"时间戳转日期: {result}",
                data={
                    'timestamp': resolved_timestamp,
                    'result': str(result),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间戳转日期失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['timestamp']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'date_from_timestamp'}


class DateToTimestampAction(BaseAction):
    """Convert date to timestamp."""
    action_type = "date2_to_timestamp"
    display_name = "日期转时间戳"
    description = "将日期转换为时间戳"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute date to timestamp.

        Args:
            context: Execution context.
            params: Dict with date_str, format, output_var.

        Returns:
            ActionResult with timestamp.
        """
        date_str = params.get('date_str', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'timestamp')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            from datetime import datetime
            dt = datetime.strptime(resolved_date, resolved_format)
            timestamp = dt.timestamp()

            context.set(output_var, timestamp)

            return ActionResult(
                success=True,
                message=f"日期转时间戳: {timestamp}",
                data={
                    'date': resolved_date,
                    'format': resolved_format,
                    'timestamp': timestamp,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期转时间戳失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'timestamp'}


class DateAddDaysAction(BaseAction):
    """Add days to date."""
    action_type = "date2_add_days"
    display_name = "日期加天数"
    description = "向日期添加天数"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add days to date.

        Args:
            context: Execution context.
            params: Dict with date_str, days, format, output_var.

        Returns:
            ActionResult with new date.
        """
        date_str = params.get('date_str', '')
        days = params.get('days', 0)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'new_date')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_days = int(context.resolve_value(days))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d'

            from datetime import datetime, timedelta
            dt = datetime.strptime(resolved_date, resolved_format)
            new_date = dt + timedelta(days=resolved_days)

            context.set(output_var, str(new_date))

            return ActionResult(
                success=True,
                message=f"日期加天数: {new_date}",
                data={
                    'original': resolved_date,
                    'days_added': resolved_days,
                    'result': str(new_date),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期加天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d', 'output_var': 'new_date'}
