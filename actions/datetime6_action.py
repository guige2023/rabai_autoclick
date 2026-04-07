"""Datetime6 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeAddDaysAction: Add days to datetime
- DatetimeSubtractDaysAction: Subtract days from datetime
- DatetimeAddHoursAction: Add hours to datetime
- DatetimeSubtractHoursAction: Subtract hours from datetime
- DatetimeCombineDateTimeAction: Combine date and time
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeAddDaysAction(BaseAction):
    """Add days to datetime."""
    action_type = "datetime6_add_days"
    display_name = "日期加天数"
    description = "向日期添加天数"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add days.

        Args:
            context: Execution context.
            params: Dict with datetime_str, days, format, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        days = params.get('days', 0)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'new_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_days = int(context.resolve_value(days))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_dt, resolved_format)
            new_dt = dt + timedelta(days=resolved_days)
            result = new_dt.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期加天数: {result}",
                data={
                    'original': resolved_dt,
                    'days_added': resolved_days,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期加天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'new_datetime'}


class DatetimeSubtractDaysAction(BaseAction):
    """Subtract days from datetime."""
    action_type = "datetime6_subtract_days"
    display_name = "日期减天数"
    description = "从日期减去天数"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract days.

        Args:
            context: Execution context.
            params: Dict with datetime_str, days, format, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        days = params.get('days', 0)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'new_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_days = int(context.resolve_value(days))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_dt, resolved_format)
            new_dt = dt - timedelta(days=resolved_days)
            result = new_dt.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期减天数: {result}",
                data={
                    'original': resolved_dt,
                    'days_subtracted': resolved_days,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期减天数失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'days']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'new_datetime'}


class DatetimeAddHoursAction(BaseAction):
    """Add hours to datetime."""
    action_type = "datetime6_add_hours"
    display_name = "时间加小时"
    description = "向时间添加小时"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add hours.

        Args:
            context: Execution context.
            params: Dict with datetime_str, hours, format, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        hours = params.get('hours', 0)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'new_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_hours = int(context.resolve_value(hours))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_dt, resolved_format)
            new_dt = dt + timedelta(hours=resolved_hours)
            result = new_dt.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间加小时: {result}",
                data={
                    'original': resolved_dt,
                    'hours_added': resolved_hours,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间加小时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'hours']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'new_datetime'}


class DatetimeSubtractHoursAction(BaseAction):
    """Subtract hours from datetime."""
    action_type = "datetime6_subtract_hours"
    display_name = "时间减小时"
    description = "从时间减去小时"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute subtract hours.

        Args:
            context: Execution context.
            params: Dict with datetime_str, hours, format, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        hours = params.get('hours', 0)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'new_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_hours = int(context.resolve_value(hours))
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            dt = datetime.strptime(resolved_dt, resolved_format)
            new_dt = dt - timedelta(hours=resolved_hours)
            result = new_dt.strftime(resolved_format)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间减小时: {result}",
                data={
                    'original': resolved_dt,
                    'hours_subtracted': resolved_hours,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"时间减小时失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'hours']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'new_datetime'}


class DatetimeCombineDateTimeAction(BaseAction):
    """Combine date and time."""
    action_type = "datetime6_combine"
    display_name = "日期时间组合"
    description = "组合日期和时间"
    version = "6.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute combine.

        Args:
            context: Execution context.
            params: Dict with date_str, time_str, date_format, time_format, output_var.

        Returns:
            ActionResult with combined datetime.
        """
        date_str = params.get('date_str', '')
        time_str = params.get('time_str', '')
        date_format = params.get('date_format', '%Y-%m-%d')
        time_format = params.get('time_format', '%H:%M:%S')
        output_var = params.get('output_var', 'combined_datetime')

        valid, msg = self.validate_type(date_str, str, 'date_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(time_str, str, 'time_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_date = context.resolve_value(date_str)
            resolved_time = context.resolve_value(time_str)
            resolved_date_format = context.resolve_value(date_format) if date_format else '%Y-%m-%d'
            resolved_time_format = context.resolve_value(time_format) if time_format else '%H:%M:%S'

            date_obj = datetime.strptime(resolved_date, resolved_date_format).date()
            time_obj = datetime.strptime(resolved_time, resolved_time_format).time()

            combined = datetime.combine(date_obj, time_obj)
            result = combined.strftime('%Y-%m-%d %H:%M:%S')

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期时间组合: {result}",
                data={
                    'date': resolved_date,
                    'time': resolved_time,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期时间组合失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['date_str', 'time_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'date_format': '%Y-%m-%d', 'time_format': '%H:%M:%S', 'output_var': 'combined_datetime'}