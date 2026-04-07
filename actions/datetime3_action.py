"""Datetime3 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeAddAction: Add time duration
- DatetimeSubtractAction: Subtract time duration
- DatetimeDiffAction: Calculate time difference
- DatetimeNowUtcAction: Get current UTC time
- DatetimeParseAction: Parse datetime string
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeAddAction(BaseAction):
    """Add time duration."""
    action_type = "datetime_add"
    display_name = "日期加法"
    description = "添加时间间隔到日期"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime add.

        Args:
            context: Execution context.
            params: Dict with datetime_str, days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'result_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_days = int(context.resolve_value(days))
            resolved_hours = int(context.resolve_value(hours))
            resolved_minutes = int(context.resolve_value(minutes))
            resolved_seconds = int(context.resolve_value(seconds))

            dt = datetime.fromisoformat(resolved_dt)
            delta = timedelta(
                days=resolved_days,
                hours=resolved_hours,
                minutes=resolved_minutes,
                seconds=resolved_seconds
            )

            result = (dt + delta).isoformat()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期加法: {result}",
                data={
                    'original': resolved_dt,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期加法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'result_datetime'}


class DatetimeSubtractAction(BaseAction):
    """Subtract time duration."""
    action_type = "datetime_subtract"
    display_name = "日期减法"
    description = "从日期减去时间间隔"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime subtract.

        Args:
            context: Execution context.
            params: Dict with datetime_str, days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime_str', '')
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'result_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_days = int(context.resolve_value(days))
            resolved_hours = int(context.resolve_value(hours))
            resolved_minutes = int(context.resolve_value(minutes))
            resolved_seconds = int(context.resolve_value(seconds))

            dt = datetime.fromisoformat(resolved_dt)
            delta = timedelta(
                days=resolved_days,
                hours=resolved_hours,
                minutes=resolved_minutes,
                seconds=resolved_seconds
            )

            result = (dt - delta).isoformat()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期减法: {result}",
                data={
                    'original': resolved_dt,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期减法失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'result_datetime'}


class DatetimeDiffAction(BaseAction):
    """Calculate time difference."""
    action_type = "datetime_diff"
    display_name = "日期差"
    description = "计算两个日期的差值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime diff.

        Args:
            context: Execution context.
            params: Dict with datetime1, datetime2, output_var.

        Returns:
            ActionResult with difference.
        """
        datetime1 = params.get('datetime1', '')
        datetime2 = params.get('datetime2', '')
        output_var = params.get('output_var', 'datetime_diff')

        valid, msg = self.validate_type(datetime1, str, 'datetime1')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(datetime2, str, 'datetime2')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt1 = context.resolve_value(datetime1)
            resolved_dt2 = context.resolve_value(datetime2)

            dt1 = datetime.fromisoformat(resolved_dt1)
            dt2 = datetime.fromisoformat(resolved_dt2)

            diff = dt1 - dt2

            result = {
                'total_seconds': diff.total_seconds(),
                'days': diff.days,
                'seconds': diff.seconds,
                'microseconds': diff.microseconds
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期差: {diff.days} 天",
                data={
                    'datetime1': resolved_dt1,
                    'datetime2': resolved_dt2,
                    'diff': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期差计算失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime1', 'datetime2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_diff'}


class DatetimeNowUtcAction(BaseAction):
    """Get current UTC time."""
    action_type = "datetime_now_utc"
    display_name = "获取UTC时间"
    description = "获取当前UTC时间"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime now UTC.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with UTC datetime.
        """
        output_var = params.get('output_var', 'utc_datetime')

        try:
            now = datetime.utcnow()
            result = now.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"UTC时间: {result}",
                data={
                    'datetime': result,
                    'timestamp': now.timestamp(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取UTC时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'utc_datetime'}


class DatetimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime_parse"
    display_name = "解析日期"
    description = "解析日期字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime parse.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format_str, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        datetime_str = params.get('datetime_str', '')
        format_str = params.get('format_str', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_datetime')

        valid, msg = self.validate_type(datetime_str, str, 'datetime_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(format_str, str, 'format_str')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_dt = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str)

            dt = datetime.strptime(resolved_dt, resolved_format)
            result = dt.isoformat()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"日期解析: {result}",
                data={
                    'original': resolved_dt,
                    'format': resolved_format,
                    'result': result,
                    'output_var': output_var
                }
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=f"日期格式错误: {str(e)}"
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"日期解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str', 'format_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'parsed_datetime'}