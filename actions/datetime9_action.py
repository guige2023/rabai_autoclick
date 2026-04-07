"""Datetime9 action module for RabAI AutoClick.

Provides additional datetime operations:
- DatetimeNowAction: Get current datetime
- DatetimeAddAction: Add time to datetime
- DatetimeDiffAction: Get time difference
- DatetimeParseAction: Parse datetime string
- DatetimeFormatAction: Format datetime
- DatetimeConvertAction: Convert timezone
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime9_now"
    display_name = "当前时间"
    description = "获取当前时间"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with current datetime.
        """
        output_var = params.get('output_var', 'current_datetime')

        try:
            from datetime import datetime

            result = datetime.now()
            context.set(output_var, result.isoformat())

            return ActionResult(
                success=True,
                message=f"当前时间: {result.isoformat()}",
                data={
                    'datetime': result.isoformat(),
                    'timestamp': result.timestamp(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取当前时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'current_datetime'}


class DatetimeAddAction(BaseAction):
    """Add time to datetime."""
    action_type = "datetime9_add"
    display_name = "增加时间"
    description = "增加时间到日期时间"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime add.

        Args:
            context: Execution context.
            params: Dict with datetime, days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_str = params.get('datetime', 'now')
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'new_datetime')

        try:
            from datetime import datetime, timedelta

            resolved_datetime = context.resolve_value(datetime_str)

            if resolved_datetime == 'now':
                dt = datetime.now()
            elif isinstance(resolved_datetime, str):
                dt = datetime.fromisoformat(resolved_datetime)
            else:
                dt = resolved_datetime

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

            result = dt + delta
            context.set(output_var, result.isoformat())

            return ActionResult(
                success=True,
                message=f"增加时间: {result.isoformat()}",
                data={
                    'original': dt.isoformat(),
                    'result': result.isoformat(),
                    'delta': str(delta),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"增加时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'new_datetime'}


class DatetimeDiffAction(BaseAction):
    """Get time difference."""
    action_type = "datetime9_diff"
    display_name = "时间差"
    description = "计算时间差"
    version = "9.0"

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
            ActionResult with time difference.
        """
        datetime1 = params.get('datetime1', 'now')
        datetime2 = params.get('datetime2', 'now')
        output_var = params.get('output_var', 'time_diff')

        try:
            from datetime import datetime

            resolved_dt1 = context.resolve_value(datetime1)
            resolved_dt2 = context.resolve_value(datetime2)

            if resolved_dt1 == 'now':
                dt1 = datetime.now()
            elif isinstance(resolved_dt1, str):
                dt1 = datetime.fromisoformat(resolved_dt1)
            else:
                dt1 = resolved_dt1

            if resolved_dt2 == 'now':
                dt2 = datetime.now()
            elif isinstance(resolved_dt2, str):
                dt2 = datetime.fromisoformat(resolved_dt2)
            else:
                dt2 = resolved_dt2

            diff = abs((dt2 - dt1).total_seconds())

            result = {
                'seconds': diff,
                'minutes': diff / 60,
                'hours': diff / 3600,
                'days': diff / 86400
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间差: {diff}秒",
                data={
                    'datetime1': dt1.isoformat(),
                    'datetime2': dt2.isoformat(),
                    'diff_seconds': diff,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"计算时间差失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime1', 'datetime2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'time_diff'}


class DatetimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime9_parse"
    display_name = "解析时间"
    description = "解析时间字符串"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime parse.

        Args:
            context: Execution context.
            params: Dict with datetime_str, format, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        datetime_str = params.get('datetime_str', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_datetime')

        try:
            from datetime import datetime

            resolved_str = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            result = datetime.strptime(resolved_str, resolved_format)
            context.set(output_var, result.isoformat())

            return ActionResult(
                success=True,
                message=f"解析时间: {result.isoformat()}",
                data={
                    'datetime_str': resolved_str,
                    'format': resolved_format,
                    'result': result.isoformat(),
                    'timestamp': result.timestamp(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"解析时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'parsed_datetime'}


class DatetimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "datetime9_format"
    display_name = "格式化时间"
    description = "格式化时间"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute datetime format.

        Args:
            context: Execution context.
            params: Dict with datetime, format, output_var.

        Returns:
            ActionResult with formatted datetime.
        """
        datetime_str = params.get('datetime', 'now')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_datetime')

        try:
            from datetime import datetime

            resolved_datetime = context.resolve_value(datetime_str)
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            if resolved_datetime == 'now':
                dt = datetime.now()
            elif isinstance(resolved_datetime, str):
                dt = datetime.fromisoformat(resolved_datetime)
            else:
                dt = resolved_datetime

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化时间: {result}",
                data={
                    'datetime': dt.isoformat(),
                    'format': resolved_format,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"格式化时间失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_datetime'}


class DatetimeConvertAction(BaseAction):
    """Convert timezone."""
    action_type = "datetime9_convert"
    display_name = "转换时区"
    description = "转换时区"
    version = "9.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute timezone convert.

        Args:
            context: Execution context.
            params: Dict with datetime, from_tz, to_tz, output_var.

        Returns:
            ActionResult with converted datetime.
        """
        datetime_str = params.get('datetime', 'now')
        from_tz = params.get('from_tz', 'UTC')
        to_tz = params.get('to_tz', 'UTC')
        output_var = params.get('output_var', 'converted_datetime')

        try:
            from datetime import datetime
            from datetime import timezone
            from datetime import timedelta

            resolved_datetime = context.resolve_value(datetime_str)

            if resolved_datetime == 'now':
                dt = datetime.now()
            elif isinstance(resolved_datetime, str):
                dt = datetime.fromisoformat(resolved_datetime)
            else:
                dt = resolved_datetime

            # Simple timezone offset conversion
            tz_offsets = {
                'UTC': 0,
                'EST': -5,
                'CST': 8,
                'PST': -8,
                'JST': 9,
                'GMT': 0
            }

            from_offset = tz_offsets.get(from_tz, 0)
            to_offset = tz_offsets.get(to_tz, 0)

            # Convert to UTC first
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            # Then convert to target timezone
            delta = to_offset - from_offset
            result = dt + timedelta(hours=delta)

            context.set(output_var, result.isoformat())

            return ActionResult(
                success=True,
                message=f"转换时区: {result.isoformat()}",
                data={
                    'original': dt.isoformat(),
                    'from_tz': from_tz,
                    'to_tz': to_tz,
                    'result': result.isoformat(),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"转换时区失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['datetime', 'from_tz', 'to_tz']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'converted_datetime'}