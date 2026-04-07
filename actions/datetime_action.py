"""DateTime action module for RabAI AutoClick.

Provides datetime operations:
- DateTimeNowAction: Get current datetime
- DateTimeParseAction: Parse datetime string
- DateTimeFormatAction: Format datetime
- DateTimeDiffAction: Calculate datetime difference
- DateTimeAddAction: Add time to datetime
- DateTimeConvertAction: Convert timezone
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateTimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime_now"
    display_name = "当前时间"
    description = "获取当前时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute now.

        Args:
            context: Execution context.
            params: Dict with timezone, format, output_var.

        Returns:
            ActionResult with current datetime.
        """
        timezone_str = params.get('timezone', 'UTC')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'datetime_now')

        try:
            resolved_tz = context.resolve_value(timezone_str) if timezone_str else 'UTC'
            resolved_format = context.resolve_value(format_str) if format_str else '%Y-%m-%d %H:%M:%S'

            if resolved_tz == 'UTC':
                now = datetime.now(timezone.utc)
            elif resolved_tz == 'Local':
                now = datetime.now()
            else:
                now = datetime.now()

            result_str = now.strftime(resolved_format)
            result = {
                'datetime': now.isoformat(),
                'formatted': result_str,
                'timestamp': now.timestamp(),
            }

            context.set(output_var, result if not format_str else result_str)

            return ActionResult(
                success=True,
                message=f"当前时间: {result_str}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"获取当前时间失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'timezone': 'UTC', 'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'datetime_now'}


class DateTimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime_parse"
    display_name = "解析时间"
    description = "解析时间字符串"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with date_string, format, output_var.

        Returns:
            ActionResult with parsed datetime.
        """
        date_string = params.get('date_string', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'datetime_parsed')

        valid, msg = self.validate_type(date_string, str, 'date_string')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_str = context.resolve_value(date_string)
            resolved_format = context.resolve_value(format_str) if format_str else None

            if resolved_format:
                dt = datetime.strptime(resolved_str, resolved_format)
            else:
                formats = [
                    '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S',
                    '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y %H:%M:%S', '%d-%m-%Y'
                ]
                dt = None
                for fmt in formats:
                    try:
                        dt = datetime.strptime(resolved_str, fmt)
                        break
                    except ValueError:
                        continue

                if dt is None:
                    return ActionResult(success=False, message=f"无法解析日期: {resolved_str}")

            result = {
                'datetime': dt.isoformat(),
                'timestamp': dt.timestamp(),
                'year': dt.year,
                'month': dt.month,
                'day': dt.day,
                'hour': dt.hour,
                'minute': dt.minute,
                'second': dt.second,
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"已解析: {dt.strftime('%Y-%m-%d %H:%M:%S')}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"解析时间失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['date_string']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': None, 'output_var': 'datetime_parsed'}


class DateTimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "datetime_format"
    display_name = "格式化时间"
    description = "格式化时间"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute format.

        Args:
            context: Execution context.
            params: Dict with datetime_val, format_str, output_var.

        Returns:
            ActionResult with formatted string.
        """
        datetime_val = params.get('datetime_val', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'datetime_formatted')

        try:
            resolved_val = context.resolve_value(datetime_val)
            resolved_format = context.resolve_value(format_str)

            if isinstance(resolved_val, str):
                dt = datetime.fromisoformat(resolved_val.replace('Z', '+00:00'))
            elif isinstance(resolved_val, (int, float)):
                dt = datetime.fromtimestamp(resolved_val)
            else:
                dt = resolved_val

            result = dt.strftime(resolved_format)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"格式化: {result}",
                data={'formatted': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"格式化时间失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['datetime_val', 'format']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_formatted'}


class DateTimeDiffAction(BaseAction):
    """Calculate datetime difference."""
    action_type = "datetime_diff"
    display_name = "时间差计算"
    description = "计算时间差"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute diff.

        Args:
            context: Execution context.
            params: Dict with datetime1, datetime2, output_var.

        Returns:
            ActionResult with difference.
        """
        datetime1 = params.get('datetime1', '')
        datetime2 = params.get('datetime2', '')
        output_var = params.get('output_var', 'datetime_diff')

        try:
            resolved_d1 = context.resolve_value(datetime1)
            resolved_d2 = context.resolve_value(datetime2)

            if isinstance(resolved_d1, str):
                dt1 = datetime.fromisoformat(resolved_d1.replace('Z', '+00:00'))
            else:
                dt1 = datetime.fromtimestamp(resolved_d1)

            if isinstance(resolved_d2, str):
                dt2 = datetime.fromisoformat(resolved_d2.replace('Z', '+00:00'))
            else:
                dt2 = datetime.fromtimestamp(resolved_d2)

            diff = dt1 - dt2
            total_seconds = diff.total_seconds()

            result = {
                'seconds': total_seconds,
                'minutes': total_seconds / 60,
                'hours': total_seconds / 3600,
                'days': diff.days,
                'microseconds': diff.microseconds,
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时间差: {diff.days} 天",
                data={'diff': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"时间差计算失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['datetime1', 'datetime2']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_diff'}


class DateTimeAddAction(BaseAction):
    """Add time to datetime."""
    action_type = "datetime_add"
    display_name = "时间加减"
    description = "时间加减"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute add.

        Args:
            context: Execution context.
            params: Dict with datetime_val, days, hours, minutes, seconds, output_var.

        Returns:
            ActionResult with new datetime.
        """
        datetime_val = params.get('datetime_val', '')
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'datetime_result')

        try:
            resolved_val = context.resolve_value(datetime_val)
            resolved_days = context.resolve_value(days)
            resolved_hours = context.resolve_value(hours)
            resolved_minutes = context.resolve_value(minutes)
            resolved_seconds = context.resolve_value(seconds)

            if isinstance(resolved_val, str):
                dt = datetime.fromisoformat(resolved_val.replace('Z', '+00:00'))
            elif isinstance(resolved_val, (int, float)):
                dt = datetime.fromtimestamp(resolved_val)
            else:
                dt = resolved_val

            delta = timedelta(
                days=int(resolved_days),
                hours=int(resolved_hours),
                minutes=int(resolved_minutes),
                seconds=int(resolved_seconds)
            )

            result_dt = dt + delta

            context.set(output_var, result_dt.isoformat())

            return ActionResult(
                success=True,
                message=f"计算结果: {result_dt.strftime('%Y-%m-%d %H:%M:%S')}",
                data={'result': result_dt.isoformat(), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"时间加减失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['datetime_val']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'datetime_result'}


class DateTimeConvertAction(BaseAction):
    """Convert timezone."""
    action_type = "datetime_convert"
    display_name = "时区转换"
    description = "时区转换"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute convert.

        Args:
            context: Execution context.
            params: Dict with datetime_val, from_tz, to_tz, output_var.

        Returns:
            ActionResult with converted datetime.
        """
        datetime_val = params.get('datetime_val', '')
        from_tz = params.get('from_tz', 'UTC')
        to_tz = params.get('to_tz', 'Asia/Shanghai')
        output_var = params.get('output_var', 'datetime_converted')

        try:
            resolved_val = context.resolve_value(datetime_val)

            if isinstance(resolved_val, str):
                dt = datetime.fromisoformat(resolved_val.replace('Z', '+00:00'))
            elif isinstance(resolved_val, (int, float)):
                dt = datetime.fromtimestamp(resolved_val)
            else:
                dt = resolved_val

            result = dt.isoformat()
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"时区转换: {result}",
                data={'result': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"时区转换失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['datetime_val', 'from_tz', 'to_tz']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'datetime_converted'}
