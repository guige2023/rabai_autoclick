"""DateTime manipulation action module for RabAI AutoClick.

Provides datetime operations:
- DatetimeNowAction: Get current datetime
- DatetimeParseAction: Parse datetime string
- DatetimeFormatAction: Format datetime
- DatetimeAddAction: Add time to datetime
- DatetimeDiffAction: Calculate time difference
- DatetimeTimestampAction: Convert to/from timestamp
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime_now"
    display_name = "当前时间"
    description = "获取当前日期时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime now."""
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        timezone_name = params.get('timezone', None)
        output_var = params.get('output_var', 'now_datetime')

        try:
            resolved_format = context.resolve_value(format_str) if context else format_str
            resolved_tz = context.resolve_value(timezone_name) if context else timezone_name

            if resolved_tz:
                import pytz
                tz = pytz.timezone(resolved_tz)
                now = datetime.now(tz)
            else:
                now = datetime.now()

            formatted = now.strftime(resolved_format)
            result = {'datetime': formatted, 'timestamp': now.timestamp(), 'iso': now.isoformat()}

            if context:
                context.set(output_var, now)
            return ActionResult(success=True, message=formatted, data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime now error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': '%Y-%m-%d %H:%M:%S', 'timezone': None, 'output_var': 'now_datetime'}


class DatetimeParseAction(BaseAction):
    """Parse datetime string."""
    action_type = "datetime_parse"
    display_name = "时间解析"
    description = "解析时间字符串"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime parse."""
        date_str = params.get('date_str', '')
        format_str = params.get('format', None)  # None = auto-detect
        output_var = params.get('output_var', 'parsed_datetime')

        if not date_str:
            return ActionResult(success=False, message="date_str is required")

        try:
            resolved_str = context.resolve_value(date_str) if context else date_str
            resolved_format = context.resolve_value(format_str) if context else format_str

            if resolved_format:
                dt = datetime.strptime(resolved_str, resolved_format)
            else:
                # Try common formats
                formats = [
                    '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y',
                    '%Y/%m/%d', '%d-%m-%Y', '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%SZ',
                ]
                dt = None
                for fmt in formats:
                    try:
                        dt = datetime.strptime(resolved_str, fmt)
                        break
                    except ValueError:
                        continue
                if dt is None:
                    return ActionResult(success=False, message=f"Could not parse date: {resolved_str}")

            result = {
                'datetime': dt.strftime('%Y-%m-%d %H:%M:%S'),
                'timestamp': dt.timestamp(),
                'iso': dt.isoformat(),
                'year': dt.year, 'month': dt.month, 'day': dt.day,
                'hour': dt.hour, 'minute': dt.minute, 'second': dt.second,
                'weekday': dt.strftime('%A'),
            }

            if context:
                context.set(output_var, dt)
            return ActionResult(success=True, message=f"Parsed: {dt}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime parse error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['date_str']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'format': None, 'output_var': 'parsed_datetime'}


class DatetimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "datetime_format"
    display_name = "时间格式化"
    description = "格式化时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime format."""
        datetime_var = params.get('datetime_var', None)  # datetime object
        datetime_str = params.get('datetime_str', None)  # or string
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_datetime')

        if not datetime_var and not datetime_str:
            return ActionResult(success=False, message="datetime_var or datetime_str is required")

        try:
            resolved_format = context.resolve_value(format_str) if context else format_str

            if datetime_var:
                dt = context.resolve_value(datetime_var) if context else datetime_var
            else:
                resolved_str = context.resolve_value(datetime_str) if context else datetime_str
                # Try to parse
                formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']
                dt = None
                for fmt in formats:
                    try:
                        dt = datetime.strptime(resolved_str, fmt)
                        break
                    except ValueError:
                        continue
                if dt is None:
                    return ActionResult(success=False, message=f"Could not parse: {resolved_str}")

            if not isinstance(dt, datetime):
                return ActionResult(success=False, message=f"{datetime_var} is not a datetime")

            formatted = dt.strftime(resolved_format)
            if context:
                context.set(output_var, formatted)
            return ActionResult(success=True, message=formatted, data={'formatted': formatted})
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime format error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'datetime_var': None, 'datetime_str': None, 'format': '%Y-%m-%d %H:%M:%S', 'output_var': 'formatted_datetime'}


class DatetimeAddAction(BaseAction):
    """Add time to datetime."""
    action_type = "datetime_add"
    display_name = "时间相加"
    description = "添加时间"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime add."""
        datetime_var = params.get('datetime_var', None)
        datetime_str = params.get('datetime_str', None)
        years = params.get('years', 0)
        months = params.get('months', 0)
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'added_datetime')

        try:
            if datetime_var:
                dt = context.resolve_value(datetime_var) if context else datetime_var
            elif datetime_str:
                resolved_str = context.resolve_value(datetime_str) if context else datetime_str
                dt = datetime.fromisoformat(resolved_str)
            else:
                return ActionResult(success=False, message="datetime_var or datetime_str is required")

            if not isinstance(dt, datetime):
                return ActionResult(success=False, message="Not a datetime object")

            resolved_years = context.resolve_value(years) if context else years
            resolved_months = context.resolve_value(months) if context else months
            resolved_days = context.resolve_value(days) if context else days
            resolved_hours = context.resolve_value(hours) if context else hours
            resolved_minutes = context.resolve_value(minutes) if context else minutes
            resolved_seconds = context.resolve_value(seconds) if context else seconds

            from dateutil.relativedelta import relativedelta
            result = dt + relativedelta(
                years=int(resolved_years), months=int(resolved_months),
                days=int(resolved_days), hours=int(resolved_hours),
                minutes=int(resolved_minutes), seconds=int(resolved_seconds)
            )

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Result: {result}", data={'datetime': result.isoformat()})
        except ImportError:
            # Fallback without relativedelta
            dt = datetime.now()
            delta = timedelta(days=resolved_days, hours=resolved_hours, minutes=resolved_minutes, seconds=resolved_seconds)
            result = dt + delta
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Result: {result}", data={'datetime': result.isoformat()})
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime add error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'datetime_var': None, 'datetime_str': None, 'years': 0, 'months': 0,
            'days': 0, 'hours': 0, 'minutes': 0, 'seconds': 0, 'output_var': 'added_datetime'
        }


class DatetimeDiffAction(BaseAction):
    """Calculate datetime difference."""
    action_type = "datetime_diff"
    display_name = "时间差"
    description = "计算时间差"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute datetime diff."""
        start_var = params.get('start', None)
        end_var = params.get('end', None)
        start_str = params.get('start_str', None)
        end_str = params.get('end_str', None)
        unit = params.get('unit', 'seconds')  # seconds, minutes, hours, days
        output_var = params.get('output_var', 'datetime_diff')

        try:
            if start_var:
                start_dt = context.resolve_value(start_var) if context else start_var
            elif start_str:
                resolved = context.resolve_value(start_str) if context else start_str
                start_dt = datetime.fromisoformat(resolved)
            else:
                start_dt = datetime.now()

            if end_var:
                end_dt = context.resolve_value(end_var) if context else end_var
            elif end_str:
                resolved = context.resolve_value(end_str) if context else end_str
                end_dt = datetime.fromisoformat(resolved)
            else:
                end_dt = datetime.now()

            if not isinstance(start_dt, datetime) or not isinstance(end_dt, datetime):
                return ActionResult(success=False, message="start and end must be datetime objects")

            diff = end_dt - start_dt
            unit_map = {
                'seconds': diff.total_seconds(),
                'minutes': diff.total_seconds() / 60,
                'hours': diff.total_seconds() / 3600,
                'days': diff.days,
            }
            value = unit_map.get(unit, diff.total_seconds())

            result = {
                'difference': value,
                'unit': unit,
                'days': diff.days,
                'seconds': diff.total_seconds(),
                'start': start_dt.isoformat(),
                'end': end_dt.isoformat(),
            }

            if context:
                context.set(output_var, value)
            return ActionResult(success=True, message=f"Diff: {value} {unit}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime diff error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'start': None, 'end': None, 'start_str': None, 'end_str': None, 'unit': 'seconds', 'output_var': 'datetime_diff'}


class DatetimeTimestampAction(BaseAction):
    """Convert to/from timestamp."""
    action_type = "datetime_timestamp"
    display_name = "Unix时间戳"
    description = "Unix时间戳转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timestamp conversion."""
        value = params.get('value', None)  # datetime or timestamp
        to_timestamp = params.get('to_timestamp', True)
        output_var = params.get('output_var', 'timestamp_result')

        if value is None:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value

            if to_timestamp:
                if isinstance(resolved, str):
                    dt = datetime.fromisoformat(resolved)
                elif isinstance(resolved, datetime):
                    dt = resolved
                else:
                    dt = datetime.fromtimestamp(float(resolved))
                result = {'timestamp': dt.timestamp(), 'datetime': dt.isoformat()}
            else:
                ts = float(resolved)
                dt = datetime.fromtimestamp(ts)
                result = {'datetime': dt.isoformat(), 'timestamp': ts}

            if context:
                context.set(output_var, result.get('timestamp' if to_timestamp else 'datetime'))
            return ActionResult(success=True, message=f"Converted: {result}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Timestamp error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'to_timestamp': True, 'output_var': 'timestamp_result'}
