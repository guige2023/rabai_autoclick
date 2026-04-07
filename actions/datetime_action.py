"""Datetime action module for RabAI AutoClick.

Provides datetime parsing and formatting utilities:
- ParseDatetimeAction: Parse string to datetime
- FormatDatetimeAction: Format datetime to string
- NowAction: Get current datetime
- TodayAction: Get today's date
- DateDiffAction: Calculate date difference
- DateArithmeticAction: Add/subtract from date
- TimestampAction: Convert to/from timestamp
- ParseDateAction: Parse date string
- FormatDateAction: Format date to string
- TimezonesAction: Timezone conversions
"""

from typing import Any, Dict, List, Optional, Union
import sys
import datetime

_parent_dir = __import__('os').path.dirname(__import__('os').path.dirname(__import__('os').path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatetimeParseAction(BaseAction):
    """Parse string to datetime."""
    action_type = "datetime_parse"
    display_name = "解析日期时间"
    description = "将字符串解析为日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute parse datetime."""
        date_string = params.get('date_string', '')
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'parsed_datetime')

        try:
            resolved_string = context.resolve_value(date_string) if isinstance(date_string, str) else date_string
            resolved_format = context.resolve_value(format_str) if isinstance(format_str, str) else format_str
            
            result = datetime.datetime.strptime(resolved_string, resolved_format)
            context.set_variable(output_var, {"datetime": result.isoformat(), "timestamp": result.timestamp()})
            return ActionResult(success=True, message=f"parsed: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"parse failed: {e}")


class DatetimeFormatAction(BaseAction):
    """Format datetime to string."""
    action_type = "datetime_format"
    display_name = "格式化日期时间"
    description = "将日期时间格式化为字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute format datetime."""
        dt_value = params.get('datetime', None)
        format_str = params.get('format', '%Y-%m-%d %H:%M:%S')
        output_var = params.get('output_var', 'formatted_datetime')

        try:
            resolved_dt = context.resolve_value(dt_value) if dt_value is not None else None
            resolved_format = context.resolve_value(format_str) if isinstance(format_str, str) else format_str
            
            if resolved_dt is None:
                resolved_dt = datetime.datetime.now()
            elif isinstance(resolved_dt, (int, float)):
                resolved_dt = datetime.datetime.fromtimestamp(resolved_dt)
            elif isinstance(resolved_dt, str):
                resolved_dt = datetime.datetime.fromisoformat(resolved_dt)
            
            result = resolved_dt.strftime(resolved_format)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"formatted: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"format failed: {e}")


class DatetimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime_now"
    display_name = "当前时间"
    description = "获取当前日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute now."""
        utc = params.get('utc', False)
        output_var = params.get('output_var', 'now_datetime')

        try:
            resolved_utc = context.resolve_value(utc) if isinstance(utc, str) else utc
            
            if resolved_utc:
                now = datetime.datetime.utcnow()
            else:
                now = datetime.datetime.now()
            
            context.set_variable(output_var, {"isoformat": now.isoformat(), "timestamp": now.timestamp()})
            return ActionResult(success=True, message=f"now: {now}")
        except Exception as e:
            return ActionResult(success=False, message=f"now failed: {e}")


class DatetimeTodayAction(BaseAction):
    """Get today's date."""
    action_type = "datetime_today"
    display_name = "今天日期"
    description = "获取今天的日期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute today."""
        output_var = params.get('output_var', 'today_date')

        try:
            today = datetime.date.today()
            context.set_variable(output_var, {"isoformat": today.isoformat(), "year": today.year, "month": today.month, "day": today.day})
            return ActionResult(success=True, message=f"today: {today}")
        except Exception as e:
            return ActionResult(success=False, message=f"today failed: {e}")


class DatetimeDiffAction(BaseAction):
    """Calculate date difference."""
    action_type = "datetime_diff"
    display_name = "日期差"
    description = "计算两个日期的差值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute date diff."""
        date1 = params.get('date1', None)
        date2 = params.get('date2', None)
        unit = params.get('unit', 'days')
        output_var = params.get('output_var', 'diff_result')

        try:
            resolved_date1 = context.resolve_value(date1) if isinstance(date1, str) else date1
            resolved_date2 = context.resolve_value(date2) if isinstance(date2, str) else date2
            
            if isinstance(resolved_date1, str):
                resolved_date1 = datetime.date.fromisoformat(resolved_date1)
            elif isinstance(resolved_date1, datetime.datetime):
                resolved_date1 = resolved_date1.date()
            
            if isinstance(resolved_date2, str):
                resolved_date2 = datetime.date.fromisoformat(resolved_date2)
            elif isinstance(resolved_date2, datetime.datetime):
                resolved_date2 = resolved_date2.date()
            
            delta = resolved_date2 - resolved_date1
            
            if unit == 'seconds':
                result = delta.total_seconds()
            elif unit == 'minutes':
                result = delta.total_seconds() / 60
            elif unit == 'hours':
                result = delta.total_seconds() / 3600
            elif unit == 'days':
                result = delta.days
            elif unit == 'weeks':
                result = delta.days / 7
            else:
                result = delta.days
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"diff: {result} {unit}")
        except Exception as e:
            return ActionResult(success=False, message=f"diff failed: {e}")


class DatetimeArithmeticAction(BaseAction):
    """Add/subtract from date."""
    action_type = "datetime_arithmetic"
    display_name = "日期运算"
    description = "对日期进行加减运算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute date arithmetic."""
        date_value = params.get('date', None)
        years = params.get('years', 0)
        months = params.get('months', 0)
        days = params.get('days', 0)
        hours = params.get('hours', 0)
        minutes = params.get('minutes', 0)
        seconds = params.get('seconds', 0)
        output_var = params.get('output_var', 'arithmetic_result')

        try:
            resolved_date = context.resolve_value(date_value) if isinstance(date_value, str) else date_value
            
            if resolved_date is None:
                dt = datetime.datetime.now()
            elif isinstance(resolved_date, str):
                try:
                    dt = datetime.datetime.fromisoformat(resolved_date)
                except ValueError:
                    dt = datetime.datetime.strptime(resolved_date, '%Y-%m-%d')
            elif isinstance(resolved_date, datetime.date):
                dt = datetime.datetime.combine(resolved_date, datetime.time())
            else:
                dt = resolved_date
            
            from dateutil.relativedelta import relativedelta
            result = dt + relativedelta(years=years, months=months, days=days, hours=hours, minutes=minutes, seconds=seconds)
            
            context.set_variable(output_var, {"isoformat": result.isoformat(), "timestamp": result.timestamp()})
            return ActionResult(success=True, message=f"result: {result}")
        except ImportError:
            return ActionResult(success=False, message="dateutil not available, use basic datetime only")
        except Exception as e:
            return ActionResult(success=False, message=f"arithmetic failed: {e}")


class DatetimeTimestampAction(BaseAction):
    """Convert to/from timestamp."""
    action_type = "datetime_timestamp"
    display_name = "时间戳转换"
    description = "在日期时间和时间戳之间转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute timestamp conversion."""
        value = params.get('value', None)
        direction = params.get('direction', 'to_timestamp')
        output_var = params.get('output_var', 'timestamp_result')

        try:
            resolved_value = context.resolve_value(value) if isinstance(value, str) else value
            
            if direction == 'to_timestamp':
                if isinstance(resolved_value, str):
                    try:
                        dt = datetime.datetime.fromisoformat(resolved_value)
                    except ValueError:
                        dt = datetime.datetime.strptime(resolved_value, '%Y-%m-%d %H:%M:%S')
                elif isinstance(resolved_value, datetime.datetime):
                    dt = resolved_value
                else:
                    dt = datetime.datetime.fromtimestamp(resolved_value)
                result = dt.timestamp()
            else:
                if isinstance(resolved_value, (int, float)):
                    dt = datetime.datetime.fromtimestamp(resolved_value)
                else:
                    dt = datetime.datetime.fromtimestamp(float(resolved_value))
                result = dt.isoformat()
            
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"{direction}: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"timestamp failed: {e}")


class DatetimeParseDateAction(BaseAction):
    """Parse date string."""
    action_type = "datetime_parse_date"
    display_name = "解析日期"
    description = "将字符串解析为日期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute parse date."""
        date_string = params.get('date_string', '')
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'parsed_date')

        try:
            resolved_string = context.resolve_value(date_string) if isinstance(date_string, str) else date_string
            resolved_format = context.resolve_value(format_str) if isinstance(format_str, str) else format_str
            
            result = datetime.datetime.strptime(resolved_string, resolved_format).date()
            context.set_variable(output_var, {"isoformat": result.isoformat(), "year": result.year, "month": result.month, "day": result.day})
            return ActionResult(success=True, message=f"parsed: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"parse date failed: {e}")


class DatetimeFormatDateAction(BaseAction):
    """Format date to string."""
    action_type = "datetime_format_date"
    display_name = "格式化日期"
    description = "将日期格式化为字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute format date."""
        date_value = params.get('date', None)
        format_str = params.get('format', '%Y-%m-%d')
        output_var = params.get('output_var', 'formatted_date')

        try:
            resolved_date = context.resolve_value(date_value) if isinstance(date_value, str) else date_value
            
            if resolved_date is None:
                resolved_date = datetime.date.today()
            elif isinstance(resolved_date, str):
                resolved_date = datetime.date.fromisoformat(resolved_date)
            elif isinstance(resolved_date, datetime.datetime):
                resolved_date = resolved_date.date()
            
            result = resolved_date.strftime(format_str)
            context.set_variable(output_var, result)
            return ActionResult(success=True, message=f"formatted: {result}")
        except Exception as e:
            return ActionResult(success=False, message=f"format date failed: {e}")


class DatetimeRangeAction(BaseAction):
    """Generate date range."""
    action_type = "datetime_range"
    display_name = "日期范围"
    description = "生成日期范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute date range."""
        start = params.get('start', None)
        end = params.get('end', None)
        step_days = params.get('step_days', 1)
        output_var = params.get('output_var', 'date_range_result')

        try:
            resolved_start = context.resolve_value(start) if isinstance(start, str) else start
            resolved_end = context.resolve_value(end) if isinstance(end, str) else end
            resolved_step = context.resolve_value(step_days) if isinstance(step_days, str) else step_days
            
            if isinstance(resolved_start, str):
                resolved_start = datetime.date.fromisoformat(resolved_start)
            elif isinstance(resolved_start, datetime.datetime):
                resolved_start = resolved_start.date()
            
            if isinstance(resolved_end, str):
                resolved_end = datetime.date.fromisoformat(resolved_end)
            elif isinstance(resolved_end, datetime.datetime):
                resolved_end = resolved_end.date()
            
            dates = []
            current = resolved_start
            while current <= resolved_end:
                dates.append(current.isoformat())
                current += datetime.timedelta(days=resolved_step)
            
            context.set_variable(output_var, dates)
            return ActionResult(success=True, message=f"generated {len(dates)} dates")
        except Exception as e:
            return ActionResult(success=False, message=f"date range failed: {e}")
