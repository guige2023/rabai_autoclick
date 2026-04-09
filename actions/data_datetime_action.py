"""Data datetime action module for RabAI AutoClick.

Provides datetime parsing, formatting, and calculation for
handling time-based data and scheduling operations.
"""

import time
import calendar
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

from core.base_action import BaseAction, ActionResult


class DateTimeParseAction(BaseAction):
    """Parse various datetime string formats into standardized objects.
    
    Supports ISO 8601, Unix timestamps, common date formats,
    and natural language dates (e.g., "yesterday", "next monday").
    """
    action_type = "datetime_parse"
    display_name = "日期时间解析"
    description = "解析各种日期时间字符串格式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse datetime from string.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, format, timezone, natural_lang.
        
        Returns:
            ActionResult with parsed datetime data.
        """
        value = params.get("value")
        fmt = params.get("format")
        tz_name = params.get("timezone", "UTC")
        natural_lang = params.get("natural_lang", True)
        
        if value is None:
            return ActionResult(success=False, message="Value is required")
        
        try:
            result = None
            parsed_format = None
            
            if isinstance(value, (int, float)):
                unix_ts = float(value)
                if unix_ts > 1e12:
                    unix_ts /= 1000
                result = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                parsed_format = "unix_timestamp"
            elif isinstance(value, str):
                if natural_lang:
                    result = self._parse_natural(value)
                    if result:
                        parsed_format = "natural_language"
                
                if not result and fmt:
                    result = datetime.strptime(value, fmt)
                    parsed_format = f"strptime:{fmt}"
                
                if not result:
                    for parse_fmt in [
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%SZ",
                        "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d",
                        "%d/%m/%Y",
                        "%m/%d/%Y",
                        "%d-%m-%Y",
                        "%Y/%m/%d",
                    ]:
                        try:
                            result = datetime.strptime(value, parse_fmt)
                            parsed_format = parse_fmt
                            break
                        except ValueError:
                            continue
                
                if not result:
                    try:
                        from dateutil import parser
                        result = parser.parse(value)
                        parsed_format = "dateutil"
                    except ImportError:
                        pass
            elif isinstance(value, datetime):
                result = value
                parsed_format = "datetime_object"
            
            if not result:
                return ActionResult(success=False, message=f"Could not parse: {value}")
            
            return ActionResult(
                success=True,
                message=f"Parsed as {parsed_format}",
                data={
                    "datetime": result.isoformat(),
                    "unix": result.timestamp(),
                    "unix_ms": int(result.timestamp() * 1000),
                    "format_used": parsed_format,
                    "date": result.date().isoformat(),
                    "time": result.time().isoformat()
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime parse failed: {e}")
    
    def _parse_natural(self, value: str) -> Optional[datetime]:
        now = datetime.now(timezone.utc)
        value_lower = value.lower().strip()
        
        if value_lower == "now":
            return now
        elif value_lower == "today":
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif value_lower == "yesterday":
            return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        elif value_lower == "tomorrow":
            return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        return None


class DateTimeFormatAction(BaseAction):
    """Format datetime objects into various string representations.
    
    Supports strftime formatting, relative expressions, and
    conversion to specific timezones.
    """
    action_type = "datetime_format"
    display_name = "日期时间格式化"
    description = "将日期时间格式化为各种字符串表示"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Format datetime to string.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, format, timezone, relative.
        
        Returns:
            ActionResult with formatted datetime string.
        """
        value = params.get("value")
        fmt = params.get("format", "%Y-%m-%d %H:%M:%S")
        tz_name = params.get("timezone")
        relative = params.get("relative", False)
        
        if value is None:
            return ActionResult(success=False, message="Value is required")
        
        try:
            if isinstance(value, str):
                try:
                    from dateutil import parser
                    dt = parser.parse(value)
                except ImportError:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif isinstance(value, (int, float)):
                unix_ts = float(value)
                if unix_ts > 1e12:
                    unix_ts /= 1000
                dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
            elif isinstance(value, datetime):
                dt = value
            else:
                return ActionResult(success=False, message=f"Unsupported value type: {type(value)}")
            
            if tz_name:
                import pytz
                tz = pytz.timezone(tz_name)
                dt = dt.astimezone(tz)
            
            formatted = dt.strftime(fmt)
            
            result_data = {
                "formatted": formatted,
                "datetime": dt.isoformat()
            }
            
            if relative:
                now = datetime.now(timezone.utc)
                diff = dt - now
                result_data["relative"] = self._format_relative(diff)
            
            return ActionResult(
                success=True,
                message=f"Formatted: {formatted}",
                data=result_data
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime format failed: {e}")
    
    def _format_relative(self, diff: timedelta) -> str:
        total_seconds = diff.total_seconds()
        
        if abs(total_seconds) < 60:
            return "just now" if total_seconds >= 0 else "just ago"
        elif abs(total_seconds) < 3600:
            mins = int(abs(total_seconds) / 60)
            return f"{mins} minute{'s' if mins != 1 else ''} {'ago' if total_seconds < 0 else 'from now'}"
        elif abs(total_seconds) < 86400:
            hours = int(abs(total_seconds) / 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} {'ago' if total_seconds < 0 else 'from now'}"
        else:
            days = int(abs(total_seconds) / 86400)
            return f"{days} day{'s' if days != 1 else ''} {'ago' if total_seconds < 0 else 'from now'}"


class DateTimeCalculateAction(BaseAction):
    """Perform datetime arithmetic and calculations.
    
    Supports adding/subtracting durations, computing differences,
    and generating time ranges.
    """
    action_type = "datetime_calculate"
    display_name = "日期时间计算"
    description = "执行日期时间算术和计算"
    VALID_OPS = ["add", "subtract", "diff", "range", "start_of", "end_of"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate datetime operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, value, amount, unit,
                   end_value, timezone.
        
        Returns:
            ActionResult with calculation result.
        """
        operation = params.get("operation", "add")
        value = params.get("value")
        amount = params.get("amount", 0)
        unit = params.get("unit", "days")
        end_value = params.get("end_value")
        tz_name = params.get("timezone")
        
        valid, msg = self.validate_in(operation, self.VALID_OPS, "operation")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        try:
            if isinstance(value, str):
                try:
                    from dateutil import parser
                    dt = parser.parse(value)
                except ImportError:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            elif isinstance(value, datetime):
                dt = value
            elif isinstance(value, (int, float)):
                unix_ts = float(value)
                if unix_ts > 1e12:
                    unix_ts /= 1000
                dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
            else:
                return ActionResult(success=False, message=f"Unsupported value type")
            
            if tz_name:
                import pytz
                tz = pytz.timezone(tz_name)
                dt = dt.astimezone(tz)
            
            unit_lower = unit.lower()
            unit_plural = unit_lower + "s" if not unit_lower.endswith("s") else unit_lower
            
            delta_kwargs = {unit_plural: amount}
            
            if operation == "add":
                result = dt + timedelta(**delta_kwargs)
                return ActionResult(
                    success=True,
                    message=f"Added {amount} {unit}(s)",
                    data={"result": result.isoformat(), "unix": result.timestamp()}
                )
            elif operation == "subtract":
                result = dt - timedelta(**delta_kwargs)
                return ActionResult(
                    success=True,
                    message=f"Subtracted {amount} {unit}(s)",
                    data={"result": result.isoformat(), "unix": result.timestamp()}
                )
            elif operation == "diff":
                if end_value is None:
                    return ActionResult(success=False, message="end_value required for diff operation")
                
                if isinstance(end_value, str):
                    try:
                        from dateutil import parser
                        end_dt = parser.parse(end_value)
                    except ImportError:
                        end_dt = datetime.fromisoformat(end_value.replace("Z", "+00:00"))
                elif isinstance(end_value, datetime):
                    end_dt = end_value
                else:
                    return ActionResult(success=False, message="Invalid end_value")
                
                diff = end_dt - dt
                
                return ActionResult(
                    success=True,
                    message=f"Difference calculated",
                    data={
                        "seconds": diff.total_seconds(),
                        "minutes": diff.total_seconds() / 60,
                        "hours": diff.total_seconds() / 3600,
                        "days": diff.days,
                        "microseconds": diff.total_seconds() * 1e6
                    }
                )
            elif operation == "range":
                if end_value is None:
                    return ActionResult(success=False, message="end_value required for range operation")
                
                if isinstance(end_value, str):
                    try:
                        from dateutil import parser
                        end_dt = parser.parse(end_value)
                    except ImportError:
                        end_dt = datetime.fromisoformat(end_value.replace("Z", "+00:00"))
                elif isinstance(end_value, datetime):
                    end_dt = end_value
                else:
                    return ActionResult(success=False, message="Invalid end_value")
                
                dates = []
                current = dt
                while current <= end_dt:
                    dates.append(current.isoformat())
                    current += timedelta(**delta_kwargs)
                
                return ActionResult(
                    success=True,
                    message=f"Generated range with {len(dates)} items",
                    data={"range": dates, "count": len(dates)}
                )
            elif operation == "start_of":
                if unit_lower == "day":
                    result = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                elif unit_lower == "week":
                    days_since_monday = dt.weekday()
                    result = (dt - timedelta(days=days_since_monday)).replace(hour=0, minute=0, second=0, microsecond=0)
                elif unit_lower == "month":
                    result = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                elif unit_lower == "year":
                    result = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    return ActionResult(success=False, message=f"Unknown unit for start_of: {unit}")
                
                return ActionResult(
                    success=True,
                    message=f"Start of {unit}: {result.isoformat()}",
                    data={"result": result.isoformat()}
                )
            elif operation == "end_of":
                if unit_lower == "day":
                    result = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                elif unit_lower == "week":
                    days_until_sunday = 6 - dt.weekday()
                    result = (dt + timedelta(days=days_until_sunday)).replace(hour=23, minute=59, second=59, microsecond=999999)
                elif unit_lower == "month":
                    last_day = calendar.monthrange(dt.year, dt.month)[1]
                    result = dt.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)
                elif unit_lower == "year":
                    result = dt.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
                else:
                    return ActionResult(success=False, message=f"Unknown unit for end_of: {unit}")
                
                return ActionResult(
                    success=True,
                    message=f"End of {unit}: {result.isoformat()}",
                    data={"result": result.isoformat()}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Datetime calculation failed: {e}")
