"""Date/time operations action module for RabAI AutoClick.

Provides date/time operations:
- DateTimeNowAction: Get current datetime
- DateTimeParseAction: Parse date string
- DateTimeFormatAction: Format datetime
- DateTimeArithmeticAction: Date arithmetic
- DateTimeDiffAction: Calculate time difference
- DateTimeConvertAction: Convert timezones
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DateTimeNowAction(BaseAction):
    """Get current datetime."""
    action_type = "datetime_now"
    display_name = "当前时间"
    description = "获取当前日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tz_name = params.get("timezone", "local")
            fmt = params.get("format", "%Y-%m-%d %H:%M:%S")

            if tz_name == "local":
                now = datetime.now()
            elif tz_name == "utc":
                now = datetime.now(timezone.utc)
            else:
                try:
                    tz = timezone(timedelta(hours=float(tz_name)))
                    now = datetime.now(tz)
                except:
                    now = datetime.now()

            result = {
                "datetime": now.strftime(fmt),
                "timestamp": now.timestamp(),
                "iso": now.isoformat(),
                "date": now.strftime("%Y-%m-%d"),
                "time": now.strftime("%H:%M:%S"),
                "weekday": now.strftime("%A"),
                "year": now.year,
                "month": now.month,
                "day": now.day,
                "hour": now.hour,
                "minute": now.minute,
                "second": now.second
            }

            return ActionResult(success=True, message=f"Current time: {result['datetime']}", data=result)

        except Exception as e:
            return ActionResult(success=False, message=f"Now error: {str(e)}")


class DateTimeParseAction(BaseAction):
    """Parse date string."""
    action_type = "datetime_parse"
    display_name = "解析日期"
    description = "解析日期字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            date_str = params.get("date_str", "")
            input_formats = params.get("formats", ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"])

            if not date_str:
                return ActionResult(success=False, message="date_str is required")

            parsed = None
            used_format = None

            for fmt in input_formats:
                try:
                    parsed = datetime.strptime(date_str.strip(), fmt)
                    used_format = fmt
                    break
                except ValueError:
                    continue

            if not parsed:
                try:
                    parsed = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    used_format = "isoformat"
                except:
                    pass

            if parsed:
                return ActionResult(
                    success=True,
                    message=f"Parsed with format: {used_format}",
                    data={
                        "datetime": parsed,
                        "timestamp": parsed.timestamp(),
                        "iso": parsed.isoformat(),
                        "format_used": used_format
                    }
                )
            else:
                return ActionResult(success=False, message="Could not parse date string")

        except Exception as e:
            return ActionResult(success=False, message=f"Parse error: {str(e)}")


class DateTimeFormatAction(BaseAction):
    """Format datetime."""
    action_type = "datetime_format"
    display_name = "格式化日期"
    description = "格式化日期时间"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            date_str = params.get("date_str", "")
            input_format = params.get("input_format", "%Y-%m-%d %H:%M:%S")
            output_format = params.get("output_format", "%Y-%m-%d")

            if not date_str:
                return ActionResult(success=False, message="date_str is required")

            try:
                if input_format:
                    dt = datetime.strptime(date_str, input_format)
                else:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except:
                return ActionResult(success=False, message="Could not parse date string")

            formatted = dt.strftime(output_format)

            return ActionResult(
                success=True,
                message=f"Formatted to: {formatted}",
                data={"result": formatted, "datetime": dt.isoformat()}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Format error: {str(e)}")


class DateTimeArithmeticAction(BaseAction):
    """Date arithmetic."""
    action_type = "datetime_arithmetic"
    display_name = "日期计算"
    description = "日期时间计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            date_str = params.get("date_str", "")
            input_format = params.get("format", "%Y-%m-%d %H:%M:%S")
            years = params.get("years", 0)
            months = params.get("months", 0)
            days = params.get("days", 0)
            hours = params.get("hours", 0)
            minutes = params.get("minutes", 0)
            seconds = params.get("seconds", 0)

            if not date_str:
                return ActionResult(success=False, message="date_str is required")

            try:
                dt = datetime.strptime(date_str, input_format)
            except:
                try:
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except:
                    return ActionResult(success=False, message="Could not parse date string")

            delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
            result = dt + delta

            if months != 0 or years != 0:
                month = result.month - 1 + months
                year = result.year + years + month // 12
                month = month % 12 + 1
                day = min(result.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
                result = result.replace(year=year, month=month, day=day)

            return ActionResult(
                success=True,
                message=f"Result: {result.strftime(input_format)}",
                data={"result": result, "timestamp": result.timestamp(), "iso": result.isoformat()}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Arithmetic error: {str(e)}")


class DateTimeDiffAction(BaseAction):
    """Calculate time difference."""
    action_type = "datetime_diff"
    display_name = "时间差计算"
    description = "计算时间差"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            start_str = params.get("start", "")
            end_str = params.get("end", "")
            format_str = params.get("format", "%Y-%m-%d %H:%M:%S")
            unit = params.get("unit", "auto")

            if not start_str or not end_str:
                return ActionResult(success=False, message="start and end are required")

            try:
                start = datetime.strptime(start_str, format_str)
            except:
                start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))

            try:
                end = datetime.strptime(end_str, format_str)
            except:
                end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))

            diff = end - start
            total_seconds = diff.total_seconds()

            result = {
                "total_seconds": total_seconds,
                "total_minutes": total_seconds / 60,
                "total_hours": total_seconds / 3600,
                "total_days": total_seconds / 86400
            }

            if unit == "seconds":
                result["value"] = total_seconds
            elif unit == "minutes":
                result["value"] = result["total_minutes"]
            elif unit == "hours":
                result["value"] = result["total_hours"]
            elif unit == "days":
                result["value"] = result["total_days"]
            else:
                result["value"] = total_seconds

            result["days"] = diff.days
            result["seconds"] = diff.seconds
            result["formatted"] = str(diff)

            return ActionResult(
                success=True,
                message=f"Time difference: {result['formatted']}",
                data=result
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Diff error: {str(e)}")


class DateTimeConvertAction(BaseAction):
    """Convert timezones."""
    action_type = "datetime_convert"
    display_name = "时区转换"
    description = "时区转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            date_str = params.get("date_str", "")
            from_tz = params.get("from_timezone", "UTC")
            to_tz = params.get("to_timezone", "UTC")
            format_str = params.get("format", "%Y-%m-%d %H:%M:%S")

            if not date_str:
                return ActionResult(success=False, message="date_str is required")

            try:
                from_offset = float(from_tz) if from_tz.replace(".", "").replace("-", "").isdigit() else 0
                from_timezone = timezone(timedelta(hours=from_offset))
            except:
                from_timezone = timezone.utc

            try:
                to_offset = float(to_tz) if to_tz.replace(".", "").replace("-", "").isdigit() else 0
                to_timezone = timezone(timedelta(hours=to_offset))
            except:
                to_timezone = timezone.utc

            try:
                dt = datetime.strptime(date_str, format_str).replace(tzinfo=from_timezone)
            except:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=from_timezone)

            converted = dt.astimezone(to_timezone)

            return ActionResult(
                success=True,
                message=f"Converted from {from_tz} to {to_tz}",
                data={
                    "result": converted.strftime(format_str),
                    "iso": converted.isoformat(),
                    "timestamp": converted.timestamp()
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Convert error: {str(e)}")
