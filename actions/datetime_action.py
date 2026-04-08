"""
Datetime utilities - parsing, formatting, timezone conversion, time arithmetic.
"""
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone, date
import time
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_datetime(dt_str: str) -> Optional[datetime]:
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
        "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d %H:%M", "%H:%M:%S", "%H:%M",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return None


def _parse_offset(offset_str: str) -> Tuple[int, str]:
    offset_str = offset_str.strip()
    if offset_str.startswith("+"):
        offset_str = offset_str[1:]
    elif offset_str.startswith("-"):
        pass
    if "h" in offset_str.lower():
        hours = int(offset_str.lower().replace("h", "").replace(" ", ""))
        return hours, "hours"
    elif "m" in offset_str.lower():
        minutes = int(offset_str.lower().replace("m", "").replace(" ", ""))
        return minutes, "minutes"
    elif "d" in offset_str.lower():
        days = int(offset_str.lower().replace("d", "").replace(" ", ""))
        return days, "days"
    elif "w" in offset_str.lower():
        weeks = int(offset_str.lower().replace("w", "").replace(" ", ""))
        return weeks, "weeks"
    return 0, "seconds"


class DateTimeAction(BaseAction):
    """Datetime operations.

    Provides parsing, formatting, timezone conversion, time arithmetic, timestamps.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "now")
        dt_str = params.get("datetime", "")
        format_str = params.get("format", "%Y-%m-%d %H:%M:%S")
        offset_str = params.get("offset", "")

        try:
            if operation == "now":
                now = datetime.now()
                return {
                    "success": True,
                    "datetime": now.strftime(format_str),
                    "timestamp": now.timestamp(),
                    "date": now.date().isoformat(),
                    "time": now.time().isoformat(),
                    "unix": int(time.time()),
                }

            elif operation == "parse":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                return {
                    "success": True,
                    "datetime": parsed.strftime(format_str),
                    "date": parsed.date().isoformat(),
                    "time": parsed.time().isoformat(),
                    "timestamp": parsed.timestamp(),
                    "weekday": parsed.strftime("%A"),
                    "year": parsed.year,
                    "month": parsed.month,
                    "day": parsed.day,
                    "hour": parsed.hour,
                    "minute": parsed.minute,
                    "second": parsed.second,
                }

            elif operation == "format":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                return {"success": True, "formatted": parsed.strftime(format_str)}

            elif operation == "add":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                amount, unit = _parse_offset(offset_str)
                delta_map = {"hours": timedelta(hours=amount), "minutes": timedelta(minutes=amount),
                             "days": timedelta(days=amount), "weeks": timedelta(weeks=amount), "seconds": timedelta(seconds=amount)}
                delta = delta_map.get(unit, timedelta(seconds=amount))
                result = parsed + delta
                return {"success": True, "result": result.strftime(format_str), "datetime": result.isoformat()}

            elif operation == "subtract":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                amount, unit = _parse_offset(offset_str)
                delta_map = {"hours": timedelta(hours=amount), "minutes": timedelta(minutes=amount),
                             "days": timedelta(days=amount), "weeks": timedelta(weeks=amount), "seconds": timedelta(seconds=amount)}
                delta = delta_map.get(unit, timedelta(seconds=amount))
                result = parsed - delta
                return {"success": True, "result": result.strftime(format_str), "datetime": result.isoformat()}

            elif operation == "diff":
                dt1_str = dt_str or params.get("datetime1", "")
                dt2_str = params.get("datetime2", "")
                if not dt1_str or not dt2_str:
                    return {"success": False, "error": "datetime1 and datetime2 required"}
                dt1 = _parse_datetime(dt1_str)
                dt2 = _parse_datetime(dt2_str)
                if dt1 is None or dt2 is None:
                    return {"success": False, "error": "Could not parse one or both datetimes"}
                diff = abs((dt1 - dt2).total_seconds())
                return {
                    "success": True,
                    "diff_seconds": diff,
                    "diff_minutes": diff / 60,
                    "diff_hours": diff / 3600,
                    "diff_days": diff / 86400,
                }

            elif operation == "timestamp":
                if dt_str:
                    parsed = _parse_datetime(dt_str)
                    if parsed is None:
                        return {"success": False, "error": f"Could not parse: {dt_str}"}
                    return {"success": True, "timestamp": parsed.timestamp(), "unix": int(parsed.timestamp())}
                else:
                    return {"success": True, "timestamp": time.time(), "unix": int(time.time())}

            elif operation == "from_timestamp":
                ts = float(params.get("timestamp", time.time()))
                result = datetime.fromtimestamp(ts)
                return {"success": True, "datetime": result.strftime(format_str), "datetime_obj": result.isoformat()}

            elif operation == "utc":
                now_utc = datetime.now(timezone.utc)
                return {"success": True, "utc": now_utc.strftime(format_str), "iso": now_utc.isoformat()}

            elif operation == "timezone_convert":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                tz_name = params.get("timezone", "UTC")
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo(tz_name)
                except ImportError:
                    try:
                        import pytz
                        tz = pytz.timezone(tz_name)
                        parsed = pytz.utc.localize(parsed).astimezone(tz)
                    except Exception:
                        return {"success": False, "error": f"Timezone not available: {tz_name}"}
                return {"success": True, "converted": parsed.strftime(format_str), "timezone": tz_name}

            elif operation == "start_of_day":
                if not dt_str:
                    d = date.today()
                else:
                    parsed = _parse_datetime(dt_str)
                    if parsed is None:
                        return {"success": False, "error": f"Could not parse: {dt_str}"}
                    d = parsed.date()
                start = datetime.combine(d, datetime.min.time())
                return {"success": True, "start": start.strftime(format_str)}

            elif operation == "end_of_day":
                if not dt_str:
                    d = date.today()
                else:
                    parsed = _parse_datetime(dt_str)
                    if parsed is None:
                        return {"success": False, "error": f"Could not parse: {dt_str}"}
                    d = parsed.date()
                end = datetime.combine(d, datetime.max.time())
                return {"success": True, "end": end.strftime(format_str)}

            elif operation == "is_weekend":
                if not dt_str:
                    d = date.today()
                else:
                    parsed = _parse_datetime(dt_str)
                    if parsed is None:
                        return {"success": False, "error": f"Could not parse: {dt_str}"}
                    d = parsed.date()
                return {"success": True, "is_weekend": d.weekday() >= 5, "weekday": d.weekday()}

            elif operation == "age":
                if not dt_str:
                    return {"success": False, "error": "datetime string required"}
                parsed = _parse_datetime(dt_str)
                if parsed is None:
                    return {"success": False, "error": f"Could not parse: {dt_str}"}
                now = datetime.now()
                diff = now - parsed
                return {"success": True, "days": diff.days, "seconds": diff.total_seconds()}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"DateTimeAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for datetime operations."""
    return DateTimeAction().execute(context, params)
