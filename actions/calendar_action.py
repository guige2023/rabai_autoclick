"""
Calendar and date recurrence utilities - iCal generation, recurrence rules, business days.
"""
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_date(date_str: str) -> date:
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _parse_datetime(dt_str: str) -> datetime:
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"]:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")


def _generate_rrule(
    freq: str, count: Optional[int], until: Optional[date],
    interval: int, byweekday: Optional[List[int]], bymonthday: Optional[List[int]],
    bymonth: Optional[List[int]], bysetpos: Optional[List[int]]
) -> str:
    parts = [f"FREQ={freq}"]
    if interval > 1:
        parts.append(f"INTERVAL={interval}")
    if count:
        parts.append(f"COUNT={count}")
    if until:
        parts.append(f"UNTIL={until.strftime('%Y%m%dT%H%M%S')}")
    if byweekday is not None:
        day_names = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]
        parts.append("BYDAY=" + ",".join(day_names[d] for d in byweekday))
    if bymonthday is not None:
        parts.append("BYMONTHDAY=" + ",".join(str(d) for d in bymonthday))
    if bymonth is not None:
        parts.append("BYMONTH=" + ",".join(str(m) for m in bymonth))
    if bysetpos is not None:
        parts.append("BYSETPOS=" + ",".join(str(p) for p in bysetpos))
    return ";".join(parts)


def _occurrences(
    start: date, freq: str, count: Optional[int], until: Optional[date],
    interval: int, byweekday: Optional[List[int]], bymonthday: Optional[List[int]],
    bymonth: Optional[List[int]], bysetpos: Optional[List[int]], limit: int = 100
) -> List[date]:
    results = []
    current = start
    n = 0
    delta_map = {"DAILY": 1, "WEEKLY": 7, "MONTHLY": 30, "YEARLY": 365}
    delta_days = delta_map.get(freq, 1)

    while len(results) < limit:
        if count and n >= count:
            break
        if until and current > until:
            break
        include = True
        if byweekday and current.weekday() not in byweekday:
            include = False
        if bymonthday and current.day not in bymonthday:
            include = False
        if bymonth and current.month not in bymonth:
            include = False
        if include:
            results.append(current)
        n += 1
        current = current + timedelta(days=delta_days * interval)
    return results


def _business_days(start: date, end: date, holidays: Optional[List[date]] = None) -> List[date]:
    holidays = holidays or []
    result = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in holidays:
            result.append(current)
        current += timedelta(days=1)
    return result


class CalendarAction(BaseAction):
    """Calendar and date recurrence operations.

    Provides iCal format generation, recurrence rule creation, business day calculations.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "rrule")
        start_str = params.get("start")
        end_str = params.get("end")
        freq = params.get("freq", "DAILY")
        interval = int(params.get("interval", 1))
        byweekday = params.get("byweekday")
        bymonthday = params.get("bymonthday")
        bymonth = params.get("bymonth")
        bysetpos = params.get("bysetpos")

        try:
            if operation == "rrule":
                if not start_str:
                    return {"success": False, "error": "start date required"}
                start = _parse_date(start_str)
                count = params.get("count")
                until = _parse_date(params["until"]) if params.get("until") else None
                rule = _generate_rrule(freq, count, until, interval, byweekday, bymonthday, bymonth, bysetpos)
                return {"success": True, "rrule": rule}

            elif operation == "occurrences":
                if not start_str:
                    return {"success": False, "error": "start date required"}
                start = _parse_date(start_str)
                until = _parse_date(end_str) if end_str else None
                count = params.get("count", 10)
                limit = int(params.get("limit", 100))
                occs = _occurrences(
                    start, freq, count, until, interval,
                    byweekday, bymonthday, bymonth, bysetpos, limit
                )
                return {"success": True, "occurrences": [o.isoformat() for o in occs], "count": len(occs)}

            elif operation == "business_days":
                if not start_str or not end_str:
                    return {"success": False, "error": "start and end required"}
                start = _parse_date(start_str)
                end = _parse_date(end_str)
                holidays_str = params.get("holidays", [])
                holidays = [_parse_date(h) for h in holidays_str]
                days = _business_days(start, end, holidays)
                return {"success": True, "days": [d.isoformat() for d in days], "count": len(days)}

            elif operation == "next_business_day":
                current = _parse_date(start_str) if start_str else date.today()
                added = int(params.get("add_days", 0))
                holidays_str = params.get("holidays", [])
                holidays = {_parse_date(h) for h in holidays_str}
                result = current
                days_added = 0
                while days_added < added + 1:
                    result += timedelta(days=1)
                    if result.weekday() < 5 and result not in holidays:
                        days_added += 1
                return {"success": True, "date": result.isoformat()}

            elif operation == "ical":
                start_dt = _parse_datetime(start_str) if start_str else datetime.now()
                end_dt = _parse_datetime(end_str) if end_str else start_dt + timedelta(hours=1)
                summary = params.get("summary", "Event")
                uid = params.get("uid", f"event-{datetime.now().timestamp()}")
                desc = params.get("description", "")
                location = params.get("location", "")
                rrule = params.get("rrule")
                lines = [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    "PRODID:-//rabai_autoclick//EN",
                    "BEGIN:VEVENT",
                    f"UID:{uid}",
                    f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}",
                    f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}",
                    f"SUMMARY:{summary}",
                    f"DESCRIPTION:{desc}",
                    f"LOCATION:{location}",
                    f"DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%S')}",
                ]
                if rrule:
                    lines.append(f"RRULE:{rrule}")
                lines.extend(["END:VEVENT", "END:VCALENDAR"])
                return {"success": True, "ical": "\r\n".join(lines)}

            elif operation == "days_between":
                if not start_str or not end_str:
                    return {"success": False, "error": "start and end required"}
                start = _parse_date(start_str)
                end = _parse_date(end_str)
                delta = abs((end - start).days)
                return {"success": True, "days": delta}

            elif operation == "add_days":
                if not start_str:
                    return {"success": False, "error": "start date required"}
                start = _parse_date(start_str)
                days = int(params.get("days", 0))
                result = start + timedelta(days=days)
                return {"success": True, "date": result.isoformat()}

            elif operation == "week_of_year":
                if not start_str:
                    return {"success": False, "error": "start date required"}
                d = _parse_date(start_str)
                week = d.isocalendar()[1]
                return {"success": True, "week": week, "year": d.year}

            elif operation == "quarter":
                if not start_str:
                    return {"success": False, "error": "start date required"}
                d = _parse_date(start_str)
                quarter = (d.month - 1) // 3 + 1
                return {"success": True, "quarter": quarter, "year": d.year}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"CalendarAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for calendar operations."""
    return CalendarAction().execute(context, params)
