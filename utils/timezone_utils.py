"""
Timezone conversion and handling utilities.

Provides timezone-aware datetime operations,
conversion helpers, and timezone database.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal


TZ_NAME_MAP: dict[str, str] = {
    "UTC": "UTC",
    "EST": "America/New_York",
    "CST": "Asia/Shanghai",
    "PST": "America/Los_Angeles",
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "GMT": "Europe/London",
    "CET": "Europe/Paris",
    "IST": "Asia/Kolkata",
}


def get_timezone(tz: str) -> timezone:
    """
    Get timezone from name or abbreviation.

    Args:
        tz: Timezone name (e.g. 'UTC', 'America/New_York')

    Returns:
        Python timezone object
    """
    import zoneinfo
    tz = TZ_NAME_MAP.get(tz, tz)
    try:
        return zoneinfo.ZoneInfo(tz)
    except KeyError:
        return timezone.utc


def now_in_tz(tz: str) -> datetime:
    """Get current datetime in specified timezone."""
    return datetime.now(get_timezone(tz))


def convert_to_tz(dt: datetime, target_tz: str) -> datetime:
    """
    Convert datetime to target timezone.

    Args:
        dt: Source datetime
        target_tz: Target timezone name

    Returns:
        Converted datetime in target timezone
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    target = get_timezone(target_tz)
    return dt.astimezone(target)


def convert_from_tz(dt: datetime, source_tz: str) -> datetime:
    """
    Convert datetime from source timezone to UTC.

    Args:
        dt: Source datetime
        source_tz: Source timezone name

    Returns:
        UTC datetime
    """
    source = get_timezone(source_tz)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=source)
    return dt.astimezone(timezone.utc)


def format_in_tz(dt: datetime, tz: str, fmt: str = "%Y-%m-%d %H:%M:%S %Z") -> str:
    """Format datetime in specified timezone."""
    return convert_to_tz(dt, tz).strftime(fmt)


def get_utc_offset(tz: str, when: datetime | None = None) -> timedelta:
    """
    Get UTC offset for timezone at given time.

    Args:
        tz: Timezone name
        when: Specific time (defaults to now)

    Returns:
        UTC offset as timedelta
    """
    if when is None:
        when = datetime.now(get_timezone(tz))
    elif when.tzinfo is None:
        when = when.replace(tzinfo=get_timezone(tz))
    offset = when.utcoffset()
    return offset if offset else timedelta(0)


def list_common_timezones() -> list[str]:
    """List common timezone names."""
    return sorted(TZ_NAME_MAP.keys()) + [
        "Africa/Cairo",
        "Africa/Johannesburg",
        "America/Argentina/Buenos_Aires",
        "America/Sao_Paulo",
        "America/Toronto",
        "America/Vancouver",
        "Asia/Bangkok",
        "Asia/Dubai",
        "Asia/Hong_Kong",
        "Asia/Jakarta",
        "Asia/Karachi",
        "Asia/Manila",
        "Asia/Singapore",
        "Australia/Melbourne",
        "Australia/Sydney",
        "Europe/Amsterdam",
        "Europe/Berlin",
        "Europe/Madrid",
        "Europe/Moscow",
        "Europe/Rome",
        "Pacific/Auckland",
        "UTC",
    ]


def is_dst(tz: str, when: datetime | None = None) -> bool:
    """Check if timezone is in daylight saving time."""
    if when is None:
        when = datetime.now(get_timezone(tz))
    elif when.tzinfo is None:
        when = when.replace(tzinfo=get_timezone(tz))
    return bool(when.dst())


def get_timezone_abbreviation(tz: str, when: datetime | None = None) -> str:
    """Get timezone abbreviation at given time."""
    if when is None:
        when = datetime.now(get_timezone(tz))
    return when.strftime("%Z")


def parse_iso_datetime(dt_str: str) -> datetime:
    """Parse ISO format datetime string."""
    dt_str = dt_str.strip().replace("Z", "+00:00")
    if "+" not in dt_str and "-" not in dt_str[10:]:
        dt_str += "+00:00"
    return datetime.fromisoformat(dt_str)


def timestamp_to_datetime(timestamp: float, tz: str = "UTC") -> datetime:
    """Convert Unix timestamp to datetime in specified timezone."""
    return datetime.fromtimestamp(timestamp, tz=get_timezone(tz))


def datetime_to_timestamp(dt: datetime) -> float:
    """Convert datetime to Unix timestamp."""
    return dt.timestamp()


class TimezoneConverter:
    """Stateful timezone converter."""

    def __init__(self, default_tz: str = "UTC"):
        self.default_tz = default_tz
        self._tz = get_timezone(default_tz)

    def to_utc(self, dt: datetime) -> datetime:
        """Convert to UTC."""
        return convert_to_tz(dt, "UTC")

    def from_utc(self, dt: datetime) -> datetime:
        """Convert from UTC to default timezone."""
        return convert_to_tz(dt, self.default_tz)

    def convert(self, dt: datetime, target_tz: str) -> datetime:
        """Convert between arbitrary timezones."""
        return convert_to_tz(dt, target_tz)
