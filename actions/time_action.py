"""
Time manipulation and formatting actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union


def parse_time(time_str: str) -> Dict[str, int]:
    """
    Parse time string to hours, minutes, seconds.

    Args:
        time_str: Time string (e.g., '14:30', '2:30 PM').

    Returns:
        Dictionary with time components.
    """
    import re

    time_str = time_str.strip()

    patterns = [
        (r'^(\d{1,2}):(\d{2}):(\d{2})$', 3),
        (r'^(\d{1,2}):(\d{2})$', 2),
        (r'^(\d{1,2}):(\d{2})\s*(AM|PM)$', 2),
        (r'^(\d{1,2}):(\d{2}):(\d{2})\s*(AM|PM)$', 3),
    ]

    for pattern, group_count in patterns:
        match = re.match(pattern, time_str, re.IGNORECASE)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2))
            second = int(match.group(3)) if group_count == 3 else 0
            meridiem = match.group(group_count) if match.group(group_count) else None

            if meridiem:
                if meridiem.upper() == 'PM' and hour != 12:
                    hour += 12
                elif meridiem.upper() == 'AM' and hour == 12:
                    hour = 0

            return {
                'hour': hour,
                'minute': minute,
                'second': second,
                'total_seconds': hour * 3600 + minute * 60 + second,
            }

    raise ValueError(f"Cannot parse time: {time_str}")


def format_time(
    hours: int,
    minutes: int,
    seconds: int = 0,
    use_12h: bool = False
) -> str:
    """
    Format time components to string.

    Args:
        hours: Hour (0-23).
        minutes: Minute (0-59).
        seconds: Second (0-59).
        use_12h: Use 12-hour format with AM/PM.

    Returns:
        Formatted time string.
    """
    if use_12h:
        if hours == 0:
            h = 12
            meridiem = 'AM'
        elif hours < 12:
            h = hours
            meridiem = 'AM'
        elif hours == 12:
            h = 12
            meridiem = 'PM'
        else:
            h = hours - 12
            meridiem = 'PM'

        if seconds:
            return f'{h}:{minutes:02d}:{seconds:02d} {meridiem}'
        return f'{h}:{minutes:02d} {meridiem}'

    if seconds:
        return f'{hours:02d}:{minutes:02d}:{seconds:02d}'
    return f'{hours:02d}:{minutes:02d}'


def seconds_to_time_components(total_seconds: int) -> Dict[str, int]:
    """
    Convert total seconds to time components.

    Args:
        total_seconds: Total seconds.

    Returns:
        Dictionary with hours, minutes, seconds.
    """
    if total_seconds < 0:
        raise ValueError("Seconds cannot be negative")

    hours = total_seconds // 3600
    remaining = total_seconds % 3600
    minutes = remaining // 60
    seconds = remaining % 60

    return {
        'hours': hours,
        'minutes': minutes,
        'seconds': seconds,
        'total_seconds': total_seconds,
    }


def time_components_to_seconds(
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> int:
    """
    Convert time components to total seconds.

    Args:
        hours: Hours.
        minutes: Minutes.
        seconds: Seconds.

    Returns:
        Total seconds.
    """
    return hours * 3600 + minutes * 60 + seconds


def add_time(
    hours: int,
    minutes: int,
    seconds: int,
    add_hours: int = 0,
    add_minutes: int = 0,
    add_seconds: int = 0
) -> Dict[str, int]:
    """
    Add time components.

    Args:
        hours: Base hours.
        minutes: Base minutes.
        seconds: Base seconds.
        add_hours: Hours to add.
        add_minutes: Minutes to add.
        add_seconds: Seconds to add.

    Returns:
        Dictionary with result components.
    """
    total = (
        hours * 3600 + minutes * 60 + seconds +
        add_hours * 3600 + add_minutes * 60 + add_seconds
    )

    result_hours = total // 3600
    remaining = total % 3600
    result_minutes = remaining // 60
    result_seconds = remaining % 60

    return {
        'hours': result_hours,
        'minutes': result_minutes,
        'seconds': result_seconds,
    }


def subtract_time(
    hours1: int,
    minutes1: int,
    seconds1: int,
    hours2: int,
    minutes2: int,
    seconds2: int
) -> Dict[str, Any]:
    """
    Subtract two times.

    Args:
        hours1, minutes1, seconds1: First time.
        hours2, minutes2, seconds2: Second time.

    Returns:
        Dictionary with difference.
    """
    total1 = hours1 * 3600 + minutes1 * 60 + seconds1
    total2 = hours2 * 3600 + minutes2 * 60 + seconds2

    diff = total1 - total2

    if diff < 0:
        return {
            'is_negative': True,
            'hours': abs(diff) // 3600,
            'minutes': (abs(diff) % 3600) // 60,
            'seconds': abs(diff) % 60,
        }

    return {
        'is_negative': False,
        'hours': diff // 3600,
        'minutes': (diff % 3600) // 60,
        'seconds': diff % 60,
    }


def get_current_time_formatted(use_12h: bool = False) -> str:
    """
    Get current time as formatted string.

    Args:
        use_12h: Use 12-hour format.

    Returns:
        Formatted time string.
    """
    now = datetime.now()
    return format_time(now.hour, now.minute, now.second, use_12h)


def get_time_difference_seconds(
    time1: str,
    time2: str
) -> int:
    """
    Get difference between two times in seconds.

    Args:
        time1: First time.
        time2: Second time.

    Returns:
        Difference in seconds.
    """
    t1 = parse_time(time1)
    t2 = parse_time(time2)

    total1 = t1['total_seconds']
    total2 = t2['total_seconds']

    return total2 - total1


def round_time(
    hours: int,
    minutes: int,
    round_to_minutes: int = 15
) -> Dict[str, int]:
    """
    Round time to nearest interval.

    Args:
        hours: Hour.
        minutes: Minutes.
        round_to_minutes: Round to this many minutes.

    Returns:
        Rounded time components.
    """
    total_minutes = hours * 60 + minutes

    rounded_minutes = round(total_minutes / round_to_minutes) * round_to_minutes

    result_hours = rounded_minutes // 60
    result_minutes = rounded_minutes % 60

    return {
        'hours': result_hours,
        'minutes': result_minutes,
    }


def get_timezone_offset_hours(timezone_name: str) -> float:
    """
    Get UTC offset in hours for timezone.

    Args:
        timezone_name: Timezone name.

    Returns:
        Offset in hours.
    """
    try:
        from datetime import timezone as tz_module
        import zoneinfo

        tz = zoneinfo.ZoneInfo(timezone_name)
        now = datetime.now()
        offset = tz.utcoffset(now)
        return offset.total_seconds() / 3600
    except Exception:
        return 0.0


def is_valid_time(hours: int, minutes: int, seconds: int = 0) -> bool:
    """
    Validate time components.

    Args:
        hours: Hour (0-23).
        minutes: Minutes (0-59).
        seconds: Seconds (0-59).

    Returns:
        True if valid.
    """
    return (
        0 <= hours <= 23 and
        0 <= minutes <= 59 and
        0 <= seconds <= 59
    )


def get_nearest_quarter_hour(hours: int, minutes: int) -> str:
    """
    Round time to nearest quarter hour.

    Args:
        hours: Hour.
        minutes: Minutes.

    Returns:
        String like "2:15 PM" or "2:30 PM".
    """
    quarter = round(minutes / 15) * 15

    if quarter == 60:
        hours += 1
        quarter = 0

    return format_time(hours, quarter, 0, use_12h=True)


def get_time_range_list(
    start_time: str,
    end_time: str,
    interval_minutes: int = 30
) -> List[str]:
    """
    Generate list of times between start and end.

    Args:
        start_time: Start time string.
        end_time: End time string.
        interval_minutes: Interval in minutes.

    Returns:
        List of formatted time strings.
    """
    start = parse_time(start_time)
    end = parse_time(end_time)

    start_total = start['total_seconds']
    end_total = end['total_seconds']

    if end_total < start_total:
        end_total += 24 * 3600

    times = []
    current = start_total

    while current <= end_total:
        hours = (current // 3600) % 24
        minutes = (current % 3600) // 60

        times.append(format_time(hours, minutes))
        current += interval_minutes * 60

    return times


def get_day_period(hours: int, minutes: int = 0) -> str:
    """
    Get period of day (morning, afternoon, evening, etc).

    Args:
        hours: Hour.
        minutes: Minutes.

    Returns:
        Period name.
    """
    if 5 <= hours < 12:
        return 'morning'
    elif hours == 12 and minutes == 0:
        return 'noon'
    elif 12 <= hours < 17:
        return 'afternoon'
    elif 17 <= hours < 21:
        return 'evening'
    else:
        return 'night'
