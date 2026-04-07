"""
Cron schedule parsing and manipulation actions.
"""
from __future__ import annotations

import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta


def parse_cron_expression(expression: str) -> Dict[str, Any]:
    """
    Parse a cron expression into components.

    Args:
        expression: Cron expression (e.g., '0 * * * *').

    Returns:
        Dictionary with parsed components.

    Raises:
        ValueError: If expression is invalid.
    """
    parts = expression.split()

    if len(parts) != 5:
        raise ValueError(f"Invalid cron expression: expected 5 fields, got {len(parts)}")

    return {
        'minute': parts[0],
        'hour': parts[1],
        'day_of_month': parts[2],
        'month': parts[3],
        'day_of_week': parts[4],
        'raw': expression,
    }


def describe_cron_schedule(expression: str) -> str:
    """
    Describe a cron schedule in human-readable format.

    Args:
        expression: Cron expression.

    Returns:
        Human-readable description.
    """
    parsed = parse_cron_expression(expression)

    minute = parsed['minute']
    hour = parsed['hour']
    dom = parsed['day_of_month']
    month = parsed['month']
    dow = parsed['day_of_week']

    if minute == '*' and hour == '*':
        return 'Every minute'
    elif minute == '0' and hour == '*':
        return 'Every hour at minute 0'
    elif minute == '0' and hour == '0':
        return 'Once daily at midnight'
    elif minute == '0' and hour == '12':
        return 'Once daily at noon'
    elif dom == '*' and month == '*' and dow == '*':
        return f'Every day at {hour}:{minute.zfill(2)}'
    elif dow != '*' and dom == '*':
        return f'Every {dow} at {hour}:{minute.zfill(2)}'
    elif dom != '*' and month == '*' and dow == '*':
        return f'On day {dom} of every month at {hour}:{minute.zfill(2)}'
    elif minute != '*' and hour != '*':
        return f'At {hour}:{minute.zfill(2)}'
    elif minute == '*/5':
        return 'Every 5 minutes'
    elif minute == '*/15':
        return 'Every 15 minutes'
    elif minute == '*/30':
        return 'Every 30 minutes'
    elif hour == '*/2':
        return 'Every 2 hours'
    elif hour == '*/4':
        return 'Every 4 hours'
    elif hour == '*/6':
        return 'Every 6 hours'
    elif hour == '*/12':
        return 'Every 12 hours'

    return f'Custom schedule: {expression}'


def get_next_run_times(
    expression: str,
    count: int = 5,
    start_time: Optional[datetime] = None
) -> List[datetime]:
    """
    Calculate next run times for a cron expression.

    Args:
        expression: Cron expression.
        count: Number of run times to calculate.
        start_time: Starting point for calculation.

    Returns:
        List of upcoming run times.
    """
    if start_time is None:
        start_time = datetime.now()

    parsed = parse_cron_expression(expression)

    runs: List[datetime] = []
    current = start_time.replace(second=0, microsecond=0)

    minute = parsed['minute']
    hour = parsed['hour']

    if minute != '*' and not minute.startswith('*/'):
        target_minute = int(minute)
    else:
        target_minute = None

    if hour != '*' and not hour.startswith('*/'):
        target_hour = int(hour)
    else:
        target_hour = None

    max_iterations = 1000
    iterations = 0

    while len(runs) < count and iterations < max_iterations:
        current += timedelta(minutes=1)
        iterations += 1

        if target_minute is not None and current.minute != target_minute:
            continue

        if target_hour is not None and current.hour != target_hour:
            continue

        if minute.startswith('*/'):
            interval = int(minute[2:])
            if current.minute % interval != 0:
                continue

        if hour.startswith('*/'):
            interval = int(hour[2:])
            if current.hour % interval != 0:
                continue

        runs.append(current)

    return runs


def build_cron_expression(
    minute: str = '*',
    hour: str = '*',
    day_of_month: str = '*',
    month: str = '*',
    day_of_week: str = '*'
) -> str:
    """
    Build a cron expression from components.

    Args:
        minute: Minute field (0-59, *, */n).
        hour: Hour field (0-23, *, */n).
        day_of_month: Day of month field (1-31, *, */n).
        month: Month field (1-12, *, */n).
        day_of_week: Day of week field (0-6, *, */n).

    Returns:
        Cron expression string.
    """
    return f'{minute} {hour} {day_of_month} {month} {day_of_week}'


def cron_to_seconds(expression: str) -> List[int]:
    """
    Convert cron to list of second offsets in an hour.

    Args:
        expression: Cron expression.

    Returns:
        List of second offsets.
    """
    parsed = parse_cron_expression(expression)

    seconds: List[int] = []

    minute = parsed['minute']

    if minute.startswith('*/'):
        interval = int(minute[2:])
        for m in range(0, 60, interval):
            seconds.append(m * 60)
    elif minute != '*':
        m = int(minute)
        seconds.append(m * 60)

    return seconds


def validate_cron_expression(expression: str) -> bool:
    """
    Validate a cron expression.

    Args:
        expression: Cron expression to validate.

    Returns:
        True if valid.
    """
    try:
        parts = expression.split()

        if len(parts) != 5:
            return False

        minute, hour, dom, month, dow = parts

        if not _validate_cron_field(minute, 0, 59):
            return False
        if not _validate_cron_field(hour, 0, 23):
            return False
        if not _validate_cron_field(dom, 1, 31):
            return False
        if not _validate_cron_field(month, 1, 12):
            return False
        if not _validate_cron_field(dow, 0, 6):
            return False

        return True
    except Exception:
        return False


def _validate_cron_field(value: str, min_val: int, max_val: int) -> bool:
    """Validate a single cron field."""
    if value == '*':
        return True

    if value.startswith('*/'):
        try:
            interval = int(value[2:])
            return interval > 0
        except ValueError:
            return False

    if ',' in value:
        for part in value.split(','):
            if not _validate_cron_field(part, min_val, max_val):
                return False
        return True

    if '-' in value:
        parts = value.split('-')
        if len(parts) != 2:
            return False
        try:
            start = int(parts[0])
            end = int(parts[1])
            return min_val <= start <= end <= max_val
        except ValueError:
            return False

    try:
        num = int(value)
        return min_val <= num <= max_val
    except ValueError:
        return False


def create_daily_schedule(hour: int, minute: int = 0) -> str:
    """
    Create a daily cron schedule.

    Args:
        hour: Hour (0-23).
        minute: Minute (0-59).

    Returns:
        Cron expression.
    """
    return f'{minute} {hour} * * *'


def create_hourly_schedule(minute: int = 0) -> str:
    """
    Create an hourly cron schedule.

    Args:
        minute: Minute past the hour.

    Returns:
        Cron expression.
    """
    return f'{minute} * * * *'


def create_minutely_schedule() -> str:
    """
    Create a minutely cron schedule.

    Returns:
        Cron expression.
    """
    return '* * * * *'


def create_weekly_schedule(
    day_of_week: int,
    hour: int,
    minute: int = 0
) -> str:
    """
    Create a weekly cron schedule.

    Args:
        day_of_week: Day of week (0=Sunday, 6=Saturday).
        hour: Hour (0-23).
        minute: Minute (0-59).

    Returns:
        Cron expression.
    """
    return f'{minute} {hour} * * {day_of_week}'


def create_monthly_schedule(
    day_of_month: int,
    hour: int,
    minute: int = 0
) -> str:
    """
    Create a monthly cron schedule.

    Args:
        day_of_month: Day of month (1-31).
        hour: Hour (0-23).
        minute: Minute (0-59).

    Returns:
        Cron expression.
    """
    return f'{minute} {hour} {day_of_month} * *'


def get_cron_human_description(
    minute: str,
    hour: str,
    day_of_month: str,
    month: str,
    day_of_week: str
) -> str:
    """
    Get human description of cron fields.

    Args:
        minute: Minute field.
        hour: Hour field.
        day_of_month: Day of month field.
        month: Month field.
        day_of_week: Day of week field.

    Returns:
        Human-readable description.
    """
    expression = f'{minute} {hour} {day_of_month} {month} {day_of_week}'
    return describe_cron_schedule(expression)


def cron_matches(cron_expr: str, dt: datetime) -> bool:
    """
    Check if a datetime matches a cron expression.

    Args:
        cron_expr: Cron expression.
        dt: Datetime to check.

    Returns:
        True if datetime matches.
    """
    parsed = parse_cron_expression(cron_expr)

    if not _field_matches(parsed['minute'], dt.minute, 0, 59):
        return False

    if not _field_matches(parsed['hour'], dt.hour, 0, 23):
        return False

    if not _field_matches(parsed['day_of_month'], dt.day, 1, 31):
        return False

    if not _field_matches(parsed['month'], dt.month, 1, 12):
        return False

    if not _field_matches(parsed['day_of_week'], dt.weekday(), 0, 6):
        return False

    return True


def _field_matches(field: str, value: int, min_val: int, max_val: int) -> bool:
    """Check if a field value matches the field specification."""
    if field == '*':
        return True

    if field.startswith('*/'):
        interval = int(field[2:])
        return value % interval == 0

    if ',' in field:
        return any(_field_matches(f, value, min_val, max_val) for f in field.split(','))

    if '-' in field:
        start, end = field.split('-')
        return int(start) <= value <= int(end)

    return int(field) == value


def get_schedule_interval_seconds(cron_expr: str) -> Optional[int]:
    """
    Estimate the interval of a cron schedule in seconds.

    Args:
        cron_expr: Cron expression.

    Returns:
        Interval in seconds, or None if irregular.
    """
    intervals = [
        ('* * * * *', 60),
        ('*/5 * * * *', 300),
        ('*/10 * * * *', 600),
        ('*/15 * * * *', 900),
        ('*/30 * * * *', 1800),
        ('0 * * * *', 3600),
        ('0 */2 * * *', 7200),
        ('0 */4 * * *', 14400),
        ('0 */6 * * *', 21600),
        ('0 */12 * * *', 43200),
        ('0 0 * * *', 86400),
    ]

    for pattern, seconds in intervals:
        if cron_expr == pattern:
            return seconds

    return None
