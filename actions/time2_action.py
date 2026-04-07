"""
Time advanced operations and timer actions.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Union
import time as time_module


def create_timer(
    duration_seconds: int,
    callback: Optional[callable] = None
) -> Dict[str, Any]:
    """
    Create a simple timer.

    Args:
        duration_seconds: Timer duration.
        callback: Optional callback function.

    Returns:
        Timer result.
    """
    start = time_module.time()
    end = start + duration_seconds

    while time_module.time() < end:
        time_module.sleep(0.1)

    result = {
        'duration_seconds': duration_seconds,
        'start_time': start,
        'end_time': end,
        'completed': True,
    }

    if callback:
        callback(result)

    return result


def get_elapsed_time(start_time: float) -> Dict[str, float]:
    """
    Get elapsed time since start.

    Args:
        start_time: Start timestamp.

    Returns:
        Elapsed time dictionary.
    """
    elapsed = time_module.time() - start_time

    return {
        'total_seconds': elapsed,
        'minutes': elapsed // 60,
        'seconds': elapsed % 60,
        'formatted': f'{int(elapsed // 60)}:{int(elapsed % 60):02d}',
    }


def measure_function_time(func: callable, *args, **kwargs) -> Dict[str, Any]:
    """
    Measure function execution time.

    Args:
        func: Function to measure.
        *args: Function arguments.
        **kwargs: Function keyword arguments.

    Returns:
        Execution time and result.
    """
    start = time_module.time()

    try:
        result = func(*args, **kwargs)
        success = True
        error = None
    except Exception as e:
        result = None
        success = False
        error = str(e)

    end = time_module.time()
    duration = end - start

    return {
        'duration_seconds': duration,
        'duration_ms': duration * 1000,
        'result': result,
        'success': success,
        'error': error,
    }


def create_countdown(
    seconds: int,
    on_tick: Optional[callable] = None,
    on_complete: Optional[callable] = None
) -> Dict[str, Any]:
    """
    Create a countdown timer with callbacks.

    Args:
        seconds: Countdown duration.
        on_tick: Callback for each second.
        on_complete: Callback when complete.

    Returns:
        Countdown result.
    """
    start = time_module.time()

    for i in range(seconds, 0, -1):
        remaining = i

        if on_tick:
            on_tick(remaining)

        time_module.sleep(1)

    if on_complete:
        on_complete()

    return {
        'duration': seconds,
        'completed': True,
    }


def format_stopwatch(elapsed_seconds: float) -> str:
    """
    Format elapsed seconds as stopwatch display.

    Args:
        elapsed_seconds: Elapsed time in seconds.

    Returns:
        Formatted string like "01:23:45".
    """
    hours = int(elapsed_seconds // 3600)
    minutes = int((elapsed_seconds % 3600) // 60)
    seconds = int(elapsed_seconds % 60)

    return f'{hours:02d}:{minutes:02d}:{seconds:02d}'


def get_timestamp() -> float:
    """
    Get current Unix timestamp.

    Returns:
        Current timestamp.
    """
    return time_module.time()


def get_timestamp_ms() -> int:
    """
    Get current Unix timestamp in milliseconds.

    Returns:
        Current timestamp in ms.
    """
    return int(time_module.time() * 1000)


def sleep(seconds: float) -> None:
    """
    Sleep for specified seconds.

    Args:
        seconds: Sleep duration.
    """
    time_module.sleep(seconds)


def sleep_until(timestamp: float) -> None:
    """
    Sleep until specified timestamp.

    Args:
        timestamp: Target timestamp.
    """
    remaining = timestamp - time_module.time()
    if remaining > 0:
        time_module.sleep(remaining)


def retry_with_backoff(
    func: callable,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    *args,
    **kwargs
) -> Dict[str, Any]:
    """
    Retry function with exponential backoff.

    Args:
        func: Function to retry.
        max_retries: Maximum retry attempts.
        initial_delay: Initial delay in seconds.
        backoff_factor: Backoff multiplier.
        *args: Function arguments.
        **kwargs: Function keyword arguments.

    Returns:
        Result dictionary.
    """
    delay = initial_delay
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            result = func(*args, **kwargs)
            return {
                'success': True,
                'result': result,
                'attempts': attempt + 1,
            }
        except Exception as e:
            last_error = e

            if attempt < max_retries:
                time_module.sleep(delay)
                delay *= backoff_factor

    return {
        'success': False,
        'error': str(last_error),
        'attempts': max_retries + 1,
    }


def get_timeout_timestamp(timeout_seconds: float) -> float:
    """
    Get future timestamp for timeout.

    Args:
        timeout_seconds: Timeout in seconds.

    Returns:
        Future timestamp.
    """
    return time_module.time() + timeout_seconds


def is_timed_out(deadline: float) -> bool:
    """
    Check if timeout deadline has passed.

    Args:
        deadline: Deadline timestamp.

    Returns:
        True if timed out.
    """
    return time_module.time() >= deadline


def create_interval_tracker(interval_seconds: float) -> callable:
    """
    Create a function that tracks intervals.

    Args:
        interval_seconds: Interval between triggers.

    Returns:
        Tracker function that returns True on interval.
    """
    next_trigger = [time_module.time() + interval_seconds]

    def tracker() -> bool:
        now = time_module.time()
        if now >= next_trigger[0]:
            next_trigger[0] = now + interval_seconds
            return True
        return False

    return tracker


def get_time_remaining(deadline: float) -> float:
    """
    Get remaining time until deadline.

    Args:
        deadline: Deadline timestamp.

    Returns:
        Remaining seconds (negative if past).
    """
    return deadline - time_module.time()


def get_current_datetime() -> datetime:
    """
    Get current datetime.

    Returns:
        Current datetime.
    """
    return datetime.now()


def get_current_utc_datetime() -> datetime:
    """
    Get current UTC datetime.

    Returns:
        Current UTC datetime.
    """
    return datetime.now(timezone.utc)


def format_datetime_timestamp(dt: datetime) -> str:
    """
    Format datetime as timestamp string.

    Args:
        dt: Datetime object.

    Returns:
        ISO formatted string.
    """
    return dt.isoformat()


def parse_timestamp(timestamp: Union[int, float]) -> datetime:
    """
    Parse Unix timestamp to datetime.

    Args:
        timestamp: Unix timestamp.

    Returns:
        Datetime object.
    """
    return datetime.fromtimestamp(timestamp)


def get_time_of_day_hours() -> Dict[str, Any]:
    """
    Get current time of day information.

    Returns:
        Time of day components.
    """
    now = datetime.now()

    hour = now.hour

    if 5 <= hour < 12:
        period = 'morning'
    elif 12 <= hour < 17:
        period = 'afternoon'
    elif 17 <= hour < 21:
        period = 'evening'
    else:
        period = 'night'

    return {
        'hour': hour,
        'minute': now.minute,
        'second': now.second,
        'period': period,
        'is_business_hours': 9 <= hour < 17 and now.weekday() < 5,
    }


def is_business_hours_now(
    start_hour: int = 9,
    end_hour: int = 17
) -> bool:
    """
    Check if current time is within business hours.

    Args:
        start_hour: Start hour.
        end_hour: End hour.

    Returns:
        True if within business hours.
    """
    now = datetime.now()
    return (
        start_hour <= now.hour < end_hour and
        now.weekday() < 5
    )


def get_next_business_hour(hour: int = 9) -> datetime:
    """
    Get next occurrence of business hour.

    Args:
        hour: Target hour.

    Returns:
        Next datetime at that hour.
    """
    now = datetime.now()
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)

    if target <= now or now.weekday() >= 5:
        target += timedelta(days=1)

        while target.weekday() >= 5:
            target += timedelta(days=1)

    return target


def time_until_target_hour(
    target_hour: int,
    target_minute: int = 0
) -> float:
    """
    Get seconds until target hour today or tomorrow.

    Args:
        target_hour: Target hour.
        target_minute: Target minute.

    Returns:
        Seconds until target.
    """
    now = datetime.now()
    target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

    if target <= now:
        target += timedelta(days=1)

        while target.weekday() >= 5:
            target += timedelta(days=1)

    return (target - now).total_seconds()


def create_timeout_checker(timeout_seconds: float) -> callable:
    """
    Create a timeout checker function.

    Args:
        timeout_seconds: Timeout duration.

    Returns:
        Checker function.
    """
    deadline = time_module.time() + timeout_seconds

    def is_timed_out() -> bool:
        return time_module.time() >= deadline

    def time_remaining() -> float:
        return deadline - time_module.time()

    return is_timed_out


class Stopwatch:
    """Simple stopwatch for timing."""

    def __init__(self):
        """Initialize stopwatch."""
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.running = False

    def start(self) -> None:
        """Start the stopwatch."""
        self.start_time = time_module.time()
        self.running = True
        self.end_time = None

    def stop(self) -> float:
        """Stop the stopwatch and return elapsed time."""
        if self.start_time is None:
            return 0.0

        self.end_time = time_module.time()
        self.running = False
        return self.elapsed()

    def elapsed(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0

        end = self.end_time if self.end_time else time_module.time()
        return end - self.start_time

    def reset(self) -> None:
        """Reset the stopwatch."""
        self.start_time = None
        self.end_time = None
        self.running = False

    def lap(self) -> float:
        """Get lap time (elapsed since last lap call)."""
        if self.start_time is None:
            return 0.0

        current = time_module.time()
        lap_time = current - (self._last_lap or self.start_time)
        self._last_lap = current
        return lap_time

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        self.stop()
