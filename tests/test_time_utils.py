"""Tests for time utilities."""

import os
import sys
import time
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.time_utils import (
    now,
    utc_now,
    timestamp,
    timestamp_ms,
    from_timestamp,
    from_timestamp_ms,
    to_timestamp,
    format_datetime,
    format_date,
    format_time,
    parse_datetime,
    add_days,
    add_hours,
    add_minutes,
    add_seconds,
    days_between,
    hours_between,
    minutes_between,
    seconds_between,
    is_weekend,
    is_weekday,
    start_of_day,
    end_of_day,
    is_same_day,
    is_today,
    is_past,
    is_future,
)


class TestNow:
    """Tests for now function."""

    def test_now_returns_datetime(self) -> None:
        """Test now returns datetime object."""
        result = now()
        assert isinstance(result, datetime)


class TestUtcNow:
    """Tests for utc_now function."""

    def test_utc_now_returns_datetime(self) -> None:
        """Test utc_now returns datetime object."""
        result = utc_now()
        assert isinstance(result, datetime)


class TestTimestamp:
    """Tests for timestamp function."""

    def test_timestamp_returns_float(self) -> None:
        """Test timestamp returns float."""
        result = timestamp()
        assert isinstance(result, float)

    def test_timestamp_is_recent(self) -> None:
        """Test timestamp is recent."""
        result = timestamp()
        assert result > 0


class TestTimestampMs:
    """Tests for timestamp_ms function."""

    def test_timestamp_ms_returns_int(self) -> None:
        """Test timestamp_ms returns int."""
        result = timestamp_ms()
        assert isinstance(result, int)


class TestFromTimestamp:
    """Tests for from_timestamp function."""

    def test_from_timestamp(self) -> None:
        """Test converting timestamp to datetime."""
        ts = 1609459200.0
        result = from_timestamp(ts)
        assert isinstance(result, datetime)


class TestFromTimestampMs:
    """Tests for from_timestamp_ms function."""

    def test_from_timestamp_ms(self) -> None:
        """Test converting milliseconds timestamp to datetime."""
        ts = 1609459200000
        result = from_timestamp_ms(ts)
        assert isinstance(result, datetime)


class TestToTimestamp:
    """Tests for to_timestamp function."""

    def test_to_timestamp(self) -> None:
        """Test converting datetime to timestamp."""
        dt = datetime(2021, 1, 1, 0, 0, 0)
        result = to_timestamp(dt)
        assert isinstance(result, float)


class TestFormatDatetime:
    """Tests for format_datetime function."""

    def test_format_datetime(self) -> None:
        """Test formatting datetime."""
        dt = datetime(2021, 6, 15, 10, 30, 45)
        result = format_datetime(dt, "%Y-%m-%d")
        assert result == "2021-06-15"


class TestFormatDate:
    """Tests for format_date function."""

    def test_format_date(self) -> None:
        """Test formatting date."""
        dt = datetime(2021, 6, 15, 10, 30, 45)
        result = format_date(dt)
        assert result == "2021-06-15"


class TestFormatTime:
    """Tests for format_time function."""

    def test_format_time(self) -> None:
        """Test formatting time."""
        dt = datetime(2021, 6, 15, 10, 30, 45)
        result = format_time(dt)
        assert result == "10:30:45"


class TestParseDatetime:
    """Tests for parse_datetime function."""

    def test_parse_datetime_valid(self) -> None:
        """Test parsing valid datetime string."""
        result = parse_datetime("2021-06-15 10:30:45", "%Y-%m-%d %H:%M:%S")
        assert result is not None
        assert result.year == 2021

    def test_parse_datetime_invalid(self) -> None:
        """Test parsing invalid datetime string."""
        result = parse_datetime("invalid")
        assert result is None


class TestAddDays:
    """Tests for add_days function."""

    def test_add_days_positive(self) -> None:
        """Test adding positive days."""
        dt = datetime(2021, 1, 1)
        result = add_days(dt, 5)
        assert result.day == 6

    def test_add_days_negative(self) -> None:
        """Test adding negative days."""
        dt = datetime(2021, 1, 1)
        result = add_days(dt, -5)
        assert result.day == 27


class TestAddHours:
    """Tests for add_hours function."""

    def test_add_hours(self) -> None:
        """Test adding hours."""
        dt = datetime(2021, 1, 1, 10, 0, 0)
        result = add_hours(dt, 5)
        assert result.hour == 15


class TestAddMinutes:
    """Tests for add_minutes function."""

    def test_add_minutes(self) -> None:
        """Test adding minutes."""
        dt = datetime(2021, 1, 1, 10, 0, 0)
        result = add_minutes(dt, 30)
        assert result.minute == 30


class TestAddSeconds:
    """Tests for add_seconds function."""

    def test_add_seconds(self) -> None:
        """Test adding seconds."""
        dt = datetime(2021, 1, 1, 10, 0, 0)
        result = add_seconds(dt, 45)
        assert result.second == 45


class TestDaysBetween:
    """Tests for days_between function."""

    def test_days_between(self) -> None:
        """Test calculating days between."""
        dt1 = datetime(2021, 1, 1)
        dt2 = datetime(2021, 1, 10)
        assert days_between(dt1, dt2) == 9


class TestHoursBetween:
    """Tests for hours_between function."""

    def test_hours_between(self) -> None:
        """Test calculating hours between."""
        dt1 = datetime(2021, 1, 1, 0, 0, 0)
        dt2 = datetime(2021, 1, 1, 12, 0, 0)
        result = hours_between(dt1, dt2)
        assert result == 12


class TestMinutesBetween:
    """Tests for minutes_between function."""

    def test_minutes_between(self) -> None:
        """Test calculating minutes between."""
        dt1 = datetime(2021, 1, 1, 0, 0, 0)
        dt2 = datetime(2021, 1, 1, 0, 30, 0)
        result = minutes_between(dt1, dt2)
        assert result == 30


class TestSecondsBetween:
    """Tests for seconds_between function."""

    def test_seconds_between(self) -> None:
        """Test calculating seconds between."""
        dt1 = datetime(2021, 1, 1, 0, 0, 0)
        dt2 = datetime(2021, 1, 1, 0, 1, 0)
        result = seconds_between(dt1, dt2)
        assert result == 60


class TestIsWeekend:
    """Tests for is_weekend function."""

    def test_is_weekend_saturday(self) -> None:
        """Test Saturday is weekend."""
        dt = datetime(2021, 6, 12)  # Saturday
        assert is_weekend(dt)

    def test_is_weekend_monday(self) -> None:
        """Test Monday is not weekend."""
        dt = datetime(2021, 6, 14)  # Monday
        assert not is_weekend(dt)


class TestIsWeekday:
    """Tests for is_weekday function."""

    def test_is_weekday_monday(self) -> None:
        """Test Monday is weekday."""
        dt = datetime(2021, 6, 14)  # Monday
        assert is_weekday(dt)

    def test_is_weekday_saturday(self) -> None:
        """Test Saturday is not weekday."""
        dt = datetime(2021, 6, 12)  # Saturday
        assert not is_weekday(dt)


class TestStartOfDay:
    """Tests for start_of_day function."""

    def test_start_of_day(self) -> None:
        """Test getting start of day."""
        dt = datetime(2021, 6, 15, 10, 30, 45)
        result = start_of_day(dt)
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0


class TestEndOfDay:
    """Tests for end_of_day function."""

    def test_end_of_day(self) -> None:
        """Test getting end of day."""
        dt = datetime(2021, 6, 15, 10, 30, 45)
        result = end_of_day(dt)
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59


class TestIsSameDay:
    """Tests for is_same_day function."""

    def test_is_same_day_true(self) -> None:
        """Test same day returns True."""
        dt1 = datetime(2021, 6, 15, 10, 0, 0)
        dt2 = datetime(2021, 6, 15, 20, 0, 0)
        assert is_same_day(dt1, dt2)

    def test_is_same_day_false(self) -> None:
        """Test different day returns False."""
        dt1 = datetime(2021, 6, 15, 10, 0, 0)
        dt2 = datetime(2021, 6, 16, 10, 0, 0)
        assert not is_same_day(dt1, dt2)


class TestIsToday:
    """Tests for is_today function."""

    def test_is_today_true(self) -> None:
        """Test today returns True."""
        dt = datetime.now()
        assert is_today(dt)

    def test_is_today_false(self) -> None:
        """Test yesterday returns False."""
        dt = datetime.now() - timedelta(days=1)
        assert not is_today(dt)


class TestIsPast:
    """Tests for is_past function."""

    def test_is_past_true(self) -> None:
        """Test past datetime returns True."""
        dt = datetime.now() - timedelta(days=1)
        assert is_past(dt)

    def test_is_past_false(self) -> None:
        """Test future datetime returns False."""
        dt = datetime.now() + timedelta(days=1)
        assert not is_past(dt)


class TestIsFuture:
    """Tests for is_future function."""

    def test_is_future_true(self) -> None:
        """Test future datetime returns True."""
        dt = datetime.now() + timedelta(days=1)
        assert is_future(dt)

    def test_is_future_false(self) -> None:
        """Test past datetime returns False."""
        dt = datetime.now() - timedelta(days=1)
        assert not is_future(dt)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
