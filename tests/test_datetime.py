"""Tests for datetime utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.datetime_utils import (
    now,
    today,
    parse_datetime,
    format_datetime,
    format_duration,
    parse_duration,
    timestamp,
    timestamp_ms,
    from_timestamp,
    is_weekend,
    is_weekday,
    start_of_week,
    start_of_month,
    end_of_month,
    age_string,
)


class TestNowAndToday:
    """Tests for now and today."""

    def test_now(self) -> None:
        """Test now function."""
        result = now()
        assert result is not None

    def test_today(self) -> None:
        """Test today function."""
        result = today()
        assert result.hour == 0
        assert result.minute == 0


class TestParseDatetime:
    """Tests for parse_datetime."""

    def test_parse_standard_format(self) -> None:
        """Test parsing standard format."""
        result = parse_datetime("2024-01-15 12:30:00")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_invalid(self) -> None:
        """Test parsing invalid string."""
        result = parse_datetime("not a date")
        assert result is None


class TestFormatDatetime:
    """Tests for format_datetime."""

    def test_format_default(self) -> None:
        """Test default formatting."""
        result = format_datetime()
        assert "2024" in result or "2025" in result

    def test_format_custom(self) -> None:
        """Test custom format."""
        result = format_datetime(format_str="%Y-%m-%d")
        assert result.count("-") == 2


class TestFormatDuration:
    """Tests for format_duration."""

    def test_seconds(self) -> None:
        """Test formatting seconds."""
        result = format_duration(45)
        assert "s" in result

    def test_minutes(self) -> None:
        """Test formatting minutes."""
        result = format_duration(120)
        assert "m" in result

    def test_hours(self) -> None:
        """Test formatting hours."""
        result = format_duration(3600)
        assert "h" in result


class TestParseDuration:
    """Tests for parse_duration."""

    def test_parse_seconds(self) -> None:
        """Test parsing seconds."""
        assert parse_duration("45s") == 45

    def test_parse_minutes(self) -> None:
        """Test parsing minutes."""
        assert parse_duration("5m") == 300

    def test_parse_hours(self) -> None:
        """Test parsing hours."""
        assert parse_duration("1h") == 3600


class TestTimestamp:
    """Tests for timestamp functions."""

    def test_timestamp(self) -> None:
        """Test timestamp function."""
        ts = timestamp()
        assert ts > 0

    def test_timestamp_ms(self) -> None:
        """Test millisecond timestamp."""
        ts = timestamp_ms()
        assert ts > 0
        assert ts % 1000 == 0


class TestFromTimestamp:
    """Tests for from_timestamp."""

    def test_from_timestamp(self) -> None:
        """Test converting timestamp."""
        ts = timestamp()
        dt = from_timestamp(ts)
        assert dt is not None


class TestWeekday:
    """Tests for weekday functions."""

    def test_is_weekend(self) -> None:
        """Test is_weekend."""
        result = is_weekday()
        assert isinstance(result, bool)

    def test_is_weekday(self) -> None:
        """Test is_weekday."""
        result = is_weekday()
        assert isinstance(result, bool)


class TestStartOfWeek:
    """Tests for start_of_week."""

    def test_start_of_week(self) -> None:
        """Test getting start of week."""
        result = start_of_week()
        assert result.weekday() == 0  # Monday


class TestAgeString:
    """Tests for age_string."""

    def test_just_now(self) -> None:
        """Test 'just now'."""
        result = age_string(now())
        assert "just" in result.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])