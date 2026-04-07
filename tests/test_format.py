"""Tests for format utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.format import (
    format_number,
    format_bytes,
    format_percent,
    format_duration_long,
    format_json,
    truncate_middle,
    format_list,
)


class TestFormatNumber:
    """Tests for format_number."""

    def test_integer(self) -> None:
        """Test formatting integer."""
        result = format_number(1000)
        assert "1" in result and "0" in result

    def test_float(self) -> None:
        """Test formatting float."""
        result = format_number(1234.56, decimals=1)
        assert "1" in result


class TestFormatBytes:
    """Tests for format_bytes."""

    def test_bytes(self) -> None:
        """Test bytes formatting."""
        result = format_bytes(500)
        assert "B" in result

    def test_kilobytes(self) -> None:
        """Test KB formatting."""
        result = format_bytes(1024)
        assert "KB" in result

    def test_megabytes(self) -> None:
        """Test MB formatting."""
        result = format_bytes(1024 * 1024)
        assert "MB" in result


class TestFormatPercent:
    """Tests for format_percent."""

    def test_basic_percent(self) -> None:
        """Test basic percentage."""
        result = format_percent(50, 100)
        assert "50" in result

    def test_zero_total(self) -> None:
        """Test zero total."""
        result = format_percent(10, 0)
        assert "0" in result


class TestFormatDurationLong:
    """Tests for format_duration_long."""

    def test_seconds(self) -> None:
        """Test seconds formatting."""
        result = format_duration_long(30)
        assert "second" in result

    def test_minutes(self) -> None:
        """Test minutes formatting."""
        result = format_duration_long(120)
        assert "minute" in result

    def test_hours(self) -> None:
        """Test hours formatting."""
        result = format_duration_long(3600)
        assert "hour" in result


class TestFormatJson:
    """Tests for format_json."""

    def test_basic_json(self) -> None:
        """Test basic JSON formatting."""
        result = format_json({"key": "value"})
        assert "key" in result
        assert "value" in result


class TestTruncateMiddle:
    """Tests for truncate_middle."""

    def test_short_text(self) -> None:
        """Test short text not truncated."""
        result = truncate_middle("hello", 10)
        assert result == "hello"

    def test_long_text(self) -> None:
        """Test long text truncated."""
        result = truncate_middle("hello world", 8)
        assert "..." in result


class TestFormatList:
    """Tests for format_list."""

    def test_empty(self) -> None:
        """Test empty list."""
        result = format_list([])
        assert result == ""

    def test_single_item(self) -> None:
        """Test single item."""
        result = format_list(["apple"])
        assert result == "apple"

    def test_two_items(self) -> None:
        """Test two items."""
        result = format_list(["apple", "banana"])
        assert "and" in result

    def test_multiple_items(self) -> None:
        """Test multiple items."""
        result = format_list(["apple", "banana", "cherry"])
        assert "," in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])