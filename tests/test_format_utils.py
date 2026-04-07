"""Tests for format utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.format_utils import (
    format_bytes,
    parse_bytes,
    format_duration,
    format_duration_long,
    format_number,
    parse_number,
    format_percent,
    format_ratio,
    truncate_string,
    pad_string,
    wrap_text,
    indent_text,
    format_table,
    format_list,
    format_phone,
    format_credit_card,
    format_ssn,
    format_zip_code,
    pluralize,
    title_case,
    snake_to_camel,
    camel_to_snake,
    kebab_to_snake,
    snake_to_kebab,
    format_boolean,
    format_currency,
    format_temperature,
)


class TestFormatBytes:
    """Tests for format_bytes function."""

    def test_format_bytes_small(self) -> None:
        """Test formatting small bytes."""
        assert format_bytes(500) == "500 B"

    def test_format_bytes_kilobytes(self) -> None:
        """Test formatting kilobytes."""
        assert format_bytes(1024) == "1.00 KB"


class TestParseBytes:
    """Tests for parse_bytes function."""

    def test_parse_bytes(self) -> None:
        """Test parsing bytes."""
        assert parse_bytes("1 KB") == 1024
        assert parse_bytes("500 B") == 500


class TestFormatDuration:
    """Tests for format_duration function."""

    def test_format_duration_seconds(self) -> None:
        """Test formatting seconds."""
        assert "s" in format_duration(30)

    def test_format_duration_minutes(self) -> None:
        """Test formatting minutes."""
        assert "m" in format_duration(90)


class TestFormatDurationLong:
    """Tests for format_duration_long function."""

    def test_format_duration_long(self) -> None:
        """Test formatting long duration."""
        result = format_duration_long(90)
        assert "minute" in result


class TestFormatNumber:
    """Tests for format_number function."""

    def test_format_number(self) -> None:
        """Test formatting number."""
        result = format_number(1234567.89)
        assert "," in result


class TestParseNumber:
    """Tests for parse_number function."""

    def test_parse_number(self) -> None:
        """Test parsing number."""
        assert parse_number("1,234") == 1234.0


class TestFormatPercent:
    """Tests for format_percent function."""

    def test_format_percent(self) -> None:
        """Test formatting percentage."""
        assert format_percent(50, 100) == "50.0%"
        assert format_percent(1, 3) == "33.3%"


class TestFormatRatio:
    """Tests for format_ratio function."""

    def test_format_ratio(self) -> None:
        """Test formatting ratio."""
        result = format_ratio(1, 3)
        assert "/" in result


class TestTruncateString:
    """Tests for truncate_string function."""

    def test_truncate_string(self) -> None:
        """Test truncating string."""
        assert truncate_string("hello world", 5) == "he..."
        assert truncate_string("hi", 10) == "hi"


class TestPadString:
    """Tests for pad_string function."""

    def test_pad_string_left(self) -> None:
        """Test padding left."""
        assert pad_string("hi", 5, align="left") == "hi   "

    def test_pad_string_right(self) -> None:
        """Test padding right."""
        assert pad_string("hi", 5, align="right") == "   hi"


class TestWrapText:
    """Tests for wrap_text function."""

    def test_wrap_text(self) -> None:
        """Test wrapping text."""
        result = wrap_text("hello world test", 5)
        assert len(result) > 1


class TestIndentText:
    """Tests for indent_text function."""

    def test_indent_text(self) -> None:
        """Test indenting text."""
        result = indent_text("hello\nworld", 4)
        assert result.startswith("    ")


class TestFormatTable:
    """Tests for format_table function."""

    def test_format_table(self) -> None:
        """Test formatting table."""
        headers = ["Name", "Age"]
        rows = [["John", 30], ["Jane", 25]]
        result = format_table(headers, rows)
        assert "Name" in result
        assert "Age" in result


class TestFormatList:
    """Tests for format_list function."""

    def test_format_list(self) -> None:
        """Test formatting list."""
        assert format_list(["a", "b", "c"]) == "a, b and c"
        assert format_list(["a"]) == "a"


class TestFormatPhone:
    """Tests for format_phone function."""

    def test_format_phone_us(self) -> None:
        """Test formatting US phone."""
        result = format_phone("5551234567", "US")
        assert "(" in result


class TestFormatCreditCard:
    """Tests for format_credit_card function."""

    def test_format_credit_card(self) -> None:
        """Test formatting credit card."""
        result = format_credit_card("1234567890123456")
        assert " " in result


class TestFormatSsn:
    """Tests for format_ssn function."""

    def test_format_ssn(self) -> None:
        """Test formatting SSN."""
        result = format_ssn("123456789")
        assert "-" in result


class TestFormatZipCode:
    """Tests for format_zip_code function."""

    def test_format_zip_code(self) -> None:
        """Test formatting ZIP code."""
        result = format_zip_code("123456789")
        assert "-" in result


class TestPluralize:
    """Tests for pluralize function."""

    def test_pluralize(self) -> None:
        """Test pluralizing."""
        assert pluralize("cat", 2) == "cats"
        assert pluralize("cat", 1) == "cat"


class TestTitleCase:
    """Tests for title_case function."""

    def test_title_case(self) -> None:
        """Test title case."""
        assert title_case("hello world") == "Hello World"


class TestSnakeToCamel:
    """Tests for snake_to_camel function."""

    def test_snake_to_camel(self) -> None:
        """Test snake to camel."""
        assert snake_to_camel("hello_world") == "helloWorld"


class TestCamelToSnake:
    """Tests for camel_to_snake function."""

    def test_camel_to_snake(self) -> None:
        """Test camel to snake."""
        assert camel_to_snake("helloWorld") == "hello_world"


class TestKebabToSnake:
    """Tests for kebab_to_snake function."""

    def test_kebab_to_snake(self) -> None:
        """Test kebab to snake."""
        assert kebab_to_snake("hello-world") == "hello_world"


class TestSnakeToKebab:
    """Tests for snake_to_kebab function."""

    def test_snake_to_kebab(self) -> None:
        """Test snake to kebab."""
        assert snake_to_kebab("hello_world") == "hello-world"


class TestFormatBoolean:
    """Tests for format_boolean function."""

    def test_format_boolean_true(self) -> None:
        """Test formatting True."""
        assert format_boolean(True) == "Yes"

    def test_format_boolean_false(self) -> None:
        """Test formatting False."""
        assert format_boolean(False) == "No"


class TestFormatCurrency:
    """Tests for format_currency function."""

    def test_format_currency(self) -> None:
        """Test formatting currency."""
        assert format_currency(1234.56) == "$1,234.56"


class TestFormatTemperature:
    """Tests for format_temperature function."""

    def test_format_temperature_celsius(self) -> None:
        """Test formatting Celsius."""
        assert format_temperature(25, "C") == "25.0°C"

    def test_format_temperature_fahrenheit(self) -> None:
        """Test formatting Fahrenheit."""
        assert "°F" in format_temperature(25, "F")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
