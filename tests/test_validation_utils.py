"""Tests for validation utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.validation_utils import (
    is_string,
    is_int,
    is_float,
    is_bool,
    is_list,
    is_dict,
    is_email,
    is_url,
    is_phone,
    is_ipv4,
    is_port,
    is_path,
    is_alphanumeric,
    is_alpha,
    is_numeric,
    is_hex_color,
    is_username,
    is_password_strong,
    is_uuid,
    in_range,
    min_length,
    max_length,
    matches_pattern,
    is_one_of,
    validate_type,
)


class TestTypeValidators:
    """Tests for type validator functions."""

    def test_is_string_true(self) -> None:
        """Test is_string returns True for string."""
        assert is_string("hello")

    def test_is_string_false(self) -> None:
        """Test is_string returns False for non-string."""
        assert not is_string(123)

    def test_is_int_true(self) -> None:
        """Test is_int returns True for int."""
        assert is_int(42)

    def test_is_int_false_for_bool(self) -> None:
        """Test is_int returns False for bool."""
        assert not is_int(True)

    def test_is_float_true(self) -> None:
        """Test is_float returns True for float."""
        assert is_float(3.14)

    def test_is_bool_true(self) -> None:
        """Test is_bool returns True for bool."""
        assert is_bool(True)

    def test_is_list_true(self) -> None:
        """Test is_list returns True for list."""
        assert is_list([1, 2, 3])

    def test_is_dict_true(self) -> None:
        """Test is_dict returns True for dict."""
        assert is_dict({})


class TestIsEmail:
    """Tests for is_email function."""

    def test_is_email_valid(self) -> None:
        """Test is_email returns True for valid email."""
        assert is_email("test@example.com")

    def test_is_email_invalid(self) -> None:
        """Test is_email returns False for invalid email."""
        assert not is_email("invalid")


class TestIsUrl:
    """Tests for is_url function."""

    def test_is_url_valid(self) -> None:
        """Test is_url returns True for valid URL."""
        assert is_url("http://example.com")

    def test_is_url_invalid(self) -> None:
        """Test is_url returns False for invalid URL."""
        assert not is_url("not a url")


class TestIsPhone:
    """Tests for is_phone function."""

    def test_is_phone_valid(self) -> None:
        """Test is_phone returns True for valid phone."""
        assert is_phone("+1 555-123-4567")

    def test_is_phone_invalid(self) -> None:
        """Test is_phone returns False for invalid phone."""
        assert not is_phone("123")


class TestIsIPv4:
    """Tests for is_ipv4 function."""

    def test_is_ipv4_valid(self) -> None:
        """Test is_ipv4 returns True for valid IP."""
        assert is_ipv4("192.168.1.1")

    def test_is_ipv4_invalid(self) -> None:
        """Test is_ipv4 returns False for invalid IP."""
        assert not is_ipv4("999.999.999.999")


class TestIsPort:
    """Tests for is_port function."""

    def test_is_port_valid(self) -> None:
        """Test is_port returns True for valid port."""
        assert is_port(8080)

    def test_is_port_invalid(self) -> None:
        """Test is_port returns False for invalid port."""
        assert not is_port(70000)


class TestIsPath:
    """Tests for is_path function."""

    def test_is_path_valid(self) -> None:
        """Test is_path returns True for valid path."""
        assert is_path("/path/to/file")

    def test_is_path_invalid(self) -> None:
        """Test is_path returns False for invalid path."""
        assert not is_path("path/with:|invalid")


class TestIsAlphanumeric:
    """Tests for is_alphanumeric function."""

    def test_is_alphanumeric_true(self) -> None:
        """Test is_alphanumeric returns True for alphanumeric."""
        assert is_alphanumeric("abc123")

    def test_is_alphanumeric_false(self) -> None:
        """Test is_alphanumeric returns False for non-alphanumeric."""
        assert not is_alphanumeric("hello world")


class TestIsAlpha:
    """Tests for is_alpha function."""

    def test_is_alpha_true(self) -> None:
        """Test is_alpha returns True for alphabetic."""
        assert is_alpha("hello")

    def test_is_alpha_false(self) -> None:
        """Test is_alpha returns False for non-alphabetic."""
        assert not is_alpha("hello123")


class TestIsNumeric:
    """Tests for is_numeric function."""

    def test_is_numeric_true(self) -> None:
        """Test is_numeric returns True for numeric string."""
        assert is_numeric("12345")

    def test_is_numeric_false(self) -> None:
        """Test is_numeric returns False for non-numeric string."""
        assert not is_numeric("123a")


class TestIsHexColor:
    """Tests for is_hex_color function."""

    def test_is_hex_color_valid(self) -> None:
        """Test is_hex_color returns True for valid hex color."""
        assert is_hex_color("#FF5733")

    def test_is_hex_color_invalid(self) -> None:
        """Test is_hex_color returns False for invalid hex color."""
        assert not is_hex_color("not a color")


class TestIsUsername:
    """Tests for is_username function."""

    def test_is_username_valid(self) -> None:
        """Test is_username returns True for valid username."""
        assert is_username("valid_user123")

    def test_is_username_too_short(self) -> None:
        """Test is_username returns False for short username."""
        assert not is_username("ab")


class TestIsPasswordStrong:
    """Tests for is_password_strong function."""

    def test_is_password_strong_valid(self) -> None:
        """Test is_password_strong returns True for strong password."""
        assert is_password_strong("Password123")

    def test_is_password_strong_too_short(self) -> None:
        """Test is_password_strong returns False for short password."""
        assert not is_password_strong("Pass1")


class TestIsUUID:
    """Tests for is_uuid function."""

    def test_is_uuid_valid(self) -> None:
        """Test is_uuid returns True for valid UUID."""
        assert is_uuid("123e4567-e89b-12d3-a456-426614174000")

    def test_is_uuid_invalid(self) -> None:
        """Test is_uuid returns False for invalid UUID."""
        assert not is_uuid("not a uuid")


class TestInRange:
    """Tests for in_range function."""

    def test_in_range_true(self) -> None:
        """Test in_range returns True when value is in range."""
        assert in_range(5, 0, 10)

    def test_in_range_false(self) -> None:
        """Test in_range returns False when value is out of range."""
        assert not in_range(15, 0, 10)


class TestMinLength:
    """Tests for min_length function."""

    def test_min_length_true(self) -> None:
        """Test min_length returns True when length is sufficient."""
        assert min_length("hello", 3)

    def test_min_length_false(self) -> None:
        """Test min_length returns False when length is insufficient."""
        assert not min_length("hi", 5)


class TestMaxLength:
    """Tests for max_length function."""

    def test_max_length_true(self) -> None:
        """Test max_length returns True when length is within limit."""
        assert max_length("hello", 10)

    def test_max_length_false(self) -> None:
        """Test max_length returns False when length exceeds limit."""
        assert not max_length("hello world", 5)


class TestMatchesPattern:
    """Tests for matches_pattern function."""

    def test_matches_pattern_true(self) -> None:
        """Test matches_pattern returns True when pattern matches."""
        assert matches_pattern("hello", r"^[a-z]+$")

    def test_matches_pattern_false(self) -> None:
        """Test matches_pattern returns False when pattern doesn't match."""
        assert not matches_pattern("hello123", r"^[a-z]+$")


class TestIsOneOf:
    """Tests for is_one_of function."""

    def test_is_one_of_true(self) -> None:
        """Test is_one_of returns True when value is in choices."""
        assert is_one_of("apple", ["apple", "banana"])

    def test_is_one_of_false(self) -> None:
        """Test is_one_of returns False when value is not in choices."""
        assert not is_one_of("cherry", ["apple", "banana"])


class TestValidateType:
    """Tests for validate_type function."""

    def test_validate_type_true(self) -> None:
        """Test validate_type returns True when type matches."""
        assert validate_type("hello", str)

    def test_validate_type_false(self) -> None:
        """Test validate_type returns False when type doesn't match."""
        assert not validate_type(123, str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
