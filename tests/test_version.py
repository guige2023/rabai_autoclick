"""Tests for version utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.version import (
    Version,
    parse_version,
    compare_versions,
    VersionConstraint,
    check_version,
)


class TestVersion:
    """Tests for Version."""

    def test_creation(self) -> None:
        """Test creating version."""
        v = Version(1, 2, 3)
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_str(self) -> None:
        """Test string representation."""
        v = Version(1, 2, 3)
        assert str(v) == "1.2.3"

    def test_str_with_prerelease(self) -> None:
        """Test string with prerelease."""
        v = Version(1, 2, 3, "alpha")
        assert str(v) == "1.2.3-alpha"

    def test_equality(self) -> None:
        """Test equality."""
        v1 = Version(1, 2, 3)
        v2 = Version(1, 2, 3)
        assert v1 == v2

    def test_inequality(self) -> None:
        """Test inequality."""
        v1 = Version(1, 2, 3)
        v2 = Version(1, 2, 4)
        assert v1 != v2

    def test_less_than(self) -> None:
        """Test less than."""
        v1 = Version(1, 0, 0)
        v2 = Version(2, 0, 0)
        assert v1 < v2

    def test_prerelease_less_than(self) -> None:
        """Test prerelease version is less than release."""
        v1 = Version(1, 0, 0, "alpha")
        v2 = Version(1, 0, 0)
        assert v1 < v2


class TestParseVersion:
    """Tests for parse_version."""

    def test_valid_version(self) -> None:
        """Test parsing valid version."""
        v = parse_version("1.2.3")
        assert v is not None
        assert v.major == 1
        assert v.minor == 2
        assert v.patch == 3

    def test_with_prerelease(self) -> None:
        """Test parsing with prerelease."""
        v = parse_version("1.2.3-beta")
        assert v is not None
        assert v.prerelease == "beta"

    def test_invalid_version(self) -> None:
        """Test parsing invalid version."""
        assert parse_version("invalid") is None
        assert parse_version("1.2") is None


class TestCompareVersions:
    """Tests for compare_versions."""

    def test_equal(self) -> None:
        """Test comparing equal versions."""
        assert compare_versions("1.0.0", "1.0.0") == 0

    def test_less_than(self) -> None:
        """Test comparing less than."""
        assert compare_versions("1.0.0", "2.0.0") == -1

    def test_greater_than(self) -> None:
        """Test comparing greater than."""
        assert compare_versions("2.0.0", "1.0.0") == 1


class TestVersionConstraint:
    """Tests for VersionConstraint."""

    def test_exact_match(self) -> None:
        """Test exact version match."""
        constraint = VersionConstraint("1.0.0")
        assert constraint.matches("1.0.0")
        assert not constraint.matches("1.0.1")

    def test_greater_than_or_equal(self) -> None:
        """Test >= constraint."""
        constraint = VersionConstraint(">=1.0.0")
        assert constraint.matches("1.0.0")
        assert constraint.matches("2.0.0")
        assert not constraint.matches("0.9.0")

    def test_range(self) -> None:
        """Test version range."""
        constraint = VersionConstraint(">=1.0.0,<2.0.0")
        assert constraint.matches("1.5.0")
        assert not constraint.matches("0.9.0")
        assert not constraint.matches("2.0.0")


class TestCheckVersion:
    """Tests for check_version."""

    def test_valid_version(self) -> None:
        """Test checking valid version."""
        valid, error = check_version("1.0.0", ">=1.0.0")
        assert valid
        assert error is None

    def test_invalid_version(self) -> None:
        """Test checking invalid version."""
        valid, error = check_version("0.9.0", ">=1.0.0")
        assert not valid
        assert error is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])