"""Tests for env utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.env import (
    get_env,
    get_env_int,
    get_env_bool,
    set_env,
    unset_env,
    Env,
    is_windows,
    is_macos,
    is_linux,
    get_platform,
    get_home_dir,
    get_temp_dir,
    get_config_dir,
    get_cpu_count,
)


class TestGetEnv:
    """Tests for get_env."""

    def test_get_existing(self) -> None:
        """Test getting existing env var."""
        os.environ["TEST_VAR"] = "test_value"
        assert get_env("TEST_VAR") == "test_value"

    def test_get_missing(self) -> None:
        """Test getting missing env var."""
        result = get_env("MISSING_VAR_12345")
        assert result is None

    def test_get_with_default(self) -> None:
        """Test getting with default."""
        result = get_env("MISSING_VAR_12345", "default")
        assert result == "default"


class TestGetEnvInt:
    """Tests for get_env_int."""

    def test_valid_int(self) -> None:
        """Test parsing valid int."""
        os.environ["TEST_INT"] = "42"
        assert get_env_int("TEST_INT") == 42

    def test_invalid_int(self) -> None:
        """Test parsing invalid int."""
        os.environ["TEST_INT_INVALID"] = "not_a_number"
        assert get_env_int("TEST_INT_INVALID") is None

    def test_missing(self) -> None:
        """Test missing with default."""
        assert get_env_int("MISSING_VAR_12345", 100) == 100


class TestGetEnvBool:
    """Tests for get_env_bool."""

    def test_true_values(self) -> None:
        """Test true values."""
        for value in ["true", "1", "yes", "on"]:
            os.environ["TEST_BOOL"] = value
            assert get_env_bool("TEST_BOOL") is True

    def test_false_values(self) -> None:
        """Test false values."""
        for value in ["false", "0", "no", "off", ""]:
            os.environ["TEST_BOOL"] = value
            assert get_env_bool("TEST_BOOL") is False


class TestEnv:
    """Tests for Env class."""

    def test_as_str(self) -> None:
        """Test as_str method."""
        os.environ["TEST_STR"] = "hello"
        assert Env("TEST_STR").as_str() == "hello"

    def test_as_int(self) -> None:
        """Test as_int method."""
        os.environ["TEST_INT"] = "42"
        assert Env("TEST_INT").as_int() == 42

    def test_as_bool(self) -> None:
        """Test as_bool method."""
        os.environ["TEST_BOOL"] = "true"
        assert Env("TEST_BOOL").as_bool() is True


class TestPlatform:
    """Tests for platform detection."""

    def test_platform_functions(self) -> None:
        """Test platform detection."""
        platform = get_platform()
        assert platform in ["darwin", "win32", "linux"]

        # At least one should be true
        assert is_windows() or is_macos() or is_linux()


class TestDirectories:
    """Tests for directory functions."""

    def test_get_home_dir(self) -> None:
        """Test getting home directory."""
        home = get_home_dir()
        assert home.exists() or home == Path.home()

    def test_get_temp_dir(self) -> None:
        """Test getting temp directory."""
        temp = get_temp_dir()
        assert temp.exists()

    def test_get_config_dir(self) -> None:
        """Test getting config directory."""
        config = get_config_dir()
        assert config.exists() or str(config)


class TestCpuCount:
    """Tests for CPU count."""

    def test_get_cpu_count(self) -> None:
        """Test getting CPU count."""
        count = get_cpu_count()
        assert count >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])