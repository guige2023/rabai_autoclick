"""Tests for environment utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.env_utils import (
    get_env,
    set_env,
    unset_env,
    get_env_int,
    get_env_bool,
    get_env_list,
    get_all_env,
    has_env,
    is_linux,
    is_macos,
    is_windows,
    get_platform,
    get_python_version,
    get_platform_info,
    get_cpu_count,
    get_home_dir,
    get_temp_dir,
    get_current_dir,
    is_64bit,
    get_hostname,
    get_username,
    get_env_with_prefix,
    set_env_from_dict,
)


class TestGetEnv:
    """Tests for get_env function."""

    def test_get_env_returns_value(self) -> None:
        """Test get_env returns environment variable."""
        os.environ["TEST_VAR"] = "test_value"
        assert get_env("TEST_VAR") == "test_value"
        unset_env("TEST_VAR")

    def test_get_env_returns_default(self) -> None:
        """Test get_env returns default for missing variable."""
        assert get_env("NONEXISTENT_VAR", "default") == "default"


class TestSetEnv:
    """Tests for set_env function."""

    def test_set_env_sets_value(self) -> None:
        """Test set_env sets environment variable."""
        set_env("TEST_VAR", "test_value")
        assert os.environ.get("TEST_VAR") == "test_value"
        unset_env("TEST_VAR")


class TestUnsetEnv:
    """Tests for unset_env function."""

    def test_unset_env_removes_variable(self) -> None:
        """Test unset_env removes environment variable."""
        os.environ["TEST_VAR"] = "test_value"
        unset_env("TEST_VAR")
        assert "TEST_VAR" not in os.environ


class TestGetEnvInt:
    """Tests for get_env_int function."""

    def test_get_env_int_returns_int(self) -> None:
        """Test get_env_int returns integer."""
        os.environ["TEST_INT"] = "42"
        assert get_env_int("TEST_INT") == 42
        unset_env("TEST_INT")

    def test_get_env_int_returns_default(self) -> None:
        """Test get_env_int returns default for invalid value."""
        os.environ["TEST_INT"] = "not_a_number"
        assert get_env_int("TEST_INT", 0) == 0
        unset_env("TEST_INT")


class TestGetEnvBool:
    """Tests for get_env_bool function."""

    def test_get_env_bool_true(self) -> None:
        """Test get_env_bool returns True for true values."""
        os.environ["TEST_BOOL"] = "true"
        assert get_env_bool("TEST_BOOL") is True
        unset_env("TEST_BOOL")

    def test_get_env_bool_false(self) -> None:
        """Test get_env_bool returns False for false values."""
        os.environ["TEST_BOOL"] = "false"
        assert get_env_bool("TEST_BOOL") is False
        unset_env("TEST_BOOL")


class TestGetEnvList:
    """Tests for get_env_list function."""

    def test_get_env_list_parsed(self) -> None:
        """Test get_env_list parses comma-separated values."""
        os.environ["TEST_LIST"] = "a, b, c"
        assert get_env_list("TEST_LIST") == ["a", "b", "c"]
        unset_env("TEST_LIST")


class TestGetAllEnv:
    """Tests for get_all_env function."""

    def test_get_all_env_returns_dict(self) -> None:
        """Test get_all_env returns dictionary."""
        result = get_all_env()
        assert isinstance(result, dict)


class TestHasEnv:
    """Tests for has_env function."""

    def test_has_env_true(self) -> None:
        """Test has_env returns True for existing variable."""
        os.environ["TEST_VAR"] = "test_value"
        assert has_env("TEST_VAR") is True
        unset_env("TEST_VAR")

    def test_has_env_false(self) -> None:
        """Test has_env returns False for missing variable."""
        assert has_env("NONEXISTENT_VAR") is False


class TestPlatformDetection:
    """Tests for platform detection functions."""

    def test_is_linux(self) -> None:
        """Test is_linux returns bool."""
        result = is_linux()
        assert isinstance(result, bool)

    def test_is_macos(self) -> None:
        """Test is_macos returns bool."""
        result = is_macos()
        assert isinstance(result, bool)

    def test_is_windows(self) -> None:
        """Test is_windows returns bool."""
        result = is_windows()
        assert isinstance(result, bool)


class TestGetPlatform:
    """Tests for get_platform function."""

    def test_get_platform_returns_string(self) -> None:
        """Test get_platform returns platform string."""
        result = get_platform()
        assert result in ("linux", "darwin", "windows")


class TestGetPythonVersion:
    """Tests for get_python_version function."""

    def test_get_python_version_returns_string(self) -> None:
        """Test get_python_version returns version string."""
        result = get_python_version()
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetPlatformInfo:
    """Tests for get_platform_info function."""

    def test_get_platform_info_returns_dict(self) -> None:
        """Test get_platform_info returns dictionary."""
        result = get_platform_info()
        assert isinstance(result, dict)
        assert "system" in result
        assert "python_version" in result


class TestGetCpuCount:
    """Tests for get_cpu_count function."""

    def test_get_cpu_count_returns_int(self) -> None:
        """Test get_cpu_count returns integer."""
        result = get_cpu_count()
        assert isinstance(result, int)
        assert result >= 1


class TestGetHomeDir:
    """Tests for get_home_dir function."""

    def test_get_home_dir_returns_string(self) -> None:
        """Test get_home_dir returns path string."""
        result = get_home_dir()
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetTempDir:
    """Tests for get_temp_dir function."""

    def test_get_temp_dir_returns_string(self) -> None:
        """Test get_temp_dir returns path string."""
        result = get_temp_dir()
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetCurrentDir:
    """Tests for get_current_dir function."""

    def test_get_current_dir_returns_string(self) -> None:
        """Test get_current_dir returns path string."""
        result = get_current_dir()
        assert isinstance(result, str)
        assert len(result) > 0


class TestIs64bit:
    """Tests for is_64bit function."""

    def test_is_64bit_returns_bool(self) -> None:
        """Test is_64bit returns bool."""
        result = is_64bit()
        assert isinstance(result, bool)


class TestGetHostname:
    """Tests for get_hostname function."""

    def test_get_hostname_returns_string(self) -> None:
        """Test get_hostname returns hostname."""
        result = get_hostname()
        assert isinstance(result, str)
        assert len(result) > 0


class TestGetUsername:
    """Tests for get_username function."""

    def test_get_username_returns_string(self) -> None:
        """Test get_username returns username."""
        result = get_username()
        assert isinstance(result, str)


class TestGetEnvWithPrefix:
    """Tests for get_env_with_prefix function."""

    def test_get_env_with_prefix(self) -> None:
        """Test getting environment variables with prefix."""
        os.environ["PREFIX_VAR1"] = "value1"
        os.environ["PREFIX_VAR2"] = "value2"
        os.environ["OTHER_VAR"] = "value3"
        result = get_env_with_prefix("PREFIX_")
        assert "PREFIX_VAR1" in result
        assert "PREFIX_VAR2" in result
        assert "OTHER_VAR" not in result
        unset_env("PREFIX_VAR1")
        unset_env("PREFIX_VAR2")
        unset_env("OTHER_VAR")


class TestSetEnvFromDict:
    """Tests for set_env_from_dict function."""

    def test_set_env_from_dict(self) -> None:
        """Test setting multiple environment variables."""
        test_dict = {"TEST_ENV_1": "value1", "TEST_ENV_2": "value2"}
        set_env_from_dict(test_dict)
        assert os.environ.get("TEST_ENV_1") == "value1"
        assert os.environ.get("TEST_ENV_2") == "value2"
        unset_env("TEST_ENV_1")
        unset_env("TEST_ENV_2")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
