"""Tests for OS utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.os_utils import (
    get_os,
    is_linux,
    is_macos,
    is_windows,
    get_hostname,
    get_username,
    get_home_dir,
    get_cwd,
    get_temp_dir,
    get_env,
    set_env,
    unset_env,
    get_all_env,
    get_pid,
    get_ppid,
    is_admin,
    get_cpu_count,
    get_memory_info,
    get_disk_info,
    run_command,
    run_command_async,
    kill_process,
    list_processes,
    get_process_name,
    file_exists,
    dir_exists,
    path_exists,
    make_dir,
    remove_file,
    remove_dir,
    copy_file,
    move_file,
    get_file_size,
    get_file_modified_time,
    list_dir,
    get_absolute_path,
    join_paths,
    split_path,
    get_extension,
    get_basename,
    get_dirname,
    expand_path,
)


class TestOSDetection:
    """Tests for OS detection functions."""

    def test_get_os(self) -> None:
        """Test getting OS name."""
        os_name = get_os()
        assert os_name in ("linux", "darwin", "windows")

    def test_is_linux(self) -> None:
        """Test Linux detection."""
        result = is_linux()
        assert isinstance(result, bool)

    def test_is_macos(self) -> None:
        """Test macOS detection."""
        result = is_macos()
        assert isinstance(result, bool)

    def test_is_windows(self) -> None:
        """Test Windows detection."""
        result = is_windows()
        assert isinstance(result, bool)


class TestSystemInfo:
    """Tests for system info functions."""

    def test_get_hostname(self) -> None:
        """Test getting hostname."""
        result = get_hostname()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_get_username(self) -> None:
        """Test getting username."""
        result = get_username()
        assert isinstance(result, str)
        assert len(result) > 0


class TestPaths:
    """Tests for path functions."""

    def test_get_home_dir(self) -> None:
        """Test getting home directory."""
        result = get_home_dir()
        assert isinstance(result, str)
        assert os.path.isdir(result)

    def test_get_cwd(self) -> None:
        """Test getting current directory."""
        result = get_cwd()
        assert isinstance(result, str)
        assert os.path.isdir(result)

    def test_get_temp_dir(self) -> None:
        """Test getting temp directory."""
        result = get_temp_dir()
        assert isinstance(result, str)
        assert os.path.isdir(result)


class TestEnvironment:
    """Tests for environment functions."""

    def test_get_env(self) -> None:
        """Test getting environment variable."""
        result = get_env("PATH")
        assert result is not None

    def test_get_env_default(self) -> None:
        """Test getting with default."""
        result = get_env("NONEXISTENT_VAR_12345", "default")
        assert result == "default"

    def test_set_env(self) -> None:
        """Test setting environment variable."""
        test_key = "TEST_VAR_12345"
        set_env(test_key, "test_value")
        assert get_env(test_key) == "test_value"
        unset_env(test_key)

    def test_unset_env(self) -> None:
        """Test unsetting environment variable."""
        test_key = "TEST_VAR_12345"
        set_env(test_key, "test_value")
        unset_env(test_key)
        assert get_env(test_key) is None

    def test_get_all_env(self) -> None:
        """Test getting all environment."""
        result = get_all_env()
        assert isinstance(result, dict)
        assert "PATH" in result


class TestProcess:
    """Tests for process functions."""

    def test_get_pid(self) -> None:
        """Test getting PID."""
        result = get_pid()
        assert isinstance(result, int)
        assert result > 0

    def test_get_ppid(self) -> None:
        """Test getting parent PID."""
        result = get_ppid()
        assert isinstance(result, int)
        assert result > 0

    def test_is_admin(self) -> None:
        """Test admin check."""
        result = is_admin()
        assert isinstance(result, bool)

    def test_get_cpu_count(self) -> None:
        """Test getting CPU count."""
        result = get_cpu_count()
        assert isinstance(result, int)
        assert result >= 1


class TestRunCommand:
    """Tests for command execution functions."""

    def test_run_command_echo(self) -> None:
        """Test running echo command."""
        if is_windows():
            code, stdout, stderr = run_command("echo hello")
        else:
            code, stdout, stderr = run_command("echo hello")
        assert code == 0
        assert "hello" in stdout.lower()

    def test_run_command_invalid(self) -> None:
        """Test running invalid command."""
        code, stdout, stderr = run_command("nonexistent_command_12345")
        assert code != 0

    def test_run_command_with_cwd(self) -> None:
        """Test running command with cwd."""
        code, stdout, stderr = run_command("pwd", shell=False, cwd="/tmp")
        assert code == 0


class TestProcessManagement:
    """Tests for process management functions."""

    def test_list_processes(self) -> None:
        """Test listing processes."""
        result = list_processes()
        assert isinstance(result, list)

    def test_get_process_name(self) -> None:
        """Test getting process name."""
        result = get_process_name(os.getpid())
        assert result is not None


class TestFileOperations:
    """Tests for file operation functions."""

    def test_file_exists(self) -> None:
        """Test file exists check."""
        assert file_exists(__file__)
        assert not file_exists("/nonexistent/file_12345.txt")

    def test_dir_exists(self) -> None:
        """Test directory exists check."""
        assert dir_exists("/tmp")
        assert not dir_exists("/nonexistent/dir_12345")

    def test_path_exists(self) -> None:
        """Test path exists check."""
        assert path_exists("/tmp")
        assert path_exists(__file__)
        assert not path_exists("/nonexistent/path_12345")

    def test_get_file_size(self) -> None:
        """Test getting file size."""
        result = get_file_size(__file__)
        assert result is not None
        assert result > 0

    def test_get_file_modified_time(self) -> None:
        """Test getting modified time."""
        result = get_file_modified_time(__file__)
        assert result is not None
        assert result > 0


class TestDirectoryOperations:
    """Tests for directory operation functions."""

    def test_make_dir(self) -> None:
        """Test making directory."""
        test_dir = os.path.join("/tmp", "test_dir_12345")
        result = make_dir(test_dir)
        assert result is True
        assert dir_exists(test_dir)
        remove_dir(test_dir)

    def test_remove_file(self) -> None:
        """Test removing file."""
        test_file = "/tmp/test_file_12345.txt"
        with open(test_file, "w") as f:
            f.write("test")
        result = remove_file(test_file)
        assert result is True
        assert not file_exists(test_file)

    def test_remove_dir(self) -> None:
        """Test removing directory."""
        test_dir = "/tmp/test_remove_dir_12345"
        make_dir(test_dir)
        result = remove_dir(test_dir)
        assert result is True


class TestPathManipulation:
    """Tests for path manipulation functions."""

    def test_get_absolute_path(self) -> None:
        """Test getting absolute path."""
        result = get_absolute_path(".")
        assert os.path.isabs(result)

    def test_join_paths(self) -> None:
        """Test joining paths."""
        result = join_paths("a", "b", "c")
        assert "a" in result
        assert "b" in result
        assert "c" in result

    def test_split_path(self) -> None:
        """Test splitting path."""
        dirname, basename = split_path(__file__)
        assert len(dirname) > 0
        assert len(basename) > 0

    def test_get_extension(self) -> None:
        """Test getting extension."""
        result = get_extension(__file__)
        assert result == ".py"

    def test_get_basename(self) -> None:
        """Test getting basename."""
        result = get_basename(__file__)
        assert "test_os_utils" in result

    def test_get_dirname(self) -> None:
        """Test getting dirname."""
        result = get_dirname(__file__)
        assert len(result) > 0

    def test_expand_path(self) -> None:
        """Test expanding path."""
        result = expand_path("~")
        assert result != "~"
        assert "~" not in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])