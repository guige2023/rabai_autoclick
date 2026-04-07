"""Tests for process utilities."""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.process_utils import (
    ProcessInfo,
    ProcessManager,
    SystemInfo,
    ResourceMonitor,
    run_command,
    daemonize,
)


class TestProcessInfo:
    """Tests for ProcessInfo."""

    def test_create(self) -> None:
        """Test creating ProcessInfo."""
        info = ProcessInfo(
            pid=123,
            name="test",
            status="running",
            cpu_percent=10.5,
            memory_mb=100.0,
            create_time=1000.0,
            cmdline=["test", "arg"],
        )
        assert info.pid == 123
        assert info.name == "test"
        assert info.status == "running"
        assert info.cpu_percent == 10.5
        assert info.memory_mb == 100.0


class TestProcessManager:
    """Tests for ProcessManager."""

    def test_get_current_pid(self) -> None:
        """Test getting current PID."""
        pid = ProcessManager.get_current_pid()
        assert isinstance(pid, int)
        assert pid > 0

    def test_get_parent_pid(self) -> None:
        """Test getting parent PID."""
        ppid = ProcessManager.get_parent_pid()
        assert isinstance(ppid, int)
        assert ppid > 0

    def test_get_process_info_current(self) -> None:
        """Test getting current process info."""
        info = ProcessManager.get_process_info()
        assert info is not None
        assert info.pid == os.getpid()

    def test_get_process_info_invalid(self) -> None:
        """Test getting info for invalid process."""
        info = ProcessManager.get_process_info(999999)
        assert info is None

    def test_list_processes(self) -> None:
        """Test listing processes."""
        processes = ProcessManager.list_processes()
        assert isinstance(processes, list)
        # At least current process should be there
        pids = [p.pid for p in processes]
        assert os.getpid() in pids

    def test_find_process_by_name(self) -> None:
        """Test finding processes by name."""
        processes = ProcessManager.find_process_by_name("python")
        assert isinstance(processes, list)

    def test_is_process_running_current(self) -> None:
        """Test checking if current process is running."""
        assert ProcessManager.is_process_running(os.getpid()) is True

    def test_is_process_running_invalid(self) -> None:
        """Test checking invalid process."""
        assert ProcessManager.is_process_running(999999) is False


class TestSystemInfo:
    """Tests for SystemInfo."""

    def test_get_platform(self) -> None:
        """Test getting platform."""
        platform = SystemInfo.get_platform()
        assert isinstance(platform, str)
        assert len(platform) > 0

    def test_get_os_version(self) -> None:
        """Test getting OS version."""
        version = SystemInfo.get_os_version()
        assert isinstance(version, str)
        assert len(version) > 0

    def test_get_cpu_count(self) -> None:
        """Test getting CPU count."""
        count = SystemInfo.get_cpu_count()
        assert isinstance(count, int)
        assert count >= 1

    def test_get_memory_info(self) -> None:
        """Test getting memory info."""
        info = SystemInfo.get_memory_info()
        assert isinstance(info, dict)
        assert 'total' in info
        assert 'available' in info
        assert 'used' in info

    def test_get_disk_info(self) -> None:
        """Test getting disk info."""
        info = SystemInfo.get_disk_info("/")
        assert isinstance(info, dict)
        assert 'total' in info
        assert 'used' in info
        assert 'free' in info

    def test_get_boot_time(self) -> None:
        """Test getting boot time."""
        boot_time = SystemInfo.get_boot_time()
        assert isinstance(boot_time, float)
        assert boot_time > 0

    def test_get_uptime(self) -> None:
        """Test getting uptime."""
        uptime = SystemInfo.get_uptime()
        assert isinstance(uptime, float)
        assert uptime >= 0


class TestResourceMonitor:
    """Tests for ResourceMonitor."""

    def test_create(self) -> None:
        """Test creating resource monitor."""
        monitor = ResourceMonitor(interval=0.1)
        assert monitor.interval == 0.1
        assert monitor.get_samples() == []

    def test_start_stop(self) -> None:
        """Test starting and stopping monitor."""
        monitor = ResourceMonitor(interval=0.05)
        monitor.start()
        time.sleep(0.2)
        monitor.stop()
        samples = monitor.get_samples()
        assert len(samples) >= 1

    def test_get_average_empty(self) -> None:
        """Test average with no samples."""
        monitor = ResourceMonitor()
        avg = monitor.get_average()
        assert avg == {}

    def test_clear(self) -> None:
        """Test clearing samples."""
        monitor = ResourceMonitor(interval=0.01)
        monitor.start()
        time.sleep(0.05)
        monitor.clear()
        assert monitor.get_samples() == []


class TestRunCommand:
    """Tests for run_command."""

    def test_run_echo(self) -> None:
        """Test running echo command."""
        code, stdout, stderr = run_command(["echo", "hello"])
        assert code == 0
        assert "hello" in stdout

    def test_run_invalid_command(self) -> None:
        """Test running invalid command."""
        code, stdout, stderr = run_command(["nonexistent_command_xyz"])
        assert code != 0

    def test_run_with_timeout(self) -> None:
        """Test running command with timeout."""
        code, stdout, stderr = run_command(["sleep", "1"], timeout=0.5)
        assert code == -1


class TestDaemonize:
    """Tests for daemonize."""

    def test_daemonize_returns_bool(self) -> None:
        """Test daemonize returns bool."""
        result = daemonize()
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])