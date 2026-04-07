"""PID utilities for RabAI AutoClick.

Provides:
- Process ID operations
- Process listing and querying
- PID file management
"""

import os
import signal
import sys
from typing import (
    List,
    Optional,
)


def get_pid() -> int:
    """Get current process ID.

    Returns:
        Current PID.
    """
    return os.getpid()


def get_ppid() -> int:
    """Get parent process ID.

    Returns:
        Parent PID.
    """
    return os.getppid()


def is_alive(pid: int) -> bool:
    """Check if a process is alive.

    Args:
        pid: Process ID.

    Returns:
        True if process is running.
    """
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_process(pid: int, sig: int = signal.SIGTERM) -> None:
    """Send a signal to a process.

    Args:
        pid: Process ID.
        sig: Signal number (default: SIGTERM).
    """
    os.kill(pid, sig)


def kill_process_tree(
    pid: int,
    sig: int = signal.SIGTERM,
    including_parent: bool = True,
) -> None:
    """Kill a process and all its children.

    Args:
        pid: Root process ID.
        sig: Signal to send.
        including_parent: If True, also kill the root process.
    """
    import psutil
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        for child in children:
            try:
                child.send_signal(sig)
            except psutil.NoSuchProcess:
                pass

        if including_parent:
            try:
                parent.send_signal(sig)
            except psutil.NoSuchProcess:
                pass
    except psutil.NoSuchProcess:
        pass


def get_process_name(pid: Optional[int] = None) -> str:
    """Get process name.

    Args:
        pid: Process ID (default: current process).

    Returns:
        Process name.
    """
    if pid is None:
        pid = get_pid()

    try:
        import psutil
        return psutil.Process(pid).name()
    except Exception:
        return f"pid_{pid}"


def get_process_cmdline(pid: int) -> List[str]:
    """Get process command line.

    Args:
        pid: Process ID.

    Returns:
        List of command line arguments.
    """
    try:
        import psutil
        return psutil.Process(pid).cmdline()
    except Exception:
        return []


def get_process_cpu_percent(pid: Optional[int] = None) -> float:
    """Get process CPU usage.

    Args:
        pid: Process ID (default: current process).

    Returns:
        CPU percent (0-100+).
    """
    try:
        import psutil
        p = psutil.Process(pid) if pid else psutil.Process()
        return p.cpu_percent(interval=0.1)
    except Exception:
        return 0.0


def get_process_memory_percent(pid: Optional[int] = None) -> float:
    """Get process memory usage.

    Args:
        pid: Process ID (default: current process).

    Returns:
        Memory percent of total system.
    """
    try:
        import psutil
        p = psutil.Process(pid) if pid else psutil.Process()
        return p.memory_percent()
    except Exception:
        return 0.0


def get_process_memory_info(pid: Optional[int] = None) -> dict:
    """Get detailed memory info for a process.

    Args:
        pid: Process ID (default: current process).

    Returns:
        Dict with rss, vms, etc. in bytes.
    """
    try:
        import psutil
        p = psutil.Process(pid) if pid else psutil.Process()
        mem = p.memory_info()
        return {
            "rss": mem.rss,
            "vms": mem.vms,
            "rss_mb": mem.rss / (1024 * 1024),
            "vms_mb": mem.vms / (1024 * 1024),
        }
    except Exception:
        return {}


def get_children_pids(pid: int) -> List[int]:
    """Get child process IDs.

    Args:
        pid: Parent process ID.

    Returns:
        List of child PIDs.
    """
    try:
        import psutil
        return [child.pid for child in psutil.Process(pid).children()]
    except Exception:
        return []


def list_pids() -> List[int]:
    """List all process IDs on the system.

    Returns:
        List of PIDs.
    """
    try:
        import psutil
        return [p.pid for p in psutil.process_iter()]
    except Exception:
        return []


def find_processes_by_name(name: str) -> List[int]:
    """Find processes by name.

    Args:
        name: Process name to search for.

    Returns:
        List of matching PIDs.
    """
    try:
        import psutil
        return [p.pid for p in psutil.process_iter(["pid", "name"]) if name.lower() in p.info["name"].lower()]
    except Exception:
        return []


class PidFile:
    """PID file manager for daemon processes."""

    def __init__(
        self,
        filepath: str,
        *,
        auto_cleanup: bool = True,
    ) -> None:
        """Initialize PID file.

        Args:
            filepath: Path to PID file.
            auto_cleanup: If True, remove PID file on exit.
        """
        self._filepath = filepath
        self._auto_cleanup = auto_cleanup

    def write(self, pid: Optional[int] = None) -> None:
        """Write current PID to file.

        Args:
            pid: PID to write (default: current).
        """
        if pid is None:
            pid = get_pid()

        with open(self._filepath, "w") as f:
            f.write(str(pid))

    def read(self) -> Optional[int]:
        """Read PID from file.

        Returns:
            PID or None if file doesn't exist.
        """
        try:
            with open(self._filepath, "r") as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def is_running(self) -> bool:
        """Check if PID file contains a running process.

        Returns:
            True if process is running.
        """
        pid = self.read()
        return pid is not None and is_alive(pid)

    def remove(self) -> None:
        """Remove the PID file."""
        try:
            os.remove(self._filepath)
        except FileNotFoundError:
            pass

    def cleanup(self) -> None:
        """Remove PID file if process is not running."""
        pid = self.read()
        if pid is None or not is_alive(pid):
            self.remove()

    def __enter__(self) -> "PidFile":
        self.write()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._auto_cleanup:
            self.cleanup()

    def __del__(self) -> None:
        if self._auto_cleanup:
            self.cleanup()


def wait_for_pid(
    pid: int,
    timeout: float = 10.0,
    poll_interval: float = 0.1,
) -> bool:
    """Wait for a process to appear.

    Args:
        pid: Process ID to wait for.
        timeout: Maximum time to wait.
        poll_interval: Poll interval.

    Returns:
        True if process appeared.
    """
    import time
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if is_alive(pid):
            return True
        time.sleep(poll_interval)
    return False
