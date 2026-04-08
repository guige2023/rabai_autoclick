"""Process control utilities for managing applications and system processes.

Provides helpers for launching, monitoring, and terminating
applications, querying process information, and handling
application state during automation.

Example:
    >>> from utils.process_utils import launch_app, is_running, terminate_app
    >>> launch_app('/Applications/Safari.app')
    >>> is_running('Safari')
    >>> terminate_app('Safari')
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, NamedTuple

__all__ = [
    "launch_app",
    "launch_app_by_bundle_id",
    "is_running",
    "get_pid",
    "terminate_app",
    "terminate_pid",
    "wait_for_app",
    "get_process_info",
    "ProcessInfo",
]


class ProcessInfo(NamedTuple):
    """Process information."""

    pid: int
    name: str
    bundle_id: Optional[str]


def launch_app(path: str, wait: bool = False) -> bool:
    """Launch an application by path or name.

    Args:
        path: Path to .app bundle or application name.
        wait: If True, wait for the app to finish launching.

    Returns:
        True if launch succeeded.

    Example:
        >>> launch_app('/Applications/Safari.app')
        >>> launch_app('Safari')
    """
    try:
        if wait:
            subprocess.run(["open", "-W", path], timeout=30, check=True)
        else:
            subprocess.run(["open", path], timeout=10, check=True)
        return True
    except Exception:
        return False


def launch_app_by_bundle_id(bundle_id: str) -> bool:
    """Launch an application by its bundle identifier.

    Args:
        bundle_id: e.g., 'com.apple.Safari'.

    Returns:
        True if launch succeeded.
    """
    try:
        subprocess.run(
            ["open", "-b", bundle_id],
            timeout=10,
            check=True,
        )
        return True
    except Exception:
        return False


def is_running(name: str) -> bool:
    """Check if a process with the given name is running.

    Args:
        name: Process name (e.g., 'Safari', 'python').

    Returns:
        True if the process exists.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-x", name],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def get_pid(name: str) -> Optional[int]:
    """Get the PID of a running process by name.

    Args:
        name: Process name.

    Returns:
        PID if found, or None.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-x", name],
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout:
            return int(result.stdout.strip().split()[0])
    except Exception:
        pass
    return None


def terminate_app(name: str, timeout: float = 5.0) -> bool:
    """Terminate a running application by name.

    Args:
        name: Application name.
        timeout: Seconds to wait for graceful termination.

    Returns:
        True if the app was terminated.
    """
    pid = get_pid(name)
    if pid is None:
        return False
    return terminate_pid(pid, timeout=timeout)


def terminate_pid(pid: int, timeout: float = 5.0) -> bool:
    """Terminate a process by PID.

    Args:
        pid: Process ID.
        timeout: Seconds to wait for termination.

    Returns:
        True if the process was terminated.
    """
    import os
    import signal

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for the process to exit
        for _ in range(int(timeout * 10)):
            try:
                os.kill(pid, 0)
            except OSError:
                return True
            time.sleep(0.1)
        # Force kill if still alive
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        return True
    except OSError:
        return False


def wait_for_app(
    name: str,
    timeout: float = 30.0,
    poll_interval: float = 0.2,
) -> bool:
    """Wait for an application to start running.

    Args:
        name: Application name.
        timeout: Maximum wait time in seconds.
        poll_interval: Time between checks.

    Returns:
        True if the app started within the timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_running(name):
            return True
        time.sleep(poll_interval)
    return False


def get_process_info(name: str) -> Optional[ProcessInfo]:
    """Get detailed information about a running process.

    Args:
        name: Process name.

    Returns:
        ProcessInfo if found, or None.
    """
    pid = get_pid(name)
    if pid is None:
        return None
    return ProcessInfo(pid=pid, name=name, bundle_id=None)


def get_all_app_processes() -> list[ProcessInfo]:
    """Get all running application processes.

    Returns:
        List of ProcessInfo for user-visible applications.
    """
    import sys

    if sys.platform != "darwin":
        return []

    results: list[ProcessInfo] = []
    try:
        script = """
        tell application "System Events"
            set appList to every process whose visible is true
            repeat with appProc in appList
                try
                    set appName to name of appProc
                    set appPID to unix id of appProc
                    copy {appName, appPID} to end of result
                end try
            end repeat
        end tell
        """
        output = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=10,
        )
        for line in output.decode().strip().split("\n"):
            if not line.strip().startswith("{"):
                continue
            parts = [p.strip() for p in line.strip().strip(",{}").split(",")]
            if len(parts) >= 2:
                try:
                    name = parts[0].strip('"')
                    pid = int(parts[1])
                    results.append(ProcessInfo(pid=pid, name=name, bundle_id=None))
                except (ValueError, IndexError):
                    continue
    except Exception:
        pass

    return results
