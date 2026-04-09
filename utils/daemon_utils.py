"""
Daemon utilities for running automation workflows as background services.

Provides daemon process management, start/stop/restart controls,
PID file management, and signal handling for long-running automation.

Example:
    >>> from daemon_utils import Daemon, DaemonContext
    >>> daemon = Daemon(pidfile="/tmp/mydaemon.pid")
    >>> daemon.start()
"""

from __future__ import annotations

import atexit
import os
import signal
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, List, Optional


# =============================================================================
# Types
# =============================================================================


class DaemonState(Enum):
    """Daemon lifecycle states."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    FAILED = "failed"


@dataclass
class DaemonConfig:
    """Configuration for daemon behavior."""
    pidfile: str = "/tmp/automation.pid"
    logfile: str = "/tmp/automation.log"
    stdin_file: str = "/dev/null"
    stdout_file: str = "/dev/null"
    stderr_file: str = "/tmp/automation.err"
    working_dir: str = "/"
    umask: int = 0o077
    foreground: bool = False


# =============================================================================
# PID File Management
# =============================================================================


class PIDFile:
    """Manages PID file for daemon processes."""

    def __init__(self, path: str):
        self.path = path

    def write(self, pid: int) -> None:
        """Write PID to file."""
        with open(self.path, "w") as f:
            f.write(str(pid))

    def read(self) -> Optional[int]:
        """Read PID from file."""
        try:
            with open(self.path, "r") as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def remove(self) -> None:
        """Remove PID file."""
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass

    def is_running(self) -> bool:
        """Check if process is running."""
        pid = self.read()
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


# =============================================================================
# Daemon Base Class
# =============================================================================


class Daemon:
    """
    Base daemon class for automation workflows.

    Example:
        >>> class MyDaemon(Daemon):
        ...     def run(self):
        ...         while True:
        ...             do_work()
        ...             time.sleep(1)
        >>> daemon = MyDaemon(pidfile="/tmp/mydaemon.pid")
        >>> daemon.start()
    """

    def __init__(self, config: Optional[DaemonConfig] = None):
        self.config = config or DaemonConfig()
        self.pidfile = PIDFile(self.config.pidfile)
        self.state = DaemonState.STOPPED
        self._should_stop = False
        self._signal_handlers: List[Callable] = []

    def start(self) -> bool:
        """
        Start the daemon.

        Returns:
            True if started successfully.
        """
        if self.pidfile.is_running():
            print(f"Daemon already running (PID: {self.pidfile.read()})")
            return False

        if not self.config.foreground:
            # Fork child process
            try:
                pid = os.fork()
                if pid > 0:
                    # Parent waits for child
                    time.sleep(0.5)
                    if self.pidfile.is_running():
                        print(f"Daemon started (PID: {self.pidfile.read()})")
                        return True
                    return False
            except OSError as e:
                print(f"Fork failed: {e}")
                return False

            # Child becomes session leader
            os.setsid()

            # Fork again to prevent daemon from acquiring a terminal
            try:
                pid = os.fork()
                if pid > 0:
                    os._exit(0)
            except OSError:
                os._exit(1)

            # Change working directory
            os.chdir(self.config.working_dir)

            # Set umask
            os.umask(self.config.umask)

            # Redirect file descriptors
            self._redirect_fds()

        # Write PID file
        self.pidfile.write(os.getpid())

        # Register cleanup
        atexit.register(self._cleanup)

        # Setup signal handlers
        self._setup_signals()

        # Start daemon
        self.state = DaemonState.RUNNING
        try:
            self.run()
        except Exception as e:
            self.state = DaemonState.FAILED
            print(f"Daemon failed: {e}")
        finally:
            self._cleanup()
            self.state = DaemonState.STOPPED

        return True

    def stop(self, timeout: float = 10.0) -> bool:
        """
        Stop the daemon.

        Args:
            timeout: Maximum seconds to wait for graceful shutdown.

        Returns:
            True if stopped successfully.
        """
        pid = self.pidfile.read()
        if pid is None:
            print("Daemon not running")
            return False

        self.state = DaemonState.STOPPING
        self._should_stop = True

        try:
            os.kill(pid, signal.SIGTERM)
            # Wait for process to exit
            for _ in range(int(timeout * 10)):
                time.sleep(0.1)
                try:
                    os.kill(pid, 0)
                except OSError:
                    break
            else:
                # Force kill
                os.kill(pid, signal.SIGKILL)

            self.pidfile.remove()
            self.state = DaemonState.STOPPED
            return True

        except OSError as e:
            print(f"Failed to stop daemon: {e}")
            return False

    def restart(self) -> bool:
        """Restart the daemon."""
        if not self.stop():
            return False
        time.sleep(1)
        return self.start()

    def status(self) -> str:
        """
        Get daemon status.

        Returns:
            Status string.
        """
        if self.pidfile.is_running():
            return f"running (PID: {self.pidfile.read()})"
        return "stopped"

    def run(self) -> None:
        """
        Main daemon loop. Override in subclass.
        """
        while not self._should_stop:
            time.sleep(1)

    def _redirect_fds(self) -> None:
        """Redirect stdin, stdout, stderr to files."""
        sys.stdout.flush()
        sys.stderr.flush()

        devnull = os.open(os.devnull, os.O_RDWR)
        stdin = os.open(self.config.stdin_file, os.O_RDONLY)
        stdout = os.open(self.config.stdout_file, os.O_WRONLY | os.O_CREAT, 0o666)
        stderr = os.open(self.config.stderr_file, os.O_WRONLY | os.O_CREAT, 0o666)

        os.dup2(stdin, sys.stdin.fileno())
        os.dup2(stdout, sys.stdout.fileno())
        os.dup2(stderr, sys.stderr.fileno())

    def _setup_signals(self) -> None:
        """Setup signal handlers."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGHUP, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle incoming signals."""
        if signum == signal.SIGTERM or signum == signal.SIGINT:
            self._should_stop = True
        elif signum == signal.SIGHUP:
            self._handle_hup()

    def _handle_hup(self) -> None:
        """Handle SIGHUP - typically reload config."""
        pass

    def _cleanup(self) -> None:
        """Cleanup on exit."""
        self.pidfile.remove()


# =============================================================================
# Daemon Context Manager
# =============================================================================


class DaemonContext:
    """
    Context manager for daemon operations.

    Example:
        >>> with DaemonContext(pidfile="/tmp/mydaemon.pid") as daemon:
        ...     daemon.run()
    """

    def __init__(self, config: Optional[DaemonConfig] = None):
        self.config = config or DaemonConfig()
        self.daemon = Daemon(config)

    def __enter__(self) -> Daemon:
        return self.daemon

    def __exit__(self, *args: Any) -> None:
        pass


# =============================================================================
# Process Utilities
# =============================================================================


def get_pid_from_file(pidfile: str) -> Optional[int]:
    """Read PID from a pidfile."""
    try:
        with open(pidfile, "r") as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


def is_process_running(pid: int) -> bool:
    """Check if a process is running."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_process(pid: int, timeout: float = 5.0, force: bool = False) -> bool:
    """
    Kill a process gracefully or forcefully.

    Args:
        pid: Process ID.
        timeout: Seconds to wait for graceful shutdown.
        force: Force kill if graceful fails.

    Returns:
        True if process was killed.
    """
    try:
        os.kill(pid, signal.SIGTERM if not force else signal.SIGKILL)

        if not force:
            for _ in range(int(timeout * 10)):
                time.sleep(0.1)
                try:
                    os.kill(pid, 0)
                except OSError:
                    return True
            # Force kill if still running
            os.kill(pid, signal.SIGKILL)

        return True

    except OSError:
        return False


def get_process_name(pid: int) -> Optional[str]:
    """Get process name for a PID."""
    try:
        return open(f"/proc/{pid}/comm", "r").read().strip()
    except (FileNotFoundError, IOError):
        return None


def get_process_cmdline(pid: int) -> Optional[str]:
    """Get command line for a process."""
    try:
        with open(f"/proc/{pid}/cmdline", "r") as f:
            return f.read().replace("\x00", " ").strip()
    except (FileNotFoundError, IOError):
        return None


def list_daemon_processes(pattern: str = "") -> List[dict]:
    """
    List daemon processes matching a pattern.

    Args:
        pattern: Pattern to match in process name/cmdline.

    Returns:
        List of dicts with pid, name, cmdline.
    """
    daemons = []
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue

            try:
                name = get_process_name(int(pid))
                cmdline = get_process_cmdline(int(pid))

                if name and pattern in name:
                    daemons.append({
                        "pid": int(pid),
                        "name": name,
                        "cmdline": cmdline,
                    })
            except (FileNotFoundError, PermissionError):
                pass

    except OSError:
        pass

    return daemons
