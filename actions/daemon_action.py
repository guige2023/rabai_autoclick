"""daemon_action module for rabai_autoclick.

Provides daemon process utilities: daemonization, process management,
PID file handling, signal handling for daemons, and daemon status.
"""

from __future__ import annotations

import os
import sys
import signal
import atexit
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

__all__ = [
    "Daemon",
    "DaemonContext",
    "PIDFile",
    "is_daemon_running",
    "kill_daemon",
    "reload_daemon",
    "start_daemon",
    "stop_daemon",
    "DaemonError",
    "StdinRedirect",
    "StdoutRedirect",
]


class DaemonError(Exception):
    """Raised when daemon operations fail."""
    pass


@dataclass
class StdinRedirect:
    """Redirect stdin from /dev/null."""
    path: str = "/dev/null"


@dataclass
class StdoutRedirect:
    """Redirect stdout to file or /dev/null."""
    path: str = "/dev/null"
    append: bool = False


class PIDFile:
    """PID file manager for daemon processes."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._pid: Optional[int] = None

    def write(self, pid: int) -> None:
        """Write PID to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(str(pid))
        self._pid = pid

    def read(self) -> Optional[int]:
        """Read PID from file."""
        if not self.path.exists():
            return None
        try:
            return int(self.path.read_text().strip())
        except (ValueError, IOError):
            return None

    def remove(self) -> bool:
        """Remove PID file."""
        try:
            if self.path.exists():
                self.path.unlink()
            return True
        except IOError:
            return False

    def is_stale(self) -> bool:
        """Check if PID file references a dead process."""
        pid = self.read()
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return False
        except OSError:
            return True


class DaemonContext:
    """Context for daemonization using start/stop/reload pattern."""

    def __init__(
        self,
        pidfile: Optional[str] = None,
        stdin: Optional[str] = "/dev/null",
        stdout: Optional[str] = "/dev/null",
        stderr: Optional[str] = "/dev/null",
        working_directory: str = "/",
        umask: int = 0,
        detach_process: bool = True,
    ) -> None:
        self.pidfile = pidfile
        self.stdin = stdin or "/dev/null"
        self.stdout = stdout or "/dev/null"
        self.stderr = stderr or "/dev/null"
        self.working_directory = working_directory
        self.umask = umask
        self.detach_process = detach_process
        self._signal_handlers: dict = {}
        self._child_pid: Optional[int] = None

    def register_signal(self, sig: int, handler: Callable) -> None:
        """Register signal handler for daemon."""
        self._signal_handlers[sig] = handler
        signal.signal(sig, handler)

    def start(self) -> bool:
        """Start daemon if not already running.

        Returns:
            True if daemon started, False if already running.
        """
        pid_file = PIDFile(self.pidfile) if self.pidfile else None

        if pid_file:
            existing = pid_file.read()
            if existing and not pid_file.is_stale():
                raise DaemonError(f"Daemon already running with PID {existing}")

        if self.detach_process:
            self._fork()
            sys.exit(0)

        os.chdir(self.working_directory)
        os.umask(self.umask)

        if self.stdin:
            sys.stdin = open(self.stdin, "r")
        if self.stdout:
            mode = "a" if hasattr(self, "stdout_append") else "w"
            sys.stdout = open(self.stdout, mode)
        if self.stderr:
            mode = "a" if hasattr(self, "stderr_append") else "w"
            sys.stderr = open(self.stderr, mode)

        if pid_file:
            pid_file.write(os.getpid())

        for sig, handler in self._signal_handlers.items():
            signal.signal(sig, handler)

        atexit.register(self._cleanup)
        return True

    def _fork(self) -> None:
        """Fork the process."""
        try:
            pid = os.fork()
            if pid > 0:
                self._child_pid = pid
        except OSError as e:
            raise DaemonError(f"Fork failed: {e}")

    def _cleanup(self) -> None:
        """Cleanup on exit."""
        if self.pidfile:
            pid_file = PIDFile(self.pidfile)
            if pid_file.read() == os.getpid():
                pid_file.remove()

    def stop(self, timeout: float = 5.0) -> bool:
        """Stop daemon process.

        Returns:
            True if stopped successfully.
        """
        if not self.pidfile:
            return False
        pid_file = PIDFile(self.pidfile)
        pid = pid_file.read()
        if pid is None:
            return True
        try:
            os.kill(pid, signal.SIGTERM)
            import time
            start = time.time()
            while time.time() - start < timeout:
                os.kill(pid, 0)
                time.sleep(0.1)
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
        pid_file.remove()
        return True

    def reload(self) -> bool:
        """Send SIGHUP to daemon to trigger reload."""
        if not self.pidfile:
            return False
        pid_file = PIDFile(self.pidfile)
        pid = pid_file.read()
        if pid is None:
            return False
        try:
            os.kill(pid, signal.SIGHUP)
            return True
        except OSError:
            return False


class Daemon:
    """Daemon process manager with easy start/stop/restart."""

    def __init__(
        self,
        name: str,
        pidfile: Optional[str] = None,
        stdin: str = "/dev/null",
        stdout: str = "/var/log/{name}.log",
        stderr: str = "/var/log/{name}.err",
        working_directory: str = "/",
        umask: int = 0o022,
    ) -> None:
        self.name = name
        self.pidfile = pidfile or f"/var/run/{name}.pid"
        self.stdin = stdin
        self.stdout = stdout.format(name=name)
        self.stderr = stderr.format(name=name)
        self.working_directory = working_directory
        self.umask = umask
        self._mainloop: Optional[Callable] = None
        self._reload_flag = False

    def set_mainloop(self, loop: Callable[[], None]) -> None:
        """Set the main loop function."""
        self._mainloop = loop

    def run(self) -> None:
        """Run the daemon main loop."""
        signal.signal(signal.SIGHUP, self._handle_sighup)
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        if self._mainloop:
            self._mainloop()
        else:
            import time
            while not self._reload_flag:
                time.sleep(1)

    def _handle_sighup(self, sig: int, frame: Any) -> None:
        """Handle SIGHUP for reload."""
        self._reload_flag = True

    def _handle_sigterm(self, sig: int, frame: Any) -> None:
        """Handle SIGTERM/SIGINT for shutdown."""
        sys.exit(0)

    def start(self, background: bool = True) -> bool:
        """Start the daemon.

        Args:
            background: If True, fork to background.

        Returns:
            True if started successfully.
        """
        ctx = DaemonContext(
            pidfile=self.pidfile,
            stdin=self.stdin,
            stdout=self.stdout,
            stderr=self.stderr,
            working_directory=self.working_directory,
            umask=self.umask,
            detach_process=background,
        )
        return ctx.start()

    def stop(self) -> bool:
        """Stop the daemon."""
        ctx = DaemonContext(pidfile=self.pidfile)
        return ctx.stop()

    def restart(self) -> bool:
        """Restart the daemon."""
        ctx = DaemonContext(pidfile=self.pidfile)
        ctx.stop()
        return ctx.start()

    def reload(self) -> bool:
        """Reload daemon configuration."""
        ctx = DaemonContext(pidfile=self.pidfile)
        return ctx.reload()

    def status(self) -> Optional[str]:
        """Get daemon status."""
        pid_file = PIDFile(self.pidfile)
        pid = pid_file.read()
        if pid is None:
            return "stopped"
        if pid_file.is_stale():
            return "stale"
        return f"running (PID {pid})"


def is_daemon_running(pidfile: str) -> bool:
    """Check if daemon is running.

    Returns:
        True if daemon process is alive.
    """
    pf = PIDFile(pidfile)
    pid = pf.read()
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def kill_daemon(pidfile: str, sig: int = signal.SIGTERM, timeout: float = 5.0) -> bool:
    """Send signal to daemon and optionally wait.

    Args:
        pidfile: Path to PID file.
        sig: Signal number to send.
        timeout: Seconds to wait for process to die.

    Returns:
        True if signal was sent.
    """
    pf = PIDFile(pidfile)
    pid = pf.read()
    if pid is None:
        return False
    try:
        os.kill(pid, sig)
        if sig == signal.SIGTERM:
            import time
            start = time.time()
            while time.time() - start < timeout:
                try:
                    os.kill(pid, 0)
                    time.sleep(0.1)
                except OSError:
                    pf.remove()
                    return True
        pf.remove()
        return True
    except OSError:
        pf.remove()
        return False


def reload_daemon(pidfile: str) -> bool:
    """Send SIGHUP to daemon for config reload."""
    return kill_daemon(pidfile, signal.SIGHUP, timeout=0)


def start_daemon(
    mainloop: Callable,
    pidfile: str,
    stdin: str = "/dev/null",
    stdout: str = "/dev/null",
    stderr: str = "/dev/null",
) -> None:
    """Simple daemon starter.

    Args:
        mainloop: Function to run in daemon main loop.
        pidfile: Path for PID file.
        stdin: stdin redirect.
        stdout: stdout redirect.
        stderr: stderr redirect.
    """
    daemon = Daemon(
        name="daemon",
        pidfile=pidfile,
        stdin=stdin,
        stdout=stdout,
        stderr=stderr,
    )
    daemon.set_mainloop(mainloop)
    daemon.start()


def stop_daemon(pidfile: str) -> bool:
    """Simple daemon stopper."""
    return kill_daemon(pidfile)
