"""Process utilities for RabAI AutoClick.

Provides:
- Process management
- System information
- Resource monitoring
"""

import io
import os
import sys
import time
import signal
import threading
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class ProcessInfo:
    """Information about a process."""
    pid: int
    name: str
    status: str
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    create_time: float = 0.0
    cmdline: List[str] = field(default_factory=list)


class ProcessManager:
    """Manage system processes.

    Provides process listing, monitoring, and control.
    """

    @staticmethod
    def get_current_pid() -> int:
        """Get current process ID."""
        return os.getpid()

    @staticmethod
    def get_parent_pid() -> int:
        """Get parent process ID."""
        return os.getppid()

    @staticmethod
    def get_process_info(pid: Optional[int] = None) -> Optional[ProcessInfo]:
        """Get information about a process.

        Args:
            pid: Process ID (defaults to current).

        Returns:
            ProcessInfo or None if process not found.
        """
        try:
            import psutil
            proc = psutil.Process(pid) if pid else psutil.Process()
            return ProcessInfo(
                pid=proc.pid,
                name=proc.name(),
                status=proc.status(),
                cpu_percent=proc.cpu_percent(interval=0.1),
                memory_mb=proc.memory_info().rss / 1024 / 1024,
                create_time=proc.create_time(),
                cmdline=proc.cmdline(),
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None

    @staticmethod
    def list_processes() -> List[ProcessInfo]:
        """List all running processes.

        Returns:
            List of ProcessInfo objects.
        """
        try:
            import psutil
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'status', 'create_time', 'cmdline']):
                try:
                    pinfo = proc.info
                    processes.append(ProcessInfo(
                        pid=pinfo['pid'],
                        name=pinfo['name'] or 'unknown',
                        status=pinfo['status'] or 'unknown',
                        create_time=pinfo['create_time'] or 0,
                        cmdline=pinfo['cmdline'] or [],
                    ))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            return processes
        except ImportError:
            return []

    @staticmethod
    def find_process_by_name(name: str) -> List[ProcessInfo]:
        """Find processes by name.

        Args:
            name: Process name to search for.

        Returns:
            List of matching ProcessInfo objects.
        """
        name_lower = name.lower()
        return [
            p for p in ProcessManager.list_processes()
            if name_lower in p.name.lower()
        ]

    @staticmethod
    def kill_process(pid: int, force: bool = False) -> bool:
        """Kill a process.

        Args:
            pid: Process ID to kill.
            force: If True, use SIGKILL.

        Returns:
            True if process was killed.
        """
        try:
            sig = signal.SIGKILL if force else signal.SIGTERM
            os.kill(pid, sig)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    @staticmethod
    def is_process_running(pid: int) -> bool:
        """Check if a process is running.

        Args:
            pid: Process ID to check.

        Returns:
            True if process is running.
        """
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


class SystemInfo:
    """System information provider."""

    @staticmethod
    def get_platform() -> str:
        """Get platform name."""
        return sys.platform

    @staticmethod
    def get_os_version() -> str:
        """Get OS version string."""
        import platform
        return platform.platform()

    @staticmethod
    def get_cpu_count() -> int:
        """Get number of CPU cores."""
        return os.cpu_count() or 1

    @staticmethod
    def get_memory_info() -> Dict[str, int]:
        """Get system memory information.

        Returns:
            Dict with total, available, used (in bytes).
        """
        try:
            import psutil
            mem = psutil.virtual_memory()
            return {
                'total': mem.total,
                'available': mem.available,
                'used': mem.used,
                'percent': mem.percent,
            }
        except ImportError:
            return {'total': 0, 'available': 0, 'used': 0, 'percent': 0}

    @staticmethod
    def get_disk_info(path: str = "/") -> Dict[str, int]:
        """Get disk usage information.

        Args:
            path: Path to check disk usage for.

        Returns:
            Dict with total, used, free (in bytes).
        """
        try:
            import psutil
            disk = psutil.disk_usage(path)
            return {
                'total': disk.total,
                'used': disk.used,
                'free': disk.free,
                'percent': disk.percent,
            }
        except ImportError:
            return {'total': 0, 'used': 0, 'free': 0, 'percent': 0}

    @staticmethod
    def get_boot_time() -> float:
        """Get system boot time."""
        try:
            import psutil
            return psutil.boot_time()
        except ImportError:
            return 0

    @staticmethod
    def get_uptime() -> float:
        """Get system uptime in seconds."""
        boot_time = SystemInfo.get_boot_time()
        if boot_time == 0:
            return 0
        return time.time() - boot_time


class ResourceMonitor:
    """Monitor system resources over time."""

    def __init__(self, interval: float = 1.0) -> None:
        """Initialize resource monitor.

        Args:
            interval: Sampling interval in seconds.
        """
        self.interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._samples: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)

    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            sample = {
                'timestamp': time.time(),
                'cpu_percent': self._get_cpu_percent(),
                'memory': SystemInfo.get_memory_info(),
                'disk': SystemInfo.get_disk_info(),
            }
            with self._lock:
                self._samples.append(sample)
            time.sleep(self.interval)

    def _get_cpu_percent(self) -> float:
        """Get current CPU usage percent."""
        try:
            import psutil
            return psutil.cpu_percent(interval=0.1)
        except ImportError:
            return 0.0

    def get_samples(self) -> List[Dict[str, Any]]:
        """Get all collected samples."""
        with self._lock:
            return self._samples.copy()

    def get_average(self) -> Dict[str, Any]:
        """Get average values across all samples."""
        with self._lock:
            if not self._samples:
                return {}
            samples = self._samples

        return {
            'cpu_percent': sum(s['cpu_percent'] for s in samples) / len(samples),
            'memory_percent': sum(s['memory']['percent'] for s in samples) / len(samples),
            'disk_percent': sum(s['disk']['percent'] for s in samples) / len(samples),
        }

    def clear(self) -> None:
        """Clear all samples."""
        with self._lock:
            self._samples.clear()


def run_command(
    cmd: List[str],
    timeout: Optional[float] = None,
    capture_output: bool = True,
) -> Tuple[int, str, str]:
    """Run a shell command.

    Args:
        cmd: Command and arguments as list.
        timeout: Optional timeout in seconds.
        capture_output: If True, capture stdout and stderr.

    Returns:
        Tuple of (return_code, stdout, stderr).
    """
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            timeout=timeout,
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def daemonize() -> bool:
    """Daemonize the current process (Unix only).

    Returns:
        True if successfully daemonized, False otherwise.
    """
    if sys.platform == 'win32':
        return False

    # In test environments, stdin may be redirected and have no fileno
    try:
        test_stdin_fileno = sys.stdin.fileno()
    except (io.UnsupportedOperation, AttributeError):
        return False

    try:
        # First fork
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError:
        return False

    # Decouple from parent environment
    os.chdir("/")
    os.setsid()
    os.umask(0)

    # Second fork
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError:
        return False

    # Redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    with open('/dev/null', 'r') as devnull:
        os.dup2(devnull.fileno(), sys.stdin.fileno())
    with open('/dev/null', 'a+') as devnull:
        os.dup2(devnull.fileno(), sys.stdout.fileno())
        os.dup2(devnull.fileno(), sys.stderr.fileno())

    return True