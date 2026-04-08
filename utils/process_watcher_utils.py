"""Process watcher utilities for monitoring running processes.

This module provides utilities for watching process start/stop events
and monitoring process states during automation.
"""

from __future__ import annotations

import platform
import subprocess
import time
from typing import Callable, Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


class ProcessInfo:
    """Information about a running process."""
    
    def __init__(
        self,
        pid: int,
        name: str,
        command: Optional[str] = None,
        cpu_percent: float = 0.0,
        memory_mb: float = 0.0,
    ):
        self.pid = pid
        self.name = name
        self.command = command
        self.cpu_percent = cpu_percent
        self.memory_mb = memory_mb
    
    def __repr__(self) -> str:
        return f"ProcessInfo(pid={self.pid}, name={self.name!r})"


def get_process_by_name(name: str) -> list[ProcessInfo]:
    """Find all processes matching the given name.
    
    Args:
        name: Process name to search for.
    
    Returns:
        List of matching ProcessInfo objects.
    """
    if IS_MACOS or IS_LINUX:
        return _get_process_by_name_unix(name)
    elif IS_WINDOWS:
        return _get_process_by_name_windows(name)
    return []


def _get_process_by_name_unix(name: str) -> list[ProcessInfo]:
    """Find processes on macOS/Linux using ps."""
    processes = []
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            timeout=5
        )
        for line in result.stdout.split("\n"):
            if name.lower() in line.lower():
                parts = line.split()
                if len(parts) >= 11:
                    pid = int(parts[1])
                    cpu = float(parts[2])
                    mem = float(parts[3])
                    command = " ".join(parts[10:])
                    processes.append(ProcessInfo(
                        pid=pid,
                        name=name,
                        command=command,
                        cpu_percent=cpu,
                        memory_mb=mem,
                    ))
    except Exception:
        pass
    return processes


def _get_process_by_name_windows(name: str) -> list[ProcessInfo]:
    """Find processes on Windows using tasklist."""
    processes = []
    try:
        result = subprocess.run(
            ["tasklist", "/fo", "csv", "/nh"],
            capture_output=True,
            text=True,
            timeout=5
        )
        for line in result.stdout.split("\n"):
            if name.lower() in line.lower():
                parts = line.strip().split('","')
                if len(parts) >= 5:
                    processes.append(ProcessInfo(
                        pid=int(parts[1].strip('"')),
                        name=parts[0].strip('"'),
                    ))
    except Exception:
        pass
    return processes


def is_process_running(name: str) -> bool:
    """Check if a process with the given name is running.
    
    Args:
        name: Process name to check.
    
    Returns:
        True if process is running.
    """
    return len(get_process_by_name(name)) > 0


def wait_for_process(
    name: str,
    timeout: float = 30.0,
    poll_interval: float = 0.5,
    state: str = "start",  # 'start' or 'stop'
) -> bool:
    """Wait for a process to start or stop.
    
    Args:
        name: Process name to watch.
        timeout: Maximum time to wait in seconds.
        poll_interval: Time between checks.
        state: 'start' to wait for process to start,
               'stop' to wait for process to stop.
    
    Returns:
        True if the desired state was reached within timeout.
    """
    start_time = time.monotonic()
    
    while time.monotonic() - start_time < timeout:
        running = is_process_running(name)
        
        if state == "start" and running:
            return True
        if state == "stop" and not running:
            return True
        
        time.sleep(poll_interval)
    
    return False


def start_process(command: list[str]) -> Optional[int]:
    """Start a new process.
    
    Args:
        command: Command and arguments as list.
    
    Returns:
        Process ID if successful, None otherwise.
    """
    try:
        proc = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return proc.pid
    except Exception:
        return None


def stop_process(name: str, force: bool = False) -> int:
    """Stop all processes matching the given name.
    
    Args:
        name: Process name to stop.
        force: Use force kill (SIGKILL on Unix, /F on Windows).
    
    Returns:
        Number of processes stopped.
    """
    count = 0
    processes = get_process_by_name(name)
    
    for proc in processes:
        try:
            if IS_MACOS or IS_LINUX:
                import signal
                sig = signal.SIGKILL if force else signal.SIGTERM
                import os
                os.kill(proc.pid, sig)
            elif IS_WINDOWS:
                cmd = ["taskkill", "/pid", str(proc.pid)]
                if force:
                    cmd.append("/F")
                subprocess.run(cmd, capture_output=True)
            count += 1
        except Exception:
            pass
    
    return count


class ProcessWatcher:
    """Watches for process start/stop events."""
    
    def __init__(self):
        self._callbacks_start: list[Callable[[ProcessInfo], None]] = []
        self._callbacks_stop: list[Callable[[str], None]] = []
        self._known_pids: set[int] = set()
        self._running = False
    
    def on_process_start(self, callback: Callable[[ProcessInfo], None]) -> None:
        """Register a callback for process start events."""
        self._callbacks_start.append(callback)
    
    def on_process_stop(self, callback: Callable[[str], None]) -> None:
        """Register a callback for process stop events."""
        self._callbacks_stop.append(callback)
    
    def watch(self, process_name: str, interval: float = 1.0) -> None:
        """Start watching for process events.
        
        This is a blocking call. Use in a thread if needed.
        
        Args:
            process_name: Name of process to watch.
            interval: Polling interval in seconds.
        """
        self._running = True
        
        # Initial scan
        for proc in get_process_by_name(process_name):
            self._known_pids.add(proc.pid)
        
        while self._running:
            current_procs = {p.pid for p in get_process_by_name(process_name)}
            
            # Check for new processes
            for pid in current_procs - self._known_pids:
                proc_info = get_process_by_name(process_name)
                for p in proc_info:
                    if p.pid == pid:
                        for cb in self._callbacks_start:
                            cb(p)
                        break
            
            # Check for stopped processes
            for pid in self._known_pids - current_procs:
                for cb in self._callbacks_stop:
                    cb(process_name)
            
            self._known_pids = current_procs
            time.sleep(interval)
    
    def stop(self) -> None:
        """Stop watching for process events."""
        self._running = False
