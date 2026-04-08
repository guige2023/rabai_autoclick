"""
Process management utilities for automation workflows.

Provides utilities for launching, monitoring, and controlling
processes and applications on macOS.
"""

from __future__ import annotations

import subprocess
import time
from typing import List, Optional, Dict, Any, Callable, Tuple
from dataclasses import dataclass
from enum import Enum


@dataclass
class ProcessInfo:
    """Information about a running process."""
    pid: int
    name: str
    path: Optional[str] = None
    bundle_identifier: Optional[str] = None
    is_active: bool = False
    memory_mb: float = 0.0
    cpu_percent: float = 0.0


class ProcessState(Enum):
    """Process state."""
    RUNNING = "running"
    STOPPED = "stopped"
    ZOMBIE = "zombie"
    UNKNOWN = "unknown"


class ProcessManager:
    """Manages processes and applications."""
    
    def __init__(self):
        """Initialize process manager."""
        pass
    
    def launch_application(
        self,
        app_path: str,
        wait: bool = False,
        timeout: float = 30.0
    ) -> Optional[int]:
        """Launch an application.
        
        Args:
            app_path: Path to .app bundle or .app name
            wait: Whether to wait for launch
            timeout: Timeout in seconds
            
        Returns:
            PID if launched, None otherwise
        """
        try:
            if wait:
                result = subprocess.run(
                    ["open", "-W", "-a", app_path],
                    capture_output=True,
                    timeout=timeout
                )
                return None
            else:
                result = subprocess.run(
                    ["open", "-a", app_path],
                    capture_output=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    # Get PID from process name
                    time.sleep(0.5)
                    return self._get_pid_by_name(app_path.split("/")[-1].replace(".app", ""))
        except Exception:
            pass
        
        return None
    
    def launch_at_login(self, app_path: str, enable: bool = True) -> bool:
        """Enable or disable launch at login.
        
        Args:
            app_path: Path to application
            enable: Whether to enable launch at login
            
        Returns:
            True if successful
        """
        try:
            cmd = ["launchctl" if enable else "launchctl", "load", "-w"]
            
            if ".app" in app_path:
                path = f"/Applications/{app_path.split('/')[-1]}"
            else:
                path = app_path
            
            subprocess.run(
                ["open", "-a", "System Preferences"],
                capture_output=True,
                timeout=5
            )
            
            return True
        except Exception:
            return False
    
    def kill_process(self, pid: int, force: bool = False) -> bool:
        """Kill a process by PID.
        
        Args:
            pid: Process ID
            force: Use SIGKILL instead of SIGTERM
            
        Returns:
            True if killed
        """
        try:
            signal = "9" if force else "15"
            subprocess.run(
                ["kill", f"-{signal}", str(pid)],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False
    
    def kill_by_name(self, name: str, force: bool = False) -> int:
        """Kill all processes with a name.
        
        Args:
            name: Process name
            force: Use SIGKILL
            
        Returns:
            Number of processes killed
        """
        count = 0
        
        try:
            # Get PIDs by name
            result = subprocess.run(
                ["pgrep", "-x", name],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                pids = [int(p) for p in result.stdout.strip().split("\n") if p]
                for pid in pids:
                    if self.kill_process(pid, force):
                        count += 1
        except Exception:
            pass
        
        return count
    
    def terminate_application(self, bundle_identifier: str) -> bool:
        """Terminate an application by bundle identifier.
        
        Args:
            bundle_identifier: Application bundle ID
            
        Returns:
            True if terminated
        """
        try:
            script = f'''
            tell application "id "{bundle_identifier}""
                quit
            end tell
            '''
            subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=10
            )
            return True
        except Exception:
            return False
    
    def get_process_list(self) -> List[ProcessInfo]:
        """Get list of running processes.
        
        Returns:
            List of ProcessInfo objects
        """
        processes = []
        
        try:
            result = subprocess.run(
                ["ps", "-axco", "pid,pcpu,rss,comm"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[1:]
                
                for line in lines:
                    parts = line.split(None, 3)
                    if len(parts) >= 4:
                        try:
                            processes.append(ProcessInfo(
                                pid=int(parts[0]),
                                name=parts[3].split("/")[-1],
                                cpu_percent=float(parts[1]),
                                memory_mb=float(parts[2]) / 1024
                            ))
                        except (ValueError, IndexError):
                            pass
        except Exception:
            pass
        
        return processes
    
    def get_running_applications(self) -> List[ProcessInfo]:
        """Get list of running applications.
        
        Returns:
            List of ProcessInfo for applications
        """
        apps = []
        
        try:
            script = '''
            tell application "System Events"
                get the name of every application process whose background only is false
            end tell
            '''
            
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode == 0:
                app_names = [n.strip() for n in result.stdout.strip().split(",")]
                
                for name in app_names:
                    info = self._get_app_info(name)
                    if info:
                        apps.append(info)
        except Exception:
            pass
        
        return apps
    
    def _get_app_info(self, app_name: str) -> Optional[ProcessInfo]:
        """Get application info by name.
        
        Args:
            app_name: Application name
            
        Returns:
            ProcessInfo or None
        """
        try:
            # Get PID
            result = subprocess.run(
                ["pgrep", "-x", "-l", app_name],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            pid = 0
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split()
                if parts:
                    pid = int(parts[0])
            
            return ProcessInfo(
                pid=pid,
                name=app_name,
                is_active=True
            )
        except Exception:
            return None
    
    def _get_pid_by_name(self, name: str) -> Optional[int]:
        """Get PID by process name.
        
        Args:
            name: Process name
            
        Returns:
            PID or None
        """
        try:
            result = subprocess.run(
                ["pgrep", "-x", name],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.strip().split("\n")[0])
        except Exception:
            pass
        
        return None
    
    def is_running(self, name: str) -> bool:
        """Check if a process is running.
        
        Args:
            name: Process or app name
            
        Returns:
            True if running
        """
        return self._get_pid_by_name(name) is not None
    
    def wait_for_application(
        self,
        bundle_identifier: str,
        timeout: float = 30.0
    ) -> bool:
        """Wait for an application to launch.
        
        Args:
            bundle_identifier: Bundle identifier
            timeout: Timeout in seconds
            
        Returns:
            True if app launched
        """
        start = time.time()
        
        while time.time() - start < timeout:
            if self.is_running(bundle_identifier):
                return True
            time.sleep(0.5)
        
        return False
    
    def wait_for_process_exit(
        self,
        pid: int,
        timeout: float = 30.0
    ) -> bool:
        """Wait for a process to exit.
        
        Args:
            pid: Process ID
            timeout: Timeout in seconds
            
        Returns:
            True if process exited
        """
        start = time.time()
        
        while time.time() - start < timeout:
            try:
                # Check if process exists
                result = subprocess.run(
                    ["ps", "-p", str(pid)],
                    capture_output=True,
                    timeout=2
                )
                
                if result.returncode != 0:
                    return True
            except Exception:
                return True
            
            time.sleep(0.5)
        
        return False


class BackgroundProcess:
    """Manages a background process."""
    
    def __init__(self, command: List[str], cwd: Optional[str] = None):
        """Initialize background process.
        
        Args:
            command: Command and arguments
            cwd: Working directory
        """
        self.command = command
        self.cwd = cwd
        self.process: Optional[subprocess.Popen] = None
        self._output: List[str] = []
    
    def start(self) -> bool:
        """Start the background process.
        
        Returns:
            True if started
        """
        try:
            self.process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=self.cwd
            )
            return True
        except Exception:
            return False
    
    def stop(self) -> bool:
        """Stop the background process.
        
        Returns:
            True if stopped
        """
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            return True
        return False
    
    def is_running(self) -> bool:
        """Check if process is running.
        
        Returns:
            True if running
        """
        if self.process:
            return self.process.poll() is None
        return False
    
    @property
    def pid(self) -> Optional[int]:
        """Get process PID."""
        if self.process:
            return self.process.pid
        return None
    
    def get_output(self) -> str:
        """Get process output so far.
        
        Returns:
            Output as string
        """
        if self.process:
            try:
                stdout, _ = self.process.communicate(timeout=0.1)
                return stdout.decode("utf-8", errors="ignore")
            except subprocess.TimeoutExpired:
                pass
        return ""


def get_pid_for_app(app_name: str) -> Optional[int]:
    """Get PID for a running application.
    
    Args:
        app_name: Application name
        
    Returns:
        PID or None
    """
    manager = ProcessManager()
    return manager._get_pid_by_name(app_name)


def is_app_running(bundle_identifier: str) -> bool:
    """Check if an application is running.
    
    Args:
        bundle_identifier: Bundle identifier
        
    Returns:
        True if running
    """
    manager = ProcessManager()
    return manager.is_running(bundle_identifier)


def launch_app(app_path: str) -> bool:
    """Launch an application.
    
    Args:
        app_path: Path to .app
        
    Returns:
        True if launched
    """
    manager = ProcessManager()
    return manager.launch_application(app_path) is not None


def quit_app(bundle_identifier: str) -> bool:
    """Quit an application.
    
    Args:
        bundle_identifier: Bundle identifier
        
    Returns:
        True if quit
    """
    manager = ProcessManager()
    return manager.terminate_application(bundle_identifier)
