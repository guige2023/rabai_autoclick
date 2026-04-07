"""OS utilities for RabAI AutoClick.

Provides:
- OS information and detection
- Process utilities
- Shell command helpers
- Environment management
"""

import os
import platform
import subprocess
import shlex
from typing import Dict, List, Optional, Tuple, Any


def get_os() -> str:
    """Get operating system name.

    Returns:
        OS name (linux, darwin, windows).
    """
    return platform.system().lower()


def is_linux() -> bool:
    """Check if running on Linux.

    Returns:
        True if Linux.
    """
    return get_os() == "linux"


def is_macos() -> bool:
    """Check if running on macOS.

    Returns:
        True if macOS.
    """
    return get_os() == "darwin"


def is_windows() -> bool:
    """Check if running on Windows.

    Returns:
        True if Windows.
    """
    return get_os() == "windows"


def get_hostname() -> str:
    """Get hostname.

    Returns:
        Hostname string.
    """
    return platform.node()


def get_username() -> str:
    """Get current username.

    Returns:
        Username string.
    """
    return os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"


def get_home_dir() -> str:
    """Get home directory.

    Returns:
        Home directory path.
    """
    return os.path.expanduser("~")


def get_cwd() -> str:
    """Get current working directory.

    Returns:
        CWD path.
    """
    return os.getcwd()


def get_temp_dir() -> str:
    """Get temporary directory.

    Returns:
        Temp directory path.
    """
    import tempfile
    return tempfile.gettempdir()


def get_env(key: str, default: str = None) -> Optional[str]:
    """Get environment variable.

    Args:
        key: Variable name.
        default: Default if not set.

    Returns:
        Variable value or default.
    """
    return os.environ.get(key, default)


def set_env(key: str, value: str) -> None:
    """Set environment variable.

    Args:
        key: Variable name.
        value: Variable value.
    """
    os.environ[key] = value


def unset_env(key: str) -> None:
    """Unset environment variable.

    Args:
        key: Variable name.
    """
    if key in os.environ:
        del os.environ[key]


def get_all_env() -> Dict[str, str]:
    """Get all environment variables.

    Returns:
        Environment dict.
    """
    return dict(os.environ)


def get_pid() -> int:
    """Get current process ID.

    Returns:
        PID.
    """
    return os.getpid()


def get_ppid() -> int:
    """Get parent process ID.

    Returns:
        Parent PID.
    """
    return os.getppid()


def is_admin() -> bool:
    """Check if running as administrator.

    Returns:
        True if admin/root.
    """
    if is_windows():
        import ctypes
        try:
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        except Exception:
            return False
    else:
        try:
            return os.getuid() == 0
        except AttributeError:
            return False


def get_cpu_count() -> int:
    """Get number of CPU cores.

    Returns:
        CPU core count.
    """
    try:
        return os.cpu_count() or 1
    except Exception:
        return 1


def get_memory_info() -> Dict[str, int]:
    """Get memory information.

    Returns:
        Dict with total and available memory in bytes.
    """
    try:
        import psutil
        mem = psutil.virtual_memory()
        return {
            "total": mem.total,
            "available": mem.available,
            "used": mem.used,
            "percent": mem.percent,
        }
    except ImportError:
        return {}


def get_disk_info(path: str = "/") -> Dict[str, Any]:
    """Get disk usage information.

    Args:
        path: Path to check (default: root).

    Returns:
        Dict with disk usage info.
    """
    try:
        import psutil
        disk = psutil.disk_usage(path)
        return {
            "total": disk.total,
            "used": disk.used,
            "free": disk.free,
            "percent": disk.percent,
        }
    except ImportError:
        return {}


def run_command(
    cmd: str,
    shell: bool = True,
    cwd: str = None,
    env: Dict[str, str] = None,
    timeout: int = None,
) -> Tuple[int, str, str]:
    """Run shell command.

    Args:
        cmd: Command to run.
        shell: Use shell.
        cwd: Working directory.
        env: Environment variables.
        timeout: Timeout in seconds.

    Returns:
        Tuple of (returncode, stdout, stderr).
    """
    try:
        if shell:
            result = subprocess.run(
                cmd,
                shell=True,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        else:
            args = shlex.split(cmd)
            result = subprocess.run(
                args,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out"
    except Exception as e:
        return -1, "", str(e)


def run_command_async(cmd: str) -> subprocess.Popen:
    """Run command asynchronously.

    Args:
        cmd: Command to run.

    Returns:
        Popen object.
    """
    return subprocess.Popen(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def kill_process(pid: int, force: bool = False) -> bool:
    """Kill process by PID.

    Args:
        pid: Process ID.
        force: Use SIGKILL.

    Returns:
        True if successful.
    """
    import signal
    try:
        sig = signal.SIGKILL if force else signal.SIGTERM
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return True  # Process already dead
    except PermissionError:
        return False
    except Exception:
        return False


def list_processes() -> List[Dict[str, Any]]:
    """List running processes.

    Returns:
        List of process info dicts.
    """
    try:
        import psutil
        processes = []
        for p in psutil.process_iter(["pid", "name", "username"]):
            try:
                processes.append({
                    "pid": p.info["pid"],
                    "name": p.info["name"],
                    "username": p.info["username"],
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        return processes
    except ImportError:
        return []


def get_process_name(pid: int) -> Optional[str]:
    """Get process name by PID.

    Args:
        pid: Process ID.

    Returns:
        Process name or None.
    """
    try:
        import psutil
        return psutil.Process(pid).name()
    except (ImportError, psutil.NoSuchProcess):
        return None


def file_exists(path: str) -> bool:
    """Check if file exists.

    Args:
        path: File path.

    Returns:
        True if exists.
    """
    return os.path.isfile(path)


def dir_exists(path: str) -> bool:
    """Check if directory exists.

    Args:
        path: Directory path.

    Returns:
        True if exists.
    """
    return os.path.isdir(path)


def path_exists(path: str) -> bool:
    """Check if path exists.

    Args:
        path: Path.

    Returns:
        True if exists.
    """
    return os.path.exists(path)


def make_dir(path: str, parents: bool = True) -> bool:
    """Create directory.

    Args:
        path: Directory path.
        parents: Create parent directories.

    Returns:
        True if successful.
    """
    try:
        if parents:
            os.makedirs(path, exist_ok=True)
        else:
            os.mkdir(path)
        return True
    except Exception:
        return False


def remove_file(path: str) -> bool:
    """Remove file.

    Args:
        path: File path.

    Returns:
        True if successful.
    """
    try:
        os.remove(path)
        return True
    except Exception:
        return False


def remove_dir(path: str, recursive: bool = False) -> bool:
    """Remove directory.

    Args:
        path: Directory path.
        recursive: Remove recursively.

    Returns:
        True if successful.
    """
    try:
        if recursive:
            import shutil
            shutil.rmtree(path)
        else:
            os.rmdir(path)
        return True
    except Exception:
        return False


def copy_file(src: str, dst: str) -> bool:
    """Copy file.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        True if successful.
    """
    try:
        import shutil
        shutil.copy2(src, dst)
        return True
    except Exception:
        return False


def move_file(src: str, dst: str) -> bool:
    """Move file.

    Args:
        src: Source path.
        dst: Destination path.

    Returns:
        True if successful.
    """
    try:
        import shutil
        shutil.move(src, dst)
        return True
    except Exception:
        return False


def get_file_size(path: str) -> Optional[int]:
    """Get file size in bytes.

    Args:
        path: File path.

    Returns:
        Size in bytes or None.
    """
    try:
        return os.path.getsize(path)
    except Exception:
        return None


def get_file_modified_time(path: str) -> Optional[float]:
    """Get file modification time.

    Args:
        path: File path.

    Returns:
        Modification timestamp or None.
    """
    try:
        return os.path.getmtime(path)
    except Exception:
        return None


def list_dir(path: str, pattern: str = None) -> List[str]:
    """List directory contents.

    Args:
        path: Directory path.
        pattern: Optional glob pattern.

    Returns:
        List of filenames.
    """
    try:
        if pattern:
            import glob
            return glob.glob(os.path.join(path, pattern))
        return os.listdir(path)
    except Exception:
        return []


def get_absolute_path(path: str) -> str:
    """Get absolute path.

    Args:
        path: Path.

    Returns:
        Absolute path.
    """
    return os.path.abspath(path)


def join_paths(*paths: str) -> str:
    """Join path components.

    Args:
        *paths: Path components.

    Returns:
        Joined path.
    """
    return os.path.join(*paths)


def split_path(path: str) -> Tuple[str, str]:
    """Split path into directory and filename.

    Args:
        path: Path.

    Returns:
        Tuple of (directory, filename).
    """
    return os.path.split(path)


def get_extension(path: str) -> str:
    """Get file extension.

    Args:
        path: File path.

    Returns:
        Extension including dot.
    """
    return os.path.splitext(path)[1]


def get_basename(path: str) -> str:
    """Get base filename without extension.

    Args:
        path: File path.

    Returns:
        Base name.
    """
    return os.path.splitext(os.path.basename(path))[0]


def get_dirname(path: str) -> str:
    """Get directory name.

    Args:
        path: File path.

    Returns:
        Directory path.
    """
    return os.path.dirname(path)


def expand_path(path: str) -> str:
    """Expand user home and environment variables.

    Args:
        path: Path to expand.

    Returns:
        Expanded path.
    """
    return os.path.expanduser(os.path.expandvars(path))