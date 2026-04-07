"""Environment utilities for RabAI AutoClick.

Provides:
- Environment variable helpers
- System information
- Platform detection
"""

import os
import platform
import sys
from typing import Any, Dict, List, Optional


def get_env(key: str, default: str = None) -> Optional[str]:
    """Get environment variable.

    Args:
        key: Variable name.
        default: Default value if not set.

    Returns:
        Environment variable value or default.
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


def get_env_int(key: str, default: int = 0) -> int:
    """Get environment variable as integer.

    Args:
        key: Variable name.
        default: Default value if not set or invalid.

    Returns:
        Environment variable as integer or default.
    """
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default


def get_env_bool(key: str, default: bool = False) -> bool:
    """Get environment variable as boolean.

    Args:
        key: Variable name.
        default: Default value if not set.

    Returns:
        Environment variable as boolean or default.
    """
    value = os.environ.get(key)
    if value is None:
        return default
    return value.lower() in ("true", "yes", "1", "on")


def get_env_list(key: str, separator: str = ",", default: List[str] = None) -> List[str]:
    """Get environment variable as list.

    Args:
        key: Variable name.
        separator: List separator.
        default: Default value if not set.

    Returns:
        Environment variable as list or default.
    """
    value = os.environ.get(key)
    if value is None:
        return default or []
    return [item.strip() for item in value.split(separator)]


def get_all_env() -> Dict[str, str]:
    """Get all environment variables.

    Returns:
        Dictionary of all environment variables.
    """
    return os.environ.copy()


def has_env(key: str) -> bool:
    """Check if environment variable is set.

    Args:
        key: Variable name.

    Returns:
        True if set.
    """
    return key in os.environ


def is_linux() -> bool:
    """Check if running on Linux.

    Returns:
        True if Linux.
    """
    return platform.system() == "Linux"


def is_macos() -> bool:
    """Check if running on macOS.

    Returns:
        True if macOS.
    """
    return platform.system() == "Darwin"


def is_windows() -> bool:
    """Check if running on Windows.

    Returns:
        True if Windows.
    """
    return platform.system() == "Windows"


def get_platform() -> str:
    """Get platform name.

    Returns:
        Platform name (linux, darwin, windows).
    """
    system = platform.system().lower()
    if system == "darwin":
        return "darwin"
    elif system == "linux":
        return "linux"
    elif system == "windows":
        return "windows"
    return system


def get_os_version() -> str:
    """Get OS version string.

    Returns:
        OS version.
    """
    return platform.version()


def get_python_version() -> str:
    """Get Python version string.

    Returns:
        Python version.
    """
    return platform.python_version()


def get_platform_info() -> Dict[str, Any]:
    """Get platform information.

    Returns:
        Dictionary with platform details.
    """
    return {
        "system": platform.system(),
        "release": platform.release(),
        "version": platform.version(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
    }


def get_cpu_count() -> int:
    """Get number of CPU cores.

    Returns:
        CPU core count.
    """
    try:
        return os.cpu_count() or 1
    except Exception:
        return 1


def get_home_dir() -> str:
    """Get home directory path.

    Returns:
        Home directory path.
    """
    return os.path.expanduser("~")


def get_temp_dir() -> str:
    """Get temporary directory path.

    Returns:
        Temporary directory path.
    """
    return tempfile.gettempdir()


def get_current_dir() -> str:
    """Get current working directory.

    Returns:
        Current directory path.
    """
    return os.getcwd()


def is_64bit() -> bool:
    """Check if running on 64-bit platform.

    Returns:
        True if 64-bit.
    """
    return platform.machine().endswith("64")


def is_admin() -> bool:
    """Check if running with admin/root privileges.

    Returns:
        True if admin/root.
    """
    try:
        if is_windows():
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        else:
            return os.getuid() == 0
    except Exception:
        return False


def get_hostname() -> str:
    """Get hostname.

    Returns:
        Hostname.
    """
    return platform.node()


def get_username() -> str:
    """Get username.

    Returns:
        Username.
    """
    return os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"


def get_env_with_prefix(prefix: str) -> Dict[str, str]:
    """Get all environment variables with prefix.

    Args:
        prefix: Variable prefix.

    Returns:
        Dictionary of matching variables.
    """
    result = {}
    prefix_upper = prefix.upper()
    for key, value in os.environ.items():
        if key.upper().startswith(prefix_upper):
            result[key] = value
    return result


def set_env_from_dict(env_dict: Dict[str, str]) -> None:
    """Set multiple environment variables.

    Args:
        env_dict: Dictionary of variables to set.
    """
    for key, value in env_dict.items():
        os.environ[key] = value


def load_env_file(file_path: str) -> None:
    """Load environment variables from file.

    Expects KEY=VALUE format, one per line.

    Args:
        file_path: Path to env file.
    """
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    if "=" in line:
                        key, value = line.split("=", 1)
                        os.environ[key.strip()] = value.strip()
    except Exception:
        pass


import tempfile
