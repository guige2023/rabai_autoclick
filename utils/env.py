"""Environment utilities for RabAI AutoClick.

Provides:
- Environment variable helpers
- OS detection
- Platform-specific utilities
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get environment variable.

    Args:
        key: Variable name.
        default: Default value.

    Returns:
        Environment value or default.
    """
    return os.environ.get(key, default)


def get_env_int(key: str, default: Optional[int] = None) -> Optional[int]:
    """Get environment variable as integer.

    Args:
        key: Variable name.
        default: Default value.

    Returns:
        Value as int or default.
    """
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_env_bool(key: str, default: Optional[bool] = None) -> Optional[bool]:
    """Get environment variable as boolean.

    Args:
        key: Variable name.
        default: Default value.

    Returns:
        Value as bool or default.
    """
    value = os.environ.get(key)
    if value is None:
        return default

    return value.lower() in ("true", "1", "yes", "on")


def set_env(key: str, value: str) -> None:
    """Set environment variable.

    Args:
        key: Variable name.
        value: Value to set.
    """
    os.environ[key] = value


def unset_env(key: str) -> None:
    """Unset environment variable.

    Args:
        key: Variable name.
    """
    os.environ.pop(key, None)


def get_all_env(prefix: Optional[str] = None) -> Dict[str, str]:
    """Get all environment variables.

    Args:
        prefix: Optional prefix to filter variables.

    Returns:
        Dict of environment variables.
    """
    if prefix:
        return {k: v for k, v in os.environ.items() if k.startswith(prefix)}
    return dict(os.environ)


class Env:
    """Environment variable access with type conversion.

    Usage:
        debug = Env("DEBUG", default=False).as_bool()
        port = Env("PORT", default=8080).as_int()
    """

    def __init__(self, key: str, default: Any = None) -> None:
        """Initialize Env helper.

        Args:
            key: Environment variable name.
            default: Default value.
        """
        self.key = key
        self.default = default
        self._value: Optional[str] = None

    def get(self) -> Optional[str]:
        """Get raw value."""
        self._value = os.environ.get(self.key)
        return self._value

    def as_str(self, default: Optional[str] = None) -> Optional[str]:
        """Get as string."""
        return os.environ.get(self.key, default or self.default)

    def as_int(self, default: Optional[int] = None) -> Optional[int]:
        """Get as integer."""
        value = os.environ.get(self.key)
        if value is None:
            return default or self.default
        try:
            return int(value)
        except ValueError:
            return default or self.default

    def as_bool(self, default: Optional[bool] = None) -> Optional[bool]:
        """Get as boolean."""
        value = os.environ.get(self.key)
        if value is None:
            return default or self.default
        return value.lower() in ("true", "1", "yes", "on")

    def as_float(self, default: Optional[float] = None) -> Optional[float]:
        """Get as float."""
        value = os.environ.get(self.key)
        if value is None:
            return default or self.default
        try:
            return float(value)
        except ValueError:
            return default or self.default

    def as_list(self, separator: str = ",", default: Optional[List[str]] = None) -> Optional[List[str]]:
        """Get as list."""
        value = os.environ.get(self.key)
        if value is None:
            return default or self.default
        return value.split(separator)


def is_windows() -> bool:
    """Check if running on Windows."""
    return sys.platform == "win32"


def is_macos() -> bool:
    """Check if running on macOS."""
    return sys.platform == "darwin"


def is_linux() -> bool:
    """Check if running on Linux."""
    return sys.platform.startswith("linux")


def get_platform() -> str:
    """Get platform name."""
    return sys.platform


def get_home_dir() -> Path:
    """Get user's home directory."""
    return Path.home()


def get_temp_dir() -> Path:
    """Get system temp directory."""
    import tempfile
    return Path(tempfile.gettempdir())


def get_config_dir() -> Path:
    """Get application config directory.

    Creates directory if it doesn't exist.
    """
    if is_windows():
        config = Path(os.environ.get("APPDATA", "")) / "rabai"
    elif is_macos():
        config = Path.home() / "Library" / "Application Support" / "rabai"
    else:
        config = Path.home() / ".config" / "rabai"

    config.mkdir(parents=True, exist_ok=True)
    return config


def get_data_dir() -> Path:
    """Get application data directory.

    Creates directory if it doesn't exist.
    """
    if is_windows():
        data = Path(os.environ.get("LOCALAPPDATA", "")) / "rabai"
    elif is_macos():
        data = Path.home() / "Library" / "Application Support" / "rabai"
    else:
        data = Path.home() / ".local" / "share" / "rabai"

    data.mkdir(parents=True, exist_ok=True)
    return data


def get_log_dir() -> Path:
    """Get application log directory.

    Creates directory if it doesn't exist.
    """
    log_dir = get_data_dir() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def is_admin() -> bool:
    """Check if running with admin/root privileges."""
    if is_windows():
        import ctypes
        try:
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        except Exception:
            return False
    else:
        return os.geteuid() == 0


def get_cpu_count() -> int:
    """Get number of CPU cores."""
    return os.cpu_count() or 1


def get_memory() -> int:
    """Get total system memory in bytes."""
    try:
        import psutil
        return psutil.virtual_memory().total
    except Exception:
        return 0