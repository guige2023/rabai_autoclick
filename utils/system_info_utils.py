"""
System Info Utilities

Collect general system information for diagnostics, logging,
and capability reporting in automation contexts.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import os
import platform
import subprocess
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class SystemInfo:
    """Comprehensive system information."""
    os_name: str
    os_version: str
    architecture: str
    hostname: str
    python_version: str
    process_id: int
    parent_process_id: int
    terminal: Optional[str]
    locale: str
    timezone: str
    screen_width: int
    screen_height: int
    cpu_count: int
    memory_total_mb: float
    metadata: dict = field(default_factory=dict)


def get_hostname() -> str:
    """Get the system hostname."""
    return platform.node()


def get_terminal() -> Optional[str]:
    """Get the terminal type for the current process."""
    return os.environ.get("TERM")


def get_locale() -> str:
    """Get the current locale string."""
    return os.environ.get("LC_ALL", os.environ.get("LANG", "unknown"))


def get_timezone() -> str:
    """Get the current timezone."""
    import time
    return time.tzname[0]


def get_screen_resolution() -> tuple[int, int]:
    """Get the primary screen resolution."""
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if displays and isinstance(displays, list):
                    first = displays[0]
                    if isinstance(first, dict):
                        res = first.get("Resolution", "")
                        # Format: "1920 x 1080"
                        parts = res.split(" x ")
                        if len(parts) == 2:
                            return int(parts[0].strip()), int(parts[1].strip())
    except Exception:
        pass
    return 1920, 1080


def get_cpu_count() -> int:
    """Get the number of CPU cores."""
    return os.cpu_count() or 1


def get_memory_total_mb() -> float:
    """Get total physical memory in MB."""
    try:
        import psutil
        return psutil.virtual_memory().total / (1024 * 1024)
    except Exception:
        return 0.0


def collect_system_info() -> SystemInfo:
    """Collect all available system information."""
    screen_w, screen_h = get_screen_resolution()
    return SystemInfo(
        os_name=platform.system(),
        os_version=platform.release(),
        architecture=platform.machine(),
        hostname=get_hostname(),
        python_version=platform.python_version(),
        process_id=os.getpid(),
        parent_process_id=os.getppid(),
        terminal=get_terminal(),
        locale=get_locale(),
        timezone=get_timezone(),
        screen_width=screen_w,
        screen_height=screen_h,
        cpu_count=get_cpu_count(),
        memory_total_mb=get_memory_total_mb(),
    )


def format_system_info(info: SystemInfo) -> str:
    """Format system info as a human-readable string."""
    lines = [
        f"OS: {info.os_name} {info.os_version} ({info.architecture})",
        f"Hostname: {info.hostname}",
        f"Python: {info.python_version}",
        f"Process ID: {info.process_id}",
        f"Terminal: {info.terminal or 'unknown'}",
        f"Locale: {info.locale}",
        f"Timezone: {info.timezone}",
        f"Screen: {info.screen_width}x{info.screen_height}",
        f"CPU Cores: {info.cpu_count}",
        f"Memory: {info.memory_total_mb:.0f} MB",
    ]
    return "\n".join(lines)
