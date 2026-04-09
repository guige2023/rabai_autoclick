"""
System Capability Utilities

Detect and query system capabilities relevant to automation:
display configuration, input devices, OS features, and performance limits.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import platform
import subprocess
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SystemCapabilities:
    """Represents detected system capabilities."""
    os_name: str
    os_version: str
    architecture: str
    display_count: int
    primary_display_width: int
    primary_display_height: int
    has_accessibility_access: bool = False
    has_screen_recording: bool = False
    supports_high_dpi: bool = False
    input_device_count: int = 0
    supports_touch: bool = False
    supports_precise_clicks: bool = True
    supports_gestures: bool = False
    metadata: dict = field(default_factory=dict)


def get_os_name() -> str:
    """Get the operating system name."""
    return platform.system()


def get_os_version() -> str:
    """Get the OS version string."""
    return platform.release()


def get_architecture() -> str:
    """Get the system architecture (e.g., 'arm64', 'x86_64')."""
    return platform.machine()


def detect_display_count() -> int:
    """Detect the number of connected displays."""
    try:
        if get_os_name() == "Darwin":
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                return len(displays) if isinstance(displays, list) else 1
    except Exception:
        pass
    return 1


def detect_touch_support() -> bool:
    """Detect whether the system supports touch input."""
    try:
        if get_os_name() == "Darwin":
            result = subprocess.run(
                ["system_profiler", "SPPointingDataType", "-json"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                pointing = data.get("SPPointingDataType", [])
                if isinstance(pointing, list):
                    for device in pointing:
                        if isinstance(device, dict):
                            transport = device.get("transport", "").lower()
                            if "touch" in transport or "touchbar" not in transport:
                                return True
    except Exception:
        pass
    return False


def detect_high_dpi_support() -> bool:
    """Detect whether the system supports high DPI displays."""
    try:
        if get_os_name() == "Darwin":
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if isinstance(displays, list):
                    for display in displays:
                        if isinstance(display, dict):
                            if display.get("Retina"):
                                return True
    except Exception:
        pass
    return False


def collect_capabilities() -> SystemCapabilities:
    """Collect all system capabilities and return as a structured object."""
    caps = SystemCapabilities(
        os_name=get_os_name(),
        os_version=get_os_version(),
        architecture=get_architecture(),
        display_count=detect_display_count(),
        primary_display_width=1920,
        primary_display_height=1080,
        supports_touch=detect_touch_support(),
        supports_high_dpi=detect_high_dpi_support(),
    )
    return caps
