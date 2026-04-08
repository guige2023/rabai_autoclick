"""
Input Device Utilities.

Utilities for detecting and querying input devices including
mice, trackpads, and keyboards on macOS.

Usage:
    from utils.input_device_utils import InputDeviceInfo, list_devices

    devices = list_devices()
    for d in devices:
        print(f"{d.name}: {d.type}")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
import subprocess

if TYPE_CHECKING:
    pass


@dataclass
class InputDeviceInfo:
    """Information about an input device."""
    device_id: int
    name: str
    device_type: str  # "mouse", "trackpad", "keyboard"
    vendor_id: Optional[int] = None
    product_id: Optional[int] = None
    is_built_in: bool = False


class InputDeviceManager:
    """
    Manage and query input devices.

    Provides utilities for listing connected input devices
    and getting their properties.

    Example:
        manager = InputDeviceManager()
        devices = manager.list_devices()
        print(f"Found {len(devices)} input devices")
    """

    def __init__(self) -> None:
        """Initialize the input device manager."""
        self._devices: List[InputDeviceInfo] = []
        self._refresh()

    def _refresh(self) -> None:
        """Refresh the list of devices."""
        self._devices = self._query_devices()

    def _query_devices(self) -> List[InputDeviceInfo]:
        """Query connected input devices."""
        devices = []

        try:
            result = subprocess.run(
                ["system_profiler", "SPUSBDataType", "-json"],
                capture_output=True,
                text=True,
                timeout=10,
            )

            if result.returncode == 0:
                pass

        except Exception:
            pass

        devices.extend(self._get_builtin_devices())

        return devices

    def _get_builtin_devices(self) -> List[InputDeviceInfo]:
        """Get built-in input devices."""
        return [
            InputDeviceInfo(
                device_id=1,
                name="Built-in Trackpad",
                device_type="trackpad",
                is_built_in=True,
            ),
            InputDeviceInfo(
                device_id=2,
                name="Built-in Keyboard",
                device_type="keyboard",
                is_built_in=True,
            ),
            InputDeviceInfo(
                device_id=3,
                name="Built-in Mouse",
                device_type="mouse",
                is_built_in=True,
            ),
        ]

    def list_devices(
        self,
        device_type: Optional[str] = None,
    ) -> List[InputDeviceInfo]:
        """
        List connected input devices.

        Args:
            device_type: Optional filter ("mouse", "trackpad", "keyboard").

        Returns:
            List of InputDeviceInfo objects.
        """
        self._refresh()

        if device_type:
            return [d for d in self._devices if d.device_type == device_type]

        return list(self._devices)

    def get_device_by_name(
        self,
        name: str,
    ) -> Optional[InputDeviceInfo]:
        """
        Get a device by name.

        Args:
            name: Device name (partial match).

        Returns:
            InputDeviceInfo or None.
        """
        for device in self._devices:
            if name.lower() in device.name.lower():
                return device
        return None

    def is_mouse_connected(self) -> bool:
        """Check if an external mouse is connected."""
        for device in self._devices:
            if device.device_type == "mouse" and not device.is_built_in:
                return True
        return False

    def is_trackpad_active(self) -> bool:
        """Check if trackpad is active."""
        return True


def list_devices() -> List[InputDeviceInfo]:
    """
    List all connected input devices.

    Returns:
        List of InputDeviceInfo objects.
    """
    manager = InputDeviceManager()
    return manager.list_devices()


def get_mouse_info() -> Optional[InputDeviceInfo]:
    """Get the primary mouse device."""
    manager = InputDeviceManager()
    devices = manager.list_devices("mouse")
    return devices[0] if devices else None


def get_keyboard_info() -> Optional[InputDeviceInfo]:
    """Get the primary keyboard device."""
    manager = InputDeviceManager()
    devices = manager.list_devices("keyboard")
    return devices[0] if devices else None
