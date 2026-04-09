"""
System volume control utilities.

Provides utilities for getting and setting the system output volume,
muting/unmuting audio, and controlling volume levels programmatically.
Useful for automation scripts that need audio feedback or need to
silence notifications during automated tasks.

Example:
    >>> from utils.system_volume_utils import set_volume, get_volume, mute
    >>> set_volume(50)       # set to 50%
    >>> get_volume()         # returns current volume (0-100)
    >>> mute()
    >>> unmute()
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Tuple

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class VolumeState:
    """Immutable system volume state."""
    volume: int          # 0-100
    is_muted: bool
    device_name: str

    @property
    def normalized(self) -> float:
        """Volume as a float from 0.0 to 1.0."""
        return self.volume / 100.0

    @property
    def is_silent(self) -> bool:
        """Return True if muted or volume is 0."""
        return self.is_muted or self.volume == 0

    def with_volume(self, volume: int) -> "VolumeState":
        """Return a new VolumeState with a different volume level."""
        return VolumeState(
            volume=max(0, min(100, volume)),
            is_muted=self.is_muted,
            device_name=self.device_name,
        )


@dataclass
class VolumeChangeOptions:
    """Options for volume change operations."""
    animate: bool = False
    steps: int = 5
    step_delay: float = 0.05
    play_feedback: bool = False


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Core Volume Functions
# ----------------------------------------------------------------------


def get_volume() -> int:
    """
    Get the current system output volume (0-100).

    Returns:
        Volume level as integer from 0 to 100.

    Example:
        >>> vol = get_volume()
        >>> print(f"Volume: {vol}%")
    """
    if get_platform() == "darwin":
        return _get_volume_macos()
    return 0


def _get_volume_macos() -> int:
    """Get system volume on macOS using osascript."""
    script = '''
    output volume of (get volume settings)
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip().isdigit():
            return int(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
        pass
    return 0


def set_volume(
    level: int,
    options: Optional[VolumeChangeOptions] = None,
) -> bool:
    """
    Set the system output volume (0-100).

    Args:
        level: Target volume level (0 to 100). Values outside
            this range will be clamped.
        options: Optional configuration for the volume change.

    Returns:
        True if the volume was set successfully, False otherwise.

    Example:
        >>> set_volume(75)       # set to 75%
        >>> set_volume(0)        # mute
        >>> set_volume(100)      # maximum volume
    """
    clamped = max(0, min(100, level))
    opts = options or VolumeChangeOptions()

    if opts.animate:
        current = get_volume()
        step_size = (clamped - current) / max(1, opts.steps)

        for step in range(1, opts.steps + 1):
            step_volume = int(current + step_size * step)
            if not _set_volume_macos(step_volume):
                return False
            time.sleep(opts.step_delay)

        # Ensure final value is exact
        _set_volume_macos(clamped)
        return True
    else:
        return _set_volume_macos(clamped)


def _set_volume_macos(level: int) -> bool:
    """Set system volume on macOS using osascript."""
    script = f'''
    set volume output volume {level}
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


# ----------------------------------------------------------------------
# Mute/Unmute
# ----------------------------------------------------------------------


def get_muted() -> bool:
    """
    Check if the system output is currently muted.

    Returns:
        True if muted, False otherwise.
    """
    if get_platform() == "darwin":
        script = '''
        output muted of (get volume settings)
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip().lower() == "true"
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return False


def mute() -> bool:
    """
    Mute the system output volume.

    Returns:
        True if mute was successful, False otherwise.

    Example:
        >>> mute()
    """
    if get_platform() == "darwin":
        script = '''
        set volume output muted true
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
    return False


def unmute() -> bool:
    """
    Unmute the system output volume.

    Returns:
        True if unmute was successful, False otherwise.

    Example:
        >>> unmute()
    """
    if get_platform() == "darwin":
        script = '''
        set volume output muted false
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False
    return False


def toggle_mute() -> bool:
    """
    Toggle the mute state (mute if unmuted, unmute if muted).

    Returns:
        True if toggle was successful, False otherwise.
        The return value indicates the operation succeeded,
        not the resulting mute state.

    Example:
        >>> toggle_mute()
    """
    if get_muted():
        return unmute()
    else:
        return mute()


# ----------------------------------------------------------------------
# State Query
# ----------------------------------------------------------------------


def get_volume_state() -> VolumeState:
    """
    Get the complete current volume state.

    Returns:
        VolumeState object with current volume and mute status.

    Example:
        >>> state = get_volume_state()
        >>> print(f"Volume: {state.volume}%, Muted: {state.is_muted}")
    """
    volume = get_volume()
    muted = get_muted()
    device_name = "Built-in Output"

    return VolumeState(
        volume=volume,
        is_muted=muted,
        device_name=device_name,
    )


# ----------------------------------------------------------------------
# Volume Adjustments
# ----------------------------------------------------------------------


def increase_volume(delta: int = 10) -> int:
    """
    Increase the volume by a delta amount.

    Args:
        delta: Amount to increase (default 10).

    Returns:
        The new volume level, or -1 on failure.

    Example:
        >>> new_vol = increase_volume(5)  # increase by 5
    """
    current = get_volume()
    new_vol = min(100, current + delta)
    if set_volume(new_vol):
        return new_vol
    return -1


def decrease_volume(delta: int = 10) -> int:
    """
    Decrease the volume by a delta amount.

    Args:
        delta: Amount to decrease (default 10).

    Returns:
        The new volume level, or -1 on failure.

    Example:
        >>> new_vol = decrease_volume(5)  # decrease by 5
    """
    current = get_volume()
    new_vol = max(0, current - delta)
    if set_volume(new_vol):
        return new_vol
    return -1


# ----------------------------------------------------------------------
# Context Manager
# ----------------------------------------------------------------------


class VolumeMuteContext:
    """
    Context manager for temporarily muting volume.

    Saves current volume state on enter and restores on exit.

    Example:
        >>> with VolumeMuteContext():
        ...     play_sound()  # sound is muted
        ... # volume restored
    """

    def __init__(self, save_volume: bool = True):
        self.save_volume = save_volume
        self._saved_state: Optional[VolumeState] = None

    def __enter__(self) -> "VolumeMuteContext":
        if self.save_volume:
            self._saved_state = get_volume_state()
        mute()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._saved_state is not None:
            unmute()
            set_volume(self._saved_state.volume)


class VolumeLockContext:
    """
    Context manager for locking volume at a specific level.

    Sets volume on enter and restores on exit.

    Example:
        >>> with VolumeLockContext(50):
        ...     run_automation()  # volume stays at 50
        ... # volume restored
    """

    def __init__(self, volume: int):
        self.target_volume = max(0, min(100, volume))
        self._previous_state: Optional[VolumeState] = None

    def __enter__(self) -> "VolumeLockContext":
        self._previous_state = get_volume_state()
        set_volume(self.target_volume)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._previous_state is not None:
            set_volume(self._previous_state.volume)
            if self._previous_state.is_muted:
                mute()
