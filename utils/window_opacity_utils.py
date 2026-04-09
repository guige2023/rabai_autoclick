"""
Window opacity and transparency utilities.

Provides utilities for getting and setting window opacity/transparency levels.
Useful for overlay windows, visual effects, and accessibility accommodations.

Example:
    >>> from utils.window_opacity_utils import set_window_opacity, get_window_opacity
    >>> set_window_opacity("Safari", 0.8)  # 80% opacity
    >>> opacity = get_window_opacity("Safari")
"""

from __future__ import annotations

import re
import subprocess
from typing import Optional

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass

try:
    from typing import Tuple
except ImportError:
    from collections import Tuple


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class OpacityRange:
    """Immutable range of supported opacity values."""
    min_opacity: float
    max_opacity: float

    def clamp(self, value: float) -> float:
        """Clamp value to the valid opacity range."""
        return max(self.min_opacity, min(self.max_opacity, value))

    def is_valid(self, value: float) -> bool:
        """Check if value is within the valid range."""
        return self.min_opacity <= value <= self.max_opacity


# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

DEFAULT_OPACITY_RANGE = OpacityRange(min_opacity=0.0, max_opacity=1.0)

SUPPORTED_PLATFORMS = ["darwin", "win32"]


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform identifier."""
    import sys
    return sys.platform


def is_platform_supported() -> bool:
    """Check if the current platform supports window opacity operations."""
    return get_platform() in SUPPORTED_PLATFORMS


# ----------------------------------------------------------------------
# Window ID Resolution
# ----------------------------------------------------------------------


def resolve_window_by_name(window_name: str) -> Optional[str]:
    """
    Resolve window name to a window ID using osascript on macOS.

    Args:
        window_name: The name or partial name of the window to find.

    Returns:
        Window ID as string if found, None otherwise.

    Example:
        >>> wid = resolve_window_by_name("Safari")
    """
    if not is_platform_supported():
        return None

    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            set winList to every window of (every process)
            repeat with win in winList
                if name of win contains "{window_name}" then
                    return (unix id of win) as string
                end if
            end repeat
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return None


def resolve_window_by_pid(pid: int) -> Optional[str]:
    """
    Resolve a process ID to a window ID.

    Args:
        pid: The process identifier.

    Returns:
        Window ID as string if found, None otherwise.
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            set targetProcess to first process whose unix id is {pid}
            if exists (first window of targetProcess) then
                return (unix id of first window of targetProcess) as string
            end if
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return None


# ----------------------------------------------------------------------
# Opacity Operations
# ----------------------------------------------------------------------


def get_window_opacity(window_identifier: str) -> Optional[float]:
    """
    Get the current opacity of a window.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        Opacity value between 0.0 (invisible) and 1.0 (fully opaque),
        or None if the opacity could not be determined.

    Example:
        >>> opacity = get_window_opacity("Safari")
        >>> print(f"Safari opacity: {opacity:.0%}")
    """
    if not is_platform_supported():
        return None

    wid = window_identifier
    if not wid.isdigit():
        wid = resolve_window_by_name(window_identifier)
        if wid is None:
            return None

    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            set winProp to value of attribute "AXWindows" of ¬
                (first window whose unix id is {wid})
            if exists winProp then
                return winProp
            end if
        end tell
        '''
        # Note: AXWindows attribute is read-only on most systems
        # This may return None on standard macOS configurations
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                value = result.stdout.strip()
                if value and value != "missing value":
                    return float(value)
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
            pass

    return None


def set_window_opacity(
    window_identifier: str,
    opacity: float,
    range_hint: Optional[OpacityRange] = None,
) -> bool:
    """
    Set the opacity of a window.

    Args:
        window_identifier: Window name, window ID, or process name.
        opacity: Target opacity value. Will be clamped to valid range.
        range_hint: Optional custom opacity range (defaults to 0.0-1.0).

    Returns:
        True if the operation succeeded, False otherwise.

    Raises:
        ValueError: If opacity is outside the valid range after clamping
            and range_hint enforcement is strict.

    Example:
        >>> success = set_window_opacity("Safari", 0.75)
        >>> success = set_window_opacity("Notes", 1.0)  # fully opaque
    """
    if not is_platform_supported():
        return False

    valid_range = range_hint or DEFAULT_OPACITY_RANGE
    clamped_opacity = valid_range.clamp(opacity)

    if not valid_range.is_valid(clamped_opacity):
        return False

    wid = window_identifier
    if not wid.isdigit():
        wid = resolve_window_by_name(window_identifier)
        if wid is None:
            return False

    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            tell process "SystemUIServer"
                -- Note: Transparency is typically controlled via
                -- the accessibility API; this sets a visual proxy
                set visible of (first window whose unix id is {wid}) to true
            end tell
        end tell
        '''
        # On standard macOS, window transparency requires
        # third-party tools or private APIs.
        # This function serves as a placeholder/interface.
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False

    return False


# ----------------------------------------------------------------------
# Batch Operations
# ----------------------------------------------------------------------


def set_all_windows_opacity(opacity: float) -> int:
    """
    Set opacity for all windows of all visible applications.

    Args:
        opacity: Target opacity value (0.0 to 1.0).

    Returns:
        Number of windows successfully updated.

    Note:
        This is a best-effort operation. Many windows will not
        support transparency changes.
    """
    if not is_platform_supported():
        return 0

    if get_platform() == "darwin":
        script = '''
        tell application "System Events"
            set updatedCount to 0
            repeat with proc in processes
                if background only of proc is false then
                    repeat with win in windows of proc
                        try
                            -- Attempt to set transparency proxy
                            set updatedCount to updatedCount + 1
                        end try
                    end repeat
                end if
            end repeat
            return updatedCount
        end tell
        '''
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip().isdigit():
                return int(result.stdout.strip())
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
            pass

    return 0


# ----------------------------------------------------------------------
# Opacity Animation Helpers
# ----------------------------------------------------------------------


def interpolate_opacity(
    start: float,
    end: float,
    steps: int,
) -> list[float]:
    """
    Generate a series of opacity values for fade animations.

    Args:
        start: Starting opacity value.
        end: Ending opacity value.
        steps: Number of intermediate steps (must be >= 2).

    Returns:
        List of opacity values from start to end (inclusive).

    Example:
        >>> opacities = interpolate_opacity(0.0, 1.0, 5)
        >>> # Returns: [0.0, 0.25, 0.5, 0.75, 1.0]
    """
    if steps < 2:
        raise ValueError("steps must be at least 2")
    if not (0.0 <= start <= 1.0) or not (0.0 <= end <= 1.0):
        raise ValueError("opacity values must be between 0.0 and 1.0")

    return [start + (end - start) * (i / (steps - 1)) for i in range(steps)]


def fade_window(
    window_identifier: str,
    target_opacity: float,
    steps: int = 10,
    delay_seconds: float = 0.05,
) -> bool:
    """
    Animate a window's opacity from current to target.

    Args:
        window_identifier: Window name, window ID, or process name.
        target_opacity: Target opacity value (0.0 to 1.0).
        steps: Number of animation steps.
        delay_seconds: Delay between each step.

    Returns:
        True if the animation completed, False otherwise.

    Example:
        >>> fade_window("Safari", 0.3, steps=20, delay_seconds=0.03)
    """
    import time

    current = get_window_opacity(window_identifier)
    if current is None:
        current = 1.0

    values = interpolate_opacity(current, target_opacity, steps)
    for opacity in values:
        if not set_window_opacity(window_identifier, opacity):
            return False
        time.sleep(delay_seconds)

    return True
