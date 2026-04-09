"""
Window resize and bounds manipulation utilities.

Provides utilities for resizing windows to specific dimensions,
changing window positions, and animating window size transitions.

Example:
    >>> from utils.window_resize_utils import resize_window, move_window
    >>> resize_window("Safari", width=1024, height=768)
    >>> move_window("Finder", x=100, y=100)
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Tuple

try:
    from dataclasses import dataclass, field
except ImportError:
    from typing import dataclass, field


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class WindowBounds:
    """
    Immutable window bounding rectangle.

    Attributes:
        x: Left coordinate of the window.
        y: Top coordinate of the window.
        width: Width of the window.
        height: Height of the window.
    """
    x: int
    y: int
    width: int
    height: int

    @property
    def left(self) -> int:
        """Alias for x (left coordinate)."""
        return self.x

    @property
    def top(self) -> int:
        """Alias for y (top coordinate)."""
        return self.y

    @property
    def right(self) -> int:
        """Right edge coordinate (x + width)."""
        return self.x + self.width

    @property
    def bottom(self) -> int:
        """Bottom edge coordinate (y + height)."""
        return self.y + self.height

    @property
    def center(self) -> Tuple[int, int]:
        """Center point as (x, y) tuple."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def area(self) -> int:
        """Window area in square pixels."""
        return self.width * self.height

    def to_tuple(self) -> Tuple[int, int, int, int]:
        """Convert to (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)

    def with_position(self, x: int, y: int) -> "WindowBounds":
        """Return a new WindowBounds with different position, same size."""
        return WindowBounds(x=x, y=y, width=self.width, height=self.height)

    def with_size(self, width: int, height: int) -> "WindowBounds":
        """Return a new WindowBounds with different size, same position."""
        return WindowBounds(x=self.x, y=self.y, width=width, height=height)

    def contains_point(self, px: int, py: int) -> bool:
        """Check if a point is within the window bounds."""
        return self.x <= px <= self.right and self.y <= py <= self.bottom

    def overlaps(self, other: "WindowBounds") -> bool:
        """Check if this bounds overlaps with another."""
        return not (
            self.right < other.x or
            other.right < self.x or
            self.bottom < other.y or
            other.bottom < self.y
        )


@dataclass
class ResizePolicy:
    """Policy controlling window resize behavior."""
    animate: bool = False
    animation_duration: float = 0.3
    anchor: str = "top-left"  # top-left, top-right, bottom-left, bottom-right, center
    respect_screen_bounds: bool = True


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Window Resolution
# ----------------------------------------------------------------------


def get_window_id_by_name(window_name: str) -> Optional[str]:
    """
    Find window ID by name.

    Args:
        window_name: Name or partial name of the window.

    Returns:
        Window ID as string if found, None otherwise.
    """
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


def get_window_bounds_by_id(window_id: str) -> Optional[WindowBounds]:
    """
    Get the current bounds of a window by ID.

    Args:
        window_id: The window identifier.

    Returns:
        WindowBounds if found, None otherwise.
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            tell (first window whose unix id is {window_id})
                set pos to position
                set dim to size
                return (item 1 of pos as string) & "," & ¬
                       (item 2 of pos as string) & "," & ¬
                       (item 1 of dim as string) & "," & ¬
                       (item 2 of dim as string)
            end tell
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
                parts = result.stdout.strip().split(",")
                if len(parts) == 4:
                    return WindowBounds(
                        x=int(parts[0].strip()),
                        y=int(parts[1].strip()),
                        width=int(parts[2].strip()),
                        height=int(parts[3].strip()),
                    )
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
            pass
    return None


# ----------------------------------------------------------------------
# Core Resize Operations
# ----------------------------------------------------------------------


def resize_window(
    window_identifier: str,
    width: Optional[int] = None,
    height: Optional[int] = None,
    policy: Optional[ResizePolicy] = None,
) -> bool:
    """
    Resize a window to specific dimensions.

    Args:
        window_identifier: Window name, window ID, or process name.
        width: Target width in pixels. None to keep current.
        height: Target height in pixels. None to keep current.
        policy: Optional resize policy.

    Returns:
        True if the resize succeeded, False otherwise.

    Example:
        >>> resize_window("Safari", width=1440, height=900)
        >>> resize_window("Terminal", height=600)
    """
    if width is None and height is None:
        return False

    policy = policy or ResizePolicy()

    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    # Get current bounds to preserve unspecified dimensions
    current = get_window_bounds_by_id(wid)
    if current is None:
        return False

    new_width = width if width is not None else current.width
    new_height = height if height is not None else current.height

    # Apply anchor-based position adjustment
    x_offset = 0
    y_offset = 0
    if policy.anchor == "bottom-right":
        x_offset = current.width - new_width
        y_offset = current.height - new_height
    elif policy.anchor == "top-right":
        x_offset = current.width - new_width
    elif policy.anchor == "bottom-left":
        y_offset = current.height - new_height
    elif policy.anchor == "center":
        x_offset = (current.width - new_width) // 2
        y_offset = (current.height - new_height) // 2

    new_x = current.x + x_offset
    new_y = current.y + y_offset

    return _set_window_bounds(wid, new_x, new_y, new_width, new_height)


def move_window(
    window_identifier: str,
    x: int,
    y: int,
) -> bool:
    """
    Move a window to a specific position without changing its size.

    Args:
        window_identifier: Window name, window ID, or process name.
        x: New left coordinate.
        y: New top coordinate.

    Returns:
        True if the move succeeded, False otherwise.

    Example:
        >>> move_window("Safari", x=0, y=0)  # move to top-left
    """
    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    current = get_window_bounds_by_id(wid)
    if current is None:
        return False

    return _set_window_bounds(wid, x, y, current.width, current.height)


def set_window_bounds(
    window_identifier: str,
    bounds: WindowBounds,
) -> bool:
    """
    Set both position and size of a window at once.

    Args:
        window_identifier: Window name, window ID, or process name.
        bounds: WindowBounds describing the target position and size.

    Returns:
        True if the operation succeeded, False otherwise.
    """
    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    return _set_window_bounds(wid, bounds.x, bounds.y, bounds.width, bounds.height)


def _set_window_bounds(
    window_id: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> bool:
    """
    Internal function to set window bounds via osascript.

    Args:
        window_id: Window identifier.
        x: Left coordinate.
        y: Top coordinate.
        width: Width in pixels.
        height: Height in pixels.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() == "darwin":
        script = f'''
        tell application "System Events"
            tell (first window whose unix id is {window_id})
                set position to {{{x}, {y}}}
                set size to {{{width}, {height}}}
            end tell
        end tell
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


# ----------------------------------------------------------------------
# Preset Sizes
# ----------------------------------------------------------------------


def maximize_window(window_identifier: str) -> bool:
    """
    Maximize a window to fill the screen.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() != "darwin":
        return False

    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    script = f'''
    tell application "System Events"
        tell (first window whose unix id is {wid})
            set position to {{0, 22}}
            tell (application "Finder")
                set screenSize to size of window of desktop
            end tell
            set bounds to {{0, 22, item 1 of screenSize, item 2 of screenSize}}
        end tell
    end tell
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


def minimize_window(window_identifier: str) -> bool:
    """
    Minimize a window to the dock.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() != "darwin":
        return False

    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    script = f'''
    tell application "System Events"
        tell (first window whose unix id is {wid})
            set miniaturized of it to true
        end tell
    end tell
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


def restore_window(window_identifier: str) -> bool:
    """
    Restore a minimized window.

    Args:
        window_identifier: Window name, window ID, or process name.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() != "darwin":
        return False

    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    script = f'''
    tell application "System Events"
        tell (first window whose unix id is {wid})
            set miniaturized of it to false
            set visible of it to true
        end tell
    end tell
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
# Animated Resize
# ----------------------------------------------------------------------


def animate_resize(
    window_identifier: str,
    target_width: int,
    target_height: int,
    steps: int = 10,
    step_delay: float = 0.05,
) -> bool:
    """
    Animate a window resize from current size to target size.

    Args:
        window_identifier: Window name, window ID, or process name.
        target_width: Target width in pixels.
        target_height: Target height in pixels.
        steps: Number of intermediate steps.
        step_delay: Delay in seconds between each step.

    Returns:
        True if the animation completed, False otherwise.

    Example:
        >>> animate_resize("Safari", 800, 600, steps=20, step_delay=0.03)
    """
    import time

    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return False

    current = get_window_bounds_by_id(wid)
    if current is None:
        return False

    if (current.width == target_width and current.height == target_height):
        return True

    for i in range(steps + 1):
        progress = i / steps
        intermediate_width = int(
            current.width + (target_width - current.width) * progress
        )
        intermediate_height = int(
            current.height + (target_height - current.height) * progress
        )
        if not _set_window_bounds(
            wid, current.x, current.y,
            intermediate_width, intermediate_height
        ):
            return False
        time.sleep(step_delay)

    return True
