"""
Window content snapshot and capture utilities.

Provides utilities for capturing the content of specific windows as images.
Useful for visual regression testing, window state documentation, and
accessibility snapshots.

Example:
    >>> from utils.window_snapshot_utils import capture_window, capture_window_region
    >>> img = capture_window("Safari")
    >>> region = capture_window("Finder", region=(0, 0, 800, 600))
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Tuple, Union

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class WindowSnapshot:
    """Immutable snapshot of a window's content and metadata."""
    image_data: Optional[bytes]
    width: int
    height: int
    window_name: str
    window_id: str
    timestamp: float
    format: str = "png"

    @property
    def size_bytes(self) -> int:
        """Return the size of the image data in bytes."""
        if self.image_data is None:
            return 0
        return len(self.image_data)

    @property
    def aspect_ratio(self) -> float:
        """Return the aspect ratio (width / height)."""
        if self.height == 0:
            return 0.0
        return self.width / self.height

    def is_valid(self) -> bool:
        """Check if the snapshot contains valid image data."""
        return self.image_data is not None and len(self.image_data) > 0


@dataclass
class CaptureOptions:
    """Options for window capture operations."""
    include_decorations: bool = True
    include_shadow: bool = False
    scale_factor: float = 1.0
    format: str = "png"
    quality: int = 95


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
    Find the window ID for a window by its name.

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


def get_window_bounds(window_id: str) -> Optional[Tuple[int, int, int, int]]:
    """
    Get the bounding rectangle of a window.

    Args:
        window_id: The window identifier.

    Returns:
        Tuple of (x, y, width, height) if available, None otherwise.
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
                    return tuple(int(p.strip()) for p in parts)
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, OSError):
            pass
    return None


# ----------------------------------------------------------------------
# Screen Capture (macOS)
# ----------------------------------------------------------------------


def _capture_screen_region_macos(
    x: int,
    y: int,
    width: int,
    height: int,
    output_path: str,
) -> bool:
    """
    Capture a screen region using screencapture on macOS.

    Args:
        x: Left coordinate.
        y: Top coordinate.
        width: Width of the region.
        height: Height of the region.
        output_path: Path to save the image.

    Returns:
        True if capture succeeded, False otherwise.
    """
    cmd = [
        "screencapture",
        "-x",                   # silent mode
        "-R", f"{x},{y},{width},{height}",
        output_path,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


# ----------------------------------------------------------------------
# Core Capture Functions
# ----------------------------------------------------------------------


def capture_window(
    window_identifier: str,
    output_path: Optional[str] = None,
    options: Optional[CaptureOptions] = None,
) -> WindowSnapshot:
    """
    Capture the content of a window as an image.

    Args:
        window_identifier: Window name, window ID, or process name.
        output_path: Optional path to save the image. If None,
            a temporary file is used.
        options: Optional capture configuration.

    Returns:
        WindowSnapshot object containing the captured image and metadata.

    Example:
        >>> snapshot = capture_window("Safari", "/tmp/safari.png")
        >>> if snapshot.is_valid():
        ...     print(f"Captured {snapshot.width}x{snapshot.height}")
    """
    opts = options or CaptureOptions()

    # Resolve window name to ID
    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return WindowSnapshot(
                image_data=None,
                width=0,
                height=0,
                window_name=window_identifier,
                window_id="",
                timestamp=time.time(),
            )

    # Get window bounds
    bounds = get_window_bounds(wid)
    if bounds is None:
        return WindowSnapshot(
            image_data=None,
            width=0,
            height=0,
            window_name=window_identifier,
            window_id=wid,
            timestamp=time.time(),
        )

    x, y, width, height = bounds

    # Determine output path
    import os
    import tempfile
    if output_path is None:
        fd, output_path = tempfile.mkstemp(suffix=".png")
        import os as os_module
        os_module.close(fd)

    # Capture
    success = False
    if get_platform() == "darwin":
        success = _capture_screen_region_macos(x, y, width, height, output_path)

    # Read result
    image_data = None
    actual_width = width
    actual_height = height

    if success:
        try:
            with open(output_path, "rb") as f:
                image_data = f.read()
        except (IOError, OSError):
            image_data = None

        # Clean up temp file
        if output_path.startswith(tempfile.gettempdir()):
            try:
                import os as os_module
                os_module.remove(output_path)
            except OSError:
                pass

    return WindowSnapshot(
        image_data=image_data,
        width=actual_width,
        height=actual_height,
        window_name=window_identifier,
        window_id=wid,
        timestamp=time.time(),
        format=opts.format,
    )


def capture_window_region(
    window_identifier: str,
    region: Tuple[int, int, int, int],
    output_path: Optional[str] = None,
) -> WindowSnapshot:
    """
    Capture a specific region of a window.

    Args:
        window_identifier: Window name, window ID, or process name.
        region: Tuple of (x_offset, y_offset, width, height) relative
            to the window's top-left corner.
        output_path: Optional path to save the image.

    Returns:
        WindowSnapshot object for the captured region.

    Example:
        >>> snap = capture_window_region("Safari", (10, 10, 400, 300))
    """
    opts = CaptureOptions()

    # Resolve window
    wid = window_identifier
    if not wid.isdigit():
        wid = get_window_id_by_name(window_identifier)
        if wid is None:
            return WindowSnapshot(
                image_data=None,
                width=0,
                height=0,
                window_name=window_identifier,
                window_id="",
                timestamp=time.time(),
            )

    # Get window bounds
    bounds = get_window_bounds(wid)
    if bounds is None:
        return WindowSnapshot(
            image_data=None,
            width=0,
            height=0,
            window_name=window_identifier,
            window_id=wid,
            timestamp=time.time(),
        )

    win_x, win_y, win_width, win_height = bounds
    x_off, y_off, reg_width, reg_height = region

    # Convert to absolute screen coordinates
    abs_x = win_x + x_off
    abs_y = win_y + y_off

    # Determine output path
    import os
    import tempfile
    if output_path is None:
        fd, output_path = tempfile.mkstemp(suffix=".png")
        os.close(fd)

    # Capture
    success = False
    if get_platform() == "darwin":
        success = _capture_screen_region_macos(
            abs_x, abs_y, reg_width, reg_height, output_path
        )

    # Read result
    image_data = None
    if success:
        try:
            with open(output_path, "rb") as f:
                image_data = f.read()
        except (IOError, OSError):
            image_data = None

        if output_path.startswith(tempfile.gettempdir()):
            try:
                os.remove(output_path)
            except OSError:
                pass

    return WindowSnapshot(
        image_data=image_data,
        width=reg_width,
        height=reg_height,
        window_name=window_identifier,
        window_id=wid,
        timestamp=time.time(),
        format=opts.format,
    )


def capture_all_windows(process_name: Optional[str] = None) -> list[WindowSnapshot]:
    """
    Capture all windows, optionally filtered by process name.

    Args:
        process_name: Optional process name filter. If None,
            captures all windows.

    Returns:
        List of WindowSnapshot objects.

    Example:
        >>> snapshots = capture_all_windows("Safari")
    """
    if get_platform() != "darwin":
        return []

    script = '''
    tell application "System Events"
        set results to {}
        repeat with proc in processes
            if background only of proc is false then
                repeat with win in windows of proc
                    set end of results to ¬
                        ((unix id of win) as string) & ¬
                        "|" & (name of win) & "|" & ¬
                        ((position of win as string) & "," & ¬
                        (size of win as string))
                end repeat
            end if
        end repeat
        return results
    end tell
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return []

        window_list = []
        for line in result.stdout.strip().split("\n"):
            if "|" not in line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue

            wid = parts[0]
            name = parts[1]
            coords_str = parts[2] if len(parts) > 2 else ""

            if process_name and process_name.lower() not in name.lower():
                continue

            snapshot = capture_window(wid)
            window_list.append(snapshot)

        return window_list
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return []
