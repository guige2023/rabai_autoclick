"""
Screen capture actions for screenshots and screen recording.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any


def take_screenshot(
    output_path: Optional[str] = None,
    include_cursor: bool = True,
    display: Optional[str] = None
) -> str:
    """
    Take a screenshot of the screen.

    Args:
        output_path: Path to save the screenshot. If None, saves to /tmp/screenshot.png.
        include_cursor: Whether to include the cursor in the screenshot.
        display: Specific display to capture (None for main display).

    Returns:
        Path to the saved screenshot file.

    Raises:
        RuntimeError: If screenshot capture fails.
    """
    if output_path is None:
        output_path = '/tmp/screenshot.png'

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    cmd = ['screencapture']

    if not include_cursor:
        cmd.append('-C')

    if display:
        cmd.extend(['-d', display])

    cmd.extend(['-x', output_path])

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"screencapture failed: {result.stderr.decode('utf-8', errors='replace')}")

        if not os.path.exists(output_path):
            raise RuntimeError(f"Screenshot file was not created at: {output_path}")

        return output_path
    except subprocess.TimeoutExpired:
        raise RuntimeError("Screenshot capture timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to take screenshot: {e}")


def capture_window(
    window_id: Optional[str] = None,
    output_path: Optional[str] = None,
    include_cursor: bool = False
) -> str:
    """
    Capture a specific window by ID.

    Args:
        window_id: Window ID to capture (None for interactive selection).
        output_path: Path to save the screenshot.
        include_cursor: Whether to include the cursor.

    Returns:
        Path to the saved screenshot.

    Raises:
        RuntimeError: If window capture fails.
    """
    if output_path is None:
        output_path = '/tmp/window_capture.png'

    cmd = ['screencapture']

    if not include_cursor:
        cmd.append('-C')

    if window_id:
        cmd.extend(['-x', '-l', window_id, output_path])
    else:
        cmd.extend(['-iW', output_path])

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Window capture failed: {result.stderr.decode('utf-8', errors='replace')}")

        return output_path
    except subprocess.TimeoutExpired:
        raise RuntimeError("Window capture timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to capture window: {e}")


def capture_region(
    x: int,
    y: int,
    width: int,
    height: int,
    output_path: Optional[str] = None
) -> str:
    """
    Capture a specific region of the screen.

    Args:
        x: X coordinate of the top-left corner.
        y: Y coordinate of the top-left corner.
        width: Width of the region.
        height: Height of the region.
        output_path: Path to save the screenshot.

    Returns:
        Path to the saved screenshot.

    Raises:
        RuntimeError: If region capture fails.
    """
    if output_path is None:
        output_path = '/tmp/region_capture.png'

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    import Quartz

    rect = Quartz.CGRectMake(x, y, width, height)

    try:
        image = Quartz.CGWindowListCreateImage(
            rect,
            Quartz.kCGWindowListOptionOnScreenOnly,
            Quartz.kCGNullWindowID,
            Quartz.kCGWindowImageBoundsIgnoreFraming
        )

        if image is None:
            raise RuntimeError("Failed to capture screen region")

        save_path = Quartz.CGImageWriteToFile(image, output_path)

        if not save_path:
            raise RuntimeError(f"Failed to save screenshot to: {output_path}")

        return output_path
    except Exception as e:
        raise RuntimeError(f"Failed to capture region: {e}")


def list_windows() -> List[Dict[str, Any]]:
    """
    List all open windows with their IDs and names.

    Returns:
        List of window information dictionaries.
    """
    import Quartz

    window_list = Quartz.CGWindowListCopyWindowInfo(
        Quartz.kCGWindowListOptionOnScreenOnly | Quartz.kCGWindowListExcludeDesktopElements,
        Quartz.kCGNullWindowID
    )

    windows: List[Dict[str, Any]] = []

    for window in window_list:
        window_id = window.get('kCGWindowNumber')
        owner_name = window.get('kCGWindowOwnerName', '')
        window_name = window.get('kCGWindowName', '')

        bounds = window.get('kCGWindowBounds', {})
        window_info = {
            'id': window_id,
            'owner': owner_name,
            'name': window_name,
            'x': bounds.get('X', 0),
            'y': bounds.get('Y', 0),
            'width': bounds.get('Width', 0),
            'height': bounds.get('Height', 0),
        }
        windows.append(window_info)

    return windows


def find_window_by_name(name: str, partial: bool = True) -> Optional[Dict[str, Any]]:
    """
    Find a window by its name.

    Args:
        name: Window name to search for.
        partial: If True, match partial names (case-insensitive).

    Returns:
        Window information dictionary or None if not found.
    """
    windows = list_windows()

    name_lower = name.lower()

    for window in windows:
        window_name = window.get('name', '').lower()
        owner_name = window.get('owner', '').lower()

        if partial:
            if name_lower in window_name or name_lower in owner_name:
                return window
        else:
            if window_name == name_lower or owner_name == name_lower:
                return window

    return None


def get_screen_dimensions(display: Optional[int] = None) -> Dict[str, int]:
    """
    Get screen dimensions for a display.

    Args:
        display: Display index (None for main display).

    Returns:
        Dictionary with 'width' and 'height' of the screen.
    """
    import Quartz

    if display is None:
        display = Quartz.CGMainDisplayID()

    mode = Quartz.CGDisplayCopyDisplayMode(display)

    if mode is None:
        raise RuntimeError(f"Could not get display mode for display {display}")

    return {
        'width': Quartz.CGDisplayPixelsWide(display),
        'height': Quartz.CGDisplayPixelsHigh(display),
        'mode_width': Quartz.CGDisplayModeGetWidth(mode),
        'mode_height': Quartz.CGDisplayModeGetHeight(mode),
    }


def start_screen_recording(
    output_path: Optional[str] = None,
    duration: Optional[int] = None
) -> str:
    """
    Start screen recording using macOS screen recording.

    Note: This requires proper permissions and uses macOS QuickTimePlayer.

    Args:
        output_path: Path to save the recording.
        duration: Recording duration in seconds (None for manual stop).

    Returns:
        Path to the saved recording file.

    Raises:
        RuntimeError: If screen recording fails.
    """
    if output_path is None:
        output_path = '/tmp/screen_recording.mov'

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    try:
        script = f'''
        tell application "QuickTime Player"
            activate
            set newMovie to new screen recording
            tell newMovie
                start
            end tell
        end tell
        '''

        if duration is not None:
            script += f'''
            delay {duration}
            tell application "System Events"
                keystroke "q" using command down
            end tell
            '''

        subprocess.run(['osascript', '-e', script], timeout=60)

        return output_path
    except subprocess.TimeoutExpired:
        raise RuntimeError("Screen recording setup timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to start screen recording: {e}")


def capture_screenshot_to_clipboard(
    include_cursor: bool = True,
    display: Optional[str] = None
) -> bool:
    """
    Capture screenshot directly to clipboard.

    Args:
        include_cursor: Whether to include the cursor.
        display: Specific display to capture.

    Returns:
        True if successful.

    Raises:
        RuntimeError: If clipboard capture fails.
    """
    cmd = ['screencapture', '-c']

    if not include_cursor:
        cmd.append('-C')

    if display:
        cmd.extend(['-d', display])

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"Clipboard capture failed: {result.stderr.decode('utf-8', errors='replace')}")
        return True
    except subprocess.TimeoutExpired:
        raise RuntimeError("Clipboard capture timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to capture to clipboard: {e}")


def capture_annotated_screenshot(
    output_path: Optional[str] = None,
    delay: int = 0
) -> str:
    """
    Capture screenshot and open in Preview for annotation.

    Args:
        output_path: Path to save the screenshot.
        delay: Delay in seconds before capture.

    Returns:
        Path to the saved screenshot.

    Raises:
        RuntimeError: If screenshot capture fails.
    """
    if output_path is None:
        output_path = '/tmp/annotated_screenshot.png'

    output_dir = os.path.dirname(output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    cmd = ['screencapture', '-iP']

    if delay > 0:
        cmd.extend(['-T', str(delay)])

    cmd.append(output_path)

    try:
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"Annotated capture failed: {result.stderr.decode('utf-8', errors='replace')}")

        return output_path
    except subprocess.TimeoutExpired:
        raise RuntimeError("Annotated capture timed out")
    except Exception as e:
        raise RuntimeError(f"Failed to capture annotated screenshot: {e}")
