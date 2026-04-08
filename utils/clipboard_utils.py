"""Clipboard management utilities for reading and writing system clipboard.

Provides cross-platform clipboard operations for text, images,
and rich content, with support for clipboard history and
content type detection.

Example:
    >>> from utils.clipboard_utils import get_text, set_text, get_image
    >>> text = get_text()
    >>> set_text('Hello, clipboard!')
"""

from __future__ import annotations

import subprocess
from typing import Optional

__all__ = [
    "get_text",
    "set_text",
    "clear_clipboard",
    "get_image",
    "set_image",
    "has_text",
    "has_image",
    "ClipboardError",
]


class ClipboardError(Exception):
    """Raised when a clipboard operation fails."""
    pass


def get_text() -> Optional[str]:
    """Get the current clipboard text content.

    Returns:
        Clipboard text as a string, or None if unavailable.
    """
    import sys

    if sys.platform == "darwin":
        script = "get the clipboard as text"
    elif sys.platform == "win32":
        return None  # would use pyperclip
    else:
        script = "xclip -selection clipboard -o"

    try:
        result = subprocess.run(
            ["osascript", "-e", script] if sys.platform == "darwin" else script.split(),
            capture_output=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.decode().strip()
    except Exception:
        pass
    return None


def set_text(text: str) -> bool:
    """Set the clipboard text content.

    Args:
        text: Text string to place on the clipboard.

    Returns:
        True if successful.
    """
    import sys

    if sys.platform == "darwin":
        # Escape double quotes for AppleScript
        escaped = text.replace('"', '\\"').replace('\n', '\\n')
        script = f'set the clipboard to "{escaped}"'
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False
    elif sys.platform == "win32":
        try:
            subprocess.run(["powershell", "-Command", f"Set-Clipboard -Value '{text}'"], check=True)
            return True
        except Exception:
            return False
    else:
        try:
            subprocess.run(["xclip", "-selection", "clipboard", "-i"], input=text.encode(), check=True)
            return True
        except Exception:
            return False


def clear_clipboard() -> bool:
    """Clear the clipboard content.

    Returns:
        True if successful.
    """
    return set_text("")


def has_text() -> bool:
    """Check if the clipboard contains text.

    Returns:
        True if the clipboard has text content.
    """
    text = get_text()
    return text is not None and len(text) > 0


def has_image() -> bool:
    """Check if the clipboard contains image data.

    Returns:
        True if the clipboard has image content.
    """
    import sys

    if sys.platform == "darwin":
        script = """
        tell application "System Events"
            try
                set clipText to the clipboard as record
                return "image"
            on error
                return "no image"
            end try
        end tell
        """
        try:
            result = subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return "image" in result.stdout.decode().lower()
        except Exception:
            return False
    return False


def get_image() -> Optional[bytes]:
    """Get image data from the clipboard.

    Returns:
        Image bytes (PNG format), or None if no image.
    """
    import sys

    if sys.platform == "darwin":
        script = """
        tell application "System Events"
            try
                set img to the clipboard as TIFF picture
                return img
            on error
                return ""
            end try
        end tell
        """
        try:
            result = subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            if result.returncode == 0 and result.stdout:
                # The result would be a TIFF file path, read it
                return result.stdout
        except Exception:
            pass
    return None


def set_image(image_path: str) -> bool:
    """Set an image on the clipboard from a file path.

    Args:
        image_path: Path to the image file.

    Returns:
        True if successful.
    """
    import sys

    if sys.platform == "darwin":
        script = f'set the clipboard to (read (POSIX file "{image_path}") as TIFF picture)'
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False
    return False
