"""Clipboard synchronization utilities.

Provides cross-platform clipboard operations with change detection
and synchronization support for automation workflows.
"""

import subprocess
from typing import Optional


def get_clipboard_text() -> str:
    """Get current clipboard text content.

    Returns:
        Clipboard text or empty string if empty.
    """
    try:
        return subprocess.run(
            ["pbpaste"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
    except Exception:
        return ""


def set_clipboard_text(text: str) -> bool:
    """Set clipboard text content.

    Args:
        text: Text to copy to clipboard.

    Returns:
        True if successful, False otherwise.
    """
    try:
        subprocess.run(
            ["pbcopy"],
            input=text,
            text=True,
            check=True,
        )
        return True
    except Exception:
        return False


def clear_clipboard() -> bool:
    """Clear the clipboard.

    Returns:
        True if successful, False otherwise.
    """
    return set_clipboard_text("")


def clipboard_has_text() -> bool:
    """Check if clipboard contains text.

    Returns:
        True if clipboard has non-empty text.
    """
    return len(get_clipboard_text().strip()) > 0


class ClipboardMonitor:
    """Monitor clipboard for changes.

    Args:
        poll_interval: Seconds between polls.
    """

    def __init__(self, poll_interval: float = 0.5) -> None:
        self._poll_interval = poll_interval
        self._last_content: str = ""
        self._has_changed = False

    def start(self) -> None:
        """Start monitoring clipboard."""
        self._last_content = get_clipboard_text()

    def check(self) -> bool:
        """Check for clipboard changes since last check.

        Returns:
            True if clipboard changed since last check().
        """
        current = get_clipboard_text()
        if current != self._last_content:
            self._last_content = current
            self._has_changed = True
            return True
        return False

    @property
    def last_content(self) -> str:
        """Get last clipboard content."""
        return self._last_content

    @property
    def changed(self) -> bool:
        """Check if changed since start."""
        return self._has_changed
