"""
Clipboard Utilities

Provides utilities for clipboard operations
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any
import subprocess


class ClipboardManager:
    """
    Manages clipboard operations.
    
    Supports reading, writing, and monitoring
    clipboard content.
    """

    def __init__(self) -> None:
        self._last_content: str | None = None
        self._history: list[str] = []
        self._max_history = 50

    def copy(self, text: str) -> bool:
        """
        Copy text to clipboard.
        
        Args:
            text: Text to copy.
            
        Returns:
            True if successful.
        """
        try:
            process = subprocess.run(
                ["pbcopy"],
                input=text.encode("utf-8"),
                shell=True,
                check=False,
            )
            if process.returncode == 0:
                self._last_content = text
                self._add_to_history(text)
                return True
            return False
        except Exception:
            return False

    def paste(self) -> str | None:
        """
        Get text from clipboard.
        
        Returns:
            Clipboard text or None.
        """
        try:
            result = subprocess.run(
                ["pbpaste"],
                capture_output=True,
                text=True,
                shell=True,
                check=False,
            )
            if result.returncode == 0:
                content = result.stdout
                self._last_content = content
                return content
            return None
        except Exception:
            return None

    def clear(self) -> bool:
        """
        Clear clipboard content.
        
        Returns:
            True if successful.
        """
        return self.copy("")

    def get_last_content(self) -> str | None:
        """Get the last known clipboard content."""
        return self._last_content

    def get_history(self) -> list[str]:
        """Get clipboard history."""
        return list(self._history)

    def _add_to_history(self, text: str) -> None:
        """Add text to history."""
        if text and text not in self._history:
            self._history.append(text)
            if len(self._history) > self._max_history:
                self._history.pop(0)

    def has_changed(self) -> bool:
        """Check if clipboard content has changed."""
        current = self.paste()
        if current != self._last_content:
            self._last_content = current
            return True
        return False


# Global clipboard manager instance
_clipboard = ClipboardManager()


def copy(text: str) -> bool:
    """Copy text to clipboard."""
    return _clipboard.copy(text)


def paste() -> str | None:
    """Get text from clipboard."""
    return _clipboard.paste()


def clear() -> bool:
    """Clear clipboard."""
    return _clipboard.clear()


def clipboard_has_changed() -> bool:
    """Check if clipboard changed since last access."""
    return _clipboard.has_changed()
