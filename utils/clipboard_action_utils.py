"""
Clipboard action utilities for copy/paste operations.

Provides clipboard management for text, images, and files
with support for format conversion and history.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ClipboardContent:
    """Content stored in clipboard."""
    text: Optional[str] = None
    image_path: Optional[str] = None
    files: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return self.text is None and self.image_path is None and not self.files

    def has_text(self) -> bool:
        return self.text is not None and len(self.text) > 0


class ClipboardManager:
    """Manages clipboard operations."""

    def __init__(self):
        self._history: list[ClipboardContent] = []
        self._max_history: int = 20

    def copy_text(self, text: str) -> bool:
        """Copy text to clipboard."""
        try:
            process = subprocess.Popen(
                ["pbcopy"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            process.communicate(input=text.encode("utf-8"))
            self._add_to_history(ClipboardContent(text=text))
            return process.returncode == 0
        except Exception:
            return False

    def paste_text(self) -> Optional[str]:
        """Get text from clipboard."""
        try:
            process = subprocess.Popen(
                ["pbpaste"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            stdout, _ = process.communicate()
            if process.returncode == 0:
                return stdout.decode("utf-8")
            return None
        except Exception:
            return None

    def copy_image(self, image_path: str) -> bool:
        """Copy image file to clipboard."""
        try:
            script = f'''
            set the clipboard to (read (POSIX file "{image_path}") as JPEG picture)
            '''
            subprocess.run(["osascript", "-e", script], check=True, capture_output=True)
            self._add_to_history(ClipboardContent(image_path=image_path))
            return True
        except Exception:
            return False

    def copy_files(self, file_paths: list[str]) -> bool:
        """Copy files to clipboard."""
        try:
            paths_str = "\n".join(file_paths)
            process = subprocess.Popen(
                ["pbcopy"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            process.communicate(input=paths_str.encode("utf-8"))
            self._add_to_history(ClipboardContent(files=file_paths))
            return process.returncode == 0
        except Exception:
            return False

    def clear(self) -> bool:
        """Clear the clipboard."""
        try:
            subprocess.run(["pbcopy"], input=b"", check=True, capture_output=True)
            return True
        except Exception:
            return False

    def has_text(self) -> bool:
        """Check if clipboard has text content."""
        return self.paste_text() is not None and len(self.paste_text() or "") > 0

    def get_current_content(self) -> ClipboardContent:
        """Get current clipboard content."""
        text = self.paste_text()
        return ClipboardContent(text=text)

    def history(self, count: int = 10) -> list[ClipboardContent]:
        """Get clipboard history."""
        return self._history[-count:]

    def _add_to_history(self, content: ClipboardContent) -> None:
        if not content.is_empty:
            self._history.append(content)
            if len(self._history) > self._max_history:
                self._history.pop(0)


def quick_copy(text: str) -> bool:
    """Quick utility to copy text to clipboard."""
    manager = ClipboardManager()
    return manager.copy_text(text)


def quick_paste() -> Optional[str]:
    """Quick utility to get text from clipboard."""
    manager = ClipboardManager()
    return manager.paste_text()


__all__ = ["ClipboardManager", "ClipboardContent", "quick_copy", "quick_paste"]
