"""Clipboard Change Detector Utilities.

Detects and monitors clipboard content changes.

Example:
    >>> from clipboard_change_detector_utils import ClipboardChangeDetector
    >>> detector = ClipboardChangeDetector()
    >>> detector.start()
    >>> if detector.has_changed():
    ...     print(detector.get_content())
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class ClipboardChange:
    """A clipboard change event."""
    content: str
    content_type: str
    timestamp: float
    source: str = "unknown"


class ClipboardChangeDetector:
    """Detects clipboard content changes."""

    def __init__(self, poll_interval: float = 0.5):
        """Initialize detector.

        Args:
            poll_interval: Seconds between clipboard polls.
        """
        self.poll_interval = poll_interval
        self._last_content: str = ""
        self._last_hash: int = 0
        self._has_changed: bool = False
        self._change: Optional[ClipboardChange] = None
        self._handlers: list[Callable[[ClipboardChange], None]] = []

    def check(self, current_content: str, source: str = "unknown") -> bool:
        """Check for clipboard changes.

        Args:
            current_content: Current clipboard content.
            source: Source of the content.

        Returns:
            True if content changed since last check.
        """
        content_hash = hash(current_content)
        if content_hash != self._last_hash:
            self._last_content = current_content
            self._last_hash = content_hash
            self._has_changed = True
            self._change = ClipboardChange(
                content=current_content,
                content_type=self._detect_type(current_content),
                timestamp=time.time(),
                source=source,
            )
            for handler in self._handlers:
                handler(self._change)
            return True
        return False

    def has_changed(self) -> bool:
        """Check if clipboard changed since last check.

        Returns:
            True if changed.
        """
        return self._has_changed

    def get_change(self) -> Optional[ClipboardChange]:
        """Get the last change.

        Returns:
            ClipboardChange or None.
        """
        return self._change

    def get_content(self) -> str:
        """Get current clipboard content.

        Returns:
            Content string.
        """
        return self._last_content

    def acknowledge_change(self) -> None:
        """Acknowledge the current change (clear flag)."""
        self._has_changed = False

    def on_change(self, handler: Callable[[ClipboardChange], None]) -> None:
        """Register a change handler.

        Args:
            handler: Called on clipboard change.
        """
        self._handlers.append(handler)

    def _detect_type(self, content: str) -> str:
        """Detect content type.

        Args:
            content: Content to analyze.

        Returns:
            Content type string.
        """
        if not content:
            return "empty"
        if content.startswith("http://") or content.startswith("https://"):
            return "url"
        if "\n" in content or "\t" in content:
            return "text"
        return "plain"
