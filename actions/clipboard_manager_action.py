"""
Clipboard Manager Action Module.

Manages system clipboard operations for text, HTML, and image
content with format conversion and clipboard history.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Optional, Union


@dataclass
class ClipboardEntry:
    """A clipboard entry with metadata."""
    content: str
    format: str
    timestamp: float
    source: Optional[str] = None


class ClipboardManager:
    """Manages clipboard operations and history."""

    def __init__(self, max_history: int = 50):
        """
        Initialize clipboard manager.

        Args:
            max_history: Maximum clipboard history entries.
        """
        self.max_history = max_history
        self._history: deque[ClipboardEntry] = deque(maxlen=max_history)
        self._current_content: Optional[str] = None

    def copy_text(
        self,
        text: str,
        format: str = "plain",
        source: Optional[str] = None,
    ) -> None:
        """
        Copy text to clipboard.

        Args:
            text: Text content.
            format: Format type ('plain', 'html', 'rtf').
            source: Optional source identifier.
        """
        self._current_content = text
        entry = ClipboardEntry(
            content=text,
            format=format,
            timestamp=time.time(),
            source=source,
        )
        self._history.append(entry)

    def paste_text(self) -> Optional[str]:
        """
        Get current clipboard text.

        Returns:
            Clipboard content or None.
        """
        return self._current_content

    def get_history(
        self,
        format: Optional[str] = None,
        limit: int = 10,
    ) -> list[ClipboardEntry]:
        """
        Get clipboard history.

        Args:
            format: Filter by format type.
            limit: Maximum entries to return.

        Returns:
            List of ClipboardEntry objects.
        """
        entries = list(self._history)

        if format:
            entries = [e for e in entries if e.format == format]

        return entries[-limit:]

    def search_history(
        self,
        query: str,
        case_sensitive: bool = False,
    ) -> list[ClipboardEntry]:
        """
        Search clipboard history.

        Args:
            query: Search query.
            case_sensitive: Whether search is case-sensitive.

        Returns:
            Matching entries.
        """
        results = []
        search_query = query if case_sensitive else query.lower()

        for entry in self._history:
            content = entry.content if case_sensitive else entry.content.lower()
            if search_query in content:
                results.append(entry)

        return results

    def clear_history(self) -> None:
        """Clear clipboard history."""
        self._history.clear()

    def get_last(
        self,
        format: Optional[str] = None,
    ) -> Optional[ClipboardEntry]:
        """
        Get most recent clipboard entry.

        Args:
            format: Optional format filter.

        Returns:
            Most recent entry or None.
        """
        history = self.get_history(format=format, limit=1)
        return history[-1] if history else None

    def has_content(self) -> bool:
        """Check if clipboard has content."""
        return self._current_content is not None
