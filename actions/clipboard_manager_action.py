"""Clipboard manager action for clipboard operations.

Provides clipboard history, formatting support, and cross-platform
clipboard operations for text, images, and files.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ClipboardFormat(Enum):
    TEXT = "text"
    IMAGE = "image"
    FILES = "files"
    HTML = "html"


@dataclass
class ClipboardEntry:
    content: Any
    format: ClipboardFormat
    timestamp: float = field(default_factory=time.time)
    source_app: Optional[str] = None


class ClipboardManagerAction:
    """Manage clipboard operations with history tracking.

    Args:
        max_history: Maximum number of clipboard entries to retain.
        auto_clear_on_exit: Clear clipboard when action is destroyed.
    """

    def __init__(
        self,
        max_history: int = 50,
        auto_clear_on_exit: bool = False,
    ) -> None:
        self._history: list[ClipboardEntry] = []
        self._max_history = max_history
        self._auto_clear_on_exit = auto_clear_on_exit
        self._current_content: Optional[Any] = None
        self._current_format: Optional[ClipboardFormat] = None

    def copy_text(self, text: str, source_app: Optional[str] = None) -> None:
        """Copy text to clipboard.

        Args:
            text: Text content to copy.
            source_app: Optional source application name.
        """
        self._current_content = text
        self._current_format = ClipboardFormat.TEXT
        self._add_entry(text, ClipboardFormat.TEXT, source_app)
        logger.debug(f"Copied text to clipboard ({len(text)} chars)")

    def copy_image(self, image_data: bytes, source_app: Optional[str] = None) -> None:
        """Copy image data to clipboard.

        Args:
            image_data: Raw image bytes.
            source_app: Optional source application name.
        """
        self._current_content = image_data
        self._current_format = ClipboardFormat.IMAGE
        self._add_entry(image_data, ClipboardFormat.IMAGE, source_app)
        logger.debug(f"Copied image to clipboard ({len(image_data)} bytes)")

    def copy_files(self, file_paths: list[str], source_app: Optional[str] = None) -> None:
        """Copy file paths to clipboard.

        Args:
            file_paths: List of file paths to copy.
            source_app: Optional source application name.
        """
        self._current_content = file_paths
        self._current_format = ClipboardFormat.FILES
        self._add_entry(file_paths, ClipboardFormat.FILES, source_app)
        logger.debug(f"Copied {len(file_paths)} files to clipboard")

    def paste(self) -> Optional[Any]:
        """Get current clipboard content.

        Returns:
            Current clipboard content or None.
        """
        return self._current_content

    def get_format(self) -> Optional[ClipboardFormat]:
        """Get current clipboard format.

        Returns:
            Current clipboard format or None.
        """
        return self._current_format

    def _add_entry(
        self,
        content: Any,
        format: ClipboardFormat,
        source_app: Optional[str] = None,
    ) -> None:
        """Add entry to clipboard history.

        Args:
            content: Clipboard content.
            format: Content format.
            source_app: Source application.
        """
        entry = ClipboardEntry(
            content=content,
            format=format,
            timestamp=time.time(),
            source_app=source_app,
        )
        self._history.append(entry)
        if len(self._history) > self._max_history:
            self._history.pop(0)

    def get_history(
        self,
        format_filter: Optional[ClipboardFormat] = None,
        limit: int = 20,
    ) -> list[ClipboardEntry]:
        """Get clipboard history.

        Args:
            format_filter: Filter by format type.
            limit: Maximum entries to return.

        Returns:
            List of clipboard entries (newest first).
        """
        entries = self._history
        if format_filter:
            entries = [e for e in entries if e.format == format_filter]
        return entries[-limit:][::-1]

    def search_history(self, query: str, case_sensitive: bool = False) -> list[ClipboardEntry]:
        """Search clipboard history for text matches.

        Args:
            query: Search query string.
            case_sensitive: Whether search is case sensitive.

        Returns:
            Matching entries (newest first).
        """
        results = []
        for entry in reversed(self._history):
            if entry.format == ClipboardFormat.TEXT:
                content = str(entry.content)
                search_in = content if case_sensitive else content.lower()
                search_for = query if case_sensitive else query.lower()
                if search_for in search_in:
                    results.append(entry)
        return results

    def clear(self) -> None:
        """Clear current clipboard content."""
        self._current_content = None
        self._current_format = None
        logger.debug("Cleared clipboard")

    def clear_history(self) -> int:
        """Clear clipboard history.

        Returns:
            Number of entries cleared.
        """
        count = len(self._history)
        self._history.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get clipboard statistics.

        Returns:
            Dictionary with clipboard stats.
        """
        text_count = sum(1 for e in self._history if e.format == ClipboardFormat.TEXT)
        image_count = sum(1 for e in self._history if e.format == ClipboardFormat.IMAGE)
        files_count = sum(1 for e in self._history if e.format == ClipboardFormat.FILES)
        return {
            "total_entries": len(self._history),
            "max_history": self._max_history,
            "by_format": {
                "text": text_count,
                "image": image_count,
                "files": files_count,
            },
            "current_format": self._current_format.value if self._current_format else None,
        }

    def __del__(self) -> None:
        """Cleanup on destruction."""
        if self._auto_clear_on_exit:
            self.clear()
