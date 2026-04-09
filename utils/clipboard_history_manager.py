"""
Clipboard history management utilities.

Provides utilities for maintaining a history of clipboard operations,
storing previous clipboard contents, and navigating through clipboard
history. Useful for automation workflows that need to preserve and
restore clipboard contents.

Example:
    >>> from utils.clipboard_history_manager import ClipboardHistory, history
    >>> history.push("copied text")
    >>> item = history.pop()
    >>> history.save_to_file()
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from typing import Any, List, Optional

try:
    from dataclasses import dataclass, field
except ImportError:
    from typing import dataclass, field


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class ClipboardItem:
    """
    Immutable clipboard history item.

    Attributes:
        content: The text content stored in the clipboard.
        timestamp: Unix timestamp when the item was added.
        label: Optional label or tag for the item.
    """
    content: str
    timestamp: float
    label: str = ""

    @property
    def age_seconds(self) -> float:
        """Return the age of this item in seconds."""
        return time.time() - self.timestamp

    @property
    def age_human(self) -> str:
        """Return a human-readable age string."""
        age = self.age_seconds
        if age < 60:
            return f"{age:.0f}s ago"
        elif age < 3600:
            return f"{age / 60:.0f}m ago"
        elif age < 86400:
            return f"{age / 3600:.0f}h ago"
        else:
            return f"{age / 86400:.0f}d ago"

    def with_label(self, label: str) -> "ClipboardItem":
        """Return a new item with a different label."""
        return ClipboardItem(
            content=self.content,
            timestamp=self.timestamp,
            label=label,
        )


@dataclass
class ClipboardHistoryConfig:
    """Configuration for clipboard history management."""
    max_size: int = 100
    persistent: bool = False
    storage_path: str = ""
    deduplicate: bool = True
    timestamp_threshold: float = 0.5  # seconds between items


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Clipboard Operations
# ----------------------------------------------------------------------


def read_clipboard_text() -> str:
    """
    Read the current text content of the system clipboard.

    Returns:
        The clipboard text content, or empty string if none.
    """
    if get_platform() == "darwin":
        try:
            result = subprocess.run(
                ["pbpaste"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return ""


def write_clipboard_text(text: str) -> bool:
    """
    Write text to the system clipboard.

    Args:
        text: The text to write to the clipboard.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() == "darwin":
        try:
            result = subprocess.run(
                ["pbcopy"],
                input=text,
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
    return False


def clear_clipboard() -> bool:
    """
    Clear the system clipboard.

    Returns:
        True if successful, False otherwise.
    """
    return write_clipboard_text("")


# ----------------------------------------------------------------------
# ClipboardHistory Class
# ----------------------------------------------------------------------


class ClipboardHistory:
    """
    Manages a history stack of clipboard items.

    Supports push/pop operations, persistence to disk,
    deduplication, and navigation through history.

    Example:
        >>> hist = ClipboardHistory(max_size=50)
        >>> hist.push("hello")
        >>> hist.push("world")
        >>> hist.pop()
        ClipboardItem(content='world', ...)
    """

    def __init__(self, config: Optional[ClipboardHistoryConfig] = None):
        self._config = config or ClipboardHistoryConfig()
        self._items: List[ClipboardItem] = []
        self._last_push_time: float = 0.0

        if self._config.persistent and self._config.storage_path:
            self._load_from_disk()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Return the number of items in history."""
        return len(self._items)

    @property
    def is_empty(self) -> bool:
        """Return True if history is empty."""
        return len(self._items) == 0

    @property
    def latest(self) -> Optional[ClipboardItem]:
        """Return the most recent item without removing it."""
        if self._items:
            return self._items[-1]
        return None

    @property
    def all_items(self) -> List[ClipboardItem]:
        """Return a copy of all items (most recent last)."""
        return list(self._items)

    # ------------------------------------------------------------------
    # Core Operations
    # ------------------------------------------------------------------

    def push(
        self,
        content: str,
        label: str = "",
        skip_duplicate: bool = True,
    ) -> bool:
        """
        Push a new item onto the clipboard history.

        Args:
            content: The text content to store.
            label: Optional label for this item.
            skip_duplicate: If True, skip if identical to the latest item.

        Returns:
            True if the item was added, False if skipped.
        """
        if not content:
            return False

        # Deduplicate if enabled and matches most recent
        if skip_duplicate and self._config.deduplicate and self._items:
            if self._items[-1].content == content:
                return False

        # Time-based threshold check
        current_time = time.time()
        if current_time - self._last_push_time < \
                self._config.timestamp_threshold:
            # Same content check
            if self._items and self._items[-1].content == content:
                return False

        self._last_push_time = current_time

        item = ClipboardItem(
            content=content,
            timestamp=current_time,
            label=label,
        )
        self._items.append(item)

        # Enforce max size
        while len(self._items) > self._config.max_size:
            self._items.pop(0)

        if self._config.persistent:
            self._save_to_disk()

        return True

    def pop(self) -> Optional[ClipboardItem]:
        """
        Pop the most recent item from history.

        Returns:
            The most recent ClipboardItem, or None if empty.
        """
        if not self._items:
            return None

        item = self._items.pop()
        if self._config.persistent:
            self._save_to_disk()
        return item

    def peek(self, index: int = -1) -> Optional[ClipboardItem]:
        """
        Peek at an item without removing it.

        Args:
            index: Index from the end (-1 is most recent, -2 is second most).

        Returns:
            The ClipboardItem at the index, or None if out of range.
        """
        try:
            return self._items[index]
        except IndexError:
            return None

    def clear(self) -> None:
        """Clear all items from history."""
        self._items.clear()
        if self._config.persistent:
            self._save_to_disk()

    def find(self, query: str) -> List[ClipboardItem]:
        """
        Find all items containing a query string.

        Args:
            query: The search string.

        Returns:
            List of matching ClipboardItems (most recent last).
        """
        return [item for item in self._items if query in item.content]

    # ------------------------------------------------------------------
    # Clipboard Integration
    # ------------------------------------------------------------------

    def copy_to_clipboard(self, content: str) -> bool:
        """
        Push content to history and copy to system clipboard.

        Args:
            content: The text to store and copy.

        Returns:
            True if both operations succeeded.
        """
        if not self.push(content):
            return False
        return write_clipboard_text(content)

    def restore_latest(self) -> bool:
        """
        Restore the most recent item to the system clipboard.

        Returns:
            True if successful, False if history is empty.
        """
        item = self.latest
        if item is None:
            return False
        return write_clipboard_text(item.content)

    def save_current_clipboard(self, label: str = "") -> bool:
        """
        Save the current system clipboard content to history.

        Args:
            label: Optional label for the saved content.

        Returns:
            True if content was saved, False if clipboard was empty.
        """
        content = read_clipboard_text()
        if not content:
            return False
        return self.push(content, label=label)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _get_storage_path(self) -> str:
        """Get the path for persistent storage."""
        if self._config.storage_path:
            return self._config.storage_path
        return os.path.expanduser("~/.clipboard_history.json")

    def _save_to_disk(self) -> None:
        """Save history to disk as JSON."""
        path = self._get_storage_path()
        data = [
            {"content": item.content, "timestamp": item.timestamp, "label": item.label}
            for item in self._items
        ]
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except (IOError, OSError):
            pass

    def _load_from_disk(self) -> None:
        """Load history from disk."""
        path = self._get_storage_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._items = [
                ClipboardItem(
                    content=item["content"],
                    timestamp=item["timestamp"],
                    label=item.get("label", ""),
                )
                for item in data
                if "content" in item and "timestamp" in item
            ]
        except (IOError, OSError, json.JSONDecodeError):
            pass

    # ------------------------------------------------------------------
    # Navigation
    # ------------------------------------------------------------------

    def navigate_previous(self) -> Optional[str]:
        """
        Navigate to the previous item in history and copy to clipboard.

        Returns:
            The content of the previous item, or None.
        """
        if not self._items:
            return None
        # Move last item to front
        if len(self._items) > 1:
            item = self._items.pop()
            self._items.insert(0, item)
            write_clipboard_text(item.content)
            if self._config.persistent:
                self._save_to_disk()
            return item.content
        return None

    def navigate_next(self) -> Optional[str]:
        """
        Navigate to the next item in history and copy to clipboard.

        Returns:
            The content of the next item, or None.
        """
        if not self._items:
            return None
        if len(self._items) > 1:
            item = self._items.pop(0)
            self._items.append(item)
            write_clipboard_text(item.content)
            if self._config.persistent:
                self._save_to_disk()
            return item.content
        return None


# ----------------------------------------------------------------------
# Global History Instance
# ----------------------------------------------------------------------


_default_config = ClipboardHistoryConfig(
    max_size=100,
    persistent=True,
    storage_path=os.path.expanduser("~/.clipboard_history.json"),
)

#: Global clipboard history instance
history = ClipboardHistory(_default_config)
