"""
Clipboard Observer Utility

Monitors system clipboard for changes and triggers callbacks.
Useful for automation workflows that react to clipboard content.

Example:
    >>> observer = ClipboardObserver()
    >>> observer.watch(lambda content: print(f"Clipboard: {content}"))
    >>> observer.start()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, Any


class ClipboardContentType(Enum):
    """Types of clipboard content."""
    UNKNOWN = "unknown"
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    HTML = "html"
    RTF = "rtf"


@dataclass
class ClipboardChange:
    """Represents a clipboard change event."""
    content: Any
    content_type: ClipboardContentType
    timestamp: float
    size_bytes: int = 0
    source_app: Optional[str] = None


class ClipboardObserver:
    """
    Observes system clipboard for changes.

    Args:
        poll_interval: Seconds between clipboard checks (default 0.5).
        callback: Function called on clipboard change.
    """

    def __init__(
        self,
        poll_interval: float = 0.5,
        callback: Optional[Callable[[ClipboardChange], None]] = None,
    ) -> None:
        self.poll_interval = poll_interval
        self.callback = callback
        self._last_content: Optional[str] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._change_count = 0
        self._handlers: list[Callable[[ClipboardChange], None]] = []

    def add_handler(self, handler: Callable[[ClipboardChange], None]) -> None:
        """Add a change handler."""
        self._handlers.append(handler)

    def remove_handler(self, handler: Callable[[ClipboardChange], None]) -> None:
        """Remove a change handler."""
        self._handlers.remove(handler)

    def start(self) -> None:
        """Start observing clipboard in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._observe_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop observing clipboard."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def get_current_content(self) -> Optional[str]:
        """Get current clipboard text content."""
        try:
            import subprocess
            result = subprocess.run(
                ["pbpaste"],
                capture_output=True,
                text=True,
                timeout=1.0,
            )
            return result.stdout if result.returncode == 0 else None
        except Exception:
            return None

    def get_change_count(self) -> int:
        """Return number of changes detected."""
        return self._change_count

    def _observe_loop(self) -> None:
        """Background observation loop."""
        while self._running:
            try:
                current = self.get_current_content()
                if current is not None and current != self._last_content:
                    self._last_content = current
                    self._change_count += 1
                    change = ClipboardChange(
                        content=current,
                        content_type=self._detect_content_type(current),
                        timestamp=time.time(),
                        size_bytes=len(current.encode("utf-8")),
                    )
                    self._dispatch_change(change)
            except Exception:
                pass
            time.sleep(self.poll_interval)

    def _detect_content_type(self, content: str) -> ClipboardContentType:
        """Detect the type of clipboard content."""
        if not content:
            return ClipboardContentType.UNKNOWN
        if content.startswith("<!DOCTYPE html") or content.startswith("<html"):
            return ClipboardContentType.HTML
        if content.startswith("{\\rtf"):
            return ClipboardContentType.RTF
        return ClipboardContentType.TEXT

    def _dispatch_change(self, change: ClipboardChange) -> None:
        """Dispatch change to all handlers."""
        if self.callback:
            self.callback(change)
        for handler in self._handlers:
            try:
                handler(change)
            except Exception:
                pass


class ClipboardHistory:
    """
    Maintains a history of clipboard changes.

    Args:
        max_size: Maximum number of entries to retain.
    """

    def __init__(self, max_size: int = 50) -> None:
        self.max_size = max_size
        self._history: list[ClipboardChange] = []
        self._lock = threading.Lock()

    def append(self, change: ClipboardChange) -> None:
        """Add a change to history."""
        with self._lock:
            self._history.append(change)
            if len(self._history) > self.max_size:
                self._history.pop(0)

    def get_history(self, limit: Optional[int] = None) -> list[ClipboardChange]:
        """Return clipboard history, newest last."""
        with self._lock:
            if limit:
                return list(self._history[-limit:])
            return list(self._history)

    def clear(self) -> None:
        """Clear history."""
        with self._lock:
            self._history.clear()

    def search(self, query: str) -> list[ClipboardChange]:
        """Search history for entries containing query."""
        results = []
        with self._lock:
            for change in self._history:
                if isinstance(change.content, str) and query.lower() in change.content.lower():
                    results.append(change)
        return results
