"""Watcher utilities for RabAI AutoClick.

Provides:
- File system watcher
- Change detection
- Watch callback management
"""

from __future__ import annotations

import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
)


class FileWatcher:
    """Watch a file for changes.

    Args:
        path: File path to watch.
        poll_interval: Polling interval in seconds.
    """

    def __init__(
        self,
        path: str,
        poll_interval: float = 1.0,
    ) -> None:
        import os
        self._path = path
        self._poll_interval = poll_interval
        self._last_mtime: Optional[float] = None
        self._last_size: Optional[int] = None
        if os.path.exists(path):
            stat = os.stat(path)
            self._last_mtime = stat.st_mtime
            self._last_size = stat.st_size

    def has_changed(self) -> bool:
        """Check if file has changed.

        Returns:
            True if file has been modified.
        """
        import os
        if not os.path.exists(self._path):
            return False
        stat = os.stat(self._path)
        if self._last_mtime is None:
            return True
        if stat.st_mtime != self._last_mtime:
            return True
        if stat.st_size != self._last_size:
            return True
        return False

    def update(self) -> None:
        """Update the last known state."""
        import os
        if os.path.exists(self._path):
            stat = os.stat(self._path)
            self._last_mtime = stat.st_mtime
            self._last_size = stat.st_size

    def wait_for_change(self, timeout: Optional[float] = None) -> bool:
        """Wait until file changes.

        Args:
            timeout: Max seconds to wait.

        Returns:
            True if changed, False if timeout.
        """
        start = time.monotonic()
        while True:
            if self.has_changed():
                self.update()
                return True
            if timeout and (time.monotonic() - start) >= timeout:
                return False
            time.sleep(self._poll_interval)


class WatchManager:
    """Manage multiple file watchers."""

    def __init__(self, poll_interval: float = 1.0) -> None:
        self._watchers: Dict[str, FileWatcher] = {}
        self._poll_interval = poll_interval
        self._callbacks: List[Callable[[str], None]] = []

    def watch(self, path: str) -> None:
        """Start watching a file.

        Args:
            path: File path.
        """
        self._watchers[path] = FileWatcher(path, self._poll_interval)

    def unwatch(self, path: str) -> None:
        """Stop watching a file.

        Args:
            path: File path.
        """
        self._watchers.pop(path, None)

    def on_change(self, callback: Callable[[str], None]) -> None:
        """Register a change callback.

        Args:
            callback: Called with path when file changes.
        """
        self._callbacks.append(callback)

    def check_all(self) -> List[str]:
        """Check all watchers for changes.

        Returns:
            List of paths that changed.
        """
        changed: List[str] = []
        for path, watcher in list(self._watchers.items()):
            if watcher.has_changed():
                watcher.update()
                changed.append(path)
                for cb in self._callbacks:
                    cb(path)
        return changed


__all__ = [
    "FileWatcher",
    "WatchManager",
]
