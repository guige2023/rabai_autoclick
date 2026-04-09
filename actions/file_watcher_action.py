"""
File Watcher Action Module.

Watches files and directories for changes, triggering
callbacks on file modifications for reactive automation.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class FileChange:
    """A detected file change."""
    path: str
    change_type: str
    timestamp: float


class FileWatcher:
    """Watches files for changes."""

    def __init__(
        self,
        poll_interval: float = 1.0,
        max_history: int = 100,
    ):
        """
        Initialize file watcher.

        Args:
            poll_interval: Polling interval in seconds.
            max_history: Maximum change history size.
        """
        self.poll_interval = poll_interval
        self.max_history = max_history
        self._watched_paths: dict[str, float] = {}
        self._change_history: deque[FileChange] = deque(maxlen=max_history)
        self._callbacks: list[Callable[[FileChange], None]] = []
        self._running = False

    def watch(self, path: str) -> None:
        """Add a path to watch."""
        try:
            import os
            mtime = os.path.getmtime(path)
            self._watched_paths[path] = mtime
        except OSError:
            pass

    def unwatch(self, path: str) -> bool:
        """Remove a path from watching."""
        if path in self._watched_paths:
            del self._watched_paths[path]
            return True
        return False

    def check_changes(self) -> list[FileChange]:
        """
        Check for file changes since last check.

        Returns:
            List of FileChange objects.
        """
        changes = []

        for path, last_mtime in list(self._watched_paths.items()):
            try:
                import os
                current_mtime = os.path.getmtime(path)

                if current_mtime != last_mtime:
                    change_type = "modified"
                    if current_mtime > last_mtime:
                        change_type = "modified"
                    elif current_mtime < last_mtime:
                        change_type = "restored"

                    change = FileChange(
                        path=path,
                        change_type=change_type,
                        timestamp=time.time(),
                    )
                    changes.append(change)
                    self._watched_paths[path] = current_mtime
                    self._change_history.append(change)

            except OSError:
                change = FileChange(
                    path=path,
                    change_type="deleted",
                    timestamp=time.time(),
                )
                changes.append(change)
                del self._watched_paths[path]

        for change in changes:
            for callback in self._callbacks:
                try:
                    callback(change)
                except Exception:
                    pass

        return changes

    def on_change(
        self,
        callback: Callable[[FileChange], None],
    ) -> None:
        """Register a change callback."""
        self._callbacks.append(callback)

    def get_history(
        self,
        path: Optional[str] = None,
        limit: int = 10,
    ) -> list[FileChange]:
        """
        Get change history.

        Args:
            path: Optional filter by path.
            limit: Maximum entries to return.

        Returns:
            List of FileChange objects.
        """
        history = list(self._change_history)

        if path:
            history = [c for c in history if c.path == path]

        return history[-limit:]
