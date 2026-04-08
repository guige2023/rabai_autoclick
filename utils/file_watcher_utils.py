"""
File Watcher Utilities

Provides utilities for watching file changes
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum, auto
import os
import time


class FileChangeType(Enum):
    """Types of file changes."""
    CREATED = auto()
    MODIFIED = auto()
    DELETED = auto()
    RENAMED = auto()


@dataclass
class FileChange:
    """Represents a file change event."""
    change_type: FileChangeType
    path: str
    timestamp: float
    old_path: str | None = None


class FileWatcher:
    """
    Watches files for changes.
    
    Monitors file system for modifications
    and notifies registered handlers.
    """

    def __init__(self, poll_interval: float = 1.0) -> None:
        self._watch_paths: dict[str, float] = {}
        self._handlers: list[Callable[[FileChange], None]] = []
        self._poll_interval = poll_interval
        self._running = False

    def watch(self, path: str) -> None:
        """Start watching a file or directory."""
        if os.path.exists(path):
            self._watch_paths[path] = os.path.getmtime(path)
        else:
            self._watch_paths[path] = 0.0

    def unwatch(self, path: str) -> None:
        """Stop watching a path."""
        self._watch_paths.pop(path, None)

    def add_handler(self, handler: Callable[[FileChange], None]) -> None:
        """Add a change handler."""
        self._handlers.append(handler)

    def check_changes(self) -> list[FileChange]:
        """
        Check for file changes since last check.
        
        Returns:
            List of detected changes.
        """
        changes = []
        current_time = time.time()
        paths_to_remove = []

        for path, last_mtime in self._watch_paths.items():
            if not os.path.exists(path):
                changes.append(FileChange(
                    change_type=FileChangeType.DELETED,
                    path=path,
                    timestamp=current_time,
                ))
                paths_to_remove.append(path)
            else:
                current_mtime = os.path.getmtime(path)
                if current_mtime > last_mtime:
                    changes.append(FileChange(
                        change_type=FileChangeType.MODIFIED,
                        path=path,
                        timestamp=current_time,
                    ))
                    self._watch_paths[path] = current_mtime

        for path in paths_to_remove:
            self._watch_paths.pop(path, None)

        return changes

    def notify_handlers(self, changes: list[FileChange]) -> None:
        """Notify all handlers of changes."""
        for change in changes:
            for handler in self._handlers:
                handler(change)

    def get_watched_paths(self) -> list[str]:
        """Get list of watched paths."""
        return list(self._watch_paths.keys())
