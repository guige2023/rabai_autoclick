"""File watcher utilities for monitoring file system changes.

Provides directory and file monitoring for triggering
automation actions when files are created, modified,
or deleted, supporting watch-for-change workflows.

Example:
    >>> from utils.file_watcher_utils import FileWatcher, watch
    >>> watcher = FileWatcher('/tmp')
    >>> watcher.on_change('*.py', lambda e: print(f'Changed: {e.path}'))
    >>> watcher.start()
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

__all__ = [
    "FileWatcher",
    "FileEvent",
    "FileEventType",
    "watch",
]


class FileEventType:
    """File event type constants."""

    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"
    ANY = "any"


@dataclass
class FileEvent:
    """A file system event.

    Attributes:
        event_type: Type of event.
        path: Path to the affected file.
        timestamp: When the event occurred.
        old_path: Previous path (for moved events).
    """

    event_type: str
    path: Path
    timestamp: float
    old_path: Optional[Path] = None


class FileWatcher:
    """Monitors a directory tree for file system changes.

    Example:
        >>> watcher = FileWatcher('/tmp/myapp')
        >>> @watcher.on('*.py')
        ... def on_python_change(event):
        ...     print(f"Python file changed: {event.path}")
        >>> watcher.start()
    """

    def __init__(
        self,
        path: str | Path,
        recursive: bool = True,
        poll_interval: float = 1.0,
    ):
        self.watch_path = Path(path)
        self.recursive = recursive
        self.poll_interval = poll_interval
        self._running = False
        self._handlers: dict[str, list[Callable]] = {}
        self._file_mtimes: dict[Path, float] = {}
        self._known_files: set[Path] = set()
        self._lock = __import__("threading").Lock()

    def on(self, pattern: str) -> Callable:
        """Decorator to register a handler for a file pattern.

        Args:
            pattern: Glob pattern (e.g., '*.py', 'config/*.json').
        """
        def decorator(fn: Callable) -> Callable:
            self._handlers.setdefault(pattern, []).append(fn)
            return fn
        return decorator

    def start(self, blocking: bool = True) -> None:
        """Start watching for file changes.

        Args:
            blocking: If True, runs in the current thread.
        """
        self._running = True
        self._scan_initial()

        if blocking:
            self._watch_loop()
        else:
            import threading
            t = threading.Thread(target=self._watch_loop, daemon=True)
            t.start()

    def stop(self) -> None:
        """Stop the file watcher."""
        self._running = False

    def _scan_initial(self) -> None:
        """Perform initial scan to populate known files."""
        if not self.watch_path.exists():
            return

        for p in self._iter_files():
            self._known_files.add(p)
            try:
                self._file_mtimes[p] = p.stat().st_mtime
            except OSError:
                pass

    def _iter_files(self):
        if self.recursive:
            return self.watch_path.rglob("*")
        return self.watch_path.glob("*")

    def _watch_loop(self) -> None:
        """Main watching loop."""
        import os

        while self._running:
            current_files: set[Path] = set()

            for p in self._iter_files():
                if not p.is_file():
                    continue
                current_files.add(p)

                try:
                    mtime = p.stat().st_mtime
                except OSError:
                    continue

                if p not in self._known_files:
                    # New file
                    self._dispatch(FileEvent(
                        event_type=FileEventType.CREATED,
                        path=p,
                        timestamp=time.time(),
                    ))
                    self._known_files.add(p)
                elif p in self._file_mtimes and self._file_mtimes[p] != mtime:
                    # Modified file
                    self._dispatch(FileEvent(
                        event_type=FileEventType.MODIFIED,
                        path=p,
                        timestamp=time.time(),
                    ))

                self._file_mtimes[p] = mtime

            # Check for deleted files
            for p in list(self._known_files):
                if p not in current_files:
                    self._dispatch(FileEvent(
                        event_type=FileEventType.DELETED,
                        path=p,
                        timestamp=time.time(),
                    ))
                    self._known_files.discard(p)
                    self._file_mtimes.pop(p, None)

            time.sleep(self.poll_interval)

    def _dispatch(self, event: FileEvent) -> None:
        """Dispatch an event to matching handlers."""
        with self._lock:
            for pattern, handlers in self._handlers.items():
                if self._matches(event.path, pattern):
                    for handler in handlers:
                        try:
                            handler(event)
                        except Exception:
                            pass

    def _matches(self, path: Path, pattern: str) -> bool:
        """Check if a path matches a glob pattern."""
        import fnmatch

        return fnmatch.fnmatch(path.name, pattern)


def watch(
    path: str | Path,
    handler: Callable[[FileEvent], None],
    pattern: str = "*",
    recursive: bool = True,
) -> FileWatcher:
    """Convenience function to create and start a file watcher.

    Args:
        path: Directory to watch.
        handler: Callback function.
        pattern: File pattern to match.
        recursive: Watch subdirectories.

    Returns:
        The FileWatcher instance (already started).
    """
    watcher = FileWatcher(path, recursive=recursive)
    watcher.on(pattern)(handler)
    watcher.start(blocking=False)
    return watcher
