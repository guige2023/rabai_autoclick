"""Watcher utilities for RabAI AutoClick.

Provides:
- File system watchers
- Change event handling
- Debouncing and filtering
"""

import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Callable,
    Dict,
    List,
    Optional,
    Set,
)


class WatchEventType(Enum):
    """File system event types."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"
    ANY = "any"


@dataclass
class WatchEvent:
    """A file system watch event."""

    path: str
    event_type: WatchEventType
    timestamp: float = field(default_factory=time.time)
    is_directory: bool = False


class FileWatcher:
    """Simple file system watcher with debouncing."""

    def __init__(
        self,
        path: str,
        callback: Callable[[WatchEvent], None],
        *,
        recursive: bool = False,
        debounce_ms: float = 100,
        event_types: Optional[List[WatchEventType]] = None,
    ) -> None:
        """Initialize file watcher.

        Args:
            path: Path to watch.
            callback: Function to call on events.
            recursive: Watch subdirectories.
            debounce_ms: Debounce delay in milliseconds.
            event_types: Filter to specific event types.
        """
        self._path = os.path.abspath(path)
        self._callback = callback
        self._recursive = recursive
        self._debounce_ms = debounce_ms
        self._event_types = set(event_types) if event_types else set(WatchEventType)
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_states: Dict[str, float] = {}
        self._lock = threading.Lock()

    def _should_process(self, event: WatchEvent) -> bool:
        """Check if event should be processed (debounce)."""
        if event.event_type not in self._event_types:
            return False

        last_time = self._last_states.get(event.path, 0)
        elapsed_ms = (event.timestamp - last_time) * 1000

        if elapsed_ms < self._debounce_ms:
            return False

        self._last_states[event.path] = event.timestamp
        return True

    def _scan_directory(self) -> List[WatchEvent]:
        """Scan directory for changes."""
        events: List[WatchEvent] = []
        current_paths: Set[str] = set()

        if not os.path.exists(self._path):
            return events

        for root, dirs, files in os.walk(self._path):
            if not self._recursive and root != self._path:
                break

            for name in files:
                filepath = os.path.join(root, name)
                filepath = os.path.abspath(filepath)
                current_paths.add(filepath)

                mtime = os.path.getmtime(filepath)
                if filepath not in self._last_states:
                    events.append(WatchEvent(
                        path=filepath,
                        event_type=WatchEventType.CREATED,
                        is_directory=False,
                    ))
                elif mtime > self._last_states[filepath]:
                    events.append(WatchEvent(
                        path=filepath,
                        event_type=WatchEventType.MODIFIED,
                        is_directory=False,
                    ))

            for name in dirs:
                dirpath = os.path.join(root, name)
                dirpath = os.path.abspath(dirpath)
                current_paths.add(dirpath)

        for path in list(self._last_states.keys()):
            if path not in current_paths:
                events.append(WatchEvent(
                    path=path,
                    event_type=WatchEventType.DELETED,
                ))

        return events

    def _run_loop(self) -> None:
        """Main watch loop."""
        while self._running:
            events = self._scan_directory()
            for event in events:
                if self._should_process(event):
                    try:
                        self._callback(event)
                    except Exception:
                        pass
            time.sleep(0.5)

    def start(self) -> None:
        """Start watching."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def is_running(self) -> bool:
        """Check if watcher is running."""
        return self._running


class PathWatcher:
    """Watcher that tracks specific paths rather than scanning."""

    def __init__(
        self,
        callback: Callable[[WatchEvent], None],
    ) -> None:
        """Initialize path watcher.

        Args:
            callback: Function to call on events.
        """
        self._watched: Dict[str, float] = {}
        self._callback = callback
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_path(self, path: str) -> None:
        """Add a path to watch.

        Args:
            path: Path to watch.
        """
        abs_path = os.path.abspath(path)
        try:
            mtime = os.path.getmtime(abs_path)
        except OSError:
            mtime = 0
        with self._lock:
            self._watched[abs_path] = mtime

    def remove_path(self, path: str) -> None:
        """Remove a path from watching.

        Args:
            path: Path to stop watching.
        """
        abs_path = os.path.abspath(path)
        with self._lock:
            self._watched.pop(abs_path, None)

    def check_changes(self) -> List[WatchEvent]:
        """Check for changes on watched paths.

        Returns:
            List of change events.
        """
        events: List[WatchEvent] = []
        to_remove: List[str] = []

        with self._lock:
            watched_items = list(self._watched.items())

        for path, last_mtime in watched_items:
            if not os.path.exists(path):
                events.append(WatchEvent(path=path, event_type=WatchEventType.DELETED))
                to_remove.append(path)
                continue

            try:
                current_mtime = os.path.getmtime(path)
                if current_mtime > last_mtime:
                    events.append(WatchEvent(path=path, event_type=WatchEventType.MODIFIED))
                    with self._lock:
                        self._watched[path] = current_mtime
            except OSError:
                pass

        for path in to_remove:
            with self._lock:
                self._watched.pop(path, None)

        for event in events:
            try:
                self._callback(event)
            except Exception:
                pass

        return events

    def start(self) -> None:
        """Start watching."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        """Main watch loop."""
        while self._running:
            self.check_changes()
            time.sleep(0.5)

    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)


class EventFilter:
    """Filters watch events based on patterns."""

    def __init__(
        self,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> None:
        """Initialize event filter.

        Args:
            include: Glob patterns to include (None = all).
            exclude: Glob patterns to exclude.
        """
        self._include = include or ["*"]
        self._exclude = exclude or []

    def should_include(self, path: str) -> bool:
        """Check if path should be included."""
        import fnmatch

        for pattern in self._exclude:
            if fnmatch.fnmatch(path, pattern):
                return False

        for pattern in self._include:
            if fnmatch.fnmatch(path, pattern):
                return True

        return False


def watch_files(
    paths: List[str],
    callback: Callable[[WatchEvent], None],
    *,
    recursive: bool = False,
) -> FileWatcher:
    """Convenience function to watch multiple paths.

    Args:
        paths: List of paths to watch.
        callback: Callback function.
        recursive: Watch subdirectories.

    Returns:
        FileWatcher instance.
    """
    if not paths:
        raise ValueError("At least one path required")

    if len(paths) == 1:
        watcher = FileWatcher(paths[0], callback, recursive=recursive)
    else:
        import tempfile
        # Create temp dir and watch parent if multiple paths
        parent = os.path.dirname(os.path.abspath(paths[0]))
        watcher = FileWatcher(parent, callback, recursive=recursive)

    watcher.start()
    return watcher
