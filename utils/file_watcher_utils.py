"""
File watcher utilities for monitoring file system changes.

Provides file system event monitoring, change detection, and
file watching for automation workflows that react to file modifications.

Example:
    >>> from file_watcher_utils import FileWatcher, watch_directory
    >>> watcher = FileWatcher()
    >>> watcher.watch("/path/to/dir", callback=on_change)
    >>> watcher.start()
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


# =============================================================================
# Types
# =============================================================================


class FileEvent(Enum):
    """File system event types."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"
    ACCESSED = "accessed"


@dataclass
class FileChange:
    """Represents a file system change event."""
    event: FileEvent
    path: str
    timestamp: float
    old_path: Optional[str] = None
    size: Optional[int] = None


@dataclass
class WatchEntry:
    """An active file or directory watch."""
    path: str
    recursive: bool
    callback: Callable[[FileChange], None]
    filter_fn: Optional[Callable[[str], bool]] = None
    debounce_ms: int = 100


# =============================================================================
# File System Watcher
# =============================================================================


class FileWatcher:
    """
    Watches files and directories for changes.

    Example:
        >>> def on_change(event):
        ...     print(f"{event.event}: {event.path}")
        >>> watcher = FileWatcher()
        >>> watcher.watch("/tmp", callback=on_change, recursive=True)
        >>> watcher.start()
    """

    def __init__(self, poll_interval: float = 1.0):
        self.poll_interval = poll_interval
        self._watches: List[WatchEntry] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_states: Dict[str, FileState] = {}
        self._lock = threading.RLock()
        self._callbacks: List[Callable[[FileChange], None]] = []

    def watch(
        self,
        path: str,
        callback: Callable[[FileChange], None],
        recursive: bool = False,
        filter_fn: Optional[Callable[[str], bool]] = None,
        debounce_ms: int = 100,
    ) -> None:
        """
        Watch a path for changes.

        Args:
            path: File or directory to watch.
            callback: Function to call on changes.
            recursive: Watch subdirectories.
            filter_fn: Optional filter function (return True to watch).
            debounce_ms: Debounce time in milliseconds.
        """
        entry = WatchEntry(
            path=os.path.abspath(path),
            recursive=recursive,
            callback=callback,
            filter_fn=filter_fn,
            debounce_ms=debounce_ms,
        )

        with self._lock:
            self._watches.append(entry)
            self._initialize_state(entry)

    def unwatch(self, path: str) -> bool:
        """Stop watching a path."""
        with self._lock:
            for i, w in enumerate(self._watches):
                if w.path == os.path.abspath(path):
                    self._watches.pop(i)
                    return True
            return False

    def on_change(self, callback: Callable[[FileChange], None]) -> None:
        """Register a global change callback."""
        self._callbacks.append(callback)

    def start(self) -> None:
        """Start watching."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def _initialize_state(self, entry: WatchEntry) -> None:
        """Initialize state for a watch entry."""
        path = entry.path

        if os.path.isfile(path):
            self._last_states[path] = FileState.from_path(path)

        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                if not entry.recursive and root != entry.path:
                    break

                for f in files:
                    fpath = os.path.join(root, f)
                    if entry.filter_fn and not entry.filter_fn(fpath):
                        continue
                    self._last_states[fpath] = FileState.from_path(fpath)

    def _watch_loop(self) -> None:
        """Main watching loop."""
        while self._running:
            changes = self._check_for_changes()

            for change in changes:
                for callback in self._callbacks:
                    try:
                        callback(change)
                    except Exception:
                        pass

                # Find matching watch entry
                with self._lock:
                    for entry in self._watches:
                        if change.path.startswith(entry.path):
                            if entry.filter_fn and not entry.filter_fn(change.path):
                                continue
                            try:
                                entry.callback(change)
                            except Exception:
                                pass

            time.sleep(self.poll_interval)

    def _check_for_changes(self) -> List[FileChange]:
        """Check all watched paths for changes."""
        changes: List[FileChange] = []
        timestamp = time.time()

        with self._lock:
            for path, last_state in list(self._last_states.items()):
                if not os.path.exists(path):
                    changes.append(FileChange(
                        event=FileEvent.DELETED,
                        path=path,
                        timestamp=timestamp,
                    ))
                    del self._last_states[path]

                else:
                    current = FileState.from_path(path)

                    if current != last_state:
                        if current.size != last_state.size:
                            changes.append(FileChange(
                                event=FileEvent.MODIFIED,
                                path=path,
                                timestamp=timestamp,
                                size=current.size,
                            ))
                        elif current.mtime != last_state.mtime:
                            changes.append(FileChange(
                                event=FileEvent.MODIFIED,
                                path=path,
                                timestamp=timestamp,
                                size=current.size,
                            ))

                        self._last_states[path] = current

            # Check for new files
            for entry in self._watches:
                if os.path.isdir(entry.path):
                    for root, dirs, files in os.walk(entry.path):
                        if not entry.recursive and root != entry.path:
                            break

                        for f in files:
                            fpath = os.path.join(root, f)
                            if entry.filter_fn and not entry.filter_fn(fpath):
                                continue

                            if fpath not in self._last_states:
                                self._last_states[fpath] = FileState.from_path(fpath)
                                changes.append(FileChange(
                                    event=FileEvent.CREATED,
                                    path=fpath,
                                    timestamp=timestamp,
                                    size=self._last_states[fpath].size,
                                ))

        return changes

    def __enter__(self) -> "FileWatcher":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


@dataclass
class FileState:
    """State of a file at a point in time."""
    path: str
    size: int
    mtime: float
    exists: bool = True

    @classmethod
    def from_path(cls, path: str) -> "FileState":
        """Create FileState from a path."""
        try:
            stat = os.stat(path)
            return cls(
                path=path,
                size=stat.st_size,
                mtime=stat.st_mtime,
                exists=True,
            )
        except OSError:
            return cls(path=path, size=0, mtime=0, exists=False)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FileState):
            return False
        return (
            self.path == other.path
            and self.size == other.size
            and self.mtime == other.mtime
            and self.exists == other.exists
        )


# =============================================================================
# Decorator
# =============================================================================


def watch_file(path: str, callback: Callable[[FileChange], None]) -> None:
    """
    Decorator-style file watching.

    Example:
        >>> @watch_file("/tmp/config.json")
        >>> def on_config_change(event):
        ...     reload_config()
    """
    watcher = FileWatcher()
    watcher.watch(path, callback=callback)
    watcher.start()


# =============================================================================
# Directory Watcher
# =============================================================================


class DirectoryWatcher:
    """
    Specialized watcher for directory contents.

    Example:
        >>> watcher = DirectoryWatcher("/path/to/dir")
        >>> watcher.on_file_created(lambda p: print(f"Created: {p}"))
        >>> watcher.start()
    """

    def __init__(self, directory: str, recursive: bool = False):
        self.directory = os.path.abspath(directory)
        self.recursive = recursive
        self._watcher = FileWatcher()
        self._callbacks: Dict[FileEvent, List[Callable]] = {
            event: [] for event in FileEvent
        }

    def on_file_created(self, callback: Callable[[str], None]) -> None:
        """Register callback for file creation."""
        self._callbacks[FileEvent.CREATED].append(callback)

    def on_file_modified(self, callback: Callable[[str], None]) -> None:
        """Register callback for file modification."""
        self._callbacks[FileEvent.MODIFIED].append(callback)

    def on_file_deleted(self, callback: Callable[[str], None]) -> None:
        """Register callback for file deletion."""
        self._callbacks[FileEvent.DELETED].append(callback)

    def start(self) -> None:
        """Start watching."""
        def handler(change: FileChange):
            for cb in self._callbacks.get(change.event, []):
                try:
                    cb(change.path)
                except Exception:
                    pass

        self._watcher.watch(self.directory, callback=handler, recursive=self.recursive)
        self._watcher.start()

    def stop(self) -> None:
        """Stop watching."""
        self._watcher.stop()


# =============================================================================
# Change Debouncer
# =============================================================================


class ChangeDebouncer:
    """
    Debounces rapid file changes.

    Useful for editors that save rapidly.

    Example:
        >>> debouncer = ChangeDebouncer(delay_ms=500)
        >>> debouncer.on_change(lambda: print("stable!"))
        >>> # rapid changes are consolidated
    """

    def __init__(self, delay_ms: float = 500):
        self.delay_ms = delay_ms
        self._pending: Dict[str, FileChange] = {}
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._callback: Optional[Callable] = None

    def on_change(self, path: str, callback: Callable) -> None:
        """
        Notify of a change, debouncing if needed.

        Args:
            path: Changed file path.
            callback: Function to call after debounce.
        """
        with self._lock:
            self._callback = callback
            self._pending[path] = time.time()

            if self._timer:
                self._timer.cancel()

            self._timer = threading.Timer(self.delay_ms / 1000, self._fire)
            self._timer.start()

    def _fire(self) -> None:
        """Fire the callback after debounce."""
        with self._lock:
            paths = list(self._pending.keys())
            self._pending.clear()
            callback = self._callback

        if callback:
            try:
                callback(paths)
            except Exception:
                pass


# =============================================================================
# File Pattern Watcher
# =============================================================================


class PatternWatcher:
    """
    Watches files matching a pattern.

    Example:
        >>> watcher = PatternWatcher("*.py")
        >>> watcher.on_match(lambda path: print(f"Python file: {path}"))
        >>> watcher.watch_directory("/path/to/dir")
    """

    def __init__(self, pattern: str):
        import fnmatch
        self.pattern = pattern
        self._match = fnmatch.fnmatch
        self._callbacks: List[Callable[[str], None]] = []
        self._watcher: Optional[FileWatcher] = None

    def on_match(self, callback: Callable[[str], None]) -> None:
        """Register callback for matching files."""
        self._callbacks.append(callback)

    def watch_directory(self, directory: str, recursive: bool = True) -> None:
        """Watch a directory for pattern matches."""
        self._watcher = FileWatcher()

        def on_change(change: FileChange):
            if self._match(os.path.basename(change.path), self.pattern):
                for cb in self._callbacks:
                    try:
                        cb(change.path)
                    except Exception:
                        pass

        self._watcher.watch(directory, callback=on_change, recursive=recursive)
        self._watcher.start()

    def stop(self) -> None:
        """Stop watching."""
        if self._watcher:
            self._watcher.stop()
