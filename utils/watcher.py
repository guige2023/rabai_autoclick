"""File watching utilities for RabAI AutoClick.

Provides:
- File system watcher
- Directory monitoring
- Change detection
"""

import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Optional, Set


class FileEventType(Enum):
    """File event types."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"


@dataclass
class FileEvent:
    """File system event."""
    event_type: FileEventType
    path: str
    old_path: Optional[str] = None


class FileWatcher:
    """Watch files for changes."""

    def __init__(
        self,
        path: str,
        recursive: bool = False,
        interval: float = 1.0,
    ) -> None:
        """Initialize watcher.

        Args:
            path: Path to watch.
            recursive: Watch subdirectories.
            interval: Check interval in seconds.
        """
        self._path = os.path.abspath(path)
        self._recursive = recursive
        self._interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: Dict[FileEventType, List[Callable]] = {}
        self._last_mtimes: Dict[str, float] = {}
        self._last_size: Dict[str, int] = {}

    def on_created(self, callback: Callable[[str], None]) -> None:
        """Register created callback.

        Args:
            callback: Function to call.
        """
        if FileEventType.CREATED not in self._callbacks:
            self._callbacks[FileEventType.CREATED] = []
        self._callbacks[FileEventType.CREATED].append(callback)

    def on_modified(self, callback: Callable[[str], None]) -> None:
        """Register modified callback.

        Args:
            callback: Function to call.
        """
        if FileEventType.MODIFIED not in self._callbacks:
            self._callbacks[FileEventType.MODIFIED] = []
        self._callbacks[FileEventType.MODIFIED].append(callback)

    def on_deleted(self, callback: Callable[[str], None]) -> None:
        """Register deleted callback.

        Args:
            callback: Function to call.
        """
        if FileEventType.DELETED not in self._callbacks:
            self._callbacks[FileEventType.DELETED] = []
        self._callbacks[FileEventType.DELETED].append(callback)

    def on_renamed(self, callback: Callable[[str, str], None]) -> None:
        """Register renamed callback.

        Args:
            callback: Function to call (old_path, new_path).
        """
        if FileEventType.RENAMED not in self._callbacks:
            self._callbacks[FileEventType.RENAMED] = []
        self._callbacks[FileEventType.RENAMED].append(callback)

    def start(self) -> None:
        """Start watching."""
        if self._running:
            return

        self._running = True
        self._scan_initial()
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _scan_initial(self) -> None:
        """Initial scan of directory."""
        if os.path.isfile(self._path):
            self._last_mtimes[self._path] = os.path.getmtime(self._path)
            self._last_size[self._path] = os.path.getsize(self._path)
        elif os.path.isdir(self._path):
            for root, dirs, files in os.walk(self._path):
                for name in files:
                    path = os.path.join(root, name)
                    try:
                        self._last_mtimes[path] = os.path.getmtime(path)
                        self._last_size[path] = os.path.getsize(path)
                    except OSError:
                        pass
                if not self._recursive:
                    break

    def _watch_loop(self) -> None:
        """Main watch loop."""
        while self._running:
            self._check_changes()
            time.sleep(self._interval)

    def _check_changes(self) -> None:
        """Check for file changes."""
        current_files: Set[str] = set()

        if os.path.isfile(self._path):
            current_files.add(self._path)
            self._check_file(self._path)
        elif os.path.isdir(self._path):
            for root, dirs, files in os.walk(self._path):
                for name in files:
                    path = os.path.join(root, name)
                    current_files.add(path)
                    self._check_file(path)
                if not self._recursive:
                    break

        # Check for deleted files
        for path in list(self._last_mtimes.keys()):
            if path not in current_files:
                self._emit_event(FileEventType.DELETED, path)
                del self._last_mtimes[path]
                self._last_size.pop(path, None)

    def _check_file(self, path: str) -> None:
        """Check a single file for changes."""
        try:
            if not os.path.exists(path):
                return

            mtime = os.path.getmtime(path)
            size = os.path.getsize(path)

            if path not in self._last_mtimes:
                self._last_mtimes[path] = mtime
                self._last_size[path] = size
                self._emit_event(FileEventType.CREATED, path)
            elif mtime != self._last_mtimes[path]:
                self._last_mtimes[path] = mtime
                self._last_size[path] = size
                self._emit_event(FileEventType.MODIFIED, path)
        except OSError:
            pass

    def _emit_event(self, event_type: FileEventType, path: str) -> None:
        """Emit event to callbacks."""
        if event_type in self._callbacks:
            for callback in self._callbacks[event_type]:
                try:
                    if event_type == FileEventType.RENAMED:
                        callback(path, path)
                    else:
                        callback(path)
                except Exception:
                    pass

    @property
    def is_running(self) -> bool:
        """Check if watching."""
        return self._running


class ConfigWatcher:
    """Watch configuration files for changes."""

    def __init__(self) -> None:
        """Initialize watcher."""
        self._watchers: Dict[str, FileWatcher] = {}
        self._callbacks: List[Callable[[str], None]] = []

    def watch(
        self,
        path: str,
        callback: Callable[[str], None],
        recursive: bool = False,
    ) -> None:
        """Watch configuration file.

        Args:
            path: Config file path.
            callback: Function to call on change.
        """
        if path in self._watchers:
            return

        watcher = FileWatcher(path, recursive=recursive)
        watcher.on_modified(callback)
        watcher.start()
        self._watchers[path] = watcher

    def unwatch(self, path: str) -> None:
        """Stop watching path.

        Args:
            path: Path to stop watching.
        """
        if path in self._watchers:
            self._watchers[path].stop()
            del self._watchers[path]

    def stop_all(self) -> None:
        """Stop all watchers."""
        for watcher in self._watchers.values():
            watcher.stop()
        self._watchers.clear()


class DirectoryWatcher:
    """Watch directory for file changes."""

    def __init__(self, path: str) -> None:
        """Initialize watcher.

        Args:
            path: Directory path.
        """
        self._watcher = FileWatcher(path, recursive=True)

    def on_file_created(self, callback: Callable[[str], None]) -> None:
        """Register file created callback."""
        self._watcher.on_created(callback)

    def on_file_modified(self, callback: Callable[[str], None]) -> None:
        """Register file modified callback."""
        self._watcher.on_modified(callback)

    def on_file_deleted(self, callback: Callable[[str], None]) -> None:
        """Register file deleted callback."""
        self._watcher.on_deleted(callback)

    def start(self) -> None:
        """Start watching."""
        self._watcher.start()

    def stop(self) -> None:
        """Stop watching."""
        self._watcher.stop()


class PathFilter:
    """Filter paths for watching."""

    def __init__(self) -> None:
        """Initialize filter."""
        self._include_exts: Set[str] = set()
        self._exclude_exts: Set[str] = set()
        self._include_names: Set[str] = set()
        self._exclude_names: Set[str] = set()

    def include_extension(self, ext: str) -> "PathFilter":
        """Include files with extension.

        Args:
            ext: File extension (e.g., '.py').

        Returns:
            Self for chaining.
        """
        self._include_exts.add(ext.lower())
        return self

    def exclude_extension(self, ext: str) -> "PathFilter":
        """Exclude files with extension.

        Args:
            ext: File extension.

        Returns:
            Self for chaining.
        """
        self._exclude_exts.add(ext.lower())
        return self

    def include_name(self, name: str) -> "PathFilter":
        """Include files with name.

        Args:
            name: File name substring.

        Returns:
            Self for chaining.
        """
        self._include_names.add(name.lower())
        return self

    def exclude_name(self, name: str) -> "PathFilter":
        """Exclude files with name.

        Args:
            name: File name substring.

        Returns:
            Self for chaining.
        """
        self._exclude_names.add(name.lower())
        return self

    def matches(self, path: str) -> bool:
        """Check if path matches filter.

        Args:
            path: Path to check.

        Returns:
            True if matches.
        """
        import os
        name = os.path.basename(path).lower()
        _, ext = os.path.splitext(path)

        # Check exclusions first
        if self._exclude_exts and ext.lower() in self._exclude_exts:
            return False
        if self._exclude_names and any(n in name for n in self._exclude_names):
            return False

        # Check inclusions
        if self._include_exts and ext.lower() not in self._include_exts:
            return False
        if self._include_names and not any(n in name for n in self._include_names):
            return False

        return True
