"""File watcher utilities for RabAI AutoClick.

Provides:
- FileWatcher: Watch files/directories for changes
- ConfigWatcher: Watch configuration files for hot-reload
"""

import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set


class FileEventType(Enum):
    """Types of file events."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"


@dataclass
class FileEvent:
    """Represents a file system event."""
    event_type: FileEventType
    path: str
    old_path: Optional[str] = None
    timestamp: float = 0

    def __post_init__(self) -> None:
        if self.timestamp == 0:
            self.timestamp = time.time()


class FileWatcher:
    """Watch files and directories for changes.

    Usage:
        def on_change(event):
            print(f"File {event.path} was {event.event_type}")

        watcher = FileWatcher()
        watcher.watch("/path/to/file.json", on_change)
        watcher.start()
        # ...
        watcher.stop()
    """

    def __init__(
        self,
        poll_interval: float = 1.0,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
    ) -> None:
        """Initialize file watcher.

        Args:
            poll_interval: Seconds between file stat checks.
            include_patterns: Glob patterns to include.
            exclude_patterns: Glob patterns to exclude.
        """
        self.poll_interval = poll_interval
        self.include_patterns = include_patterns
        self.exclude_patterns = exclude_patterns or [".git/*", "__pycache__/*", "*.pyc"]

        self._watchers: Dict[str, Callable[[FileEvent], None]] = {}
        self._file_states: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def watch(self, path: str, callback: Callable[[FileEvent], None]) -> None:
        """Watch a file or directory for changes.

        Args:
            path: Path to watch.
            callback: Function called on changes.
        """
        with self._lock:
            self._watchers[path] = callback
            self._file_states[path] = self._get_initial_state(path)

    def unwatch(self, path: str) -> None:
        """Stop watching a path.

        Args:
            path: Path to stop watching.
        """
        with self._lock:
            if path in self._watchers:
                del self._watchers[path]
            if path in self._file_states:
                del self._file_states[path]

    def start(self) -> None:
        """Start watching for file changes."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching for file changes."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)

    def _watch_loop(self) -> None:
        """Main watching loop."""
        while self._running:
            self._check_for_changes()
            time.sleep(self.poll_interval)

    def _check_for_changes(self) -> None:
        """Check all watched paths for changes."""
        with self._lock:
            paths = list(self._watchers.keys())
            states = dict(self._file_states)

        for path in paths:
            try:
                self._check_path(path, states.get(path, {}))
            except Exception:
                pass

    def _check_path(self, path: str, old_state: Dict[str, Any]) -> None:
        """Check a single path for changes."""
        callback = self._watchers.get(path)
        if not callback:
            return

        if not os.path.exists(path):
            if old_state.get("exists", True):
                callback(FileEvent(FileEventType.DELETED, path))
                with self._lock:
                    self._file_states[path] = {"exists": False}
            return

        new_mtime = os.path.getmtime(path)
        old_mtime = old_state.get("mtime", 0)

        if new_mtime != old_mtime:
            callback(FileEvent(FileEventType.MODIFIED, path))
            with self._lock:
                self._file_states[path] = self._get_initial_state(path)

    def _get_initial_state(self, path: str) -> Dict[str, Any]:
        """Get initial state of a path."""
        return {
            "exists": os.path.exists(path),
            "mtime": os.path.getmtime(path) if os.path.exists(path) else 0,
        }

    def _should_include(self, path: str) -> bool:
        """Check if path should be included."""
        path_obj = Path(path)

        for pattern in self.exclude_patterns:
            if path_obj.match(pattern):
                return False

        if not self.include_patterns:
            return True

        for pattern in self.include_patterns:
            if path_obj.match(pattern):
                return True

        return False

    @property
    def is_running(self) -> bool:
        """Check if watcher is running."""
        return self._running


class ConfigWatcher:
    """Watch configuration files for changes and reload.

    Usage:
        config = ConfigWatcher()
        config.watch("config.json", my_config)
        config.on_reload(lambda cfg: print("Reloaded!"))
        config.start()
    """

    def __init__(
        self,
        poll_interval: float = 2.0,
        max_retries: int = 3,
    ) -> None:
        """Initialize config watcher.

        Args:
            poll_interval: Seconds between file checks.
            max_retries: Maximum reload retries on failure.
        """
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self._watcher = FileWatcher(poll_interval=poll_interval)
        self._configs: Dict[str, Any] = {}
        self._loaders: Dict[str, Callable[[str], Any]] = {}
        self._reload_callbacks: List[Callable[[Any], None]] = []
        self._lock = threading.Lock()

    def watch(
        self,
        path: str,
        config: Any,
        loader: Optional[Callable[[str], Any]] = None,
    ) -> None:
        """Watch a configuration file.

        Args:
            path: Path to config file.
            config: Initial config object.
            loader: Function to reload config from file.
        """
        import json

        def default_loader(p: str) -> Any:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)

        with self._lock:
            self._configs[path] = config
            self._loaders[path] = loader or default_loader
            self._watcher.watch(path, self._on_file_change)

    def on_reload(self, callback: Callable[[Any], None]) -> None:
        """Register callback for config reload.

        Args:
            callback: Called with reloaded config.
        """
        with self._lock:
            self._reload_callbacks.append(callback)

    def start(self) -> None:
        """Start watching for config changes."""
        self._watcher.start()

    def stop(self) -> None:
        """Stop watching for config changes."""
        self._watcher.stop()

    def _on_file_change(self, event: FileEvent) -> None:
        """Handle file change event."""
        if event.event_type != FileEventType.MODIFIED:
            return

        path = event.path
        loader = self._loaders.get(path)
        if not loader:
            return

        for attempt in range(self.max_retries):
            try:
                new_config = loader(path)
                with self._lock:
                    old_config = self._configs.get(path)
                    self._configs[path] = new_config

                    for callback in self._reload_callbacks:
                        callback(new_config)

                return
            except Exception:
                time.sleep(0.5 * (attempt + 1))

    def reload(self, path: str) -> Optional[Any]:
        """Manually reload a config.

        Args:
            path: Config file path.

        Returns:
            Reloaded config or None on failure.
        """
        with self._lock:
            loader = self._loaders.get(path)
            if not loader:
                return None

            try:
                new_config = loader(path)
                self._configs[path] = new_config
                return new_config
            except Exception:
                return None


class DirectoryWatcher:
    """Watch a directory for file changes.

    Provides recursive watching with file type filtering.
    """

    def __init__(
        self,
        path: str,
        recursive: bool = True,
        poll_interval: float = 1.0,
    ) -> None:
        """Initialize directory watcher.

        Args:
            path: Directory path to watch.
            recursive: Watch subdirectories.
            poll_interval: Seconds between checks.
        """
        self.path = path
        self.recursive = recursive
        self.poll_interval = poll_interval

        self._watcher = FileWatcher(poll_interval=poll_interval)
        self._callbacks: List[Callable[[FileEvent], None]] = []
        self._known_files: Set[str] = set()
        self._lock = threading.Lock()

    def watch(self, callback: Callable[[FileEvent], None]) -> None:
        """Register a callback for file events."""
        with self._lock:
            self._callbacks.append(callback)

    def start(self) -> None:
        """Start watching directory."""
        self._scan_directory()

        for file_path in self._known_files:
            self._watcher.watch(file_path, self._handle_event)

        self._watcher.start()

    def stop(self) -> None:
        """Stop watching directory."""
        self._watcher.stop()

    def _scan_directory(self) -> None:
        """Scan directory for initial file list."""
        self._known_files.clear()

        if not os.path.exists(self.path):
            return

        for root, dirs, files in os.walk(self.path):
            for file in files:
                file_path = os.path.join(root, file)
                self._known_files.add(file_path)

            if not self.recursive:
                break

    def _handle_event(self, event: FileEvent) -> None:
        """Handle file event."""
        with self._lock:
            callbacks = self._callbacks.copy()

        for callback in callbacks:
            try:
                callback(event)
            except Exception:
                pass