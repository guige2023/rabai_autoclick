"""File watcher action for monitoring file system changes.

This module provides file and directory monitoring with
support for events, filtering, and recursive watching.

Example:
    >>> action = FileWatcherAction()
    >>> result = action.execute(command="watch", path="/tmp", events=["created"])
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class FileEvent:
    """Represents a file system event."""
    event_type: str
    path: str
    timestamp: float
    is_directory: bool = False


@dataclass
class WatchConfig:
    """Configuration for file watching."""
    recursive: bool = True
    ignored_patterns: list[str] = field(default_factory=lambda: ["*.pyc", ".git", "__pycache__"])
    debounce_seconds: float = 0.5


class FileWatcherAction:
    """File system watcher action.

    Monitors files and directories for changes with
    support for event filtering and callbacks.

    Example:
        >>> action = FileWatcherAction()
        >>> result = action.execute(
        ...     command="watch",
        ...     path="/tmp",
        ...     callback=my_callback
        ... )
    """

    def __init__(self, config: Optional[WatchConfig] = None) -> None:
        """Initialize file watcher.

        Args:
            config: Optional watch configuration.
        """
        self.config = config or WatchConfig()
        self._watching = False
        self._events: list[FileEvent] = []

    def execute(
        self,
        command: str,
        path: Optional[str] = None,
        events: Optional[list[str]] = None,
        pattern: Optional[str] = None,
        callback: Optional[Callable[[FileEvent], None]] = None,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute file watcher command.

        Args:
            command: Watch command (watch, stop, get_events, wait_for).
            path: Path to watch.
            events: Event types to watch (created, modified, deleted, moved).
            pattern: File pattern to match.
            callback: Event callback function.
            timeout: Wait timeout.
            **kwargs: Additional parameters.

        Returns:
            Watch result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd == "watch":
            if not path:
                raise ValueError("path required for 'watch' command")
            result.update(self._start_watch(path, events, pattern, callback))

        elif cmd == "stop":
            result.update(self._stop_watch())

        elif cmd == "get_events":
            result["events"] = [
                {"type": e.event_type, "path": e.path, "timestamp": e.timestamp}
                for e in self._events
            ]
            result["count"] = len(self._events)

        elif cmd == "wait_for":
            event_type = kwargs.get("event_type")
            file_path = kwargs.get("path")
            result.update(self._wait_for_event(event_type, file_path, timeout))

        elif cmd == "clear":
            self._events.clear()
            result["cleared"] = True

        elif cmd == "exists":
            check_path = kwargs.get("path")
            if not check_path:
                raise ValueError("path required for 'exists'")
            result["exists"] = os.path.exists(check_path)
            result["is_file"] = os.path.isfile(check_path) if result["exists"] else False
            result["is_dir"] = os.path.isdir(check_path) if result["exists"] else False

        elif cmd == "list_dir":
            list_path = kwargs.get("path", ".")
            result.update(self._list_directory(list_path, kwargs.get("recursive", False)))

        elif cmd == "get_info":
            info_path = kwargs.get("path")
            if not info_path:
                raise ValueError("path required for 'get_info'")
            result.update(self._get_file_info(info_path))

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def _start_watch(
        self,
        path: str,
        events: Optional[list[str]],
        pattern: Optional[str],
        callback: Optional[Callable[[FileEvent], None]],
    ) -> dict[str, Any]:
        """Start watching path.

        Args:
            path: Path to watch.
            events: Event types to watch.
            pattern: File pattern.
            callback: Event callback.

        Returns:
            Result dictionary.
        """
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler, FileSystemEvent
        except ImportError:
            return {
                "success": False,
                "error": "watchdog not installed. Run: pip install watchdog",
            }

        if self._watching:
            return {"success": False, "error": "Already watching"}

        events = events or ["created", "modified", "deleted", "moved"]

        class EventHandler(FileSystemEventHandler):
            def __init__(self, outer):
                self.outer = outer

            def on_any_event(self, event: FileSystemEvent):
                if event.is_directory:
                    return
                if self.outer._should_ignore(event.src_path):
                    return

                event_type = event.event_type
                file_event = FileEvent(
                    event_type=event_type,
                    path=event.src_path,
                    timestamp=time.time(),
                )
                self.outer._events.append(file_event)
                if self.outer._callback:
                    self.outer._callback(file_event)

        self._callback = callback
        self._observer = Observer()
        handler = EventHandler(self)
        self._observer.schedule(handler, path, recursive=self.config.recursive)
        self._observer.start()
        self._watching = True

        return {
            "watching": True,
            "path": path,
            "events": events,
            "recursive": self.config.recursive,
        }

    def _stop_watch(self) -> dict[str, Any]:
        """Stop watching.

        Returns:
            Result dictionary.
        """
        if hasattr(self, "_observer") and self._observer:
            self._observer.stop()
            self._observer.join()
            self._watching = False
            return {"stopped": True}

        return {"stopped": True, "was_not_watching": True}

    def _should_ignore(self, path: str) -> bool:
        """Check if path should be ignored.

        Args:
            path: File path.

        Returns:
            True if should ignore.
        """
        import fnmatch
        for pattern in self.config.ignored_patterns:
            if fnmatch.fnmatch(path, pattern):
                return True
        return False

    def _wait_for_event(
        self,
        event_type: Optional[str],
        path: Optional[str],
        timeout: float,
    ) -> dict[str, Any]:
        """Wait for specific event.

        Args:
            event_type: Type of event to wait for.
            path: Optional file path.
            timeout: Maximum wait time.

        Returns:
            Result dictionary.
        """
        start_time = time.time()
        initial_count = len(self._events)

        while time.time() - start_time < timeout:
            for event in self._events[initial_count:]:
                if event_type and event.event_type != event_type:
                    continue
                if path and path not in event.path:
                    continue
                return {
                    "found": True,
                    "event": {
                        "type": event.event_type,
                        "path": event.path,
                        "timestamp": event.timestamp,
                    },
                    "wait_time": time.time() - start_time,
                }
            time.sleep(0.1)

        return {
            "found": False,
            "timeout": True,
            "wait_time": timeout,
        }

    def _list_directory(self, path: str, recursive: bool) -> dict[str, Any]:
        """List directory contents.

        Args:
            path: Directory path.
            recursive: Whether to recurse.

        Returns:
            Result dictionary.
        """
        files = []
        dirs = []

        try:
            if recursive:
                for root, directories, filenames in os.walk(path):
                    for d in directories:
                        dirs.append(os.path.join(root, d))
                    for f in filenames:
                        files.append(os.path.join(root, f))
            else:
                for item in os.listdir(path):
                    full_path = os.path.join(path, item)
                    if os.path.isdir(full_path):
                        dirs.append(full_path)
                    else:
                        files.append(full_path)

        except Exception as e:
            return {"success": False, "error": str(e)}

        return {
            "files": files,
            "directories": dirs,
            "file_count": len(files),
            "dir_count": len(dirs),
        }

    def _get_file_info(self, path: str) -> dict[str, Any]:
        """Get file information.

        Args:
            path: File path.

        Returns:
            Result dictionary.
        """
        try:
            stat = os.stat(path)
            return {
                "path": path,
                "exists": True,
                "size": stat.st_size,
                "modified": stat.st_mtime,
                "created": stat.st_ctime,
                "is_file": os.path.isfile(path),
                "is_dir": os.path.isdir(path),
                "is_link": os.path.islink(path),
            }
        except Exception as e:
            return {"exists": False, "error": str(e)}

    def is_watching(self) -> bool:
        """Check if currently watching.

        Returns:
            True if watching.
        """
        return self._watching

    def __del__(self) -> None:
        """Cleanup on destruction."""
        if self._watching:
            self._stop_watch()
