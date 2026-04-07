"""File watcher utilities: monitor file system changes and trigger callbacks."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "FileEvent",
    "FileWatcher",
    "watch_directory",
]


@dataclass
class FileEvent:
    """A file system event."""

    event_type: str
    path: str
    timestamp: float = field(default_factory=time.time)
    size: int | None = None


class FileWatcher:
    """Watch a directory for file system changes."""

    def __init__(
        self,
        path: str,
        callback: Callable[[FileEvent], None],
        recursive: bool = False,
        poll_interval: float = 1.0,
    ) -> None:
        self.path = path
        self.callback = callback
        self.recursive = recursive
        self.poll_interval = poll_interval
        self._running = False
        self._thread: threading.Thread | None = None
        self._file_mtimes: dict[str, float] = {}

    def _get_files(self) -> list[str]:
        """Get all files under watched path."""
        files = []
        if self.recursive:
            for root, dirs, filenames in os.walk(self.path):
                for filename in filenames:
                    files.append(os.path.join(root, filename))
        else:
            if os.path.isdir(self.path):
                for name in os.listdir(self.path):
                    full_path = os.path.join(self.path, name)
                    if os.path.isfile(full_path):
                        files.append(full_path)
            elif os.path.isfile(self.path):
                files.append(self.path)
        return files

    def _check_changes(self) -> None:
        """Check for file system changes."""
        files = self._get_files()
        current_mtimes = {}

        for filepath in files:
            try:
                mtime = os.path.getmtime(filepath)
                size = os.path.getsize(filepath)
                current_mtimes[filepath] = mtime

                if filepath not in self._file_mtimes:
                    self.callback(FileEvent(
                        event_type="created",
                        path=filepath,
                        size=size,
                    ))
                elif self._file_mtimes[filepath] < mtime:
                    self.callback(FileEvent(
                        event_type="modified",
                        path=filepath,
                        size=size,
                    ))
            except OSError:
                pass

        for filepath in list(self._file_mtimes.keys()):
            if filepath not in current_mtimes:
                self.callback(FileEvent(
                    event_type="deleted",
                    path=filepath,
                ))

        self._file_mtimes = current_mtimes

    def start(self) -> None:
        """Start watching for file changes."""
        if self._running:
            return
        self._running = True
        self._file_mtimes = {f: os.path.getmtime(f) for f in self._get_files() if os.path.exists(f)}

        def run():
            while self._running:
                self._check_changes()
                time.sleep(self.poll_interval)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching for file changes."""
        self._running = False


def watch_directory(
    path: str,
    callback: Callable[[FileEvent], None],
    **kwargs,
) -> FileWatcher:
    """Convenience function to start watching a directory."""
    watcher = FileWatcher(path, callback, **kwargs)
    watcher.start()
    return watcher
