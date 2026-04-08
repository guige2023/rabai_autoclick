"""File watching utilities.

Monitors files and directories for changes using polling,
useful when inotify/FSEvents are not available.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


@dataclass
class FileEvent:
    """File change event."""
    path: str
    event_type: str  # "created", "modified", "deleted"
    timestamp: float = field(default_factory=time.time)


class FileWatcher:
    """Poll-based file and directory watcher.

    Args:
        poll_interval: Seconds between checks.

    Example:
        watcher = FileWatcher(poll_interval=1.0)
        watcher.watch("/path/to/file")
        for event in watcher:
            print(f"Changed: {event.path}")
    """

    def __init__(self, poll_interval: float = 1.0) -> None:
        self._poll_interval = poll_interval
        self._watched: Dict[str, float] = {}
        self._callback: Optional[Callable[[FileEvent], None]] = None

    def watch(self, path: str) -> None:
        """Start watching a file or directory.

        Args:
            path: File or directory path.
        """
        if os.path.isfile(path):
            self._watched[path] = os.path.getmtime(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for f in files:
                    fp = os.path.join(root, f)
                    try:
                        self._watched[fp] = os.path.getmtime(fp)
                    except OSError:
                        pass

    def unwatch(self, path: str) -> None:
        """Stop watching a file or directory.

        Args:
            path: File or directory path.
        """
        if path in self._watched:
            del self._watched[path]
        else:
            to_remove = [k for k in self._watched if k.startswith(path)]
            for k in to_remove:
                del self._watched[k]

    def set_callback(self, callback: Callable[[FileEvent], None]) -> None:
        """Set change callback.

        Args:
            callback: Function to call on file changes.
        """
        self._callback = callback

    def check(self) -> List[FileEvent]:
        """Check for file changes.

        Returns:
            List of file events since last check.
        """
        events: List[FileEvent] = []
        current_paths: Set[str] = set()
        new_paths: Set[str] = set(self._watched.keys())

        for path in list(self._watched.keys()):
            if not os.path.exists(path):
                events.append(FileEvent(path, "deleted"))
                del self._watched[path]
                new_paths.discard(path)
            else:
                current_paths.add(path)

        for path in current_paths:
            try:
                mtime = os.path.getmtime(path)
                if mtime != self._watched.get(path):
                    self._watched[path] = mtime
                    events.append(FileEvent(path, "modified"))
            except OSError:
                events.append(FileEvent(path, "deleted"))
                del self._watched[path]

        for path in new_paths - current_paths:
            if os.path.isfile(path):
                try:
                    self._watched[path] = os.path.getmtime(path)
                    events.append(FileEvent(path, "created"))
                except OSError:
                    pass

        for event in events:
            if self._callback:
                self._callback(event)

        return events

    def watch_loop(self, duration: Optional[float] = None) -> List[FileEvent]:
        """Run watcher loop for specified duration.

        Args:
            duration: Seconds to watch. None for infinite.

        Returns:
            All events collected.
        """
        all_events: List[FileEvent] = []
        end_time = None if duration is None else time.time() + duration
        while end_time is None or time.time() < end_time:
            events = self.check()
            all_events.extend(events)
            time.sleep(self._poll_interval)
        return all_events
