"""
Automation Watcher Action Module.

Watches files, directories, and resources for changes
 and triggers automation actions on detected changes.
"""

from __future__ import annotations

import os
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class WatchEvent(Enum):
    """Type of watch event."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"
    ANY = "any"


@dataclass
class WatchedResource:
    """A resource being watched."""
    path: str
    event_types: list[WatchEvent]
    callback: Callable


@dataclass
class WatchEventResult:
    """A detected watch event."""
    path: str
    event_type: WatchEvent
    timestamp: float
    metadata: dict[str, Any] = field(default_factory=dict)


class AutomationWatcherAction:
    """
    File and resource watcher for automation triggers.

    Monitors files, directories, and other resources for changes
    and triggers callback actions when changes are detected.

    Example:
        watcher = AutomationWatcherAction()
        watcher.watch_file("/tmp/data.csv", WatchEvent.MODIFIED, on_modified)
        watcher.watch_directory("/var/log", WatchEvent.CREATED, on_new_file)
        watcher.start()
    """

    def __init__(
        self,
        poll_interval: float = 1.0,
    ) -> None:
        self.poll_interval = poll_interval
        self._resources: list[WatchedResource] = []
        self._running = False
        self._file_states: dict[str, tuple[float, int]] = {}

    def watch_file(
        self,
        path: str,
        event: WatchEvent,
        callback: Callable[[WatchEventResult], None],
    ) -> "AutomationWatcherAction":
        """Watch a single file for changes."""
        resource = WatchedResource(
            path=path,
            event_types=[event],
            callback=callback,
        )
        self._resources.append(resource)
        self._initialize_file_state(path)
        return self

    def watch_directory(
        self,
        path: str,
        event: WatchEvent,
        callback: Callable[[WatchEventResult], None],
        recursive: bool = False,
    ) -> "AutomationWatcherAction":
        """Watch a directory for changes."""
        resource = WatchedResource(
            path=path,
            event_types=[event],
            callback=callback,
        )
        self._resources.append(resource)

        if recursive:
            for root, dirs, files in os.walk(path):
                for f in files:
                    filepath = os.path.join(root, f)
                    self._initialize_file_state(filepath)

        return self

    def start(self) -> None:
        """Start watching resources."""
        self._running = True
        logger.info(f"Watcher started, monitoring {len(self._resources)} resources")

    def stop(self) -> None:
        """Stop watching resources."""
        self._running = False
        logger.info("Watcher stopped")

    async def check_changes(self) -> list[WatchEventResult]:
        """Check for any changes since last check."""
        events: list[WatchEventResult] = []

        for resource in self._resources:
            if os.path.isfile(resource.path):
                event = self._check_file(resource)
                if event:
                    events.append(event)
                    self._initialize_file_state(resource.path)

            elif os.path.isdir(resource.path):
                dir_events = self._check_directory(resource)
                events.extend(dir_events)

        return events

    def _initialize_file_state(self, path: str) -> None:
        """Initialize state tracking for a file."""
        try:
            stat = os.stat(path)
            self._file_states[path] = (stat.st_mtime, stat.st_size)
        except OSError:
            self._file_states[path] = (0, 0)

    def _check_file(self, resource: WatchedResource) -> Optional[WatchEventResult]:
        """Check a single file for changes."""
        path = resource.path

        if not os.path.exists(path):
            if WatchEvent.DELETED in resource.event_types or WatchEvent.ANY in resource.event_types:
                return WatchEventResult(
                    path=path,
                    event_type=WatchEvent.DELETED,
                    timestamp=time.time(),
                )
            return None

        try:
            stat = os.stat(path)
            old_state = self._file_states.get(path)

            if old_state is None:
                if WatchEvent.CREATED in resource.event_types or WatchEvent.ANY in resource.event_types:
                    return WatchEventResult(
                        path=path,
                        event_type=WatchEvent.CREATED,
                        timestamp=time.time(),
                    )
            else:
                old_mtime, old_size = old_state
                new_mtime, new_size = stat.st_mtime, stat.st_size

                if new_size != old_size or abs(new_mtime - old_mtime) > 0.001:
                    if WatchEvent.MODIFIED in resource.event_types or WatchEvent.ANY in resource.event_types:
                        return WatchEventResult(
                            path=path,
                            event_type=WatchEvent.MODIFIED,
                            timestamp=time.time(),
                            metadata={"old_size": old_size, "new_size": new_size},
                        )

        except OSError:
            pass

        return None

    def _check_directory(self, resource: WatchedResource) -> list[WatchEventResult]:
        """Check a directory for changes."""
        events: list[WatchEventResult] = []
        path = resource.path

        if not os.path.exists(path):
            return events

        try:
            current_files: set[str] = set()
            for root, dirs, files in os.walk(path):
                for f in files:
                    filepath = os.path.join(root, f)
                    current_files.add(filepath)

                    if filepath not in self._file_states:
                        if WatchEvent.CREATED in resource.event_types or WatchEvent.ANY in resource.event_types:
                            events.append(WatchEventResult(
                                path=filepath,
                                event_type=WatchEvent.CREATED,
                                timestamp=time.time(),
                            ))
                        self._initialize_file_state(filepath)

            removed_files = set(self._file_states.keys()) - current_files
            for filepath in removed_files:
                if WatchEvent.DELETED in resource.event_types or WatchEvent.ANY in resource.event_types:
                    events.append(WatchEventResult(
                        path=filepath,
                        event_type=WatchEvent.DELETED,
                        timestamp=time.time(),
                    ))
                del self._file_states[filepath]

        except Exception as e:
            logger.error(f"Error checking directory {path}: {e}")

        return events
