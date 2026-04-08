"""
Data Watch Action Module.

Monitors data for changes using polling or callbacks,
detects additions, modifications, and deletions.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Protocol
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
import asyncio
import threading

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of data changes detected."""
    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"


@dataclass
class DataChange:
    """Detected data change."""
    change_type: ChangeType
    key: str
    old_value: Any = None
    new_value: Any = None
    timestamp: float = field(default_factory=time.time)


class DataWatchAction:
    """
    Monitors data structures for changes.

    Supports polling mode with configurable intervals,
    and callback-based change notifications.

    Example:
        watcher = DataWatchAction(poll_interval=1.0)
        watcher.watch(my_dict, callback=on_change)
        watcher.start()
    """

    def __init__(
        self,
        poll_interval: float = 1.0,
        comparator: Optional[Callable[[Any, Any], bool]] = None,
    ) -> None:
        self.poll_interval = poll_interval
        self.comparator = comparator or (lambda a, b: a == b)
        self._watched: dict[str, tuple[Any, Optional[Callable[[DataChange], None]]]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._changes: list[DataChange] = []

    def watch(
        self,
        data: Any,
        key: str = "default",
        callback: Optional[Callable[[DataChange], None]] = None,
    ) -> None:
        """Register a data source to watch."""
        with self._lock:
            self._watched[key] = (data, callback)

    def unwatch(self, key: str) -> None:
        """Stop watching a data source."""
        with self._lock:
            if key in self._watched:
                del self._watched[key]

    def start(self) -> None:
        """Start the watcher in background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("DataWatchAction started")

    def stop(self) -> None:
        """Stop the watcher."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("DataWatchAction stopped")

    def get_changes(
        self,
        since: Optional[float] = None,
        key: Optional[str] = None,
    ) -> list[DataChange]:
        """Get recorded changes, optionally filtered."""
        changes = self._changes
        if key:
            changes = [c for c in changes if c.key == key]
        if since:
            changes = [c for c in changes if c.timestamp >= since]
        return changes

    def clear_changes(self) -> None:
        """Clear recorded changes."""
        self._changes.clear()

    def _poll_loop(self) -> None:
        """Main polling loop."""
        previous: dict[str, Any] = {}

        while self._running:
            try:
                with self._lock:
                    watched_items = dict(self._watched)

                for key, (data, _) in watched_items.items():
                    old_snapshot = previous.get(key)
                    new_snapshot = self._deep_copy(data)

                    detected = self._detect_changes(
                        old_snapshot,
                        new_snapshot,
                        key,
                    )

                    for change in detected:
                        self._changes.append(change)
                        if key in self._watched:
                            _, callback = self._watched[key]
                            if callback:
                                callback(change)

                    previous[key] = new_snapshot

            except Exception as e:
                logger.error("Watch poll error: %s", e)

            time.sleep(self.poll_interval)

    def _detect_changes(
        self,
        old_data: Any,
        new_data: Any,
        key: str,
    ) -> list[DataChange]:
        """Detect changes between old and new data."""
        changes: list[DataChange] = []

        if old_data is None:
            if new_data is not None:
                changes.append(DataChange(
                    change_type=ChangeType.ADDED,
                    key=key,
                    new_value=new_data,
                ))
            return changes

        if isinstance(old_data, dict) and isinstance(new_data, dict):
            all_keys = set(old_data.keys()) | set(new_data.keys())

            for k in all_keys:
                if k not in new_data:
                    changes.append(DataChange(
                        change_type=ChangeType.REMOVED,
                        key=f"{key}.{k}",
                        old_value=old_data.get(k),
                    ))
                elif k not in old_data:
                    changes.append(DataChange(
                        change_type=ChangeType.ADDED,
                        key=f"{key}.{k}",
                        new_value=new_data.get(k),
                    ))
                elif not self.comparator(old_data.get(k), new_data.get(k)):
                    changes.append(DataChange(
                        change_type=ChangeType.MODIFIED,
                        key=f"{key}.{k}",
                        old_value=old_data.get(k),
                        new_value=new_data.get(k),
                    ))

        else:
            if not self.comparator(old_data, new_data):
                changes.append(DataChange(
                    change_type=ChangeType.MODIFIED,
                    key=key,
                    old_value=old_data,
                    new_value=new_data,
                ))

        return changes

    def _deep_copy(self, data: Any) -> Any:
        """Create a deep copy for comparison."""
        import copy
        try:
            return copy.deepcopy(data)
        except Exception:
            return data
