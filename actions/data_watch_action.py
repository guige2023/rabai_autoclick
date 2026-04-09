"""Data Watch Action Module.

Monitors data structures for changes and triggers callbacks on
detected modifications with debouncing and coalescing support.
"""

import time
import hashlib
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class ChangeEvent:
    event_id: str
    timestamp: float
    path: str
    change_type: str
    old_value: Any
    new_value: Any
    coalesced_count: int = 1


@dataclass
class WatchConfig:
    debounce_ms: float = 100.0
    coalesce_window_ms: float = 500.0
    max_coalesce_count: int = 100
    detect_nested: bool = True
    detect_type_changes: bool = True


class DataWatchAction:
    """Watches data for changes and fires callbacks on modifications."""

    def __init__(
        self,
        config: Optional[WatchConfig] = None,
    ) -> None:
        self._config = config or WatchConfig()
        self._callbacks: Dict[str, List[Callable]] = defaultdict(list)
        self._watched_paths: Dict[str, Any] = {}
        self._path_hashes: Dict[str, str] = {}
        self._pending_changes: Dict[str, List[ChangeEvent]] = defaultdict(list)
        self._lock = threading.RLock()
        self._watching = False
        self._event_counter = 0

    def watch(
        self,
        path: str,
        data: Any,
        callback: Optional[Callable[[ChangeEvent], None]] = None,
    ) -> None:
        with self._lock:
            self._watched_paths[path] = data
            self._path_hashes[path] = self._compute_hash(data)
            if callback:
                self._callbacks[path].append(callback)
            self._watching = True

    def unwatch(self, path: str) -> bool:
        with self._lock:
            if path in self._watched_paths:
                del self._watched_paths[path]
                del self._path_hashes[path]
                self._callbacks.pop(path, None)
                return True
            return False

    def update(
        self,
        path: str,
        new_data: Any,
        trigger_callbacks: bool = True,
    ) -> Optional[ChangeEvent]:
        with self._lock:
            old_data = self._watched_paths.get(path)
            old_hash = self._path_hashes.get(path)
            new_hash = self._compute_hash(new_data)
            if old_hash == new_hash:
                return None
            self._watched_paths[path] = new_data
            self._path_hashes[path] = new_hash
            event = ChangeEvent(
                event_id=f"evt_{self._event_counter}",
                timestamp=time.time(),
                path=path,
                change_type=self._detect_change_type(old_data, new_data),
                old_value=old_data,
                new_value=new_data,
            )
            self._event_counter += 1
            if trigger_callbacks:
                self._pending_changes[path].append(event)
                self._process_pending(path)
            return event

    def check(self) -> List[ChangeEvent]:
        events: List[ChangeEvent] = []
        with self._lock:
            for path in list(self._watched_paths.keys()):
                current = self._watched_paths[path]
                current_hash = self._path_hashes[path]
                new_hash = self._compute_hash(current)
                if current_hash != new_hash:
                    event = ChangeEvent(
                        event_id=f"evt_{self._event_counter}",
                        timestamp=time.time(),
                        path=path,
                        change_type="external",
                        old_value=None,
                        new_value=current,
                    )
                    self._event_counter += 1
                    events.append(event)
        return events

    def _process_pending(self, path: str) -> None:
        pending = self._pending_changes.get(path, [])
        if not pending:
            return
        if len(pending) == 1:
            self._fire_callbacks(path, pending[0])
            self._pending_changes[path] = []
            return
        coalesced = ChangeEvent(
            event_id=f"evt_{self._event_counter}",
            timestamp=time.time(),
            path=path,
            change_type="coalesced",
            old_value=pending[0].old_value,
            new_value=pending[-1].new_value,
            coalesced_count=len(pending),
        )
        self._event_counter += 1
        self._pending_changes[path] = []
        self._fire_callbacks(path, coalesced)

    def _fire_callbacks(self, path: str, event: ChangeEvent) -> None:
        for cb in self._callbacks.get(path, []):
            try:
                cb(event)
            except Exception as e:
                logger.error(f"Watch callback failed for {path}: {e}")

    def _detect_change_type(self, old: Any, new: Any) -> str:
        if old is None:
            return "added"
        if new is None:
            return "deleted"
        if type(old) != type(new):
            if self._config.detect_type_changes:
                return "type_changed"
        if isinstance(old, dict) and isinstance(new, dict):
            if len(new) > len(old):
                return "added"
            if len(new) < len(old):
                return "removed"
        return "modified"

    def _compute_hash(self, data: Any) -> str:
        try:
            serialized = str(data).encode("utf-8")
        except Exception:
            serialized = repr(data).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()[:16]

    def get_watched_paths(self) -> List[str]:
        with self._lock:
            return list(self._watched_paths.keys())

    def is_watching(self, path: str) -> bool:
        with self._lock:
            return path in self._watched_paths
