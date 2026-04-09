"""
File System Watcher and Event Monitor Module.

Watches files and directories for changes, triggering automation
actions on create, modify, delete, and rename events.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class FileEventType(Enum):
    CREATED = auto()
    MODIFIED = auto()
    DELETED = auto()
    RENAMED = auto()
    ACCESSED = auto()
    ERROR = auto()


@dataclass(frozen=True)
class FileEvent:
    event_type: FileEventType
    path: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    old_path: Optional[str] = None
    size_bytes: Optional[int] = None
    is_directory: bool = False


@dataclass
class WatchPattern:
    path: str
    recursive: bool = True
    include_patterns: FrozenSet[str] = field(default_factory=frozenset)
    exclude_patterns: FrozenSet[str] = field(default_factory=frozenset)
    debounce_ms: float = 500.0
    ignore_hidden: bool = True


@dataclass
class FileSnapshot:
    path: str
    mtime: float
    size: int
    content_hash: Optional[str] = None


class FileHasher:
    """Computes content hashes for file change detection."""

    CHUNK_SIZE = 65536

    @classmethod
    def hash_file(cls, path: str, algorithm: str = "sha256") -> Optional[str]:
        try:
            h = hashlib.new(algorithm)
            with open(path, "rb") as f:
                while chunk := f.read(cls.CHUNK_SIZE):
                    h.update(chunk)
            return h.hexdigest()
        except Exception as exc:
            logger.error("Hash failed for %s: %s", path, exc)
            return None

    @classmethod
    def hash_bytes(cls, data: bytes, algorithm: str = "sha256") -> str:
        h = hashlib.new(algorithm)
        h.update(data)
        return h.hexdigest()


class FileWatcher:
    """
    Watches file system paths for change events.
    """

    def __init__(self):
        self._watches: Dict[str, WatchPattern] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        self._snapshots: Dict[str, FileSnapshot] = {}
        self._running: bool = False
        self._watch_task: Optional[asyncio.Task] = None
        self._pending_events: Dict[str, float] = {}

    def add_watch(
        self,
        path: str,
        recursive: bool = True,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
        debounce_ms: float = 500.0,
    ) -> str:
        watch_id = hashlib.md5(path.encode()).hexdigest()[:8]
        pattern = WatchPattern(
            path=path,
            recursive=recursive,
            include_patterns=frozenset(include_patterns or ["*"]),
            exclude_patterns=frozenset(exclude_patterns or []),
            debounce_ms=debounce_ms,
        )
        self._watches[watch_id] = pattern
        logger.info("Added watch: %s -> %s", watch_id, path)
        return watch_id

    def remove_watch(self, watch_id: str) -> bool:
        if watch_id in self._watches:
            del self._watches[watch_id]
            logger.info("Removed watch: %s", watch_id)
            return True
        return False

    def register_handler(
        self, event_type: FileEventType, handler: Callable[[FileEvent], None]
    ) -> None:
        key = event_type.name
        if key not in self._handlers:
            self._handlers[key] = []
        self._handlers[key].append(handler)

    def register_global_handler(
        self, handler: Callable[[FileEvent], None]
    ) -> None:
        key = "GLOBAL"
        if key not in self._handlers:
            self._handlers[key] = []
        self._handlers[key].append(handler)

    async def start(self) -> None:
        self._running = True
        self._watch_task = asyncio.create_task(self._watch_loop())
        logger.info("File watcher started")

    async def stop(self) -> None:
        self._running = False
        if self._watch_task:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
        logger.info("File watcher stopped")

    async def _watch_loop(self) -> None:
        import aiofiles.os

        last_state: Dict[str, Tuple[float, int]] = {}

        while self._running:
            for watch_id, pattern in list(self._watches.items()):
                try:
                    await self._check_path(pattern, last_state)
                except Exception as exc:
                    logger.error("Watch error for %s: %s", pattern.path, exc)

            await asyncio.sleep(0.5)

    async def _check_path(
        self, pattern: WatchPattern, last_state: Dict[str, Tuple[float, int]]
    ) -> None:
        import os
        import aiofiles.os

        paths_to_check = []
        if os.path.isdir(pattern.path):
            if pattern.recursive:
                for root, dirs, files in os.walk(pattern.path):
                    if pattern.ignore_hidden:
                        dirs[:] = [d for d in dirs if not d.startswith(".")]
                        files = [f for f in files if not f.startswith(".")]

                    for fname in files:
                        full_path = os.path.join(root, fname)
                        paths_to_check.append(full_path)
                    for dname in dirs:
                        full_path = os.path.join(root, dname)
                        paths_to_check.append(full_path)
            else:
                try:
                    for entry in os.listdir(pattern.path):
                        paths_to_check.append(os.path.join(pattern.path, entry))
                except PermissionError:
                    pass
        else:
            paths_to_check.append(pattern.path)

        current_files: Set[str] = set()
        for fpath in paths_to_check:
            if not os.path.exists(fpath):
                continue

            if fpath in last_state:
                old_mtime, old_size = last_state[fpath]
                try:
                    stat = os.stat(fpath)
                    if stat.st_mtime != old_mtime or stat.st_size != old_size:
                        event = FileEvent(
                            event_type=FileEventType.MODIFIED,
                            path=fpath,
                            size_bytes=stat.st_size,
                            is_directory=os.path.isdir(fpath),
                        )
                        await self._emit_event(event)
                        last_state[fpath] = (stat.st_mtime, stat.st_size)
                except OSError:
                    pass
            else:
                try:
                    stat = os.stat(fpath)
                    event = FileEvent(
                        event_type=FileEventType.CREATED,
                        path=fpath,
                        size_bytes=stat.st_size,
                        is_directory=os.path.isdir(fpath),
                    )
                    await self._emit_event(event)
                    last_state[fpath] = (stat.st_mtime, stat.st_size)
                except OSError:
                    pass

            current_files.add(fpath)

        for fpath in list(last_state.keys()):
            if fpath not in current_files and os.path.exists(os.path.dirname(fpath)):
                event = FileEvent(
                    event_type=FileEventType.DELETED,
                    path=fpath,
                )
                await self._emit_event(event)
                del last_state[fpath]

    async def _emit_event(self, event: FileEvent) -> None:
        event_key = f"{event.path}:{event.event_type.name}"
        now = time.time()

        if event_key in self._pending_events:
            if now - self._pending_events[event_key] < 0.5:
                return

        self._pending_events[event_key] = now

        handlers = self._handlers.get(event.event_type.name, [])
        handlers.extend(self._handlers.get("GLOBAL", []))

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as exc:
                logger.error("Event handler error: %s", exc)

    def take_snapshot(self, path: str) -> Optional[FileSnapshot]:
        import os

        try:
            stat = os.stat(path)
            snapshot = FileSnapshot(
                path=path,
                mtime=stat.st_mtime,
                size=stat.st_size,
            )
            if not os.path.isdir(path):
                snapshot.content_hash = FileHasher.hash_file(path)
            self._snapshots[path] = snapshot
            return snapshot
        except Exception as exc:
            logger.error("Snapshot failed for %s: %s", path, exc)
            return None

    def detect_changes(self, path: str) -> Tuple[bool, Optional[str]]:
        """Compare current file state to last snapshot."""
        import os

        if path not in self._snapshots:
            return (True, "No previous snapshot")

        old = self._snapshots[path]
        try:
            stat = os.stat(path)
            if stat.st_mtime != old.mtime:
                new_hash = FileHasher.hash_file(path)
                if new_hash != old.content_hash:
                    return (True, f"Content changed: {old.content_hash} -> {new_hash}")
                return (True, f"Mtime changed but content same")
            return (False, None)
        except OSError:
            return (True, "File no longer exists")
