"""File watch action module for RabAI AutoClick.

Provides file system monitoring with watch events,
debouncing, and automatic reaction to file changes.
"""

import sys
import os
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import hashlib

try:
    import watchdog.observers
    import watchdog.events
    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WatchEvent(Enum):
    """File watch event types."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"
    ANY = "any"


@dataclass
class FileWatch:
    """A file system watch."""
    watch_id: str
    path: str
    patterns: List[str]
    recursive: bool = True
    enabled: bool = True
    debounce_ms: int = 500
    on_created: Optional[str] = None
    on_modified: Optional[str] = None
    on_deleted: Optional[str] = None
    on_moved: Optional[str] = None


class FileWatchAction(BaseAction):
    """Monitor file system changes and react.
    
    Uses watchdog library for cross-platform file monitoring
    with pattern matching, debouncing, and event actions.
    """
    action_type = "file_watch"
    display_name = "文件监控"
    description = "文件系统监控和自动响应，支持模式匹配和防抖"

    _watches: Dict[str, FileWatch] = {}
    _observers: Dict[str, Any] = {}
    _event_queues: Dict[str, deque] = {}
    _locks: Dict[str, threading.Lock] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage file watches.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str (watch/list_events/get_events/stop/clear)
                - watch_id: str, unique watch identifier
                - path: str, directory path to watch
                - patterns: list of glob patterns (e.g., ["*.txt", "*.json"])
                - recursive: bool, watch subdirectories
                - debounce_ms: int, debounce delay in milliseconds
                - timeout: int, timeout for get_events
                - save_to_var: str
        
        Returns:
            ActionResult with operation result.
        """
        if not WATCHDOG_AVAILABLE:
            return ActionResult(
                success=False,
                message="watchdog library not installed. Run: pip install watchdog"
            )

        operation = params.get('operation', '')
        watch_id = params.get('watch_id', '')
        path = params.get('path', '')
        patterns = params.get('patterns', ['*'])
        recursive = params.get('recursive', True)
        debounce_ms = params.get('debounce_ms', 500)
        timeout = params.get('timeout', 5)
        save_to_var = params.get('save_to_var', None)

        if operation == 'watch':
            return self._start_watch(context, params, watch_id, path, patterns, recursive, debounce_ms)
        elif operation == 'list_events':
            return self._list_events(watch_id, params, save_to_var)
        elif operation == 'get_events':
            return self._get_events(watch_id, timeout, params, save_to_var)
        elif operation == 'stop':
            return self._stop_watch(watch_id)
        elif operation == 'clear':
            return self._clear_events(watch_id)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

    def _start_watch(
        self, context: Any, params: Dict, watch_id: str,
        path: str, patterns: List[str], recursive: bool, debounce_ms: int
    ) -> ActionResult:
        """Start watching a path."""
        if not path:
            return ActionResult(success=False, message="path is required")
        if not os.path.exists(path):
            return ActionResult(success=False, message=f"Path does not exist: {path}")
        if not os.path.isdir(path):
            return ActionResult(success=False, message=f"Path is not a directory: {path}")

        if watch_id in self._watches and self._watches[watch_id].enabled:
            return ActionResult(success=True, message=f"Watch '{watch_id}' already active")

        watch = FileWatch(
            watch_id=watch_id,
            path=path,
            patterns=patterns,
            recursive=recursive,
            debounce_ms=debounce_ms
        )
        self._watches[watch_id] = watch
        self._event_queues[watch_id] = deque(maxlen=1000)
        self._locks[watch_id] = threading.Lock()

        handler = self._create_event_handler(watch_id, params.get('context'))
        observer = watchdog.observers.Observer()
        observer.schedule(handler, path, recursive=recursive)
        observer.daemon = True
        observer.start()
        self._observers[watch_id] = observer

        return ActionResult(
            success=True,
            message=f"Started watching: {path}",
            data={'watch_id': watch_id, 'path': path, 'patterns': patterns}
        )

    def _create_event_handler(self, watch_id: str, context: Any) -> Any:
        """Create watchdog event handler."""
        watch = self._watches.get(watch_id)
        if not watch:
            return None

        debounce_key = {'path': '', 'last_event': 0, 'debounce_ms': watch.debounce_ms}

        class WatchdogHandler(watchdog.events.FileSystemEventHandler):
            def __handler__(self, event):
                if not watch.enabled:
                    return

                event_type = event.event_type
                src_path = event.src_path

                # Check patterns
                import fnmatch
                matched = any(fnmatch.fnmatch(src_path, p) for p in watch.patterns)
                if not matched and watch.patterns != ['*']:
                    return

                # Debounce
                now = time.time() * 1000
                if (src_path == debounce_key['path'] and
                        now - debounce_key['last_event'] < debounce_key['debounce_ms']):
                    return
                debounce_key['path'] = src_path
                debounce_key['last_event'] = now

                with self._locks[watch_id]:
                    self._event_queues[watch_id].append({
                        'type': event_type,
                        'path': src_path,
                        'timestamp': time.time()
                    })

        handler = WatchdogHandler()
        handler._locks = self._locks
        handler._event_queues = self._event_queues
        return handler

    def _list_events(self, watch_id: str, params: Dict, save_to_var: Optional[str]) -> ActionResult:
        """List all queued events without consuming."""
        if watch_id not in self._watches:
            return ActionResult(success=False, message=f"Watch '{watch_id}' not found")

        with self._locks[watch_id]:
            events = list(self._event_queues[watch_id])

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = events

        return ActionResult(
            success=True,
            message=f"{len(events)} event(s) queued",
            data=events
        )

    def _get_events(
        self, watch_id: str, timeout: int,
        params: Dict, save_to_var: Optional[str]
    ) -> ActionResult:
        """Get events with timeout."""
        if watch_id not in self._watches:
            return ActionResult(success=False, message=f"Watch '{watch_id}' not found")

        events = []
        deadline = time.time() + timeout

        while time.time() < deadline:
            with self._locks[watch_id]:
                if self._event_queues[watch_id]:
                    events.append(self._event_queues[watch_id].popleft())
            if events:
                break
            time.sleep(0.1)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = events

        return ActionResult(
            success=True,
            message=f"Retrieved {len(events)} event(s)",
            data=events
        )

    def _stop_watch(self, watch_id: str) -> ActionResult:
        """Stop a watch."""
        if watch_id in self._observers:
            self._observers[watch_id].stop()
            del self._observers[watch_id]
        if watch_id in self._watches:
            self._watches[watch_id].enabled = False

        return ActionResult(success=True, message=f"Stopped watch '{watch_id}'")

    def _clear_events(self, watch_id: str) -> ActionResult:
        """Clear queued events."""
        if watch_id not in self._watches:
            return ActionResult(success=False, message=f"Watch '{watch_id}' not found")

        with self._locks[watch_id]:
            count = len(self._event_queues[watch_id])
            self._event_queues[watch_id].clear()

        return ActionResult(success=True, message=f"Cleared {count} events")

    def get_required_params(self) -> List[str]:
        return ['operation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'watch_id': '',
            'path': '',
            'patterns': ['*'],
            'recursive': True,
            'debounce_ms': 500,
            'timeout': 5,
            'save_to_var': None,
        }
