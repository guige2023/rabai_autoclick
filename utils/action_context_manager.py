"""
Action Context Manager.

Manage the context (state, environment, metadata) surrounding
UI automation actions for better logging, debugging, and replay.

Usage:
    from utils.action_context_manager import ActionContext, ActionContextManager

    ctx_mgr = ActionContextManager()
    with ctx_mgr.context(app="Finder", action="click") as ctx:
        ctx.log("Starting click at (100, 200)")
        bridge.click_at(100, 200)
        ctx.log("Click completed")
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import contextmanager
import time

if TYPE_CHECKING:
    pass


@dataclass
class ActionContext:
    """Context information for a single action or action sequence."""
    action_name: str
    app_name: Optional[str] = None
    app_bundle_id: Optional[str] = None
    window_title: Optional[str] = None
    screen_region: Optional[Tuple[int, int, int, int]] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    success: bool = True
    error: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        """Duration of the action in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    @property
    def formatted_start(self) -> str:
        """Formatted start time string."""
        return datetime.fromtimestamp(self.start_time).strftime("%H:%M:%S.%f")[:-3]


@dataclass
class ActionContextSnapshot:
    """A snapshot of all active contexts."""
    active_contexts: List[ActionContext] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


class ActionContextManager:
    """
    Manage action execution contexts.

    Provides context managers and utilities for tracking what
    actions are being performed, their environment, and results.

    Example:
        manager = ActionContextManager()
        with manager.context(action="drag") as ctx:
            bridge.drag(start, end)
    """

    def __init__(self, max_history: int = 1000) -> None:
        """
        Initialize the context manager.

        Args:
            max_history: Maximum number of completed contexts to retain.
        """
        self._current: Optional[ActionContext] = None
        self._history: List[ActionContext] = []
        self._max_history = max_history
        self._listeners: List[Callable[[ActionContext], None]] = []
        self._log_handlers: List[Callable[[str], None]] = []

    @contextmanager
    def context(
        self,
        action_name: str,
        app_name: Optional[str] = None,
        **metadata: Any,
    ):
        """
        Create a new action context as a context manager.

        Args:
            action_name: Name of the action (e.g., "click", "drag").
            app_name: Optional name of the target application.
            **metadata: Additional metadata fields.

        Yields:
            ActionContext that can be logged to and updated.
        """
        ctx = ActionContext(
            action_name=action_name,
            app_name=app_name,
            metadata=dict(metadata),
        )
        previous = self._current
        self._current = ctx

        try:
            yield ctx
        except Exception as e:
            ctx.success = False
            ctx.error = str(e)
            raise
        finally:
            ctx.end_time = time.time()
            self._history.append(ctx)
            if len(self._history) > self._max_history:
                self._history.pop(0)
            self._current = previous

            for listener in self._listeners:
                try:
                    listener(ctx)
                except Exception:
                    pass

    def log(self, message: str) -> None:
        """
        Log a message to the current context.

        Args:
            message: Log message string.
        """
        if self._current:
            timestamp = datetime.fromtimestamp(time.time()).strftime("%H:%M:%S.%f")[:-3]
            self._current.logs.append(f"[{timestamp}] {message}")

        for handler in self._log_handlers:
            try:
                handler(message)
            except Exception:
                pass

    def on_context_complete(
        self,
        callback: Callable[[ActionContext], None],
    ) -> None:
        """
        Register a callback for when a context completes.

        Args:
            callback: Function called with the completed ActionContext.
        """
        if callback not in self._listeners:
            self._listeners.append(callback)

    def on_log(
        self,
        handler: Callable[[str], None],
    ) -> None:
        """
        Register a log message handler.

        Args:
            handler: Function called with each log message.
        """
        if handler not in self._log_handlers:
            self._log_handlers.append(handler)

    def get_current(self) -> Optional[ActionContext]:
        """Get the currently active context, if any."""
        return self._current

    def get_history(
        self,
        since: Optional[float] = None,
        action_name: Optional[str] = None,
        app_name: Optional[str] = None,
        successful_only: bool = False,
    ) -> List[ActionContext]:
        """
        Get historical contexts with optional filters.

        Args:
            since: Optional Unix timestamp filter.
            action_name: Optional action name filter.
            app_name: Optional app name filter.
            successful_only: If True, only return successful contexts.

        Returns:
            List of matching ActionContext objects.
        """
        results = list(reversed(self._history))

        if since is not None:
            results = [c for c in results if c.start_time >= since]
        if action_name:
            results = [c for c in results if c.action_name == action_name]
        if app_name:
            results = [c for c in results if c.app_name == app_name]
        if successful_only:
            results = [c for c in results if c.success]

        return results

    def snapshot(self) -> ActionContextSnapshot:
        """Get a snapshot of all current context state."""
        return ActionContextSnapshot(
            active_contexts=[self._current] if self._current else [],
            timestamp=time.time(),
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about recorded contexts.

        Returns:
            Dictionary with counts and timing stats.
        """
        if not self._history:
            return {"total": 0, "successful": 0, "failed": 0}

        successful = [c for c in self._history if c.success]
        durations = [c.duration for c in successful if c.duration is not None]

        return {
            "total": len(self._history),
            "successful": len(successful),
            "failed": len(self._history) - len(successful),
            "avg_duration": sum(durations) / len(durations) if durations else None,
            "min_duration": min(durations) if durations else None,
            "max_duration": max(durations) if durations else None,
        }

