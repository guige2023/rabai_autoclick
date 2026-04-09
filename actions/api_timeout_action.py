"""API Timeout and Deadline Manager.

This module provides timeout management for API operations:
- Per-request timeout tracking
- Deadline propagation
- Timeout retry handling
- Resource cleanup hooks

Example:
    >>> from actions.api_timeout_action import TimeoutManager
    >>> mgr = TimeoutManager(default_timeout=30.0)
    >>> with mgr.timeout_context("fetch_data"):
    ...     fetch_data()
"""

from __future__ import annotations

import time
import logging
import threading
import contextlib
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class TimeoutError(Exception):
    """Raised when an operation times out."""
    pass


class TimeoutStrategy(Enum):
    """Timeout handling strategies."""
    RAISE = "raise"
    RETRY = "retry"
    FALLBACK = "fallback"
    EXTEND = "extend"


@dataclass
class TimeoutContext:
    """A timeout context for tracking."""
    name: str
    start_time: float
    deadline: float
    timeout_seconds: float
    is_running: bool = True


class TimeoutManager:
    """Manages timeouts for API operations."""

    def __init__(
        self,
        default_timeout: float = 30.0,
        max_timeout: float = 300.0,
    ) -> None:
        """Initialize the timeout manager.

        Args:
            default_timeout: Default timeout in seconds.
            max_timeout: Maximum allowed timeout in seconds.
        """
        self._default_timeout = default_timeout
        self._max_timeout = max_timeout
        self._contexts: dict[str, TimeoutContext] = {}
        self._cleanup_hooks: dict[str, list[Callable]] = {}
        self._lock = threading.RLock()
        self._stats = {"timeouts_triggered": 0, "operations_completed": 0}

    @contextlib.contextmanager
    def timeout_context(
        self,
        name: str,
        timeout: Optional[float] = None,
        on_timeout: Optional[Callable] = None,
    ):
        """Context manager for timeout tracking.

        Args:
            name: Operation name.
            timeout: Timeout in seconds. None = use default.
            on_timeout: Callback when timeout occurs.

        Yields:
            TimeoutContext object.
        """
        timeout_seconds = min(timeout or self._default_timeout, self._max_timeout)
        deadline = time.time() + timeout_seconds
        start_time = time.time()

        ctx = TimeoutContext(
            name=name,
            start_time=start_time,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
        )

        with self._lock:
            self._contexts[name] = ctx

        try:
            yield ctx
            self._check_timeout(ctx)
        except TimeoutError:
            if on_timeout:
                on_timeout()
            raise
        finally:
            with self._lock:
                self._contexts.pop(name, None)
                self._stats["operations_completed"] += 1

    def _check_timeout(self, ctx: TimeoutContext) -> None:
        """Check if a context has exceeded its timeout."""
        elapsed = time.time() - ctx.start_time
        if elapsed > ctx.timeout_seconds:
            raise TimeoutError(f"Operation '{ctx.name}' timed out after {elapsed:.2f}s")

    def is_timed_out(self, name: str) -> bool:
        """Check if an operation is timed out.

        Args:
            name: Operation name.

        Returns:
            True if timed out.
        """
        with self._lock:
            ctx = self._contexts.get(name)
            if ctx is None:
                return False
            return time.time() > ctx.deadline

    def get_remaining_time(self, name: str) -> float:
        """Get remaining time before timeout.

        Args:
            name: Operation name.

        Returns:
            Seconds remaining, or 0 if timed out.
        """
        with self._lock:
            ctx = self._contexts.get(name)
            if ctx is None:
                return 0.0
            remaining = ctx.deadline - time.time()
            return max(0.0, remaining)

    def extend_timeout(self, name: str, additional_seconds: float) -> bool:
        """Extend the timeout for an operation.

        Args:
            name: Operation name.
            additional_seconds: Seconds to add.

        Returns:
            True if extended, False if not found.
        """
        with self._lock:
            ctx = self._contexts.get(name)
            if ctx is None:
                return False

            new_deadline = ctx.deadline + additional_seconds
            max_deadline = time.time() + self._max_timeout
            ctx.deadline = min(new_deadline, max_deadline)
            ctx.timeout_seconds = ctx.deadline - ctx.start_time
            return True

    def cancel_timeout(self, name: str) -> bool:
        """Cancel a timeout context.

        Args:
            name: Operation name.

        Returns:
            True if cancelled, False if not found.
        """
        with self._lock:
            if name in self._contexts:
                self._contexts[name].is_running = False
                self._contexts.pop(name, None)
                return True
            return False

    def register_cleanup(self, name: str, hook: Callable) -> None:
        """Register a cleanup hook for when timeout occurs.

        Args:
            name: Operation name.
            hook: Callable to execute on cleanup.
        """
        with self._lock:
            if name not in self._cleanup_hooks:
                self._cleanup_hooks[name] = []
            self._cleanup_hooks[name].append(hook)

    def get_active_contexts(self) -> list[str]:
        """Get list of active context names."""
        with self._lock:
            return [ctx.name for ctx in self._contexts.values() if ctx.is_running]

    def get_stats(self) -> dict[str, Any]:
        """Get timeout statistics."""
        with self._lock:
            return {
                **self._stats,
                "active_contexts": len(self._contexts),
            }

    def create_deadline_tracker(
        self,
        deadline: float,
        on_timeout: Optional[Callable] = None,
    ) -> Callable[[], bool]:
        """Create a deadline tracker function.

        Args:
            deadline: Unix timestamp deadline.
            on_timeout: Callback on timeout.

        Returns:
            A function that returns True if deadline passed.
        """
        def check_deadline() -> bool:
            if time.time() > deadline:
                if on_timeout:
                    with self._lock:
                        self._stats["timeouts_triggered"] += 1
                    on_timeout()
                return True
            return False

        return check_deadline
