"""
Debounce Policy Utilities for Input Handling.

This module provides configurable debounce policies for filtering
rapid input events in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, List
from enum import Enum
import threading


class DebouncePolicy(Enum):
    """Debounce policies."""
    LEADING = "leading"
    TRAILING = "trailing"
    LEADING_TRAILING = "leading_trailing"
    COUNT_BASED = "count_based"
    ADAPTIVE = "adaptive"


@dataclass
class DebounceConfig:
    """Configuration for debouncing."""
    wait_ms: int = 100
    max_wait_ms: int = 1000
    min_calls: int = 1
    policy: DebouncePolicy = DebouncePolicy.TRAILING
    leading_interval_ms: int = 100


@dataclass
class DebounceEvent:
    """Event in debounce tracking."""
    timestamp: float
    call_count: int
    last_value: Any


class Debouncer:
    """
    Configurable debouncer for input events.
    """

    def __init__(self, config: Optional[DebounceConfig] = None):
        """
        Initialize debouncer.

        Args:
            config: Debounce configuration
        """
        self.config = config or DebounceConfig()
        self._timer: Optional[threading.Timer] = None
        self._last_call_time: float = 0.0
        self._call_count: int = 0
        self._pending_value: Any = None
        self._lock = threading.Lock()

    def __call__(self, func: Callable) -> Callable:
        """
        Decorator for debounced function.

        Args:
            func: Function to debounce

        Returns:
            Wrapped function
        """
        def wrapper(*args, **kwargs):
            return self.execute(func, args, kwargs)
        return wrapper

    def execute(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None
    ) -> Any:
        """
        Execute function with debouncing.

        Args:
            func: Function to execute
            args: Function arguments
            kwargs: Function keyword arguments

        Returns:
            Function result or None
        """
        kwargs = kwargs or {}
        current_time = time.time()
        wait_seconds = self.config.wait_ms / 1000.0

        with self._lock:
            self._call_count += 1
            self._last_call_time = current_time
            self._pending_value = (func, args, kwargs)

            if self.config.policy == DebouncePolicy.LEADING:
                if self._timer:
                    self._timer.cancel()
                self._timer = threading.Timer(wait_seconds, self._execute_pending)
                self._timer.start()
                return None

            elif self.config.policy == DebouncePolicy.TRAILING:
                if self._timer:
                    self._timer.cancel()
                self._timer = threading.Timer(wait_seconds, self._execute_pending)
                self._timer.start()
                return None

            elif self.config.policy == DebouncePolicy.COUNT_BASED:
                if self._call_count >= self.config.min_calls:
                    return self._execute_now()
                return None

            return None

    def _execute_pending(self) -> Any:
        """Execute pending function."""
        with self._lock:
            if self._pending_value:
                func, args, kwargs = self._pending_value
                self._pending_value = None
                return func(*args, **kwargs)
        return None

    def _execute_now(self) -> Any:
        """Execute immediately and reset."""
        with self._lock:
            self._call_count = 0
            if self._timer:
                self._timer.cancel()
                self._timer = None
            if self._pending_value:
                func, args, kwargs = self._pending_value
                self._pending_value = None
                return func(*args, **kwargs)
        return None

    def cancel(self) -> None:
        """Cancel pending execution."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            self._pending_value = None

    def flush(self) -> Any:
        """Flush pending execution immediately."""
        return self._execute_now()

    @property
    def call_count(self) -> int:
        """Get current call count."""
        return self._call_count


class MultiDebouncer:
    """
    Manage multiple debouncers with different keys.
    """

    def __init__(self, config: Optional[DebounceConfig] = None):
        """
        Initialize multi-debouncer.

        Args:
            config: Default configuration
        """
        self.config = config or DebounceConfig()
        self._debouncers: Dict[str, Debouncer] = {}
        self._lock = threading.Lock()

    def get_debouncer(self, key: str) -> Debouncer:
        """Get or create debouncer for key."""
        with self._lock:
            if key not in self._debouncers:
                self._debouncers[key] = Debouncer(self.config)
            return self._debouncers[key]

    def execute(
        self,
        key: str,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None
    ) -> Any:
        """
        Execute debounced function for key.

        Args:
            key: Debouncer key
            func: Function to execute
            args: Function arguments
            kwargs: Function keyword arguments

        Returns:
            Function result or None
        """
        debouncer = self.get_debouncer(key)
        return debouncer.execute(func, args, kwargs)

    def cancel_all(self) -> None:
        """Cancel all pending executions."""
        with self._lock:
            for debouncer in self._debouncers.values():
                debouncer.cancel()

    def flush_all(self) -> List[Any]:
        """Flush all pending executions."""
        results = []
        with self._lock:
            for debouncer in self._debouncers.values():
                result = debouncer.flush()
                if result is not None:
                    results.append(result)
        return results


def create_debounce_decorator(
    wait_ms: int = 100,
    policy: DebouncePolicy = DebouncePolicy.TRAILING
) -> Callable[[Callable], Callable]:
    """
    Create a debounce decorator.

    Args:
        wait_ms: Wait time in milliseconds
        policy: Debounce policy

    Returns:
        Decorator function
    """
    config = DebounceConfig(wait_ms=wait_ms, policy=policy)
    debouncer = Debouncer(config)

    def decorator(func: Callable) -> Callable:
        return debouncer(func)

    return decorator
