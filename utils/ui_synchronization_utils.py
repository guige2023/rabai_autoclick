"""UI Synchronization utilities for coordinating UI operations.

This module provides utilities for synchronizing UI operations,
including waiting for elements, conditions, and async coordination.
"""

import asyncio
import threading
import time
from typing import Callable, Optional, Any, List, Dict, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import deque


T = TypeVar('T')


class WaitState(Enum):
    """States for wait operations."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    TIMED_OUT = auto()
    CANCELLED = auto()


@dataclass
class WaitResult(Generic[T]):
    """Result of a wait operation."""
    state: WaitState
    value: Optional[T] = None
    elapsed_time: float = 0.0
    attempts: int = 0
    error: Optional[str] = None


@dataclass
class Condition:
    """A condition to wait for."""
    name: str
    check_fn: Callable[[], bool]
    timeout: float = 30.0
    poll_interval: float = 0.1
    description: str = ""


class SynchronizationManager:
    """Manages synchronization primitives for UI operations."""

    def __init__(self):
        self._locks: Dict[str, threading.Lock] = {}
        self._semaphores: Dict[str, threading.Semaphore] = {}
        self._events: Dict[str, threading.Event] = {}
        self._conditions: Dict[str, threading.Condition] = {}
        self._barriers: Dict[str, threading.Barrier] = {}
        self._lock = threading.Lock()

    def get_lock(self, name: str) -> threading.Lock:
        """Get or create a named lock."""
        with self._lock:
            if name not in self._locks:
                self._locks[name] = threading.Lock()
            return self._locks[name]

    def get_semaphore(self, name: str, value: int = 1) -> threading.Semaphore:
        """Get or create a named semaphore."""
        with self._lock:
            if name not in self._semaphores:
                self._semaphores[name] = threading.Semaphore(value)
            return self._semaphores[name]

    def get_event(self, name: str) -> threading.Event:
        """Get or create a named event."""
        with self._lock:
            if name not in self._events:
                self._events[name] = threading.Event()
            return self._events[name]

    def get_condition(self, name: str) -> threading.Condition:
        """Get or create a named condition."""
        with self._lock:
            if name not in self._conditions:
                self._conditions[name] = threading.Condition()
            return self._conditions[name]

    def get_barrier(self, name: str, parties: int) -> threading.Barrier:
        """Get or create a named barrier."""
        with self._lock:
            if name not in self._barriers:
                self._barriers[name] = threading.Barrier(parties)
            return self._barriers[name]

    def reset_barrier(self, name: str) -> None:
        """Reset a barrier."""
        with self._lock:
            if name in self._barriers:
                self._barriers[name].reset()

    def cleanup(self, name: str) -> None:
        """Remove a synchronization primitive."""
        with self._lock:
            self._locks.pop(name, None)
            self._semaphores.pop(name, None)
            self._events.pop(name, None)
            self._conditions.pop(name, None)
            self._barriers.pop(name, None)


class AsyncSynchronizationManager:
    """Async version of synchronization manager."""

    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._conditions: Dict[str, asyncio.Condition] = {}
        self._barriers: Dict[str, asyncio.Barrier] = {}

    async def get_lock(self, name: str) -> asyncio.Lock:
        """Get or create a named async lock."""
        if name not in self._locks:
            self._locks[name] = asyncio.Lock()
        return self._locks[name]

    async def get_semaphore(self, name: str, value: int = 1) -> asyncio.Semaphore:
        """Get or create a named async semaphore."""
        if name not in self._semaphores:
            self._semaphores[name] = asyncio.Semaphore(value)
        return self._semaphores[name]

    async def get_event(self, name: str) -> asyncio.Event:
        """Get or create a named async event."""
        if name not in self._events:
            self._events[name] = asyncio.Event()
        return self._events[name]

    async def get_condition(self, name: str) -> asyncio.Condition:
        """Get or create a named async condition."""
        if name not in self._conditions:
            self._conditions[name] = asyncio.Condition()
        return self._conditions[name]


async def wait_for(
    condition: Callable[[], bool],
    timeout: float = 30.0,
    poll_interval: float = 0.1,
    error_message: str = "Condition not met"
) -> WaitResult:
    """Wait for a condition to become true.

    Args:
        condition: Function that returns True when condition is met
        timeout: Maximum time to wait in seconds
        poll_interval: Time between condition checks
        error_message: Error message if timeout occurs

    Returns:
        WaitResult with state and timing information
    """
    start_time = time.time()
    attempts = 0

    while True:
        attempts += 1
        if condition():
            elapsed = time.time() - start_time
            return WaitResult(
                state=WaitState.COMPLETED,
                elapsed_time=elapsed,
                attempts=attempts
            )

        elapsed = time.time() - start_time
        if elapsed >= timeout:
            return WaitResult(
                state=WaitState.TIMED_OUT,
                elapsed_time=elapsed,
                attempts=attempts,
                error=error_message
            )

        await asyncio.sleep(poll_interval)


async def wait_for_all(
    conditions: List[Callable[[], bool]],
    timeout: float = 30.0,
    poll_interval: float = 0.1
) -> WaitResult:
    """Wait for all conditions to be met.

    Args:
        conditions: List of condition functions
        timeout: Maximum time to wait
        poll_interval: Time between checks

    Returns:
        WaitResult with aggregated state
    """
    start_time = time.time()
    attempts = 0
    results = [False] * len(conditions)

    while True:
        attempts += 1
        all_met = True

        for i, condition in enumerate(conditions):
            if not results[i]:
                if condition():
                    results[i] = True
                else:
                    all_met = False

        if all_met:
            elapsed = time.time() - start_time
            return WaitResult(
                state=WaitState.COMPLETED,
                elapsed_time=elapsed,
                attempts=attempts
            )

        elapsed = time.time() - start_time
        if elapsed >= timeout:
            return WaitResult(
                state=WaitState.TIMED_OUT,
                elapsed_time=elapsed,
                attempts=attempts
            )

        await asyncio.sleep(poll_interval)


async def wait_for_any(
    conditions: List[Callable[[], bool]],
    timeout: float = 30.0,
    poll_interval: float = 0.1
) -> WaitResult[int]:
    """Wait for any condition to be met.

    Args:
        conditions: List of condition functions
        timeout: Maximum time to wait
        poll_interval: Time between checks

    Returns:
        WaitResult with index of first met condition
    """
    start_time = time.time()
    attempts = 0

    while True:
        attempts += 1

        for i, condition in enumerate(conditions):
            if condition():
                elapsed = time.time() - start_time
                return WaitResult(
                    state=WaitState.COMPLETED,
                    value=i,
                    elapsed_time=elapsed,
                    attempts=attempts
                )

        elapsed = time.time() - start_time
        if elapsed >= timeout:
            return WaitResult(
                state=WaitState.TIMED_OUT,
                elapsed_time=elapsed,
                attempts=attempts
            )

        await asyncio.sleep(poll_interval)


class RateLimiter:
    """Rate limiter for controlling operation frequency."""

    def __init__(self, max_calls: int, time_window: float):
        """Initialize rate limiter.

        Args:
            max_calls: Maximum number of calls allowed
            time_window: Time window in seconds
        """
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = deque()
        self.lock = threading.Lock()

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire permission to make a call.

        Args:
            blocking: Whether to block if rate limit exceeded
            timeout: Maximum time to wait

        Returns:
            True if acquired, False otherwise
        """
        start_time = time.time()

        while True:
            with self.lock:
                # Remove old calls outside the time window
                now = time.time()
                while self.calls and self.calls[0] < now - self.time_window:
                    self.calls.popleft()

                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return True

                if not blocking:
                    return False

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            time.sleep(0.01)

    async def acquire_async(self, blocking: bool = True,
                           timeout: Optional[float] = None) -> bool:
        """Async version of acquire."""
        start_time = time.time()

        while True:
            async with asyncio.Lock():
                now = time.time()
                while self.calls and self.calls[0] < now - self.time_window:
                    self.calls.popleft()

                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return True

                if not blocking:
                    return False

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

            await asyncio.sleep(0.01)


class CircuitBreaker:
    """Circuit breaker pattern for handling failures."""

    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = "closed"  # closed, open, half_open
        self.lock = threading.Lock()

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection."""
        with self.lock:
            if self.state == "open":
                if (time.time() - self.last_failure_time) >= self.recovery_timeout:
                    self.state = "half_open"
                else:
                    raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e

    def _on_success(self) -> None:
        """Handle successful call."""
        with self.lock:
            self.failure_count = 0
            if self.state == "half_open":
                self.state = "closed"

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"

    def reset(self) -> None:
        """Reset circuit breaker to closed state."""
        with self.lock:
            self.failure_count = 0
            self.state = "closed"
