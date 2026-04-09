"""
Semaphore utilities for concurrency control.

Provides specialized semaphore implementations for UI automation workflows,
including weighted, adaptive, and fairness-aware semaphore variants.

Example:
    >>> from semaphore_utils import WeightedSemaphore, AdaptiveSemaphore
    >>> sem = WeightedSemaphore(weight=5)
    >>> with sem.acquire(weight=2):
    ...     perform_action()
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Optional


# =============================================================================
# Exceptions
# =============================================================================


class SemaphoreError(Exception):
    """Base exception for semaphore-related errors."""
    pass


class SemaphoreTimeoutError(SemaphoreError):
    """Raised when semaphore acquisition times out."""

    def __init__(self, weight: int, timeout: float):
        self.weight = weight
        self.timeout = timeout
        super().__init__(
            f"Failed to acquire semaphore weight={weight} within {timeout}s"
        )


# =============================================================================
# Weighted Semaphore
# =============================================================================


@dataclass
class WeightedSemaphore:
    """
    A semaphore that tracks weighted resources.

    Unlike a standard semaphore that counts individual slots, a weighted
    semaphore tracks resource weight, allowing acquisition of variable-sized
    resource blocks.

    Attributes:
        weight: Total weight of the semaphore.
        _lock: Internal lock for thread safety.
        _condition: Condition variable for waiting.
        _current_weight: Currently consumed weight.
        _waiters: Queue of waiting threads with their weights.

    Example:
        >>> sem = WeightedSemaphore(weight=10)
        >>> with sem.acquire(weight=3):
        ...     # 3 weight consumed, 7 remaining
        ...     pass
    """

    weight: int
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _condition: threading.Condition = field(default_factory=lambda: threading.Condition(threading.Lock()))
    _current_weight: int = field(default=0)
    _waiters: Deque = field(default_factory=deque)

    def acquire(self, weight: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Acquire the specified weight from the semaphore.

        Args:
            weight: The weight to acquire (must be > 0 and <= total weight).
            timeout: Maximum seconds to wait. None means wait indefinitely.

        Returns:
            True if acquisition succeeded.

        Raises:
            SemaphoreTimeoutError: If timeout expires before acquisition.
            ValueError: If weight is invalid.
        """
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight}")
        if weight > self.weight:
            raise ValueError(
                f"Weight {weight} exceeds semaphore capacity {self.weight}"
            )

        deadline = None if timeout is None else time.monotonic() + timeout

        with self._condition:
            while self._current_weight + weight > self.weight:
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    raise SemaphoreTimeoutError(weight, timeout)
                self._condition.wait(timeout=remaining if remaining else None)

            self._current_weight += weight
            return True

    def release(self, weight: int = 1) -> None:
        """
        Release the specified weight back to the semaphore.

        Args:
            weight: The weight to release (must be <= acquired weight).
        """
        with self._condition:
            self._current_weight = max(0, self._current_weight - weight)
            self._condition.notify_all()

    @contextmanager
    def acquire_context(self, weight: int = 1, timeout: Optional[float] = None):
        """
        Context manager for acquiring and releasing semaphore weight.

        Args:
            weight: The weight to acquire.
            timeout: Maximum seconds to wait.

        Yields:
            None

        Example:
            >>> sem = WeightedSemaphore(weight=10)
            >>> with sem.acquire_context(weight=3):
            ...     # do work
            ...     pass
        """
        self.acquire(weight=weight, timeout=timeout)
        try:
            yield
        finally:
            self.release(weight=weight)

    @property
    def available(self) -> int:
        """Return the currently available weight."""
        with self._lock:
            return self.weight - self._current_weight

    def __enter__(self) -> "WeightedSemaphore":
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


# =============================================================================
# Async Adaptive Semaphore
# =============================================================================


@dataclass
class AsyncAdaptiveSemaphore:
    """
    An async semaphore that adapts its capacity based on demand.

    Monitors wait times and can dynamically adjust the effective capacity
    to prevent starvation of low-weight requests.

    Attributes:
        initial_capacity: Starting capacity of the semaphore.
        min_capacity: Minimum allowed capacity.
        max_capacity: Maximum allowed capacity.
        adaptation_threshold: Wait time (seconds) before increasing capacity.
        adaptation_factor: Multiplier for capacity adjustment.
    """

    initial_capacity: int = 10
    min_capacity: int = 1
    max_capacity: int = 100
    adaptation_threshold: float = 1.0
    adaptation_factor: float = 1.5
    _lock: asyncio.Lock = field(default_factory=asyncio.create_task)
    _semaphore: asyncio.Semaphore = field(init=False)
    _current_capacity: int = field(init=False)
    _wait_times: Deque[float] = field(default_factory=deque)
    _total_wait_time: float = field(default=0.0, init=False)

    def __post_init__(self):
        # Use factory to avoid mutable default
        object.__setattr__(self, '_semaphore', asyncio.Semaphore(self.initial_capacity))
        object.__setattr__(self, '_current_capacity', self.initial_capacity)

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire a slot from the semaphore.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        start = time.monotonic()
        try:
            result = await asyncio.wait_for(
                self._semaphore.acquire(), timeout=timeout
            )
            return result is None or result is True
        except asyncio.TimeoutError:
            wait_time = time.monotonic() - start
            await self._record_wait(wait_time)
            raise

    async def _record_wait(self, wait_time: float) -> None:
        """Record wait time and potentially adapt capacity."""
        async with self._lock:
            self._wait_times.append(wait_time)
            self._total_wait_time += wait_time
            if len(self._wait_times) > 100:
                old = self._wait_times.popleft()
                self._total_wait_time -= old

            avg_wait = self._total_wait_time / len(self._wait_times)
            if avg_wait > self.adaptation_threshold:
                new_capacity = min(
                    int(self._current_capacity * self.adaptation_factor),
                    self.max_capacity
                )
                if new_capacity > self._current_capacity:
                    await self._resize(new_capacity)

    async def _resize(self, new_capacity: int) -> None:
        """Resize the semaphore to a new capacity."""
        old_sem = self._semaphore
        object.__setattr__(self, '_semaphore', asyncio.Semaphore(new_capacity))
        object.__setattr__(self, '_current_capacity', new_capacity)
        old_sem.close()

    async def release(self) -> None:
        """Release a slot back to the semaphore."""
        self._semaphore.release()

    @contextmanager
    async def acquire_context(self, timeout: Optional[float] = None):
        """Context manager for async semaphore acquisition."""
        await self.acquire(timeout=timeout)
        try:
            yield
        finally:
            await self.release()

    @property
    def available(self) -> int:
        """Return number of available slots."""
        return self._current_capacity


# =============================================================================
# Fair Semaphore
# =============================================================================


@dataclass
class FairSemaphore:
    """
    A fair semaphore that guarantees FIFO ordering.

    Uses a condition variable to ensure that threads are served in the exact
    order they requested, preventing thread starvation.

    Attributes:
        capacity: Maximum number of concurrent holders.
    """

    capacity: int
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _condition: threading.Condition = field(
        default_factory=lambda: threading.Condition(threading.Lock())
    )
    _held: bool = field(default=False)
    _queue: Deque = field(default_factory=deque)

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire the semaphore fairly.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if acquisition succeeded.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while self._held:
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    return False
                notified = self._condition.wait(timeout=remaining if remaining else None)
                if not notified and deadline and time.monotonic() >= deadline:
                    return False
            self._held = True
            return True

    def release(self) -> None:
        """Release the semaphore."""
        with self._condition:
            self._held = False
            self._condition.notify_all()

    @contextmanager
    def acquire_context(self, timeout: Optional[float] = None):
        """Context manager for fair semaphore acquisition."""
        if not self.acquire(timeout=timeout):
            raise SemaphoreTimeoutError(1, timeout or 0)
        try:
            yield
        finally:
            self.release()


# =============================================================================
# Slot Pool
# =============================================================================


@dataclass
class SlotPool:
    """
    A pool of named slots with acquire/release semantics.

    Useful for managing a fixed set of named resources like windows,
    tabs, or connections.

    Example:
        >>> pool = SlotPool(names=["win1", "win2", "win3"])
        >>> slot = pool.acquire("win1")
        >>> pool.release(slot)
    """

    names: list[str]
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _available: set = field(default_factory=set)

    def __post_init__(self):
        object.__setattr__(self, '_available', set(self.names))

    def acquire(self, name: str, timeout: Optional[float] = None) -> str:
        """
        Acquire a named slot.

        Args:
            name: Name of the slot to acquire.
            timeout: Maximum seconds to wait.

        Returns:
            The acquired slot name.

        Raises:
            SemaphoreTimeoutError: If slot is not available within timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while name not in self._available:
                remaining = deadline - time.monotonic() if deadline else None
                if remaining is not None and remaining <= 0:
                    raise SemaphoreTimeoutError(1, timeout)
                self._lock.wait(timeout=remaining if remaining else None)
            self._available.discard(name)
            return name

    def release(self, name: str) -> None:
        """
        Release a named slot.

        Args:
            name: Name of the slot to release.
        """
        with self._lock:
            if name in self.names:
                self._available.add(name)
                self._lock.notify_all()

    def is_available(self, name: str) -> bool:
        """Check if a slot is available."""
        with self._lock:
            return name in self._available

    @property
    def available_slots(self) -> list[str]:
        """Return list of available slot names."""
        with self._lock:
            return list(self._available)
