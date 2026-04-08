"""
Resource Pool Utilities

Provides utilities for managing pools of
reusable resources in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Generic, TypeVar
import threading

T = TypeVar("T")


class ResourcePool(Generic[T]):
    """
    Pool of reusable resources.
    
    Manages acquire/release lifecycle and
    pool sizing.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 10,
    ) -> None:
        self._factory = factory
        self._max_size = max_size
        self._available: list[T] = []
        self._in_use: set[T] = set()
        self._lock = threading.Lock()

    def acquire(self) -> T:
        """Acquire a resource from the pool."""
        with self._lock:
            if self._available:
                resource = self._available.pop()
            elif len(self._in_use) < self._max_size:
                resource = self._factory()
            else:
                resource = self._available.pop()
            self._in_use.add(resource)
            return resource

    def release(self, resource: T) -> None:
        """Release a resource back to the pool."""
        with self._lock:
            if resource in self._in_use:
                self._in_use.remove(resource)
                self._available.append(resource)

    def size(self) -> tuple[int, int]:
        """Get (available, in_use) counts."""
        with self._lock:
            return len(self._available), len(self._in_use)

    def clear(self) -> None:
        """Clear all resources."""
        with self._lock:
            self._available.clear()
            self._in_use.clear()
