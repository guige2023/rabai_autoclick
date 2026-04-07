"""
Flyweight Pattern Implementation

Uses sharing to support large numbers of fine-grained objects efficiently.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
TState = TypeVar("TState")


class Flyweight(ABC):
    """Abstract flyweight interface."""

    @abstractmethod
    def operation(self, extrinsic_state: Any) -> None:
        """Operation that uses extrinsic state."""
        pass


@dataclass
class ConcreteFlyweight(Flyweight):
    """
    Concrete flyweight with intrinsic state.
    Intrinsic state is stored in the flyweight itself.
    """
    intrinsic_state: str = ""

    def operation(self, extrinsic_state: Any) -> None:
        print(f"ConcreteFlyweight: intrinsic={self.intrinsic_state}, extrinsic={extrinsic_state}")


@dataclass
class UnsharedConcreteFlyweight(Flyweight):
    """
    Flyweight that doesn't share all state.
    Some state is kept in the concrete flyweight.
    """
    all_state: dict[str, Any] = field(default_factory=dict)

    def operation(self, extrinsic_state: Any) -> None:
        print(f"UnsharedFlyweight: all_state={self.all_state}, extrinsic={extrinsic_state}")


class FlyweightFactory:
    """
    Factory that manages shared flyweight objects.
    """

    def __init__(self):
        self._flyweights: dict[str, Flyweight] = {}
        self._metrics: dict[str, int] = {"hits": 0, "misses": 0, "created": 0}

    def get_flyweight(self, key: str, factory_func: Callable[[], Flyweight]) -> Flyweight:
        """
        Get a flyweight by key, creating it if necessary.

        Args:
            key: The flyweight key.
            factory_func: Function to create the flyweight if not found.

        Returns:
            The flyweight instance.
        """
        if key in self._flyweights:
            self._metrics["hits"] += 1
            return self._flyweights[key]

        self._metrics["misses"] += 1
        self._metrics["created"] += 1
        flyweight = factory_func()
        self._flyweights[key] = flyweight
        return flyweight

    def get(self, key: str) -> Flyweight | None:
        """Get a flyweight by key without creating."""
        if key in self._flyweights:
            self._metrics["hits"] += 1
            return self._flyweights[key]
        self._metrics["misses"] += 1
        return None

    def count(self) -> int:
        """Count of unique flyweights."""
        return len(self._flyweights)

    @property
    def metrics(self) -> dict[str, int]:
        return copy.copy(self._metrics)


class FlyweightRegistry(Generic[T]):
    """
    Generic registry for managing flyweight objects.

    Type Parameters:
        T: The type of flyweight object.
    """

    def __init__(self):
        self._flyweights: dict[str, T] = {}
        self._creation_count: int = 0

    def register(self, key: str, flyweight: T) -> None:
        """Register a flyweight."""
        self._flyweights[key] = flyweight
        self._creation_count += 1

    def get(self, key: str) -> T | None:
        """Get a flyweight by key."""
        return self._flyweights.get(key)

    def has(self, key: str) -> bool:
        """Check if flyweight exists."""
        return key in self._flyweights

    def list_keys(self) -> list[str]:
        """List all flyweight keys."""
        return list(self._flyweights.keys())

    def unregister(self, key: str) -> bool:
        """Unregister a flyweight."""
        if key in self._flyweights:
            del self._flyweights[key]
            return True
        return False

    @property
    def count(self) -> int:
        return len(self._flyweights)

    @property
    def total_created(self) -> int:
        return self._creation_count


@dataclass
class FlyweightStats:
    """Statistics for flyweight usage."""
    total_objects: int
    shared_count: int
    unique_count: int
    memory_savings_percent: float


def compute_stats(total: int, shared: int, avg_size_per_object: int) -> FlyweightStats:
    """
    Compute flyweight statistics.

    Args:
        total: Total number of objects that would be needed.
        shared: Number of shared flyweights.
        avg_size_per_object: Average size per object in bytes.

    Returns:
        Statistics about the flyweight usage.
    """
    unique = total - shared
    shared_memory = shared * avg_size_per_object
    without_flyweight = total * avg_size_per_object
    savings = ((without_flyweight - shared_memory) / without_flyweight * 100
               if without_flyweight > 0 else 0)

    return FlyweightStats(
        total_objects=total,
        shared_count=shared,
        unique_count=unique,
        memory_savings_percent=savings,
    )
