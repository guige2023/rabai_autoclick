"""
Prototype Pattern Implementation

Creates new objects by cloning existing ones (prototypes).
Useful when object creation is expensive.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class Prototype(ABC, Generic[T]):
    """
    Abstract prototype interface.
    """

    @abstractmethod
    def clone(self) -> T:
        """Create a clone of this prototype."""
        pass

    @abstractmethod
    def deep_clone(self) -> T:
        """Create a deep clone of this prototype."""
        pass


@dataclass
class ConcretePrototypeA(Prototype["ConcretePrototypeA"]):
    """
    Concrete prototype A.
    """
    value: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    def clone(self) -> "ConcretePrototypeA":
        """Create a shallow clone."""
        return ConcretePrototypeA(value=self.value, data=self.data.copy())

    def deep_clone(self) -> "ConcretePrototypeA":
        """Create a deep clone."""
        return ConcretePrototypeA(
            value=self.value,
            data=copy.deepcopy(self.data),
        )


@dataclass
class ConcretePrototypeB(Prototype["ConcretePrototypeB"]):
    """
    Concrete prototype B.
    """
    count: int = 0
    items: list[Any] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def clone(self) -> "ConcretePrototypeB":
        """Create a shallow clone."""
        return ConcretePrototypeB(
            count=self.count,
            items=self.items.copy(),
            metadata=self.metadata.copy(),
        )

    def deep_clone(self) -> "ConcretePrototypeB":
        """Create a deep clone."""
        return ConcretePrototypeB(
            count=self.count,
            items=copy.deepcopy(self.items),
            metadata=copy.deepcopy(self.metadata),
        )


class PrototypeRegistry(Generic[T]):
    """
    Registry for managing prototypes.
    """

    def __init__(self):
        self._prototypes: dict[str, T] = {}
        self._clone_funcs: dict[str, Callable[[], T]] = {}

    def register(self, name: str, prototype: T) -> PrototypeRegistry[T]:
        """Register a prototype."""
        self._prototypes[name] = prototype
        return self

    def register_factory(self, name: str, clone_func: Callable[[], T]) -> PrototypeRegistry[T]:
        """Register a clone factory function."""
        self._clone_funcs[name] = clone_func
        return self

    def get(self, name: str) -> T | None:
        """Get a prototype by name."""
        return self._prototypes.get(name)

    def create(self, name: str, shallow: bool = False) -> T | None:
        """
        Create a clone of a prototype.

        Args:
            name: The prototype name.
            shallow: If True, create shallow clone.

        Returns:
            A clone of the prototype, or None if not found.
        """
        prototype = self._prototypes.get(name)
        if prototype is None:
            factory = self._clone_funcs.get(name)
            if factory:
                return factory()

        if prototype is None:
            return None

        if shallow:
            return prototype.clone() if hasattr(prototype, "clone") else copy.copy(prototype)
        return prototype.deep_clone() if hasattr(prototype, "deep_clone") else copy.deepcopy(prototype)

    def list_prototypes(self) -> list[str]:
        """List all registered prototype names."""
        return list(self._prototypes.keys())

    def unregister(self, name: str) -> bool:
        """Unregister a prototype."""
        if name in self._prototypes:
            del self._prototypes[name]
            return True
        return False


@dataclass
class CloneMetrics:
    """Metrics for clone operations."""
    shallow_clones: int = 0
    deep_clones: int = 0
    failed_clones: int = 0
    total_time_ms: float = 0.0


class MeasuredPrototype(Generic[T]):
    """
    Prototype wrapper with metrics collection.
    """

    def __init__(self, prototype: T):
        self._prototype = prototype
        self._metrics = CloneMetrics()

    def clone(self, shallow: bool = False) -> T | None:
        """Clone with metrics collection."""
        import time
        start = time.time()

        try:
            if shallow:
                self._metrics.shallow_clones += 1
                if hasattr(self._prototype, "clone"):
                    return self._prototype.clone()
                return copy.copy(self._prototype)
            else:
                self._metrics.deep_clones += 1
                if hasattr(self._prototype, "deep_clone"):
                    return self._prototype.deep_clone()
                return copy.deepcopy(self._prototype)
        except Exception:
            self._metrics.failed_clones += 1
            raise
        finally:
            self._metrics.total_time_ms += (time.time() - start) * 1000

    @property
    def metrics(self) -> CloneMetrics:
        """Get clone metrics."""
        return self._metrics


def clone(obj: T, deep: bool = True) -> T:
    """
    Generic clone function for any object.

    Args:
        obj: The object to clone.
        deep: If True, create deep clone.

    Returns:
        The cloned object.
    """
    if deep:
        return copy.deepcopy(obj)
    return copy.copy(obj)
