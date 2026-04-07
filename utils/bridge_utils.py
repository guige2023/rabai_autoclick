"""
Bridge Pattern Implementation

Decouples an abstraction from its implementation so that
the two can vary independently.
"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TImpl = TypeVar("TImpl")


class Implementor(ABC):
    """Abstract implementor interface."""

    @abstractmethod
    def operation_impl(self, data: Any) -> Any:
        """Implementation of the operation."""
        pass


class ConcreteImplementorA(Implementor):
    """Concrete implementor A."""

    def operation_impl(self, data: Any) -> Any:
        return f"ConcreteImplementorA: {data}"


class ConcreteImplementorB(Implementor):
    """Concrete implementor B."""

    def operation_impl(self, data: Any) -> Any:
        return f"ConcreteImplementorB: {data}"


class Abstraction(ABC, Generic[TImpl]):
    """
    Abstraction class that defines the high-level control.

    Type Parameters:
        TImpl: The type of implementor.
    """

    def __init__(self, implementor: TImpl):
        self._implementor = implementor

    @property
    def implementor(self) -> TImpl:
        """Get the implementor."""
        return self._implementor

    @implementor.setter
    def implementor(self, value: TImpl) -> None:
        """Set a new implementor."""
        self._implementor = value

    @abstractmethod
    def operation(self, data: Any) -> Any:
        """High-level operation that uses the implementor."""
        pass


class RefinedAbstraction(Abstraction[TImpl]):
    """Refined abstraction with additional functionality."""

    def operation(self, data: Any) -> Any:
        """Execute operation with refined behavior."""
        return self._implementor.operation_impl(data)


class BridgeRegistry(Generic[TImpl]):
    """Registry for managing implementor variants."""

    def __init__(self):
        self._implementors: dict[str, type[TImpl]] = {}
        self._instances: dict[str, TImpl] = {}

    def register(self, name: str, implementor_class: type[TImpl]) -> BridgeRegistry:
        """Register an implementor class."""
        self._implementors[name] = implementor_class
        return self

    def get(self, name: str, *args: Any, **kwargs: Any) -> TImpl | None:
        """Get an implementor instance."""
        if name not in self._implementors:
            return None
        return self._implementors[name](*args, **kwargs)

    def list_implementors(self) -> list[str]:
        """List all registered implementor names."""
        return list(self._implementors.keys())


@dataclass
class BridgeMetrics:
    """Metrics for bridge pattern usage."""
    total_operations: int = 0
    by_implementor: dict[str, int] = field(default_factory=dict)


class MeasuredAbstraction(Abstraction[TImpl]):
    """Abstraction that tracks metrics."""

    def __init__(self, implementor: TImpl):
        super().__init__(implementor)
        self._metrics = BridgeMetrics()

    @property
    def metrics(self) -> BridgeMetrics:
        return self._metrics

    def operation(self, data: Any) -> Any:
        """Execute operation with metrics tracking."""
        impl_name = type(self._implementor).__name__
        self._metrics.total_operations += 1
        self._metrics.by_implementor[impl_name] = (
            self._metrics.by_implementor.get(impl_name, 0) + 1
        )
        return self._implementor.operation_impl(data)
