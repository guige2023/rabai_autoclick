"""
Facade Pattern Implementation

Provides a simplified interface to complex subsystems,
with optional detailed access when needed.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class SubsystemError(Exception):
    """Base exception for subsystem errors."""
    pass


class SubsystemNotAvailableError(SubsystemError):
    """Raised when a subsystem is not available."""
    pass


class SubsystemOperationError(SubsystemError):
    """Raised when a subsystem operation fails."""
    pass


class Subsystem(ABC):
    """Base class for subsystem components."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the subsystem name."""
        pass

    @abstractmethod
    def initialize(self) -> None:
        """Initialize the subsystem."""
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if subsystem is available."""
        pass


class FacadeOperation:
    """Metadata for a facade operation."""

    def __init__(
        self,
        name: str,
        description: str = "",
        required_subsystems: list[str] | None = None,
        cacheable: bool = False,
        cache_ttl: float = 60.0,
    ):
        self.name = name
        self.description = description
        self.required_subsystems = required_subsystems or []
        self.cacheable = cacheable
        self.cache_ttl = cache_ttl


@dataclass
class FacadeMetrics:
    """Metrics for facade operations."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    total_latency_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    by_operation: dict[str, int] = field(default_factory=dict)
    by_subsystem: dict[str, int] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        return self.successful_calls / self.total_calls if self.total_calls > 0 else 0.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.total_calls if self.total_calls > 0 else 0.0


class BaseFacade(ABC, Generic[T]):
    """
    Abstract base class for facades.

    Type Parameters:
        T: The type of the underlying subsystem or context.
    """

    def __init__(
        self,
        subsystems: dict[str, Subsystem] | None = None,
        auto_initialize: bool = True,
        lazy_init: bool = True,
    ):
        self._subsystems: dict[str, Subsystem] = subsystems or {}
        self._initialized = False
        self._lazy_init = lazy_init
        self._metrics = FacadeMetrics()
        self._operation_cache: dict[str, tuple[Any, float]] = {}
        self._call_history: list[dict[str, Any]] = []

        if auto_initialize and not lazy_init:
            self.initialize()

    @property
    def metrics(self) -> FacadeMetrics:
        return self._metrics

    @property
    def subsystems(self) -> dict[str, Subsystem]:
        """Access to registered subsystems."""
        return copy.copy(self._subsystems)

    def register_subsystem(self, name: str, subsystem: Subsystem) -> None:
        """Register a subsystem."""
        self._subsystems[name] = subsystem

    def get_subsystem(self, name: str) -> Subsystem | None:
        """Get a subsystem by name."""
        return self._subsystems.get(name)

    def initialize(self) -> None:
        """Initialize all subsystems."""
        for name, subsystem in self._subsystems.items():
            if subsystem.is_available():
                try:
                    subsystem.initialize()
                except Exception as e:
                    raise SubsystemOperationError(
                        f"Failed to initialize subsystem '{name}': {e}"
                    ) from e
        self._initialized = True

    def _check_subsystems(self, required: list[str]) -> None:
        """Check that all required subsystems are available."""
        unavailable = [name for name in required if name not in self._subsystems]
        if unavailable:
            raise SubsystemNotAvailableError(f"Subsystems not found: {unavailable}")

        for name in required:
            subsystem = self._subsystems[name]
            if not subsystem.is_available():
                raise SubsystemNotAvailableError(f"Subsystem '{name}' is not available")

    def _execute_with_metrics(
        self,
        operation_name: str,
        required_subsystems: list[str],
        func: Callable[[], T],
    ) -> T:
        """Execute an operation with metrics collection."""
        start = time.time()
        self._metrics.total_calls += 1
        self._metrics.by_operation[operation_name] = self._metrics.by_operation.get(operation_name, 0) + 1

        for sub in required_subsystems:
            self._metrics.by_subsystem[sub] = self._metrics.by_subsystem.get(sub, 0) + 1

        self._call_history.append({
            "operation": operation_name,
            "subsystems": required_subsystems,
            "timestamp": start,
        })

        try:
            if required_subsystems:
                self._check_subsystems(required_subsystems)

            result = func()
            self._metrics.successful_calls += 1

        except Exception as e:
            self._metrics.failed_calls += 1
            self._call_history[-1]["error"] = str(e)
            raise

        finally:
            elapsed = (time.time() - start) * 1000
            self._metrics.total_latency_ms += elapsed
            self._call_history[-1]["latency_ms"] = elapsed

        return result  # type: ignore

    def get_call_history(self) -> list[dict[str, Any]]:
        """Get the call history."""
        return copy.copy(self._call_history)

    def clear_cache(self) -> None:
        """Clear the operation cache."""
        self._operation_cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._operation_cache),
            "hits": self._metrics.cache_hits,
            "misses": self._metrics.cache_misses,
        }


class FacadeBuilder:
    """Builder for constructing facades with fluent interface."""

    def __init__(self):
        self._subsystems: dict[str, Subsystem] = {}
        self._operations: dict[str, FacadeOperation] = {}
        self._middleware: list[Callable] = []

    def with_subsystem(self, name: str, subsystem: Subsystem) -> FacadeBuilder:
        """Add a subsystem."""
        self._subsystems[name] = subsystem
        return self

    def with_operation(
        self,
        name: str,
        description: str = "",
        required_subsystems: list[str] | None = None,
    ) -> FacadeBuilder:
        """Add an operation metadata."""
        self._operations[name] = FacadeOperation(
            name=name,
            description=description,
            required_subsystems=required_subsystems,
        )
        return self

    def with_middleware(self, middleware: Callable) -> FacadeBuilder:
        """Add middleware function."""
        self._middleware.append(middleware)
        return self

    def build(self) -> BaseFacade:
        """Build the facade."""
        # This is a placeholder - actual implementation depends on specific facade
        class BuiltFacade(BaseFacade):
            pass

        facade = BuiltFacade(subsystems=self._subsystems)
        return facade


class CompositeFacade(BaseFacade[T]):
    """
    Facade that can coordinate multiple sub-facades.
    """

    def __init__(self, facades: dict[str, BaseFacade] | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self._facades = facades or {}

    def register_facade(self, name: str, facade: BaseFacade) -> None:
        """Register a sub-facade."""
        self._facades[name] = facade

    def get_facade(self, name: str) -> BaseFacade | None:
        """Get a sub-facade by name."""
        return self._facades.get(name)

    def execute_cross_facade(
        self,
        facade_ops: list[tuple[str, str]],
    ) -> dict[str, Any]:
        """
        Execute operations across multiple facades.

        Args:
            facade_ops: List of (facade_name, operation_name) tuples.

        Returns:
            Dict mapping facade names to their operation results.
        """
        results: dict[str, Any] = {}

        for facade_name, op_name in facade_ops:
            facade = self._facades.get(facade_name)
            if facade is None:
                results[facade_name] = {"error": f"Facade '{facade_name}' not found"}
                continue

            # In real implementation, would call the actual operation
            results[facade_name] = {"operation": op_name, "status": "executed"}

        return results


def create_simple_facade(
    subsystems: dict[str, Subsystem],
    operations: dict[str, Callable],
) -> BaseFacade:
    """
    Create a simple facade from subsystems and operations.

    Args:
        subsystems: Dict of subsystem name -> subsystem instance.
        operations: Dict of operation name -> callable.

    Returns:
        A configured facade instance.
    """

    class SimpleFacade(BaseFacade):
        pass

    facade = SimpleFacade(subsystems=subsystems, auto_initialize=True)

    for name, op in operations.items():
        setattr(facade, name, op)

    return facade
