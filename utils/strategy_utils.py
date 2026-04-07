"""
Strategy Pattern Implementation

Provides strategy pattern with context, strategy interfaces,
selection strategies, and runtime strategy switching.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TResult = TypeVar("TResult")
TStrategy = TypeVar("TStrategy", bound="Strategy")


class Strategy(ABC, Generic[T, TResult]):
    """
    Abstract base class for strategies.

    Type Parameters:
        T: The type of context this strategy operates on.
        TResult: The type of result this strategy produces.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the strategy name."""
        pass

    @property
    def description(self) -> str:
        """Return a description of this strategy."""
        return ""

    @abstractmethod
    def execute(self, context: T, *args: Any, **kwargs: Any) -> TResult:
        """Execute the strategy with the given context."""
        pass

    def __repr__(self) -> str:
        return f"<Strategy: {self.name}>"


class Context(ABC, Generic[T, TResult]):
    """
    Context class that uses a strategy.
    Can dynamically switch strategies at runtime.
    """

    def __init__(
        self,
        strategy: Strategy[T, TResult] | None = None,
        auto_copy_context: bool = True,
    ):
        self._strategy = strategy
        self._auto_copy_context = auto_copy_context
        self._execution_history: list[dict[str, Any]] = []
        self._metrics: dict[str, Any] = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "total_time_ms": 0.0,
        }

    @property
    def strategy(self) -> Strategy[T, TResult] | None:
        """Get the current strategy."""
        return self._strategy

    @strategy.setter
    def strategy(self, value: Strategy[T, TResult]) -> None:
        """Set a new strategy."""
        self._strategy = value

    def set_strategy(self, strategy: Strategy[T, TResult]) -> Context[T, TResult]:
        """Set a new strategy (fluent interface)."""
        self._strategy = strategy
        return self

    def execute(self, context: T, *args: Any, **kwargs: Any) -> TResult | None:
        """Execute the current strategy."""
        if self._strategy is None:
            return None

        start = time.time()
        self._metrics["total_executions"] += 1

        try:
            if self._auto_copy_context:
                context = copy.deepcopy(context)

            result = self._strategy.execute(context, *args, **kwargs)
            self._metrics["successful_executions"] += 1
            self._execution_history.append({
                "strategy": self._strategy.name,
                "success": True,
                "time_ms": (time.time() - start) * 1000,
                "timestamp": time.time(),
            })
            return result

        except Exception as e:
            self._metrics["failed_executions"] += 1
            self._execution_history.append({
                "strategy": self._strategy.name,
                "success": False,
                "error": str(e),
                "time_ms": (time.time() - start) * 1000,
                "timestamp": time.time(),
            })
            raise

        finally:
            self._metrics["total_time_ms"] += (time.time() - start) * 1000

    @property
    def metrics(self) -> dict[str, Any]:
        """Get execution metrics."""
        return copy.copy(self._metrics)

    def get_execution_history(self) -> list[dict[str, Any]]:
        """Get execution history."""
        return copy.copy(self._execution_history)

    def clear_history(self) -> None:
        """Clear execution history."""
        self._execution_history.clear()


@dataclass
class StrategySpec(Generic[TStrategy]):
    """Specification for a registered strategy."""
    id: str
    name: str
    description: str = ""
    strategy_class: type[TStrategy] | None = None
    factory_func: Callable[..., TStrategy] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def create(self, *args: Any, **kwargs: Any) -> TStrategy:
        """Create a strategy instance."""
        if self.factory_func:
            return self.factory_func(*args, **kwargs)
        if self.strategy_class:
            return self.strategy_class(*args, **kwargs)
        raise ValueError(f"Cannot create strategy '{self.id}': no factory available")


class StrategyRegistry(Generic[TStrategy]):
    """
    Registry for managing multiple strategies with selection logic.
    """

    def __init__(self, default_strategy_id: str = ""):
        self._strategies: dict[str, StrategySpec[TStrategy]] = {}
        self._instances: dict[str, TStrategy] = {}
        self._default_strategy_id = default_strategy_id
        self._selection_callbacks: list[Callable[[list[StrategySpec]], str | None]] = []

    def register(
        self,
        strategy_id: str,
        strategy_or_class: TStrategy | type[TStrategy] | Callable[..., TStrategy],
        name: str = "",
        description: str = "",
        set_default: bool = False,
        **metadata: Any,
    ) -> StrategyRegistry[TStrategy]:
        """
        Register a strategy.

        Args:
            strategy_id: Unique identifier for the strategy.
            strategy_or_class: Strategy instance, class, or factory function.
            name: Human-readable name.
            description: Description of the strategy.
            set_default: If True, set as the default strategy.
            **metadata: Additional metadata.

        Returns:
            Self for chaining.
        """
        if strategy_id in self._strategies:
            raise ValueError(f"Strategy '{strategy_id}' already registered")

        factory_func: Callable[..., TStrategy] | None = None
        strategy_class: type[TStrategy] | None = None

        if callable(strategy_or_class) and not isinstance(strategy_or_class, type):
            factory_func = strategy_or_class  # type: ignore
        elif isinstance(strategy_or_class, type):
            strategy_class = strategy_or_class

        spec = StrategySpec(
            id=strategy_id,
            name=name or strategy_id,
            description=description,
            strategy_class=strategy_class,
            factory_func=factory_func,
            metadata=metadata,
        )

        self._strategies[strategy_id] = spec

        if set_default or not self._default_strategy_id:
            self._default_strategy_id = strategy_id

        return self

    def get(self, strategy_id: str, **kwargs: Any) -> TStrategy | None:
        """
        Get a strategy instance by ID.

        Args:
            strategy_id: The strategy to get.
            **kwargs: Arguments to pass to the factory.

        Returns:
            Strategy instance.
        """
        if strategy_id not in self._strategies:
            return None

        # Cache instance with kwargs as key part
        cache_key = f"{strategy_id}:{hash(tuple(sorted(kwargs.items())))}"

        if cache_key not in self._instances:
            spec = self._strategies[strategy_id]
            self._instances[cache_key] = spec.create(**kwargs)

        return self._instances.get(cache_key)

    def get_spec(self, strategy_id: str) -> StrategySpec[TStrategy] | None:
        """Get a strategy specification."""
        return self._strategies.get(strategy_id)

    def select(self, **context: Any) -> str | None:
        """
        Select a strategy based on context using registered selectors.

        Args:
            **context: Context information for selection.

        Returns:
            Selected strategy ID or None.
        """
        specs = list(self._strategies.values())

        for callback in self._selection_callbacks:
            result = callback(specs, **context)
            if result is not None:
                return result

        return self._default_strategy_id

    def on_select(self, callback: Callable[[list[StrategySpec], Any], str | None]) -> None:
        """Register a selection callback."""
        self._selection_callbacks.append(callback)

    def list_strategies(self) -> list[str]:
        """List all registered strategy IDs."""
        return list(self._strategies.keys())

    def unregister(self, strategy_id: str) -> bool:
        """Unregister a strategy."""
        if strategy_id in self._strategies:
            del self._strategies[strategy_id]
            if self._default_strategy_id == strategy_id:
                self._default_strategy_id = next(iter(self._strategies), "")
            return True
        return False

    def get_default_id(self) -> str:
        """Get the default strategy ID."""
        return self._default_strategy_id


class StrategyWithFallback(Generic[T, TResult]):
    """
    Strategy wrapper that supports fallback strategies.
    """

    def __init__(self, primary: Strategy[T, TResult]):
        self._primary = primary
        self._fallbacks: list[Strategy[T, TResult]] = []

    def add_fallback(self, strategy: Strategy[T, TResult]) -> StrategyWithFallback[T, TResult]:
        """Add a fallback strategy."""
        self._fallbacks.append(strategy)
        return self

    def execute(self, context: T, *args: Any, **kwargs: Any) -> TResult | None:
        """Execute with fallback support."""
        try:
            return self._primary.execute(context, *args, **kwargs)
        except Exception:
            for fallback in self._fallbacks:
                try:
                    return fallback.execute(context, *args, **kwargs)
                except Exception:
                    continue
        return None


class AdaptiveStrategy(Strategy[T, TResult]):
    """
    Strategy that adapts its behavior based on performance metrics.
    """

    def __init__(
        self,
        strategies: list[Strategy[T, TResult]],
        selection_metric: str = "success_rate",
    ):
        self._strategies = strategies
        self._selection_metric = selection_metric
        self._performance: dict[str, dict[str, float]] = {}
        self._total_runs: int = 0

        for s in strategies:
            self._performance[s.name] = {
                "successes": 0,
                "failures": 0,
                "total_time_ms": 0,
            }

    @property
    def name(self) -> str:
        return "AdaptiveStrategy"

    def execute(self, context: T, *args: Any, **kwargs: Any) -> TResult:
        """Execute the best-performing strategy."""
        best = self._select_best_strategy()
        return best.execute(context, *args, **kwargs)

    def _select_best_strategy(self) -> Strategy[T, TResult]:
        """Select the best strategy based on metrics."""
        if self._selection_metric == "success_rate":
            return max(
                self._strategies,
                key=lambda s: (
                    self._performance[s.name]["successes"]
                    / max(1, self._performance[s.name]["successes"] + self._performance[s.name]["failures"])
                ),
            )
        elif self._selection_metric == "speed":
            return min(
                self._strategies,
                key=lambda s: self._performance[s.name]["total_time_ms"]
                / max(1, self._total_runs),
            )
        return self._strategies[0]

    def record_execution(self, strategy_name: str, success: bool, time_ms: float) -> None:
        """Record execution metrics for a strategy."""
        self._total_runs += 1
        perf = self._performance[strategy_name]
        if success:
            perf["successes"] += 1
        else:
            perf["failures"] += 1
        perf["total_time_ms"] += time_ms


def create_strategy_context(
    strategies: dict[str, Strategy],
    default: str | None = None,
) -> Context:
    """
    Create a pre-configured strategy context.

    Args:
        strategies: Dict of strategy_id -> Strategy.
        default: Default strategy ID.

    Returns:
        Configured Context instance.
    """
    ctx = Context()
    if strategies:
        first = next(iter(strategies.values()))
        ctx._strategy = first
    return ctx
