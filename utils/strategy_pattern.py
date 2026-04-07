"""Strategy pattern utilities for RabAI AutoClick.

Provides:
- Strategy selector
- Strategy registry
- Conditional strategy routing
- Strategy composition
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")


class Strategy(Generic[T, U]):
    """Base strategy class."""

    def execute(self, context: T) -> U:
        """Execute the strategy."""
        raise NotImplementedError


class StrategySelector(Generic[T, U]):
    """Selects and executes strategies based on context.

    Example:
        selector = StrategySelector[str, int]()

        def short_string(s: str) -> int:
            return len(s)

        def long_string(s: str) -> int:
            return len(s) * 2

        selector.add_strategy(lambda ctx: len(ctx) < 10, short_string)
        selector.add_strategy(lambda ctx: len(ctx) >= 10, long_string)

        result = selector.execute("hello")      # 5
        result = selector.execute("hello world")  # 11
    """

    def __init__(self) -> None:
        self._strategies: list[tuple[Callable[[T], bool], Callable[[T], U]]] = []
        self._default: Optional[Callable[[T], U]] = None

    def add_strategy(
        self,
        condition: Callable[[T], bool],
        strategy: Callable[[T], U],
    ) -> StrategySelector[T, U]:
        """Add a strategy with condition.

        Args:
            condition: Function that returns True when this strategy applies.
            strategy: Strategy function.

        Returns:
            Self for chaining.
        """
        self._strategies.append((condition, strategy))
        return self

    def set_default(
        self,
        strategy: Callable[[T], U],
    ) -> StrategySelector[T, U]:
        """Set default strategy when no conditions match.

        Args:
            strategy: Default strategy function.

        Returns:
            Self for chaining.
        """
        self._default = strategy
        return self

    def execute(self, context: T) -> U:
        """Execute strategy for given context.

        Args:
            context: Context to select strategy for.

        Returns:
            Result of selected strategy.

        Raises:
            ValueError: If no strategy matches and no default set.
        """
        for condition, strategy in self._strategies:
            if condition(context):
                return strategy(context)

        if self._default is not None:
            return self._default(context)

        raise ValueError(f"No strategy matched context: {context}")

    def __call__(self, context: T) -> U:
        return self.execute(context)


class StrategyRegistry(Generic[T, U]):
    """Registry of named strategies.

    Example:
        registry = StrategyRegistry[str, int]("length")

        @registry.register("default")
        def length_strategy(s: str) -> int:
            return len(s)

        @registry.register("words")
        def word_strategy(s: str) -> int:
            return len(s.split())

        result = registry.execute("default", "hello world")  # 11
        result = registry.execute("words", "hello world")    # 2
    """

    def __init__(
        self,
        default_key: Optional[str] = None,
    ) -> None:
        self._strategies: Dict[str, Callable[[T], U]] = {}
        self._default_key = default_key

    def register(
        self,
        name: str,
    ) -> Callable[[Callable[[T], U]], Callable[[T], U]]:
        """Decorator to register a strategy.

        Args:
            name: Strategy name.

        Returns:
            Decorator function.
        """
        def decorator(func: Callable[[T], U]) -> Callable[[T], U]:
            self._strategies[name] = func
            return func
        return decorator

    def add(self, name: str, strategy: Callable[[T], U]) -> None:
        """Add a strategy by name.

        Args:
            name: Strategy name.
            strategy: Strategy function.
        """
        self._strategies[name] = strategy

    def get(self, name: str) -> Callable[[T], U]:
        """Get a strategy by name.

        Args:
            name: Strategy name.

        Returns:
            Strategy function.

        Raises:
            KeyError: If strategy not found.
        """
        if name not in self._strategies:
            raise KeyError(f"Strategy not found: {name}")
        return self._strategies[name]

    def execute(self, name: str, context: T) -> U:
        """Execute a named strategy.

        Args:
            name: Strategy name.
            context: Context to pass to strategy.

        Returns:
            Strategy result.
        """
        return self.get(name)(context)

    def execute_default(self, context: T) -> U:
        """Execute the default strategy.

        Args:
            context: Context to pass to strategy.

        Returns:
            Strategy result.

        Raises:
            ValueError: If no default strategy set.
        """
        if self._default_key is None:
            raise ValueError("No default strategy key set")
        return self.execute(self._default_key, context)

    def keys(self) -> list[str]:
        """Get list of registered strategy names."""
        return list(self._strategies.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._strategies


def compose_strategies(
    *strategies: Callable[[T], U],
) -> Callable[[T], list[U]]:
    """Compose multiple strategies into one that returns all results.

    Args:
        *strategies: Strategies to compose.

    Returns:
        Function that returns list of results.
    """
    def composed(context: T) -> list[U]:
        return [s(context) for s in strategies]
    return composed


def chain_strategies(
    *strategies: Callable[[T], T],
) -> Callable[[T], T]:
    """Chain strategies where each transforms the previous output.

    Args:
        *strategies: Strategies to chain.

    Returns:
        Function that applies all strategies in sequence.
    """
    def chained(context: T) -> T:
        result = context
        for strategy in strategies:
            result = strategy(result)
        return result
    return chained
