"""Strategy pattern utilities for RabAI AutoClick.

Provides:
- Strategy selection
- Strategy registry
"""

from typing import Any, Callable, Dict, Generic, Optional, TypeVar


T = TypeVar("T")


class Strategy(Generic[T]):
    """Base strategy class."""

    def select(self, options: list) -> Optional[T]:
        """Select from options.

        Args:
            options: List of options.

        Returns:
            Selected option or None.
        """
        raise NotImplementedError


class RandomStrategy(Strategy[T]):
    """Random selection strategy."""

    def select(self, options: list) -> Optional[T]:
        """Select random option."""
        import random
        if not options:
            return None
        return random.choice(options)


class RoundRobinStrategy(Strategy[T]):
    """Round-robin selection strategy."""

    def __init__(self) -> None:
        self._index = 0

    def select(self, options: list) -> Optional[T]:
        """Select next option in round-robin."""
        if not options:
            return None

        option = options[self._index % len(options)]
        self._index += 1
        return option


class FirstValidStrategy(Strategy[T]):
    """Select first valid option."""

    def __init__(self, validator: Optional[Callable[[T], bool]] = None) -> None:
        """Initialize.

        Args:
            validator: Optional function to validate options.
        """
        self._validator = validator or (lambda x: True)

    def select(self, options: list) -> Optional[T]:
        """Select first valid option."""
        for option in options:
            if self._validator(option):
                return option
        return None


class StrategyRegistry(Generic[T]):
    """Registry for strategies.

    Allows registering and selecting strategies dynamically.
    """

    def __init__(self, default_name: str = "default") -> None:
        """Initialize registry.

        Args:
            default_name: Name of default strategy.
        """
        self._strategies: Dict[str, Strategy[T]] = {}
        self._default_name = default_name

    def register(self, name: str, strategy: Strategy[T]) -> None:
        """Register a strategy.

        Args:
            name: Strategy name.
            strategy: Strategy instance.
        """
        self._strategies[name] = strategy

    def get(self, name: str) -> Optional[Strategy[T]]:
        """Get strategy by name.

        Args:
            name: Strategy name.

        Returns:
            Strategy or None.
        """
        return self._strategies.get(name)

    def select(self, name: str, options: list) -> Optional[T]:
        """Select using named strategy.

        Args:
            name: Strategy name.
            options: Options to select from.

        Returns:
            Selected option.
        """
        strategy = self._strategies.get(name) or self._strategies.get(self._default_name)
        if strategy:
            return strategy.select(options)
        return None

    def set_default(self, name: str) -> bool:
        """Set default strategy.

        Args:
            name: Strategy name.

        Returns:
            True if strategy exists.
        """
        if name in self._strategies:
            self._default_name = name
            return True
        return False


class AdaptiveStrategy(Strategy[T]):
    """Strategy that adapts based on success/failure.

    Tracks success rates and favors better-performing options.
    """

    def __init__(self) -> None:
        self._successes: Dict[int, int] = {}
        self._attempts: Dict[int, int] = {}

    def record_success(self, option_index: int) -> None:
        """Record successful use of option.

        Args:
            option_index: Index of option.
        """
        self._successes[option_index] = self._successes.get(option_index, 0) + 1
        self._attempts[option_index] = self._attempts.get(option_index, 0) + 1

    def record_failure(self, option_index: int) -> None:
        """Record failed use of option.

        Args:
            option_index: Index of option.
        """
        self._attempts[option_index] = self._attempts.get(option_index, 0) + 1

    def select(self, options: list) -> Optional[T]:
        """Select best-performing option.

        Returns:
            Selected option or None.
        """
        if not options:
            return None

        if len(options) == 1:
            return options[0]

        best_index = 0
        best_score = -1

        for i, _ in enumerate(options):
            attempts = self._attempts.get(i, 0)
            if attempts == 0:
                # Unevaluated option gets priority
                return options[i]

            successes = self._successes.get(i, 0)
            score = successes / attempts

            if score > best_score:
                best_score = score
                best_index = i

        return options[best_index]


class StrategyContext:
    """Context for applying a strategy.

    Allows changing strategy at runtime.
    """

    def __init__(self, strategy: Strategy) -> None:
        """Initialize context.

        Args:
            strategy: Initial strategy.
        """
        self._strategy = strategy

    def set_strategy(self, strategy: Strategy) -> None:
        """Change strategy.

        Args:
            strategy: New strategy.
        """
        self._strategy = strategy

    def execute(self, options: list) -> Any:
        """Execute strategy selection.

        Args:
            options: Options to select from.

        Returns:
            Selected option.
        """
        return self._strategy.select(options)