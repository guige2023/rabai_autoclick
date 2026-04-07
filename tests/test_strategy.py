"""Tests for strategy pattern utilities."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.strategy import (
    Strategy,
    RandomStrategy,
    RoundRobinStrategy,
    FirstValidStrategy,
    StrategyRegistry,
    AdaptiveStrategy,
    StrategyContext,
)


class TestRandomStrategy:
    """Tests for RandomStrategy."""

    def test_select_returns_option(self) -> None:
        """Test selecting from options."""
        strategy = RandomStrategy[int]()
        options = [1, 2, 3]
        result = strategy.select(options)
        assert result in options

    def test_select_empty_returns_none(self) -> None:
        """Test empty options return None."""
        strategy = RandomStrategy[int]()
        assert strategy.select([]) is None


class TestRoundRobinStrategy:
    """Tests for RoundRobinStrategy."""

    def test_round_robin_order(self) -> None:
        """Test round-robin ordering."""
        strategy = RoundRobinStrategy[int]()
        options = [1, 2, 3]

        results = [strategy.select(options) for _ in range(3)]
        assert results == [1, 2, 3]

    def test_round_robin_wraps(self) -> None:
        """Test round-robin wraps after full cycle."""
        strategy = RoundRobinStrategy[int]()
        options = [1, 2, 3]

        for _ in range(3):
            strategy.select(options)

        assert strategy.select(options) == 1


class TestFirstValidStrategy:
    """Tests for FirstValidStrategy."""

    def test_select_first_valid(self) -> None:
        """Test selecting first valid option."""
        strategy = FirstValidStrategy[int](validator=lambda x: x > 0)
        options = [-1, 0, 1, 2]
        assert strategy.select(options) == 1

    def test_no_valid_returns_none(self) -> None:
        """Test no valid option returns None."""
        strategy = FirstValidStrategy[int](validator=lambda x: x > 10)
        options = [1, 2, 3]
        assert strategy.select(options) is None


class TestStrategyRegistry:
    """Tests for StrategyRegistry."""

    def test_register_and_get(self) -> None:
        """Test registering and getting strategy."""
        registry = StrategyRegistry[int]()
        strategy = RandomStrategy[int]()
        registry.register("random", strategy)
        assert registry.get("random") is strategy

    def test_select_uses_named_strategy(self) -> None:
        """Test selecting with named strategy."""
        registry = StrategyRegistry[int]()
        registry.register("test", RoundRobinStrategy[int]())
        options = [1, 2]
        result = registry.select("test", options)
        assert result == 1

    def test_select_unknown_uses_default(self) -> None:
        """Test unknown strategy uses default."""
        registry = StrategyRegistry[int](default_name="random")
        registry.register("random", RandomStrategy[int]())
        options = [1, 2]
        result = registry.select("unknown", options)
        assert result in options


class TestAdaptiveStrategy:
    """Tests for AdaptiveStrategy."""

    def test_select_unevaluated_first(self) -> None:
        """Test unevaluated options get priority."""
        strategy = AdaptiveStrategy[int]()
        options = [1, 2, 3]
        result = strategy.select(options)
        assert result == 1

    def test_record_success(self) -> None:
        """Test recording success."""
        strategy = AdaptiveStrategy[int]()
        strategy.record_success(0)
        assert strategy._successes[0] == 1

    def test_record_failure(self) -> None:
        """Test recording failure."""
        strategy = AdaptiveStrategy[int]()
        strategy.record_failure(0)
        assert strategy._attempts[0] == 1


class TestStrategyContext:
    """Tests for StrategyContext."""

    def test_execute(self) -> None:
        """Test executing strategy."""
        strategy = RandomStrategy[int]()
        context = StrategyContext(strategy)
        result = context.execute([1, 2])
        assert result in [1, 2]

    def test_set_strategy(self) -> None:
        """Test changing strategy."""
        context = StrategyContext(RandomStrategy[int]())
        context.set_strategy(RoundRobinStrategy[int]())
        assert isinstance(context._strategy, RoundRobinStrategy)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])