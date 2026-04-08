"""
Condition Utilities

Provides utilities for evaluating conditions
and predicates in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Generic, TypeVar
from dataclasses import dataclass

T = TypeVar("T")


@dataclass
class ConditionResult:
    """Result of a condition evaluation."""
    satisfied: bool
    value: Any = None
    message: str = ""


class Condition(Generic[T]):
    """
    Represents a condition that can be evaluated.
    
    Supports simple conditions, composed conditions,
    and delayed evaluation.
    """

    def __init__(
        self,
        predicate: Callable[[T], bool],
        description: str = "",
    ) -> None:
        self._predicate = predicate
        self._description = description

    def evaluate(self, value: T) -> ConditionResult:
        """Evaluate the condition."""
        try:
            satisfied = self._predicate(value)
            return ConditionResult(
                satisfied=satisfied,
                value=value,
                message="" if satisfied else f"Condition not satisfied: {self._description}",
            )
        except Exception as e:
            return ConditionResult(
                satisfied=False,
                value=value,
                message=f"Condition error: {e}",
            )

    def __call__(self, value: T) -> bool:
        """Evaluate condition as callable."""
        return self._predicate(value)

    def __and__(self, other: Condition[T]) -> Condition[T]:
        """Logical AND of two conditions."""
        def combined(v: T) -> bool:
            return self._predicate(v) and other._predicate(v)
        return Condition(combined, f"{self._description} AND {other._description}")

    def __or__(self, other: Condition[T]) -> Condition[T]:
        """Logical OR of two conditions."""
        def combined(v: T) -> bool:
            return self._predicate(v) or other._predicate(v)
        return Condition(combined, f"{self._description} OR {other._description}")

    def __invert__(self) -> Condition[T]:
        """Logical NOT of condition."""
        def negated(v: T) -> bool:
            return not self._predicate(v)
        return Condition(negated, f"NOT {self._description}")


# Common condition factories
def always_true() -> Condition[Any]:
    """Condition that always returns True."""
    return Condition(lambda _: True, "always_true")


def always_false() -> Condition[Any]:
    """Condition that always returns False."""
    return Condition(lambda _: False, "always_false")


def equals(expected: Any) -> Condition[Any]:
    """Condition that checks equality."""
    return Condition(lambda v: v == expected, f"equals({expected!r})")


def not_none() -> Condition[Any]:
    """Condition that checks value is not None."""
    return Condition(lambda v: v is not None, "not_none")


def in_range(min_val: float, max_val: float) -> Condition[float]:
    """Condition that checks value is in range."""
    return Condition(
        lambda v: min_val <= v <= max_val,
        f"in_range({min_val}, {max_val})",
    )


def matches_pattern(pattern: str) -> Condition[str]:
    """Condition that checks string matches pattern."""
    import re
    regex = re.compile(pattern)
    return Condition(
        lambda v: bool(regex.match(v)),
        f"matches({pattern!r})",
    )
