"""
Chain Utilities

Provides functional chain composition utilities
for UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Callable, TypeVar, Any

T = TypeVar("T")
R = TypeVar("R")


class Chain:
    """
    Functional chain composition for transforming data.
    
    Allows building transformation pipelines using
    method chaining pattern.
    """

    def __init__(self, value: Any) -> None:
        self._value = value

    def pipe(self, func: Callable[..., R]) -> Chain:
        """Pipe value through a function."""
        if isinstance(self._value, list) and not isinstance(func(self._value), list):
            return Chain([func(item) for item in self._value])
        return Chain(func(self._value))

    def pipe_optional(
        self,
        func: Callable[..., R] | None,
    ) -> Chain:
        """Pipe value through a function if it's not None."""
        if func is None:
            return self
        return self.pipe(func)

    def filter(self, predicate: Callable[[Any], bool]) -> Chain:
        """Filter chain values with a predicate."""
        if isinstance(self._value, list):
            return Chain([item for item in self._value if predicate(item)])
        return self

    def map(self, func: Callable[[Any], Any]) -> Chain:
        """Map a function over chain values."""
        if isinstance(self._value, list):
            return Chain([func(item) for item in self._value])
        return Chain(func(self._value))

    def flat_map(self, func: Callable[[Any], list[Any]]) -> Chain:
        """Flat map a function over chain values."""
        if isinstance(self._value, list):
            result = []
            for item in self._value:
                result.extend(func(item))
            return Chain(result)
        return Chain(func(self._value))

    def reduce(
        self,
        func: Callable[[Any, Any], Any],
        initial: Any = None,
    ) -> Chain:
        """Reduce chain values with a function."""
        if isinstance(self._value, list):
            if initial is None:
                return Chain(func(self._value[0], self._value[1]))
            return Chain(func(initial, self._value[0]))
        return self

    def value(self) -> Any:
        """Get the final value."""
        return self._value

    def tap(self, func: Callable[[Any], None]) -> Chain:
        """Execute a side effect without modifying the value."""
        func(self._value)
        return self


def pipe(value: Any, *funcs: Callable[..., Any]) -> Any:
    """
    Pipe a value through a series of functions.
    
    Args:
        value: Initial value.
        *funcs: Functions to pipe through.
        
    Returns:
        Final transformed value.
    """
    result = value
    for func in funcs:
        result = func(result)
    return result
