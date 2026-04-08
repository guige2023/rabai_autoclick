"""
Result/Either type utilities.

Provides functional-style Result type for
explicit error handling without exceptions.
"""

from __future__ import annotations

from typing import Callable, Generic, TypeVar


T = TypeVar("T")
U = TypeVar("U")
E = TypeVar("E")


class Result(Generic[T, E]):
    """Represents either a success value or an error."""

    def __init__(self, _is_success: bool, _value: T | E | None = None):
        self._is_success = _is_success
        self._value = _value

    @classmethod
    def ok(cls, value: T) -> "Result[T, E]":
        """Create a success result."""
        return cls(True, value)

    @classmethod
    def err(cls, error: E) -> "Result[T, E]":
        """Create an error result."""
        return cls(False, error)

    @property
    def is_ok(self) -> bool:
        return self._is_success

    @property
    def is_err(self) -> bool:
        return not self._is_success

    @property
    def value(self) -> T | E | None:
        return self._value

    def unwrap(self) -> T:
        """Unwrap value, raise if error."""
        if not self._is_success:
            raise ResultError(f"Cannot unwrap error result: {self._value}")
        return self._value  # type: ignore

    def unwrap_err(self) -> E:
        """Unwrap error, raise if success."""
        if self._is_success:
            raise ResultError(f"Cannot unwrap success result: {self._value}")
        return self._value  # type: ignore

    def unwrap_or(self, default: T) -> T:
        """Unwrap value or return default if error."""
        if self._is_success:
            return self._value  # type: ignore
        return default

    def unwrap_or_else(self, fn: Callable[[E], T]) -> T:
        """Unwrap value or compute from error."""
        if self._is_success:
            return self._value  # type: ignore
        return fn(self._value)  # type: ignore

    def map(self, fn: Callable[[T], U]) -> "Result[U, E]":
        """Map success value."""
        if self._is_success:
            return Result.ok(fn(self._value))  # type: ignore
        return Result.err(self._value)  # type: ignore

    def map_err(self, fn: Callable[[E], U]) -> "Result[T, U]":
        """Map error value."""
        if self._is_success:
            return Result.ok(self._value)  # type: ignore
        return Result.err(fn(self._value))  # type: ignore

    def flat_map(self, fn: Callable[[T], "Result[U, E]"]) -> "Result[U, E]":
        """Chain Result-producing functions."""
        if self._is_success:
            return fn(self._value)  # type: ignore
        return Result.err(self._value)  # type: ignore

    def fold(
        self,
        on_ok: Callable[[T], U],
        on_err: Callable[[E], U],
    ) -> U:
        """Fold to single value."""
        if self._is_success:
            return on_ok(self._value)  # type: ignore
        return on_err(self._value)  # type: ignore

    def __repr__(self) -> str:
        if self._is_success:
            return f"Result.ok({self._value!r})"
        return f"Result.err({self._value!r})"


class ResultError(Exception):
    """Error when unwrapping Result."""
    pass


def safe_call(
    fn: Callable[..., T],
    *args: object,
    **kwargs: object,
) -> Result[T, Exception]:
    """
    Call function and wrap in Result.

    Args:
        fn: Function to call
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Result containing value or exception
    """
    try:
        return Result.ok(fn(*args, **kwargs))
    except Exception as e:
        return Result.err(e)


class Option(Generic[T]):
    """Optional value type - Some or Nothing."""

    def __init__(self, _has_value: bool, _value: T | None = None):
        self._has_value = _has_value
        self._value = _value

    @classmethod
    def some(cls, value: T) -> "Option[T]":
        return cls(True, value)

    @classmethod
    def nothing(cls) -> "Option[T]":
        return cls(False, None)

    @property
    def is_some(self) -> bool:
        return self._has_value

    @property
    def is_nothing(self) -> bool:
        return not self._has_value

    def unwrap(self) -> T:
        if not self._has_value:
            raise ResultError("Cannot unwrap nothing")
        return self._value  # type: ignore

    def unwrap_or(self, default: T) -> T:
        return self._value if self._has_value else default

    def map(self, fn: Callable[[T], U]) -> "Option[U]":
        if self._has_value:
            return Option.some(fn(self._value))  # type: ignore
        return Option.nothing()

    def flat_map(self, fn: Callable[[T], "Option[U]"]) -> "Option[U]":
        if self._has_value:
            return fn(self._value)  # type: ignore
        return Option.nothing()

    def __repr__(self) -> str:
        if self._has_value:
            return f"Option.some({self._value!r})"
        return "Option.nothing()"
