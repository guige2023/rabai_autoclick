"""Typing utilities v3 - protocol and structural typing.

Protocol-based typing utilities for structural
 subtyping and interface checking.
"""

from __future__ import annotations

from typing import Any, Callable, Generic, Protocol, TypeVar, runtime_checkable

__all__ = [
    "SupportsRichComparison",
    "SupportsEquality",
    "SupportsHash",
    "SupportsAdd",
    "SupportsMul",
    "Drawable",
    "Serializable",
    "Comparable",
    "Hashable",
    "Sized",
    "Iterable",
    "CallableProtocol",
    "StructuralChecker",
    "check_protocol",
    "implements",
]


T = TypeVar("T")


@runtime_checkable
class SupportsRichComparison(Protocol):
    """Protocol for rich comparison support."""
    def __lt__(self, other: Any) -> bool: ...
    def __le__(self, other: Any) -> bool: ...
    def __gt__(self, other: Any) -> bool: ...
    def __ge__(self, other: Any) -> bool: ...


@runtime_checkable
class SupportsEquality(Protocol):
    """Protocol for equality support."""
    def __eq__(self, other: object) -> bool: ...


@runtime_checkable
class SupportsHash(Protocol):
    """Protocol for hash support."""
    def __hash__(self) -> int: ...


@runtime_checkable
class SupportsAdd(Protocol):
    """Protocol for add support."""
    def __add__(self, other: Any) -> Any: ...


@runtime_checkable
class SupportsMul(Protocol):
    """Protocol for multiplication support."""
    def __mul__(self, other: Any) -> Any: ...


@runtime_checkable
class Drawable(Protocol):
    """Protocol for drawable objects."""
    def draw(self) -> None: ...


@runtime_checkable
class Serializable(Protocol):
    """Protocol for serializable objects."""
    def to_dict(self) -> dict: ...


@runtime_checkable
class Comparable(Protocol):
    """Protocol for comparable objects."""
    def compare(self, other: Any) -> int: ...


@runtime_checkable
class Hashable(Protocol):
    """Protocol for hashable objects."""
    def __hash__(self) -> int: ...


@runtime_checkable
class Sized(Protocol):
    """Protocol for sized objects."""
    def __len__(self) -> int: ...


@runtime_checkable
class Iterable(Protocol[T]):
    """Protocol for iterable objects."""
    def __iter__(self) -> Iterator[T]: ...


@runtime_checkable
class CallableProtocol(Protocol):
    """Protocol for callable objects."""
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class StructuralChecker:
    """Check structural typing compliance."""

    @staticmethod
    def check_protocol(obj: Any, protocol: type) -> bool:
        """Check if object implements protocol.

        Args:
            obj: Object to check.
            protocol: Protocol type.

        Returns:
            True if implements.
        """
        try:
            return isinstance(obj, protocol)
        except TypeError:
            return False

    @staticmethod
    def implements(obj: Any, *protocols: type) -> list[type]:
        """Check which protocols an object implements.

        Args:
            obj: Object to check.
            *protocols: Protocol types.

        Returns:
            List of implemented protocols.
        """
        return [p for p in protocols if StructuralChecker.check_protocol(obj, p)]


def check_protocol(obj: Any, protocol: type) -> bool:
    """Check if object implements protocol.

    Args:
        obj: Object to check.
        protocol: Protocol type.

    Returns:
        True if implements.
    """
    return StructuralChecker.check_protocol(obj, protocol)


def implements(obj: Any, *protocols: type) -> list[type]:
    """Check which protocols an object implements."""
    return StructuralChecker.implements(obj, *protocols)
