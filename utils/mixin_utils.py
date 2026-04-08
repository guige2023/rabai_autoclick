"""Mixin utilities for RabAI AutoClick.

Provides:
- Common mixin base classes
- Mixin composition helpers
- Method resolution order utilities
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    List,
    Optional,
    Type,
)


class ReprMixin:
    """Mixin that provides a meaningful __repr__."""

    def __repr__(self) -> str:
        sig = ", ".join(
            f"{k}={v!r}"
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        )
        return f"{self.__class__.__name__}({sig})"


class EqMixin:
    """Mixin that implements __eq__ based on __dict__."""

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.__dict__.items())))


class ComparableMixin:
    """Mixin that implements comparison operators."""

    def _compare_value(self) -> Any:
        """Override this to specify what to compare."""
        return self.__dict__

    def __eq__(self, other: Any) -> bool:
        return self._compare_value() == other._compare_value()  # type: ignore

    def __lt__(self, other: Any) -> bool:
        return self._compare_value() < other._compare_value()  # type: ignore

    def __le__(self, other: Any) -> bool:
        return self == other or self < other

    def __gt__(self, other: Any) -> bool:
        return other < self

    def __ge__(self, other: Any) -> bool:
        return not self < other


class SerializationMixin:
    """Mixin for dict serialization."""

    def to_dict(self) -> dict:
        """Convert instance to dictionary."""
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_")
        }

    @classmethod
    def from_dict(cls: Type[T], data: dict) -> T:
        """Create instance from dictionary."""
        return cls(**{
            k: v
            for k, v in data.items()
            if k in cls.__init__.__code__.co_varnames  # type: ignore
        })


class CloneMixin:
    """Mixin that provides a clone method."""

    def clone(self: T, **overrides: Any) -> T:
        """Create a shallow copy with optional overrides.

        Args:
            **overrides: Fields to override in the clone.

        Returns:
            Cloned instance.
        """
        cls = self.__class__
        new_obj = cls.__new__(cls)
        new_obj.__dict__.update(self.__dict__)
        new_obj.__dict__.update(overrides)
        return new_obj


class ObserverMixin:
    """Mixin that provides observer pattern support."""

    def __init__(self) -> None:
        self._observers: List[Callable[..., None]] = []

    def add_observer(self, callback: Callable[..., None]) -> None:
        """Add an observer callback.

        Args:
            callback: Function to call on notification.
        """
        if callback not in self._observers:
            self._observers.append(callback)

    def remove_observer(self, callback: Callable[..., None]) -> None:
        """Remove an observer callback.

        Args:
            callback: Function to remove.
        """
        if callback in self._observers:
            self._observers.remove(callback)

    def notify_observers(self, *args: Any, **kwargs: Any) -> None:
        """Notify all observers.

        Args:
            *args: Positional args to pass to observers.
            **kwargs: Keyword args to pass to observers.
        """
        for observer in self._observers[:]:
            observer(*args, **kwargs)


class MROMixin:
    """Mixin with method resolution order utilities."""

    @classmethod
    def get_mro(cls) -> List[Type]:
        """Get the method resolution order.

        Returns:
            List of classes in MRO.
        """
        return list(cls.__mro__)

    @classmethod
    def get_mixins(cls) -> List[Type]:
        """Get all mixin classes in the MRO.

        Returns:
            List of mixin types.
        """
        return [
            c for c in cls.__mro__
            if c not in (cls, object) and "Mixin" in c.__name__
        ]


__all__ = [
    "ReprMixin",
    "EqMixin",
    "ComparableMixin",
    "SerializationMixin",
    "CloneMixin",
    "ObserverMixin",
    "MROMixin",
]


from typing import TypeVar  # noqa: E402

T = TypeVar("T")
