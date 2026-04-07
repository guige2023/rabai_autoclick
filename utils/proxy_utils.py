"""Proxy and delegation utilities for RabAI AutoClick.

Provides:
- Proxy objects
- Delegation patterns
- Lazy loading
"""

from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class Proxy:
    """Base proxy class."""

    def __init__(self, target: Any) -> None:
        self._target = target

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_target":
            object.__setattr__(self, name, value)
        else:
            setattr(self._target, name, value)


class LazyProxy(Proxy):
    """Proxy that lazily initializes the target.

    Example:
        proxy = LazyProxy(lambda: ExpensiveClass())
        proxy.method()  # Initializes only when called
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._target: Optional[T] = None

    def _get_target(self) -> T:
        if self._target is None:
            self._target = self._factory()
        return self._target

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get_target(), name)


class CachingProxy(Proxy):
    """Proxy that caches method results."""

    def __init__(self, target: Any) -> None:
        super().__init__(target)
        self._cache: Dict[str, Any] = {}


class PropertyProxy:
    """Proxy specific properties to another object."""

    def __init__(self, target: Any, properties: list[str]) -> None:
        self._target = target
        self._properties = properties

    def __getattr__(self, name: str) -> Any:
        if name in self._properties:
            return getattr(self._target, name)
        return getattr(self._target, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        elif name in self._properties:
            setattr(self._target, name, value)
        else:
            setattr(self._target, name, value)


def delegate(
    source: Any,
    target: Any,
    attrs: list[str],
) -> None:
    """Delegate attributes from source to target.

    Args:
        source: Object to add delegation to.
        target: Object to delegate to.
        attrs: List of attribute names to delegate.
    """
    for attr in attrs:
        if hasattr(target, attr):
            setattr(source, attr, getattr(target, attr))
