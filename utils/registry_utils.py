"""Registry utilities for RabAI AutoClick.

Provides:
- Class/object registry
- Singleton registry
- Plugin registry pattern
- Registry with lifecycle hooks
"""

from __future__ import annotations

import threading
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
)


T = TypeVar("T")


class Registry(Generic[T]):
    """A thread-safe registry for objects.

    Args:
        name: Registry name.
    """

    def __init__(self, name: str = "default") -> None:
        self._name = name
        self._items: Dict[str, T] = {}
        self._lock = threading.RLock()

    def register(
        self,
        key: str,
        item: T,
    ) -> None:
        """Register an item.

        Args:
            key: Unique key for the item.
            item: Item to register.
        """
        with self._lock:
            if key in self._items:
                raise KeyError(f"Key already registered: {key}")
            self._items[key] = item

    def register_or_replace(
        self,
        key: str,
        item: T,
    ) -> None:
        """Register or replace an item.

        Args:
            key: Unique key for the item.
            item: Item to register.
        """
        with self._lock:
            self._items[key] = item

    def get(self, key: str, default: Optional[T] = None) -> Optional[T]:
        """Get an item from the registry.

        Args:
            key: Item key.
            default: Default if not found.

        Returns:
            Item or default.
        """
        with self._lock:
            return self._items.get(key, default)

    def unregister(self, key: str) -> bool:
        """Unregister an item.

        Args:
            key: Item key.

        Returns:
            True if item was removed.
        """
        with self._lock:
            if key in self._items:
                del self._items[key]
                return True
            return False

    def list_keys(self) -> list:
        """List all registered keys."""
        with self._lock:
            return list(self._items.keys())

    def list_items(self) -> Dict[str, T]:
        """Get all items as a dict."""
        with self._lock:
            return dict(self._items)

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self._items

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


class ClassRegistry(Generic[T]):
    """Registry that auto-registers classes by name or decorator."""

    def __init__(self) -> None:
        self._classes: Dict[str, Type[T]] = {}

    def register(
        self,
        cls: Optional[Type[T]] = None,
        *,
        name: Optional[str] = None,
    ) -> Type[T]:
        """Register a class.

        Can be used as a decorator or decorator factory.

        Args:
            cls: Class to register (if used as decorator).
            name: Optional custom name.

        Returns:
            The registered class.
        """
        def decorator(c: Type[T]) -> Type[T]:
            key = name or c.__name__
            self._classes[key] = c
            return c

        if cls is not None:
            return decorator(cls)
        return decorator

    def get(self, name: str) -> Optional[Type[T]]:
        """Get a registered class by name."""
        return self._classes.get(name)

    def create(self, name: str, *args: Any, **kwargs: Any) -> T:
        """Instantiate a registered class.

        Args:
            name: Class name.
            *args: Positional args for constructor.
            **kwargs: Keyword args for constructor.

        Returns:
            Instance of the class.
        """
        cls = self.get(name)
        if cls is None:
            raise KeyError(f"No class registered with name: {name}")
        return cls(*args, **kwargs)


def plugin_registry(
    name: str,
) -> Callable[[Type[T]], Type[T]]:
    """Create a plugin registry decorator.

    Args:
        name: Registry name.

    Returns:
        Decorator that registers classes.
    """
    registry = Registry[name]  # type: ignore

    def decorator(cls: Type[T]) -> Type[T]:
        registry.register(cls.__name__, cls)
        return cls

    return decorator


__all__ = [
    "Registry",
    "ClassRegistry",
    "plugin_registry",
]
