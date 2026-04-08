"""Singleton utilities for RabAI AutoClick.

Provides:
- Singleton pattern implementations
- Thread-safe singleton
- Singleton registry
"""

from __future__ import annotations

import threading
from typing import (
    Any,
    Callable,
    Generic,
    Type,
    TypeVar,
)


T = TypeVar("T")


class Singleton(Generic[T]):
    """Thread-safe singleton base class.

    Use by inheriting: class MyClass(Singleton[MyClass]):
    """

    _instance: Optional[T] = None
    _lock = threading.Lock()

    def __new__(cls: Type[T]) -> T:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls: Type[T]) -> T:
        """Get the singleton instance.

        Returns:
            The singleton instance.
        """
        return cls()

    @classmethod
    def reset(cls: Type[T]) -> None:
        """Reset the singleton instance (for testing)."""
        with cls._lock:
            cls._instance = None


def singleton(
    cls: Optional[Type[T]] = None,
    *,
    thread_safe: bool = True,
) -> Type[T]:
    """Singleton decorator.

    Args:
        cls: Class to make singleton.
        thread_safe: Whether to use thread-safe implementation.

    Returns:
        Singleton class.
    """
    instances: Dict[str, T] = {}
    lock = threading.Lock()

    def decorator(c: Type[T]) -> Type[T]:
        _instance: T

        def get_instance() -> T:
            if thread_safe:
                with lock:
                    if c.__name__ not in instances:
                        instances[c.__name__] = c()
                    return instances[c.__name__]
            if c.__name__ not in instances:
                instances[c.__name__] = c()
            return instances[c.__name__]

        class SingletonClass(c):  # type: ignore
            pass

        SingletonClass.get_instance = staticmethod(get_instance)  # type: ignore
        SingletonClass.reset = classmethod(  # type: ignore
            lambda cls: instances.pop(cls.__name__, None)
        )
        return SingletonClass

    if cls is not None:
        return decorator(cls)
    return decorator


class Borg:
    """Borg singleton pattern - all instances share state."""

    _shared_state: Dict[str, Any] = {}
    _lock = threading.Lock()

    def __new__(cls: Type[T]) -> T:
        obj = super().__new__(cls)
        obj.__dict__ = cls._shared_state
        return obj

    @classmethod
    def reset(cls) -> None:
        """Reset shared state."""
        with cls._lock:
            cls._shared_state.clear()


__all__ = [
    "Singleton",
    "singleton",
    "Borg",
]


from typing import Dict, Optional  # noqa: E402
