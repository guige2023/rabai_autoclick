"""Singleton pattern utilities for RabAI AutoClick.

Provides:
- Singleton decorators
- Thread-safe singleton
"""

import threading
from typing import Any, Callable, Type, TypeVar


T = TypeVar("T")


def singleton(cls: Type[T]) -> Type[T]:
    """Singleton decorator.

    Makes a class a singleton. Thread-safe.

    Args:
        cls: Class to make singleton.

    Returns:
        Decorated class.
    """
    _instance: Type[T] = None
    _lock = threading.Lock()

    def get_instance(*args: Any, **kwargs: Any) -> T:
        nonlocal _instance
        if _instance is None:
            with _lock:
                if _instance is None:
                    _instance = cls(*args, **kwargs)
        return _instance

    # Copy class attributes
    get_instance.__name__ = cls.__name__
    get_instance.__doc__ = cls.__doc__

    return get_instance  # type: ignore


def singleton_with_lock(lock: threading.Lock) -> Callable[[Type[T]], Type[T]]:
    """Create singleton with custom lock.

    Args:
        lock: Custom lock to use.

    Returns:
        Decorator function.
    """
    def decorator(cls: Type[T]) -> Type[T]:
        _instance: Type[T] = None

        def get_instance(*args: Any, **kwargs: Any) -> T:
            nonlocal _instance
            if _instance is None:
                with lock:
                    if _instance is None:
                        _instance = cls(*args, **kwargs)
            return _instance

        return get_instance  # type: ignore
    return decorator


class SingletonMeta(type):
    """Singleton metaclass.

    Usage:
        class MySingleton(metaclass=SingletonMeta):
            pass
    """
    _instances: dict = {}
    _lock = threading.Lock()

    def __call__(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class LazySingleton:
    """Lazy singleton with deferred initialization.

    Instance is only created on first access.
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        """Initialize lazy singleton.

        Args:
            factory: Factory function to create instance.
        """
        self._factory = factory
        self._instance: T = None
        self._initialized = False
        self._lock = threading.Lock()

    def get_instance(self) -> T:
        """Get singleton instance.

        Returns:
            Singleton instance.
        """
        if not self._initialized:
            with self._lock:
                if not self._initialized:
                    self._instance = self._factory()
                    self._initialized = True
        return self._instance

    def reset(self) -> None:
        """Reset singleton instance.

        Allows recreation on next get_instance call.
        """
        with self._lock:
            self._instance = None
            self._initialized = False

    def __call__(self) -> T:
        """Allow instance to be called like a function."""
        return self.get_instance()


class ThreadLocalSingleton:
    """Thread-local singleton.

    Each thread gets its own instance.
    """

    def __init__(self, factory: Callable[[], T]) -> None:
        """Initialize thread-local singleton.

        Args:
            factory: Factory function to create instance.
        """
        self._factory = factory
        self._local = threading.local()

    def get_instance(self) -> T:
        """Get thread-local instance.

        Returns:
            Thread-local instance.
        """
        if not hasattr(self._local, 'instance'):
            self._local.instance = self._factory()
        return self._local.instance

    def __call__(self) -> T:
        """Allow instance to be called like a function."""
        return self.get_instance()


def threaded_singleton(cls: Type[T]) -> Type[T]:
    """Thread-safe singleton decorator using double-checked locking.

    Args:
        cls: Class to make singleton.

    Returns:
        Decorated class.
    """
    _instance = None
    _lock = threading.Lock()

    def get_singleton(*args: Any, **kwargs: Any) -> T:
        nonlocal _instance
        if _instance is None:
            with _lock:
                if _instance is None:
                    _instance = cls(*args, **kwargs)
        return _instance

    get_singleton.__name__ = cls.__name__
    get_singleton.__doc__ = cls.__doc__
    return get_singleton  # type: ignore