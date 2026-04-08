"""Singleton pattern utilities.

Provides thread-safe singleton implementations for
managing shared resources in automation workflows.
"""

import threading
from typing import Any, Callable, Generic, TypeVar


T = TypeVar("T")


def singleton(cls: Type[T]) -> Type[T]:
    """Decorator to make a class a singleton.

    Example:
        @singleton
        class Config:
            pass
    """
    _instance: Type[T] = None  # type: ignore
    _lock = threading.Lock()

    def get_instance(*args: Any, **kwargs: Any) -> Type[T]:
        nonlocal _instance
        if _instance is None:
            with _lock:
                if _instance is None:
                    _instance = cls(*args, **kwargs)
        return _instance

    return get_instance  # type: ignore


class Singleton(Generic[T]):
    """Base class for singleton instances.

    Example:
        class MySingleton(Singleton[MySingleton]):
            def __init__(self):
                self.value = 0

        obj = MySingleton.get_instance()
    """
    _instance: Optional[T] = None
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls: type) -> T:
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls.__new__(cls)
                    cls._instance.__init__()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton instance (for testing)."""
        with cls._lock:
            cls._instance = None

    @classmethod
    def has_instance(cls) -> bool:
        """Check if instance exists."""
        return cls._instance is not None


class LazySingleton(Generic[T]):
    """Lazy initialization singleton with thread safety.

    Example:
        class Registry(LazySingleton["Registry"]):
            def __init__(self):
                self.data = {}
    """
    _instance: Optional[T] = None
    _initialized = False
    _lock = threading.Lock()

    @classmethod
    def get_instance(cls: type) -> T:
        """Get or create singleton instance."""
        if not cls._initialized:
            with cls._lock:
                if not cls._initialized:
                    cls._instance = cls.__new__(cls)
                    cls._instance.__init__()
                    cls._initialized = True
        return cls._instance  # type: ignore


def lazy_singleton(cls: Type[T]) -> Callable[[], T]:
    """Create a lazy singleton accessor.

    Args:
        cls: Class to make lazy singleton.

    Returns:
        Function that returns the singleton instance.

    Example:
        get_config = lazy_singleton(Config)
        config = get_config()
    """
    _instance: T = None  # type: ignore
    _lock = threading.Lock()

    def get_instance() -> T:
        nonlocal _instance
        if _instance is None:
            with _lock:
                if _instance is None:
                    _instance = cls()
        return _instance

    return get_instance


from typing import Optional
