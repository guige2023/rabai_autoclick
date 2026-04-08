"""
Deferred Evaluation Utilities

Provides lazy evaluation patterns, lazy properties, and deferred computation
for performance optimization and resource management.

License: MIT
"""

from __future__ import annotations

import threading
import weakref
from abc import ABC, abstractmethod
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    overload,
    TYPE_CHECKING,
)
from functools import wraps
from collections importabc

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class Lazy(Generic[T]):
    """Thread-safe lazy evaluation wrapper.
    
    The wrapped value is only computed on first access.
    Supports caching and optional reset functionality.
    
    Example:
        >>> lazy_sum = Lazy(lambda: sum(range(1000)))
        >>> lazy_sum.value  # computation happens here
        499500
        >>> lazy_sum.value  # cached, no recomputation
        499500
    """
    
    __slots__ = ("_factory", "_value", "_cached", "_lock", "_evaluated")
    
    def __init__(
        self,
        factory: Callable[[], T],
        cache: bool = True,
        thread_safe: bool = True,
    ) -> None:
        self._factory = factory
        self._cached = cache
        self._lock = threading.RLock() if thread_safe else None
        self._value: T | None = None
        self._evaluated = False
    
    @property
    def value(self) -> T:
        if not self._evaluated:
            with self._lock if self._lock else nullcontext:
                if not self._evaluated:
                    self._value = self._factory()
                    self._evaluated = True
        elif not self._cached:
            self._value = self._factory()
        return self._value
    
    def reset(self) -> None:
        with self._lock if self._lock else nullcontext:
            self._evaluated = False
            self._value = None
    
    @property
    def is_evaluated(self) -> bool:
        return self._evaluated
    
    def __repr__(self) -> str:
        status = "evaluated" if self._evaluated else "pending"
        return f"Lazy({status})"


class nullcontext:
    """Context manager that does nothing."""
    __slots__ = ("enter_result",)
    
    def __init__(self, enter_result: Any = None) -> None:
        self.enter_result = enter_result
    
    def __enter__(self) -> Any:
        return self.enter_result
    
    def __exit__(self, *args: Any) -> bool:
        return False


class LazyProperty(Generic[T]):
    """Descriptor-based lazy property with optional reset.
    
    Use as a class attribute decorator. Computation happens on first
    access and is cached by default.
    
    Example:
        class Config:
            @LazyProperty
            def database_url(self) -> str:
                return connect_to_db()
    """
    
    __slots__ = ("func", "name", "cache", "thread_safe")
    
    def __init__(
        self,
        func: Callable[[Any], T],
        cache: bool = True,
        thread_safe: bool = True,
    ) -> None:
        self.func = func
        self.name = func.__name__ if hasattr(func, "__name__") else ""
        self.cache = cache
        self.thread_safe = thread_safe
    
    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name
    
    def __get__(self, obj: Any, objtype: type | None = None) -> T:
        if obj is None:
            return self
        
        cache_attr = f"_lazy_{self.name}_cache"
        lock_attr = f"_lazy_{self.name}_lock"
        
        if not hasattr(obj, cache_attr):
            lock = threading.RLock() if self.thread_safe else nullcontext()
            setattr(obj, cache_attr, (False, None))
            setattr(obj, lock_attr, lock)
        
        cached, value = getattr(obj, cache_attr)
        
        if cached and self.cache:
            return value
        
        lock = getattr(obj, lock_attr)
        with lock:
            cached, value = getattr(obj, cache_attr)
            if not cached:
                value = self.func(obj)
                setattr(obj, cache_attr, (True, value))
        
        return value
    
    def __set__(self, obj: Any, value: Any) -> None:
        raise AttributeError("LazyProperty is read-only")
    
    def reset(self, obj: Any) -> None:
        cache_attr = f"_lazy_{self.name}_cache"
        if hasattr(obj, cache_attr):
            setattr(obj, cache_attr, (False, None))


def lazy(
    func: Callable[..., T] | None = None,
    *,
    cache: bool = True,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for lazy function evaluation.
    
    The function is only called when the returned callable is invoked.
    Results are cached by default.
    
    Example:
        @lazy
        def expensive_computation():
            return sum(range(10000))
        
        result = expensive_computation()  # actually runs
    """
    def decorator(f: Callable[..., T]) -> Callable[..., T]:
        lazy_val = Lazy(f, cache=cache)
        
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return lazy_val.value
        
        wrapper._lazy = lazy_val
        wrapper.reset = lambda: lazy_val.reset()
        return wrapper
    
    if func is not None:
        return decorator(func)
    return decorator


class Thunk(Generic[T]):
    """Explicit lazy thunk for delayed computation.
    
    A Thunk represents a computation that has not yet been executed.
    Call .force() to trigger evaluation.
    
    Example:
        thunk = Thunk(lambda: load_large_file())
        # file not loaded yet
        data = thunk.force()  # now it's loaded
    """
    
    __slots__ = ("_factory", "_value", "_forced")
    
    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._value: T | None = None
        self._forced = False
    
    def force(self) -> T:
        if not self._forced:
            self._value = self._factory()
            self._forced = True
        return self._value
    
    @property
    def is_forced(self) -> bool:
        return self._forced
    
    def __repr__(self) -> str:
        status = "forced" if self._forced else "thunk"
        return f"Thunk({status})"


class Evaluator(ABC):
    """Abstract base for custom evaluation strategies."""
    
    @abstractmethod
    def evaluate(self, factory: Callable[[], T]) -> T:
        """Evaluate the factory function."""
        ...


class MemoizingEvaluator(Evaluator):
    """Evaluation strategy that memoizes results."""
    
    def __init__(self) -> None:
        self._cache: dict[str, Any] = {}
        self._lock = threading.Lock()
    
    def evaluate(self, factory: Callable[[], T]) -> T:
        key = id(factory)
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        result = factory()
        with self._lock:
            self._cache[key] = result
        return result


class DeferredChain(Generic[T]):
    """Chain of deferred computations.
    
    Allows building a pipeline of lazy operations that are
    only executed when the final value is accessed.
    """
    
    __slots__ = ("_initial", "_operations")
    
    def __init__(self, initial: Lazy[T] | Thunk[T] | Callable[[], T]) -> None:
        if callable(initial) and not isinstance(initial, (Lazy, Thunk)):
            self._initial: Any = Lazy(initial)
        else:
            self._initial = initial
        self._operations: list[tuple[Callable[[Any], Any], tuple, dict]] = []
    
    def map(self, func: Callable[[T], U]) -> DeferredChain[U]:
        self._operations.append((func, (), {}))
        return self
    
    def flat_map(self, func: Callable[[T], DeferredChain[U]]) -> DeferredChain[U]:
        def new_func(x: T) -> U:
            result = func(x)
            if hasattr(result, "value"):
                return result.value
            return result.force()
        self._operations.append((new_func, (), {}))
        return self
    
    def filter(self, predicate: Callable[[T], bool]) -> DeferredChain[T]:
        def filtered(x: T) -> T:
            if not predicate(x):
                raise ValueError("Filter condition not met")
            return x
        self._operations.append((filtered, (), {}))
        return self
    
    @property
    def value(self) -> T:
        if hasattr(self._initial, "value"):
            result = self._initial.value
        elif hasattr(self._initial, "force"):
            result = self._initial.force()
        else:
            result = self._initial()
        
        for op, args, kwargs in self._operations:
            result = op(result)
        return result


__all__ = [
    "Lazy",
    "LazyProperty",
    "lazy",
    "Thunk",
    "Evaluator",
    "MemoizingEvaluator",
    "DeferredChain",
    "nullcontext",
]
