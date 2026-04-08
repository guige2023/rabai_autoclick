"""
Promise / Future Utilities

Async promise pattern implementation for synchronous code,
with completion callbacks, error handling, and chaining.

License: MIT
"""

from __future__ import annotations

import threading
import queue
from typing import (
    Any,
    Callable,
    Generic,
    TypeVar,
    Union,
    Optional,
    overload,
    Awaitable,
)
from enum import Enum, auto
from dataclasses import dataclass, field
from collections.abc import Awaitable
import time

T = TypeVar("T")
U = TypeVar("U")


class PromiseState(Enum):
    """Possible states of a Promise."""
    PENDING = auto()
    FULFILLED = auto()
    REJECTED = auto()


@dataclass
class Promise(Generic[T]):
    """Promise implementation with callbacks and chaining.
    
    Similar to JavaScript Promises, but for synchronous Python code.
    Use .resolve() to fulfill and .reject() to reject.
    
    Example:
        def fetch_data():
            promise = Promise()
            # async operation...
            promise.resolve(data)
            return promise
        
        fetch_data().then(lambda d: print(d))
    """
    
    _state: PromiseState = field(default=PromiseState.PENDING, init=False)
    _value: Any = field(default=None, init=False, repr=False)
    _error: Any = field(default=None, init=False, repr=False)
    _callbacks: list = field(default_factory=list, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    
    def resolve(self, value: T) -> Promise[T]:
        """Fulfill the promise with a value."""
        with self._lock:
            if self._state != PromiseState.PENDING:
                return self
            self._state = PromiseState.FULFILLED
            self._value = value
        self._notify()
        return self
    
    def reject(self, error: Exception) -> Promise[T]:
        """Reject the promise with an error."""
        with self._lock:
            if self._state != PromiseState.PENDING:
                return self
            self._state = PromiseState.REJECTED
            self._error = error
        self._notify()
        return self
    
    def then(
        self,
        on_fulfilled: Callable[[T], U] | None = None,
        on_rejected: Callable[[Exception], U] | None = None,
    ) -> Promise[U]:
        """Chain a callback when the promise settles."""
        new_promise: Promise[U] = Promise()
        
        def callback(state: PromiseState, value: Any, error: Any) -> None:
            try:
                if state == PromiseState.FULFILLED:
                    if on_fulfilled:
                        result = on_fulfilled(value)
                        new_promise.resolve(result)
                    else:
                        new_promise.resolve(value)
                else:
                    if on_rejected:
                        result = on_rejected(error)
                        new_promise.resolve(result)
                    else:
                        new_promise.reject(error)
            except Exception as e:
                new_promise.reject(e)
        
        self._callbacks.append(callback)
        if self._state != PromiseState.PENDING:
            callback(self._state, self._value, self._error)
        return new_promise
    
    def catch(self, on_rejected: Callable[[Exception], T]) -> Promise[T]:
        """Handle rejection, returning a fulfilled promise."""
        return self.then(on_fulfilled=None, on_rejected=on_rejected)
    
    def finally_(self, on_settled: Callable[[], None]) -> Promise[T]:
        """Execute callback regardless of settlement."""
        def cleanup(state: PromiseState, value: Any, error: Any) -> Any:
            on_settled()
            if state == PromiseState.FULFILLED:
                return value
            raise error
        
        return self.then(cleanup, cleanup)
    
    @property
    def state(self) -> PromiseState:
        return self._state
    
    @property
    def value(self) -> T | None:
        return self._value if self._state == PromiseState.FULFILLED else None
    
    @property
    def error(self) -> Exception | None:
        return self._error if self._state == PromiseState.REJECTED else None
    
    def _notify(self) -> None:
        for callback in self._callbacks:
            callback(self._state, self._value, self._error)
        self._callbacks.clear()
    
    @staticmethod
    def resolved(value: T) -> Promise[T]:
        """Create an already-resolved promise."""
        p: Promise[T] = Promise()
        p._state = PromiseState.FULFILLED
        p._value = value
        return p
    
    @staticmethod
    def rejected(error: Exception) -> Promise[Any]:
        """Create an already-rejected promise."""
        p: Promise[Any] = Promise()
        p._state = PromiseState.REJECTED
        p._error = error
        return p
    
    @staticmethod
    def race(promises: list[Promise[T]]) -> Promise[T]:
        """Return first promise to settle."""
        result: Promise[T] = Promise()
        
        def handler(state: PromiseState, value: Any, error: Any) -> None:
            if result._state == PromiseState.PENDING:
                if state == PromiseState.FULFILLED:
                    result.resolve(value)
                else:
                    result.reject(error)
        
        for p in promises:
            p._callbacks.append(handler)
            if p._state == PromiseState.FULFILLED:
                result.resolve(p._value)
            elif p._state == PromiseState.REJECTED:
                result.reject(p._error)
        return result
    
    @staticmethod
    def all(promises: list[Promise[T]]) -> Promise[list[T]]:
        """Resolve when all promises fulfill, reject on first rejection."""
        result: Promise[list[T]] = Promise()
        values: list[Any] = [None] * len(promises)
        count = [0]
        
        def check_complete() -> None:
            if count[0] == len(promises):
                result.resolve(values)
        
        def make_handler(i: int):
            def handler(state: PromiseState, value: Any, error: Any) -> None:
                if result._state != PromiseState.PENDING:
                    return
                if state == PromiseState.FULFILLED:
                    values[i] = value
                    count[0] += 1
                    check_complete()
                else:
                    result.reject(error)
            return handler
        
        for i, p in enumerate(promises):
            p._callbacks.append(make_handler(i))
            if p._state == PromiseState.REJECTED:
                result.reject(p._error)
                break
            elif p._state == PromiseState.FULFILLED:
                values[i] = p._value
                count[0] += 1
        
        if count[0] == len(promises) and result._state == PromiseState.PENDING:
            result.resolve(values)
        return result


class Future(Generic[T]):
    """Lightweight future/promise hybrid for async-like patterns.
    
    Supports timeout, polling, and callback registration.
    """
    
    __slots__ = ("_value", "_error", "_done", "_callbacks", "_lock")
    
    def __init__(self) -> None:
        self._value: T | None = None
        self._error: Exception | None = None
        self._done = False
        self._callbacks: list[Callable[[], None]] = []
        self._lock = threading.Lock()
    
    def set_result(self, value: T) -> None:
        with self._lock:
            self._value = value
            self._done = True
        for cb in self._callbacks:
            cb()
    
    def set_exception(self, error: Exception) -> None:
        with self._lock:
            self._error = error
            self._done = True
        for cb in self._callbacks:
            cb()
    
    def result(self, timeout: float | None = None) -> T:
        if not self._done:
            evt = threading.Event()
            self._callbacks.append(evt.set)
            if not evt.wait(timeout):
                raise TimeoutError("Future result timeout")
        if self._error:
            raise self._error
        return self._value  # type: ignore
    
    def done(self) -> bool:
        return self._done
    
    def add_done_callback(self, fn: Callable[[], None]) -> None:
        with self._lock:
            if self._done:
                fn()
            else:
                self._callbacks.append(fn)


def delay(seconds: float, value: T) -> Promise[T]:
    """Return a promise that resolves after delay."""
    p: Promise[T] = Promise()
    
    def resolve_after() -> None:
        time.sleep(seconds)
        p.resolve(value)
    
    t = threading.Thread(target=resolve_after, daemon=True)
    t.start()
    return p


__all__ = [
    "Promise",
    "PromiseState",
    "Future",
    "delay",
]
