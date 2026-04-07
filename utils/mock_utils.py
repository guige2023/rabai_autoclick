"""Mock utilities for unit testing: spies, stubs, and fake objects."""

from __future__ import annotations

import functools
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

__all__ = ["Spy", "Stub", "Fake", "MockCall", "patch_context"]


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class MockCall:
    """Record of a single function call."""
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: Any = None
    exception: Exception | None = None
    timestamp: float = field(default_factory=time.time)


class Spy(Generic[T]):
    """Wraps a function/object to record all calls while delegating to real implementation."""

    def __init__(self, fn: Callable[..., R]) -> None:
        self._fn = fn
        self._calls: list[MockCall] = []
        self._call_count = 0
        self._wrapped = fn

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._call_count += 1
        call = MockCall(args=args, kwargs=kwargs)
        try:
            result = self._fn(*args, **kwargs)
            call.result = result
            return result
        except Exception as e:
            call.exception = e
            raise
        finally:
            self._calls.append(call)

    @property
    def calls(self) -> list[MockCall]:
        return list(self._calls)

    @property
    def call_count(self) -> int:
        return self._call_count

    def called_with(self, **kwargs: Any) -> list[MockCall]:
        return [c for c in self._calls if all(c.kwargs.get(k) == v for k, v in kwargs.items())]

    def called_once_with(self, *args: Any, **kwargs: Any) -> bool:
        return len(self._calls) == 1 and (
            self._calls[0].args == args and self._calls[0].kwargs == kwargs
        )


class Stub(Generic[T]):
    """Returns pre-configured responses without calling real implementation."""

    def __init__(self, fn: Callable[..., R] | None = None) -> None:
        self._fn = fn
        self._responses: dict[tuple, R] = {}
        self._default_response: R | None = None
        self._side_effects: list[Callable[..., R]] = []
        self._calls: list[MockCall] = []

    def returns(self, *args: Any, result: R) -> "Stub[T]":
        """Return result when called with specific args."""
        self._responses[args] = result
        return self

    def returns_default(self, result: R) -> "Stub[T]":
        self._default_response = result
        return self

    def raises(self, exc: Exception) -> "Stub[T]":
        """Raise an exception when called."""
        def side_effect(*args: Any, **kwargs: Any) -> R:
            raise exc
        self._side_effects.append(side_effect)
        return self

    def call_real(self) -> "Stub[T]":
        """Delegate to the real function if provided."""
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> R:
        call = MockCall(args=args, kwargs=kwargs)
        self._calls.append(call)

        if self._side_effects:
            fn = self._side_effects[0]
            if len(self._side_effects) > 1:
                self._side_effects.pop(0)
            return fn(*args, **kwargs)

        if args in self._responses:
            return self._responses[args]

        if self._default_response is not None:
            return self._default_response

        if self._fn is not None:
            return self._fn(*args, **kwargs)

        return None  # type: ignore

    @property
    def calls(self) -> list[MockCall]:
        return list(self._calls)


class Fake(Generic[T]):
    """Fake implementation for testing: in-memory store with controlled behavior."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
        self._calls: list[MockCall] = []
        self._sequence = 0
        self._latency_ms: float = 0.0
        self._error_rate: float = 0.0
        self._error_type: type[Exception] = RuntimeError

    def set_latency(self, ms: float) -> "Fake[T]":
        self._latency_ms = ms
        return self

    def set_error_rate(self, rate: float, error_type: type[Exception] = RuntimeError) -> "Fake[T]":
        self._error_rate = rate
        self._error_type = error_type
        return self

    def _maybe_delay(self) -> None:
        if self._latency_ms > 0:
            time.sleep(self._latency_ms / 1000.0)

    def _maybe_error(self) -> None:
        import random
        if self._error_rate > 0 and random.random() < self._error_rate:
            raise self._error_type("Fake error")

    def get(self, key: str, default: Any = None) -> Any:
        self._maybe_delay()
        self._maybe_error()
        self._sequence += 1
        return self._store.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._maybe_delay()
        self._maybe_error()
        self._store[key] = value
        self._sequence += 1

    def delete(self, key: str) -> bool:
        self._maybe_delay()
        self._maybe_error()
        if key in self._store:
            del self._store[key]
            self._sequence += 1
            return True
        return False

    def keys(self) -> list[str]:
        return list(self._store.keys())

    def __len__(self) -> int:
        return len(self._store)

    def clear(self) -> None:
        self._store.clear()


class patch_context:
    """Context manager for temporarily patching objects."""

    def __init__(self, target: str, new: Any) -> None:
        import sys
        self.target_path = target
        self.new = new
        self._original: Any = None
        self._module_path: str
        self._attr_name: str
        parts = target.rsplit(".", 1)
        if len(parts) == 1:
            self._module_path = "__main__"
            self._attr_name = parts[0]
        else:
            self._module_path, self._attr_name = parts

    def __enter__(self) -> Any:
        import sys
        module = sys.modules.get(self._module_path)
        if module is None:
            import importlib
            parts = self._module_path.split(".")
            for i in range(len(parts), 0, -1):
                sub = ".".join(parts[:i])
                if sub in sys.modules:
                    module = sys.modules[sub]
                    break
        if module is not None:
            self._original = getattr(module, self._attr_name, None)
            setattr(module, self._attr_name, self.new)
        return self.new

    def __exit__(self, *args: Any) -> None:
        import sys
        module = sys.modules.get(self._module_path)
        if module is not None and self._original is not None:
            setattr(module, self._attr_name, self._original)
