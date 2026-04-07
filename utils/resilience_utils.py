"""Resilience utilities: bulkhead pattern, retry with jitter, and graceful degradation."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "Bulkhead",
    "ResilienceConfig",
    "ResilienceDecorator",
    "fallback",
    "bulkhead",
]


@dataclass
class ResilienceConfig:
    """Configuration for resilience patterns."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


class Bulkhead:
    """Bulkhead pattern - limit concurrent executions."""

    def __init__(self, max_concurrent: int) -> None:
        self.max_concurrent = max_concurrent
        self._semaphore: Any = None
        self._init_semaphore()

    def _init_semaphore(self) -> None:
        import threading
        self._semaphore = threading.Semaphore(self.max_concurrent)

    def execute(self, func: Callable[[], Any]) -> Any:
        with self._semaphore:
            return func()

    def __enter__(self) -> "Bulkhead":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class ResilienceDecorator:
    """Decorator for applying resilience patterns to functions."""

    def __init__(self, config: ResilienceConfig | None = None) -> None:
        self.config = config or ResilienceConfig()

    def retry(self, func: Callable[[], Any]) -> Callable[[], Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            cfg = self.config
            last_error: Exception | None = None
            for attempt in range(1, cfg.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < cfg.max_attempts:
                        delay = min(
                            cfg.base_delay * (cfg.exponential_base ** (attempt - 1)),
                            cfg.max_delay,
                        )
                        if cfg.jitter:
                            delay *= 0.5 + random.random() * 0.5
                        time.sleep(delay)
            raise last_error
        return wrapper

    def circuit_breaker(
        self,
        func: Callable[[], Any],
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ) -> Callable[[], Any]:
        state = {"failures": 0, "last_failure": 0.0, "open": False}

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            now = time.time()
            if state["open"]:
                if now - state["last_failure"] >= recovery_timeout:
                    state["open"] = False
                    state["failures"] = 0
                else:
                    raise RuntimeError("Circuit breaker is OPEN")

            try:
                result = func(*args, **kwargs)
                state["failures"] = 0
                return result
            except Exception as e:
                state["failures"] += 1
                state["last_failure"] = now
                if state["failures"] >= failure_threshold:
                    state["open"] = True
                raise

        return wrapper


def fallback(default: Any):
    """Decorator to return a default value on exception."""
    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception:
                return default
        return wrapper
    return decorator


def bulkhead(max_concurrent: int) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Decorator for bulkhead pattern."""
    b = Bulkhead(max_concurrent)
    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return b.execute(lambda: func(*args, **kwargs))
        return wrapper
    return decorator
