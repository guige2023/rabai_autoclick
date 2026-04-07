"""Thread pool utilities: configurable thread pools, task submission, and result handling."""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "ThreadPool",
    "ThreadPoolConfig",
    "submit_task",
]


@dataclass
class ThreadPoolConfig:
    """Configuration for a thread pool."""

    min_workers: int = 2
    max_workers: int = 8
    keep_alive_seconds: float = 60.0
    queue_size: int = 0


class ThreadPool:
    """Configurable thread pool executor."""

    def __init__(self, config: ThreadPoolConfig | None = None) -> None:
        self.config = config or ThreadPoolConfig()
        self._executor: ThreadPoolExecutor | None = None

    def __enter__(self) -> "ThreadPool":
        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_workers,
            thread_name_prefix="pool-",
        )
        return self

    def __exit__(self, *args: Any) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)

    def submit(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Future:
        if not self._executor:
            self._executor = ThreadPoolExecutor(
                max_workers=self.config.max_workers,
            )
        return self._executor.submit(func, *args, **kwargs)

    def map(
        self,
        func: Callable[..., Any],
        items: list[Any],
    ) -> list[Any]:
        if not self._executor:
            self._executor = ThreadPoolExecutor(
                max_workers=self.config.max_workers,
            )
        return list(self._executor.map(func, items))

    def shutdown(self, wait: bool = True) -> None:
        if self._executor:
            self._executor.shutdown(wait=wait)


def submit_task(
    func: Callable[..., Any],
    *args: Any,
    max_workers: int = 4,
    **kwargs: Any,
) -> Future:
    """Submit a task to a thread pool."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return executor.submit(func, *args, **kwargs)
