"""Process pool utilities: parallel task execution across multiple processes."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, Future
from dataclasses import dataclass
from typing import Any, Callable, Iterator

__all__ = [
    "ProcessPool",
    "WorkerPool",
    "map_parallel",
    "submit_task",
]


@dataclass
class ProcessPool:
    """Process pool for CPU-bound parallel execution."""

    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self._executor: ProcessPoolExecutor | None = None

    def __enter__(self) -> "ProcessPool":
        self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._executor:
            self._executor.shutdown(wait=True)

    def map(self, func: Callable[[Any], Any], items: list[Any]) -> list[Any]:
        if not self._executor:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return list(self._executor.map(func, items))

    def submit(self, func: Callable[[Any], Any], *args: Any, **kwargs: Any) -> Future:
        if not self._executor:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._executor.submit(func, *args, **kwargs)

    def imap(self, func: Callable[[Any], Any], items: list[Any]) -> Iterator[Any]:
        if not self._executor:
            self._executor = ProcessPoolExecutor(max_workers=self.max_workers)
        return self._executor.map(func, items)


class WorkerPool:
    """Simple worker pool for distributing tasks across processes."""

    def __init__(self, num_workers: int = 4) -> None:
        self.num_workers = num_workers

    def execute(self, tasks: list[tuple[Callable, tuple, dict]]) -> list[Any]:
        """Execute tasks in parallel.

        Each task is a tuple of (func, args_tuple, kwargs_dict).
        """
        results: list[Any] = []

        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            for func, args, kwargs in tasks:
                future = executor.submit(func, *args, **kwargs)
                futures.append(future)

            for future in futures:
                try:
                    results.append(future.result(timeout=300))
                except Exception as e:
                    results.append(e)

        return results


def map_parallel(
    func: Callable[[Any], Any],
    items: list[Any],
    max_workers: int = 4,
) -> list[Any]:
    """Map a function over items in parallel using processes."""
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(func, items))


def submit_task(
    func: Callable[[Any], Any],
    *args: Any,
    max_workers: int = 4,
) -> Future:
    """Submit a single task to the process pool."""
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        return executor.submit(func, *args)
