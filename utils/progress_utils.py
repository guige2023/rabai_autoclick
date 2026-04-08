"""Progress utilities for RabAI AutoClick.

Provides:
- Progress bar generation
- Progress tracking for iterables
- ETA calculations
- Progress callbacks
"""

from __future__ import annotations

import time
from typing import (
    Any,
    Callable,
    Iterator,
    Optional,
)


class ProgressTracker:
    """Track progress of an operation.

    Args:
        total: Total number of steps.
        description: Description of the operation.
    """

    def __init__(
        self,
        total: int,
        description: str = "",
        width: int = 40,
    ) -> None:
        if total <= 0:
            raise ValueError("total must be positive")
        self.total = total
        self.description = description
        self.width = width
        self.current = 0
        self._start_time = time.monotonic()

    def update(self, n: int = 1) -> None:
        """Update progress by n steps.

        Args:
            n: Number of steps to increment.
        """
        self.current = min(self.current + n, self.total)

    def set_progress(self, value: int) -> None:
        """Set absolute progress value.

        Args:
            value: New progress value.
        """
        self.current = max(0, min(value, self.total))

    @property
    def percentage(self) -> float:
        """Get progress as percentage (0-100)."""
        return (self.current / self.total) * 100 if self.total > 0 else 0.0

    @property
    def eta_seconds(self) -> Optional[float]:
        """Estimated seconds remaining."""
        if self.current == 0:
            return None
        elapsed = time.monotonic() - self._start_time
        rate = self.current / elapsed
        remaining = self.total - self.current
        return remaining / rate if rate > 0 else None

    def __str__(self) -> str:
        filled = int(self.width * self.current / self.total) if self.total > 0 else 0
        bar = "=" * filled + "-" * (self.width - filled)
        pct = f"{self.percentage:.1f}%"
        desc = f"{self.description} " if self.description else ""
        return f"{desc}[{bar}] {pct}"


def progress_bar(
    iterable: Iterator[T],
    total: Optional[int] = None,
    description: str = "",
) -> Iterator[T]:
    """Wrap an iterable with progress tracking.

    Args:
        iterable: Source iterable.
        total: Total items. Auto-detected if None.
        description: Progress description.

    Yields:
        Items from the iterable.
    """
    if total is None:
        source_list = list(iterable)
        iterable = iter(source_list)
        total = len(source_list)

    tracker = ProgressTracker(total, description)
    for item in iterable:
        yield item
        tracker.update()
        print(f"\r{tracker}", end="", flush=True)
    print()


def with_progress(
    func: Callable[..., Any],
    total: int,
    description: str = "",
) -> Callable[..., Any]:
    """Decorator to track progress of a function call.

    Args:
        func: Function to wrap.
        total: Total number of steps.
        description: Progress description.

    Returns:
        Decorated function.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracker = ProgressTracker(total, description)
        print(f"\r{tracker}", end="", flush=True)
        result = func(*args, **kwargs)
        tracker.set_progress(total)
        print(f"\r{tracker}")
        return result
    return wrapper


__all__ = [
    "ProgressTracker",
    "progress_bar",
    "with_progress",
]


from typing import TypeVar  # noqa: E402

T = TypeVar("T")
