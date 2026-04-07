"""window_func action module for rabai_autoclick.

Provides SQL-style window functions for data processing:
ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE,
LAST_VALUE, SUM, AVG, COUNT over sliding windows.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Union

__all__ = [
    "WindowFunc",
    "RowNumber",
    "Rank",
    "DenseRank",
    "PercentRank",
    "CumeDist",
    "NTile",
    "Lag",
    "Lead",
    "FirstValue",
    "LastValue",
    "NthValue",
    "Sum",
    "Avg",
    "Count",
    "Min",
    "Max",
    "Window",
    "WindowBuilder",
    "Partition",
    "WindowFrame",
    "FrameType",
    "WindowDefinition",
    "apply_window_funcs",
]


T = TypeVar("T")
V = TypeVar("V")


class FrameType(Enum):
    """Window frame types."""
    ROWS = auto()
    RANGE = auto()


@dataclass
class WindowFrame:
    """Window frame specification."""
    frame_type: FrameType = FrameType.ROWS
    start_offset: int = 0
    end_offset: int = 0
    start_unbounded: bool = True
    end_unbounded: bool = False

    def is_unbounded_start(self) -> bool:
        return self.start_unbounded

    def is_unbounded_end(self) -> bool:
        return self.end_unbounded


@dataclass
class WindowDefinition:
    """Complete window definition with partition and frame."""
    partition_by: List[Callable[[T], Any]] = field(default_factory=list)
    order_by: Optional[List[tuple]] = None
    frame: WindowFrame = field(default_factory=WindowFrame)
    partition_key: Optional[Any] = None


class WindowFunc(Generic[T, V]):
    """Base window function."""

    def __init__(self) -> None:
        self._window: Optional[Window] = None

    def set_window(self, window: "Window") -> None:
        """Set associated window for accessing partition data."""
        self._window = window

    def compute(self, items: List[T], current_idx: int) -> V:
        """Compute window function value.

        Args:
            items: All items in current partition.
            current_idx: Index of current row.

        Returns:
            Computed value.
        """
        raise NotImplementedError

    def reset(self) -> None:
        """Reset state between partitions."""
        pass


class RowNumber(WindowFunc[T, int]):
    """ROW_NUMBER() - sequential row number within partition."""

    def compute(self, items: List[T], current_idx: int) -> int:
        return current_idx + 1


class Rank(WindowFunc[T, int]):
    """RANK() - rank with gaps for ties."""

    def __init__(self, order_by_fn: Optional[Callable[[T], Any]] = None) -> None:
        super().__init__()
        self.order_by_fn = order_by_fn
        self._rank: int = 0
        self._prev_value: Any = None

    def compute(self, items: List[T], current_idx: int) -> int:
        if self.order_by_fn is None:
            return current_idx + 1
        current_value = self.order_by_fn(items[current_idx])
        if current_value != self._prev_value:
            self._rank = current_idx + 1
            self._prev_value = current_value
        return self._rank

    def reset(self) -> None:
        self._rank = 0
        self._prev_value = None


class DenseRank(WindowFunc[T, int]):
    """DENSE_RANK() - rank without gaps."""

    def __init__(self, order_by_fn: Optional[Callable[[T], Any]] = None) -> None:
        super().__init__()
        self.order_by_fn = order_by_fn
        self._rank: int = 0
        self._prev_value: Any = None

    def compute(self, items: List[T], current_idx: int) -> int:
        if self.order_by_fn is None:
            return current_idx + 1
        current_value = self.order_by_fn(items[current_idx])
        if current_value != self._prev_value:
            self._rank += 1
            self._prev_value = current_value
        return self._rank

    def reset(self) -> None:
        self._rank = 0
        self._prev_value = None


class PercentRank(WindowFunc[T, float]):
    """PERCENT_RANK() - percentage rank within partition."""

    def compute(self, items: List[T], current_idx: int) -> float:
        n = len(items)
        if n <= 1:
            return 0.0
        rank = current_idx
        return (rank - 1) / (n - 1)


class CumeDist(WindowFunc[T, float]):
    """CUME_DIST() - cumulative distribution."""

    def compute(self, items: List[T], current_idx: int) -> float:
        n = len(items)
        if n == 0:
            return 0.0
        rank = current_idx + 1
        return rank / n


class NTile(WindowFunc[T, int]):
    """NTile(n) - divide partition into n approximately equal buckets."""

    def __init__(self, buckets: int = 2) -> None:
        super().__init__()
        self.buckets = buckets

    def compute(self, items: List[T], current_idx: int) -> int:
        n = len(items)
        if n == 0:
            return 0
        bucket_size = n / self.buckets
        return int(current_idx / bucket_size) + 1


class Lag(WindowFunc[T, Any]):
    """LAG(value, offset) - value from preceding row."""

    def __init__(self, offset: int = 1, default: Any = None) -> None:
        super().__init__()
        self.offset = offset
        self.default = default

    def compute(self, items: List[T], current_idx: int) -> Any:
        target_idx = current_idx - self.offset
        if 0 <= target_idx < len(items):
            return items[target_idx]
        return self.default


class Lead(WindowFunc[T, Any]):
    """LEAD(value, offset) - value from following row."""

    def __init__(self, offset: int = 1, default: Any = None) -> None:
        super().__init__()
        self.offset = offset
        self.default = default

    def compute(self, items: List[T], current_idx: int) -> Any:
        target_idx = current_idx + self.offset
        if 0 <= target_idx < len(items):
            return items[target_idx]
        return self.default


class FirstValue(WindowFunc[T, T]):
    """FIRST_VALUE() - first value in window frame."""

    def __init__(self, frame: Optional[WindowFrame] = None) -> None:
        super().__init__()
        self.frame = frame or WindowFrame()

    def compute(self, items: List[T], current_idx: int) -> T:
        if self.frame.start_unbounded:
            start_idx = 0
        else:
            start_idx = max(0, current_idx + self.frame.start_offset)
        if 0 <= start_idx < len(items):
            return items[start_idx]
        return items[0] if items else None


class LastValue(WindowFunc[T, T]):
    """LAST_VALUE() - last value in window frame."""

    def __init__(self, frame: Optional[WindowFrame] = None) -> None:
        super().__init__()
        self.frame = frame or WindowFrame()

    def compute(self, items: List[T], current_idx: int) -> T:
        n = len(items)
        if self.frame.end_unbounded:
            end_idx = n - 1
        else:
            end_idx = min(n - 1, current_idx + self.frame.end_offset)
        if 0 <= end_idx < n:
            return items[end_idx]
        return items[-1] if items else None


class NthValue(WindowFunc[T, Any]):
    """NTH_VALUE(n) - nth value in window frame."""

    def __init__(self, n: int = 1, default: Any = None) -> None:
        super().__init__()
        self.n = n
        self.default = default

    def compute(self, items: List[T], current_idx: int) -> Any:
        target_idx = current_idx + self.n - 1
        if 0 <= target_idx < len(items):
            return items[target_idx]
        return self.default


class Sum(WindowFunc[T, Union[int, float]]):
    """SUM() - sum of values in window frame."""

    def __init__(
        self,
        value_fn: Optional[Callable[[T], Union[int, float]]] = None,
        frame: Optional[WindowFrame] = None,
    ) -> None:
        super().__init__()
        self.value_fn = value_fn or (lambda x: x)
        self.frame = frame or WindowFrame()

    def _get_frame_indices(self, items: List[T], current_idx: int) -> tuple[int, int]:
        n = len(items)
        if self.frame.start_unbounded:
            start_idx = 0
        else:
            start_idx = max(0, current_idx + self.frame.start_offset)
        if self.frame.end_unbounded:
            end_idx = n - 1
        else:
            end_idx = min(n - 1, current_idx + self.frame.end_offset)
        return start_idx, end_idx

    def compute(self, items: List[T], current_idx: int) -> Union[int, float]:
        start_idx, end_idx = self._get_frame_indices(items, current_idx)
        return sum(self.value_fn(items[i]) for i in range(start_idx, end_idx + 1))


class Avg(Sum[T]):
    """AVG() - average of values in window frame."""

    def compute(self, items: List[T], current_idx: int) -> float:
        start_idx, end_idx = self._get_frame_indices(items, current_idx)
        values = [self.value_fn(items[i]) for i in range(start_idx, end_idx + 1)]
        if not values:
            return 0.0
        return sum(values) / len(values)


class Count(WindowFunc[T, int]):
    """COUNT() - count of values in window frame."""

    def __init__(
        self,
        predicate: Optional[Callable[[T], bool]] = None,
        frame: Optional[WindowFrame] = None,
    ) -> None:
        super().__init__()
        self.predicate = predicate or (lambda x: True)
        self.frame = frame or WindowFrame()

    def _get_frame_indices(self, items: List[T], current_idx: int) -> tuple[int, int]:
        n = len(items)
        if self.frame.start_unbounded:
            start_idx = 0
        else:
            start_idx = max(0, current_idx + self.frame.start_offset)
        if self.frame.end_unbounded:
            end_idx = n - 1
        else:
            end_idx = min(n - 1, current_idx + self.frame.end_offset)
        return start_idx, end_idx

    def compute(self, items: List[T], current_idx: int) -> int:
        start_idx, end_idx = self._get_frame_indices(items, current_idx)
        return sum(1 for i in range(start_idx, end_idx + 1) if self.predicate(items[i]))


class Min(WindowFunc[T, Any]):
    """MIN() - minimum value in window frame."""

    def __init__(
        self,
        value_fn: Optional[Callable[[T], Any]] = None,
        frame: Optional[WindowFrame] = None,
    ) -> None:
        super().__init__()
        self.value_fn = value_fn or (lambda x: x)
        self.frame = frame or WindowFrame()

    def _get_frame_indices(self, items: List[T], current_idx: int) -> tuple[int, int]:
        n = len(items)
        if self.frame.start_unbounded:
            start_idx = 0
        else:
            start_idx = max(0, current_idx + self.frame.start_offset)
        if self.frame.end_unbounded:
            end_idx = n - 1
        else:
            end_idx = min(n - 1, current_idx + self.frame.end_offset)
        return start_idx, end_idx

    def compute(self, items: List[T], current_idx: int) -> Any:
        start_idx, end_idx = self._get_frame_indices(items, current_idx)
        values = [self.value_fn(items[i]) for i in range(start_idx, end_idx + 1)]
        return min(values) if values else None


class Max(WindowFunc[T, Any]):
    """MAX() - maximum value in window frame."""

    def __init__(
        self,
        value_fn: Optional[Callable[[T], Any]] = None,
        frame: Optional[WindowFrame] = None,
    ) -> None:
        super().__init__()
        self.value_fn = value_fn or (lambda x: x)
        self.frame = frame or WindowFrame()

    def _get_frame_indices(self, items: List[T], current_idx: int) -> tuple[int, int]:
        n = len(items)
        if self.frame.start_unbounded:
            start_idx = 0
        else:
            start_idx = max(0, current_idx + self.frame.start_offset)
        if self.frame.end_unbounded:
            end_idx = n - 1
        else:
            end_idx = min(n - 1, current_idx + self.frame.end_offset)
        return start_idx, end_idx

    def compute(self, items: List[T], current_idx: int) -> Any:
        start_idx, end_idx = self._get_frame_indices(items, current_idx)
        values = [self.value_fn(items[i]) for i in range(start_idx, end_idx + 1)]
        return max(values) if values else None


class Partition(Generic[T]):
    """Represents a partition of data for window functions."""

    def __init__(
        self,
        key: Any,
        items: List[T],
        window_funcs: Optional[List[WindowFunc]] = None,
    ) -> None:
        self.key = key
        self.items = items
        self.window_funcs = window_funcs or []
        for wf in self.window_funcs:
            wf.set_window(self)  # type: ignore

    def compute(self) -> List[dict]:
        """Compute all window functions for each row."""
        results = []
        for idx, item in enumerate(self.items):
            row_result: dict = {"_item": item, "_index": idx}
            for wf in self.window_funcs:
                try:
                    row_result[wf.__class__.__name__] = wf.compute(self.items, idx)
                except Exception:
                    row_result[wf.__class__.__name__] = None
            results.append(row_result)
        return results


class Window:
    """Window helper for computing window functions."""

    def __init__(self, items: List[T]) -> None:
        self.items = items
        self._funcs: List[WindowFunc] = []

    def row_number(self) -> "Window":
        self._funcs.append(RowNumber())
        return self

    def rank(self, order_by: Optional[Callable] = None) -> "Window":
        self._funcs.append(Rank(order_by))
        return self

    def dense_rank(self, order_by: Optional[Callable] = None) -> "Window":
        self._funcs.append(DenseRank(order_by))
        return self

    def lag(self, offset: int = 1, default: Any = None) -> "Window":
        self._funcs.append(Lag(offset, default))
        return self

    def lead(self, offset: int = 1, default: Any = None) -> "Window":
        self._funcs.append(Lead(offset, default))
        return self

    def sum(self, value_fn: Optional[Callable] = None) -> "Window":
        self._funcs.append(Sum(value_fn))
        return self

    def avg(self, value_fn: Optional[Callable] = None) -> "Window":
        self._funcs.append(Avg(value_fn))
        return self

    def count(self) -> "Window":
        self._funcs.append(Count())
        return self

    def min(self, value_fn: Optional[Callable] = None) -> "Window":
        self._funcs.append(Min(value_fn))
        return self

    def max(self, value_fn: Optional[Callable] = None) -> "Window":
        self._funcs.append(Max(value_fn))
        return self

    def compute(self) -> List[dict]:
        """Compute all window functions."""
        partition = Partition(None, self.items, self._funcs)
        return partition.compute()


class WindowBuilder:
    """Builder for complex window specifications."""

    def __init__(self) -> None:
        self._partition_by: List[Callable[[T], Any]] = []
        self._order_by: List[tuple] = []
        self._frame: WindowFrame = WindowFrame()
        self._funcs: List[WindowFunc] = []

    def partition_by(self, *fns: Callable[[T], Any]) -> "WindowBuilder":
        self._partition_by.extend(fns)
        return self

    def order_by(self, fn: Callable[[T], Any], asc: bool = True) -> "WindowBuilder":
        self._order_by.append((fn, asc))
        return self

    def rows(self, start_offset: int = 0, end_offset: int = 0) -> "WindowBuilder":
        self._frame = WindowFrame(
            frame_type=FrameType.ROWS,
            start_offset=start_offset,
            end_offset=end_offset,
        )
        return self

    def range_between(self, start: int, end: int) -> "WindowBuilder":
        self._frame = WindowFrame(
            frame_type=FrameType.RANGE,
            start_offset=start,
            end_offset=end,
        )
        return self

    def unbounded_preceding(self) -> "WindowBuilder":
        self._frame.start_unbounded = True
        return self

    def unbounded_following(self) -> "WindowBuilder":
        self._frame.end_unbounded = True
        return self

    def current_row(self) -> "WindowBuilder":
        self._frame.start_offset = 0
        self._frame.end_offset = 0
        return self

    def add_func(self, func: WindowFunc) -> "WindowBuilder":
        self._funcs.append(func)
        return self

    def sum(self, value_fn: Optional[Callable] = None) -> "WindowBuilder":
        self._funcs.append(Sum(value_fn, self._frame))
        return self

    def avg(self, value_fn: Optional[Callable] = None) -> "WindowBuilder":
        self._funcs.append(Avg(value_fn, self._frame))
        return self

    def count(self) -> "WindowBuilder":
        self._funcs.append(Count(frame=self._frame))
        return self

    def row_number(self) -> "WindowBuilder":
        self._funcs.append(RowNumber())
        return self

    def rank(self) -> "WindowBuilder":
        self._funcs.append(Rank())
        return self

    def dense_rank(self) -> "WindowBuilder":
        self._funcs.append(DenseRank())
        return self

    def lag(self, offset: int = 1, default: Any = None) -> "WindowBuilder":
        self._funcs.append(Lag(offset, default))
        return self

    def lead(self, offset: int = 1, default: Any = None) -> "WindowBuilder":
        self._funcs.append(Lead(offset, default))
        return self


def apply_window_funcs(
    items: List[T],
    window_funcs: List[WindowFunc],
) -> List[dict]:
    """Apply window functions to items.

    Args:
        items: Input items.
        window_funcs: List of window functions to apply.

    Returns:
        List of dicts with original item and window function results.
    """
    window = Window(items)
    for func in window_funcs:
        if isinstance(func, RowNumber):
            window.row_number()
        elif isinstance(func, Rank):
            window.rank()
        elif isinstance(func, DenseRank):
            window.dense_rank()
        elif isinstance(func, Lag):
            window.lag(func.offset, func.default)
        elif isinstance(func, Lead):
            window.lead(func.offset, func.default)
        elif isinstance(func, Sum):
            window.sum(func.value_fn)
        elif isinstance(func, Avg):
            window.avg(func.value_fn)
        elif isinstance(func, Count):
            window.count()
        elif isinstance(func, Min):
            window.min(func.value_fn)
        elif isinstance(func, Max):
            window.max(func.value_fn)
    return window.compute()
