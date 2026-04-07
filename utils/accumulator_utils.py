"""Accumulator utilities for RabAI AutoClick.

Provides:
- Running statistics (mean, variance, min, max)
- Accumulator combinators
- Windowed accumulation
- Grouped accumulation
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)


T = TypeVar("T")
U = TypeVar("U")


@dataclass
class Accumulator(Generic[T]):
    """Base accumulator class."""

    value: T
    count: int = field(default=1)

    def update(self, value: T) -> None:
        """Update accumulator with new value."""
        raise NotImplementedError

    def merge(self, other: Accumulator[T]) -> Accumulator[T]:
        """Merge another accumulator into this one."""
        raise NotImplementedError


@dataclass
class SumAccumulator(Accumulator[float]):
    """Accumulates sum of values."""

    value: float = 0.0
    count: int = 0

    def update(self, value: float) -> None:
        self.value += value
        self.count += 1

    def merge(self, other: SumAccumulator) -> SumAccumulator:
        return SumAccumulator(
            value=self.value + other.value,
            count=self.count + other.count,
        )


@dataclass
class MinMaxAccumulator(Accumulator[float]):
    """Accumulates min and max values."""

    value: float = 0.0
    count: int = 0
    min_val: Optional[float] = None
    max_val: Optional[float] = None

    def update(self, value: float) -> None:
        if self.count == 0:
            self.value = value
            self.min_val = value
            self.max_val = value
        else:
            self.value += value
            self.min_val = min(self.min_val, value)  # type: ignore
            self.max_val = max(self.max_val, value)  # type: ignore
        self.count += 1

    def merge(self, other: MinMaxAccumulator) -> MinMaxAccumulator:
        if other.count == 0:
            return self
        if self.count == 0:
            return other
        return MinMaxAccumulator(
            value=self.value + other.value,
            count=self.count + other.count,
            min_val=min(self.min_val, other.min_val),  # type: ignore
            max_val=max(self.max_val, other.max_val),  # type: ignore
        )


@dataclass
class MeanVarianceAccumulator:
    """Accumulates mean and variance (Welford's algorithm)."""

    mean: float = 0.0
    m2: float = 0.0
    count: int = 0

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        return self.m2 / (self.count - 1)

    @property
    def std_dev(self) -> float:
        return self.variance ** 0.5

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def merge(self, other: MeanVarianceAccumulator) -> MeanVarianceAccumulator:
        if other.count == 0:
            return self
        if self.count == 0:
            return other
        new_count = self.count + other.count
        delta = other.mean - self.mean
        new_mean = (self.count * self.mean + other.count * other.mean) / new_count
        new_m2 = (
            self.m2
            + other.m2
            + delta * delta * self.count * other.count / new_count
        )
        return MeanVarianceAccumulator(mean=new_mean, m2=new_m2, count=new_count)


@dataclass
class CountAccumulator(Accumulator[int]):
    """Counts occurrences of values."""

    value: int = 0
    count: int = 0
    _counts: Dict[Any, int] = field(default_factory=dict)

    def update(self, value: Any) -> None:
        self.value += 1
        self.count += 1
        self._counts[value] = self._counts.get(value, 0) + 1

    def merge(self, other: CountAccumulator) -> CountAccumulator:
        result = CountAccumulator(
            value=self.value + other.value,
            count=self.count + other.count,
        )
        for k, v in self._counts.items():
            result._counts[k] = v
        for k, v in other._counts.items():
            result._counts[k] = result._counts.get(k, 0) + v
        return result

    def frequency(self, value: Any) -> float:
        if self.count == 0:
            return 0.0
        return self._counts.get(value, 0) / self.count

    def most_common(self, n: int = 5) -> List[Tuple[Any, int]]:
        return sorted(self._counts.items(), key=lambda x: x[1], reverse=True)[:n]


class RunningStats:
    """Running statistics for a stream of values.

    Tracks: count, sum, min, max, mean, variance.
    """

    def __init__(self) -> None:
        self.sum_acc = SumAccumulator()
        self.minmax_acc = MinMaxAccumulator()
        self.meanvar_acc = MeanVarianceAccumulator()

    def update(self, value: float) -> None:
        self.sum_acc.update(value)
        self.minmax_acc.update(value)
        self.meanvar_acc.update(value)

    def update_many(self, values: List[float]) -> None:
        for value in values:
            self.update(value)

    @property
    def count(self) -> int:
        return self.minmax_acc.count

    @property
    def sum(self) -> float:
        return self.sum_acc.value

    @property
    def min(self) -> Optional[float]:
        return self.minmax_acc.min_val

    @property
    def max(self) -> Optional[float]:
        return self.minmax_acc.max_val

    @property
    def mean(self) -> float:
        return self.meanvar_acc.mean

    @property
    def variance(self) -> float:
        return self.meanvar_acc.variance

    @property
    def std_dev(self) -> float:
        return self.meanvar_acc.std_dev

    def merge(self, other: RunningStats) -> RunningStats:
        result = RunningStats()
        result.sum_acc = self.sum_acc.merge(other.sum_acc)
        result.minmax_acc = self.minmax_acc.merge(other.minmax_acc)
        result.meanvar_acc = self.meanvar_acc.merge(other.meanvar_acc)
        return result

    def to_dict(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "sum": self.sum,
            "min": self.min or 0.0,
            "max": self.max or 0.0,
            "mean": self.mean,
            "variance": self.variance,
            "std_dev": self.std_dev,
        }


def accumulate(
    iterable: Iterator[float],
    func: Callable[[float, float], float] = lambda a, b: a + b,
    initial: float = 0.0,
) -> Iterator[float]:
    """Running accumulation.

    Args:
        iterable: Input values.
        func: Accumulation function (current, new) -> result.
        initial: Initial value.

    Yields:
        Running accumulated values.
    """
    total = initial
    for value in iterable:
        total = func(total, value)
        yield total


def window_accumulate(
    iterable: Iterator[float],
    size: int,
    func: Callable[[List[float]], float] = sum,
) -> Iterator[float]:
    """Windowed accumulation.

    Args:
        iterable: Input values.
        size: Window size.
        func: Function to apply to window.

    Yields:
        Windowed accumulated values.
    """
    window: List[float] = []
    for value in iterable:
        window.append(value)
        if len(window) > size:
            window.pop(0)
        yield func(window)
