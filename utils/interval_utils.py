"""Interval utilities for RabAI AutoClick.

Provides:
- Interval arithmetic (union, intersection, difference)
- Interval containment checks
- Numeric interval creation and manipulation
"""

from __future__ import annotations

from typing import (
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)


class Interval(NamedTuple):
    """A numeric interval [start, end] (inclusive)."""
    start: float
    end: float

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(
                f"Invalid interval: start ({self.start}) > end ({self.end})"
            )

    def __contains__(self, value: float) -> bool:
        return self.start <= value <= self.end

    def __len__(self) -> float:
        return self.end - self.start

    def overlaps(self, other: Interval) -> bool:
        """Check if this interval overlaps with another."""
        return self.start <= other.end and other.start <= self.end

    def contains_interval(self, other: Interval) -> bool:
        """Check if this interval fully contains another."""
        return self.start <= other.start and self.end >= other.end

    def intersection(self, other: Interval) -> Optional[Interval]:
        """Get the intersection with another interval."""
        if not self.overlaps(other):
            return None
        return Interval(max(self.start, other.start), min(self.end, other.end))

    def union(self, other: Interval) -> List[Interval]:
        """Get the union with another interval."""
        if not self.overlaps(other) and not self.end == other.start:
            return [self, other]
        return [Interval(min(self.start, other.start), max(self.end, other.end))]


def merge_intervals(intervals: List[Interval]) -> List[Interval]:
    """Merge a list of overlapping intervals.

    Args:
        intervals: List of intervals to merge.

    Returns:
        List of merged non-overlapping intervals.
    """
    if not intervals:
        return []

    sorted_ints = sorted(intervals, key=lambda x: x.start)
    merged: List[Interval] = [sorted_ints[0]]

    for current in sorted_ints[1:]:
        last = merged[-1]
        if current.start <= last.end:
            merged[-1] = Interval(last.start, max(last.end, current.end))
        else:
            merged.append(current)

    return merged


def subtract_interval(
    interval: Interval,
    other: Interval,
) -> List[Interval]:
    """Subtract another interval from an interval.

    Args:
        interval: Source interval.
        other: Interval to subtract.

    Returns:
        List of resulting intervals (0, 1, or 2).
    """
    if not interval.overlaps(other):
        return [interval]

    result: List[Interval] = []

    if interval.start < other.start:
        result.append(Interval(interval.start, other.start))

    if interval.end > other.end:
        result.append(Interval(other.end, interval.end))

    return result


def interval_gaps(intervals: List[Interval]) -> List[Interval]:
    """Find gaps between intervals.

    Args:
        intervals: List of intervals.

    Returns:
        List of gaps (intervals not covered).
    """
    if not intervals:
        return []

    merged = merge_intervals(intervals)
    gaps: List[Interval] = []

    for i in range(len(merged) - 1):
        a = merged[i]
        b = merged[i + 1]
        if a.end < b.start:
            gaps.append(Interval(a.end, b.start))

    return gaps


def is_disjoint(intervals: List[Interval]) -> bool:
    """Check if intervals are all disjoint (no overlaps).

    Args:
        intervals: List of intervals.

    Returns:
        True if no two intervals overlap.
    """
    if not intervals:
        return True
    merged = merge_intervals(intervals)
    return len(merged) == len(intervals)


def point_coverage(
    intervals: List[Interval],
    point: float,
) -> bool:
    """Check if a point is covered by any interval.

    Args:
        intervals: List of intervals.
        point: Point to check.

    Returns:
        True if point is in any interval.
    """
    return any(point in interval for interval in intervals)


def total_length(intervals: List[Interval]) -> float:
    """Compute total covered length of intervals.

    Args:
        intervals: List of intervals.

    Returns:
        Sum of interval lengths (accounting for overlaps).
    """
    if not intervals:
        return 0.0
    merged = merge_intervals(intervals)
    return sum(len(i) for i in merged)


__all__ = [
    "Interval",
    "merge_intervals",
    "subtract_interval",
    "interval_gaps",
    "is_disjoint",
    "point_coverage",
    "total_length",
]
