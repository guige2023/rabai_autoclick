"""
Interval utilities for working with numeric ranges and intervals.

This module provides comprehensive interval operations including:
- Interval arithmetic (union, intersection, difference)
- Interval comparisons and containment checks
- Interval generation and iteration
- Numeric range utilities

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Iterator, List, Optional, Tuple, Union


@dataclass(frozen=True, order=True)
class Interval:
    """
    Immutable interval representation with comprehensive operations.
    
    Attributes:
        start: Start of the interval (inclusive).
        end: End of the interval (inclusive for closed intervals).
        left_open: Whether the left boundary is open (exclusive).
        right_open: Whether the right boundary is open (exclusive).
    
    Example:
        >>> interval = Interval(0, 10)
        >>> interval.contains(5)
        True
        >>> interval.length
        11
    """
    start: float = field(compare=True)
    end: float = field(compare=True)
    left_open: bool = field(default=False, compare=False)
    right_open: bool = field(default=False, compare=False)
    
    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError(f"Invalid interval: start ({self.start}) > end ({self.end})")
    
    @property
    def length(self) -> float:
        """Return the length of the interval."""
        return self.end - self.start
    
    @property
    def is_open(self) -> bool:
        """Check if the interval is open on both sides."""
        return self.left_open and self.right_open
    
    @property
    def is_closed(self) -> bool:
        """Check if the interval is closed on both sides."""
        return not self.left_open and not self.right_open
    
    @property
    def midpoint(self) -> float:
        """Return the midpoint of the interval."""
        return (self.start + self.end) / 2.0
    
    @property
    def is_empty(self) -> bool:
        """Check if the interval is empty (zero or negative length with closed boundaries)."""
        if self.length < 0:
            return True
        if self.length == 0:
            return self.left_open or self.right_open
        return False
    
    def contains(self, value: float) -> bool:
        """
        Check if a value is contained within the interval.
        
        Args:
            value: The numeric value to check.
            
        Returns:
            True if the value is within the interval boundaries.
        """
        if self.left_open:
            if value <= self.start:
                return False
        else:
            if value < self.start:
                return False
        
        if self.right_open:
            if value >= self.end:
                return False
        else:
            if value > self.end:
                return False
        
        return True
    
    def contains_interval(self, other: Interval) -> bool:
        """
        Check if another interval is completely contained within this one.
        
        Args:
            other: The interval to check for containment.
            
        Returns:
            True if other is fully contained within self.
        """
        start_cond = self.start <= other.start if not self.left_open else self.start < other.start
        end_cond = self.end >= other.end if not self.right_open else self.end > other.end
        return start_cond and end_cond
    
    def overlaps(self, other: Interval) -> bool:
        """
        Check if this interval overlaps with another interval.
        
        Args:
            other: The interval to check for overlap.
            
        Returns:
            True if the intervals share any common points.
        """
        return self.start <= other.end and other.start <= self.end
    
    def intersection(self, other: Interval) -> Optional[Interval]:
        """
        Compute the intersection of two intervals.
        
        Args:
            other: The interval to intersect with.
            
        Returns:
            The intersection interval, or None if no intersection exists.
        """
        if not self.overlaps(other):
            return None
        
        new_start = max(self.start, other.start)
        new_end = min(self.end, other.end)
        
        left_open = new_start == self.start and (self.left_open or other.left_open)
        right_open = new_end == self.end and (self.right_open or other.right_open)
        
        return Interval(new_start, new_end, left_open, right_open)
    
    def union(self, other: Interval) -> List[Interval]:
        """
        Compute the union of two intervals.
        
        If intervals are adjacent or overlapping, returns a single merged interval.
        Otherwise, returns both intervals as separate elements.
        
        Args:
            other: The interval to union with.
            
        Returns:
            List of intervals representing the union (1 or 2 intervals).
        """
        if not self.overlaps(other) and not self.adjacent_to(other):
            return sorted([self, other], key=lambda x: x.start)
        
        new_start = min(self.start, other.start)
        new_end = max(self.end, other.end)
        left_open = self.start == new_start and self.left_open and other.start == new_start and other.left_open
        right_open = self.end == new_end and self.right_open and other.end == new_end and other.right_open
        
        return [Interval(new_start, new_end, left_open, right_open)]
    
    def adjacent_to(self, other: Interval) -> bool:
        """
        Check if this interval is adjacent to another (touching but not overlapping).
        
        Args:
            other: The interval to check.
            
        Returns:
            True if intervals touch at exactly one point.
        """
        if self.left_open or other.right_open:
            return math.isclose(self.start, other.end)
        if self.right_open or other.left_open:
            return math.isclose(self.end, other.start)
        return math.isclose(self.end, other.start) or math.isclose(self.start, other.end)
    
    def difference(self, other: Interval) -> List[Interval]:
        """
        Compute the set difference: self - other.
        
        Args:
            other: The interval to subtract.
            
        Returns:
            List of intervals representing the difference.
        """
        if not self.overlaps(other):
            return [self]
        
        result: List[Interval] = []
        
        if self.start < other.start:
            left_open = self.left_open
            right_open = other.left_open if math.isclose(self.start, other.start) else False
            result.append(Interval(self.start, other.start, left_open, right_open))
        
        if self.end > other.end:
            left_open = other.right_open if math.isclose(self.end, other.end) else False
            right_open = self.right_open
            result.append(Interval(other.end, self.end, left_open, right_open))
        
        return result
    
    def split(self, num_parts: int) -> List[Interval]:
        """
        Split the interval into equal sub-intervals.
        
        Args:
            num_parts: Number of sub-intervals to create.
            
        Returns:
            List of evenly distributed sub-intervals.
            
        Raises:
            ValueError: If num_parts is less than 1.
        """
        if num_parts < 1:
            raise ValueError(f"num_parts must be >= 1, got {num_parts}")
        
        if num_parts == 1:
            return [self]
        
        step = self.length / num_parts
        intervals = []
        
        for i in range(num_parts):
            sub_start = self.start + i * step
            sub_end = self.start + (i + 1) * step
            left_open = i == 0 and self.left_open
            right_open = i == num_parts - 1 and self.right_open
            intervals.append(Interval(sub_start, sub_end, left_open, right_open))
        
        return intervals
    
    def expand(self, amount: float) -> Interval:
        """
        Expand the interval symmetrically by adding amount to both ends.
        
        Args:
            amount: Amount to expand on each side.
            
        Returns:
            Expanded interval.
        """
        return Interval(
            self.start - amount,
            self.end + amount,
            self.left_open,
            self.right_open
        )
    
    def shrink(self, amount: float) -> Optional[Interval]:
        """
        Shrink the interval symmetrically by subtracting amount from both ends.
        
        Args:
            amount: Amount to shrink from each side.
            
        Returns:
            Shrunk interval, or None if resulting interval is empty.
        """
        new_interval = self.expand(-amount)
        if new_interval.length <= 0:
            return None
        return new_interval
    
    def random_point(self) -> float:
        """
        Generate a random point uniformly within the interval.
        
        Returns:
            A random float within the interval boundaries.
        """
        return random.uniform(self.start, self.end)
    
    def iterate(self, step: float) -> Generator[float, None, None]:
        """
        Iterate over points in the interval with a given step size.
        
        Args:
            step: Distance between consecutive points.
            
        Yields:
            Points within the interval.
        """
        if step <= 0:
            raise ValueError(f"step must be positive, got {step}")
        
        current = self.start
        while current < self.end:
            yield current
            current += step
        
        if current >= self.end and (not self.right_open or math.isclose(current, self.end)):
            if current > self.end:
                yield self.end
            else:
                yield self.end


@dataclass
class IntervalSet:
    """
    A collection of intervals with efficient operations.
    
    The intervals are kept in a normalized form with no overlaps
    (except for touching intervals which may be merged).
    
    Example:
        >>> interval_set = IntervalSet([Interval(0, 5), Interval(3, 10)])
        >>> interval_set.merge_overlaps()
        >>> len(interval_set.intervals)
        1
    """
    intervals: List[Interval] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        self._normalize()
    
    def _normalize(self) -> None:
        """Sort and merge overlapping intervals."""
        if not self.intervals:
            return
        
        self.intervals.sort(key=lambda x: (x.start, x.end))
        merged: List[Interval] = [self.intervals[0]]
        
        for current in self.intervals[1:]:
            last = merged[-1]
            combined = last.union(current)
            if len(combined) == 1:
                merged[-1] = combined[0]
            else:
                merged.append(current)
        
        self.intervals = merged
    
    def add(self, interval: Interval) -> None:
        """
        Add an interval to the set.
        
        Args:
            interval: The interval to add.
        """
        self.intervals.append(interval)
        self._normalize()
    
    def remove(self, interval: Interval) -> None:
        """
        Remove an interval from the set.
        
        Args:
            interval: The interval to remove.
        """
        for i, existing in enumerate(self.intervals):
            if existing == interval:
                self.intervals.pop(i)
                return
    
    def contains(self, value: float) -> bool:
        """
        Check if a value is contained in any interval in the set.
        
        Args:
            value: The value to check.
            
        Returns:
            True if value is within any interval.
        """
        return any(interval.contains(value) for interval in self.intervals)
    
    def total_length(self) -> float:
        """
        Calculate the total covered length of all intervals.
        
        Returns:
            Sum of lengths of all intervals.
        """
        return sum(interval.length for interval in self.intervals)


def generate_fibonacci_intervals(n: int, start: float = 0.0) -> List[Interval]:
    """
    Generate intervals with lengths following the Fibonacci sequence.
    
    Args:
        n: Number of intervals to generate.
        start: Starting value for the Fibonacci sequence.
        
    Returns:
        List of intervals with Fibonacci-distributed lengths.
    """
    if n < 1:
        return []
    
    fib = [start, start]
    for i in range(2, n):
        fib.append(fib[-1] + fib[-2])
    
    intervals = []
    current_pos = 0.0
    
    for i in range(n):
        length = fib[i] if i > 0 else start
        intervals.append(Interval(current_pos, current_pos + length))
        current_pos += length
    
    return intervals


def discretize_interval(
    start: float,
    end: float,
    num_points: int,
    mode: str = "linear"
) -> List[float]:
    """
    Discretize a continuous interval into discrete points.
    
    Args:
        start: Start of the interval.
        end: End of the interval.
        num_points: Number of discrete points.
        mode: Distribution mode - "linear", "log", or "exp".
        
    Returns:
        List of discretized points.
        
    Raises:
        ValueError: If num_points < 2 or mode is invalid.
    """
    if num_points < 2:
        raise ValueError(f"num_points must be >= 2, got {num_points}")
    
    if mode == "linear":
        step = (end - start) / (num_points - 1)
        return [start + i * step for i in range(num_points)]
    
    elif mode == "log":
        if start <= 0:
            raise ValueError("start must be positive for log mode")
        log_start = math.log(start)
        log_end = math.log(end)
        step = (log_end - log_start) / (num_points - 1)
        return [math.exp(log_start + i * step) for i in range(num_points)]
    
    elif mode == "exp":
        step = (end - start) / (num_points - 1)
        return [math.exp(start + i * step) for i in range(num_points)]
    
    else:
        raise ValueError(f"Unknown mode: {mode}. Expected 'linear', 'log', or 'exp'.")


def clamp(value: float, min_val: float, max_val: float) -> float:
    """
    Clamp a value to a specified range.
    
    Args:
        value: The value to clamp.
        min_val: Minimum allowed value.
        max_val: Maximum allowed value.
        
    Returns:
        The clamped value.
    """
    return max(min_val, min(max_val, value))


def map_value(
    value: float,
    in_start: float,
    in_end: float,
    out_start: float,
    out_end: float,
    clamp_result: bool = True
) -> float:
    """
    Map a value from one range to another.
    
    Args:
        value: The value to map.
        in_start: Start of the input range.
        in_end: End of the input range.
        out_start: Start of the output range.
        out_end: End of the output range.
        clamp_result: Whether to clamp the result to output range.
        
    Returns:
        The mapped value.
    """
    if math.isclose(in_start, in_end):
        raise ValueError(f"in_start and in_end must be different, got {in_start}")
    
    ratio = (value - in_start) / (in_end - in_start)
    result = out_start + ratio * (out_end - out_start)
    
    if clamp_result:
        if out_start <= out_end:
            return clamp(result, out_start, out_end)
        else:
            return clamp(result, out_end, out_start)
    
    return result
