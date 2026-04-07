"""Math utilities for RabAI AutoClick.

Provides:
- Statistical functions
- Number utilities
- Math helpers
"""

from __future__ import annotations

import math
from typing import List


def mean(values: List[float]) -> float:
    """Calculate mean."""
    if not values:
        return 0.0
    return sum(values) / len(values)


def median(values: List[float]) -> float:
    """Calculate median."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]


def percentile(values: List[float], p: float) -> float:
    """Calculate percentile."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    index = (len(sorted_vals) - 1) * p / 100
    floor = int(math.floor(index))
    ceil = int(math.ceil(index))
    if floor == ceil:
        return sorted_vals[floor]
    return sorted_vals[floor] * (ceil - index) + sorted_vals[ceil] * (index - floor)


def variance(values: List[float]) -> float:
    """Calculate variance."""
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return sum((x - m) ** 2 for x in values) / (len(values) - 1)


def std_dev(values: List[float]) -> float:
    """Calculate standard deviation."""
    return math.sqrt(variance(values))


def round_to(value: float, precision: int) -> float:
    """Round to specified precision."""
    multiplier = 10 ** precision
    return math.round(value * multiplier) / multiplier


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value to range."""
    return max(min_val, min(max_val, value))


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation."""
    return a + (b - a) * t
