"""
Histogram metric action for tracking distributions of values.

This module provides actions for recording and analyzing histogram metrics,
supporting percentile calculations, bucket configurations, and aggregation.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import math
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class BucketType(Enum):
    """Types of histogram bucket configurations."""
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    CUSTOM = "custom"
    PRECISION = "precision"


@dataclass
class Bucket:
    """A single histogram bucket."""
    lower: float
    upper: float
    count: int = 0

    @property
    def range(self) -> Tuple[float, float]:
        """Get the bucket range as a tuple."""
        return (self.lower, self.upper)

    def contains(self, value: float) -> bool:
        """Check if a value falls within this bucket."""
        return self.lower <= value < self.upper

    def __repr__(self) -> str:
        return f"Bucket({self.lower}, {self.upper}, count={self.count})"


@dataclass
class BucketConfig:
    """Configuration for histogram buckets."""
    bucket_type: BucketType = BucketType.EXPONENTIAL
    num_buckets: int = 20
    scale: float = 1.5
    base: float = 1.0
    offset: float = 0.0
    custom_bounds: Optional[List[float]] = None

    def __post_init__(self):
        if self.bucket_type == BucketType.LINEAR:
            if self.num_buckets < 2:
                raise ValueError("num_buckets must be at least 2 for linear buckets")
        elif self.bucket_type == BucketType.EXPONENTIAL:
            if self.num_buckets < 2:
                raise ValueError("num_buckets must be at least 2 for exponential buckets")
            if self.scale <= 1.0:
                raise ValueError("scale must be greater than 1.0 for exponential buckets")
            if self.base <= 0:
                raise ValueError("base must be positive for exponential buckets")
        elif self.bucket_type == BucketType.CUSTOM:
            if not self.custom_bounds or len(self.custom_bounds) < 2:
                raise ValueError("custom_bounds must have at least 2 values")


@dataclass
class HistogramSnapshot:
    """A point-in-time snapshot of a histogram."""
    name: str
    count: int
    sum: float
    min: float
    max: float
    mean: float
    variance: float
    std_dev: float
    percentiles: Dict[str, float]
    buckets: List[Dict[str, Any]]
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "name": self.name,
            "count": self.count,
            "sum": self.sum,
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "variance": self.variance,
            "std_dev": self.std_dev,
            "percentiles": self.percentiles,
            "buckets": self.buckets,
            "timestamp": self.timestamp.isoformat(),
        }


class Histogram:
    """
    Histogram metric for tracking distributions of values.

    Supports linear and exponential bucket configurations, percentile
    calculations, and concurrent access.
    """

    DEFAULT_PERCENTILES = [0.5, 0.75, 0.9, 0.95, 0.99, 0.999]

    def __init__(
        self,
        name: str,
        bucket_config: Optional[BucketConfig] = None,
        max_value: Optional[float] = None,
        min_value: Optional[float] = None,
        percentiles: Optional[List[float]] = None,
    ):
        """
        Initialize a histogram.

        Args:
            name: Name of the histogram metric.
            bucket_config: Configuration for bucket creation.
            max_value: Optional maximum value cap.
            min_value: Optional minimum value floor.
            percentiles: List of percentiles to calculate (0.0 to 1.0).
        """
        self.name = name
        self.bucket_config = bucket_config or BucketConfig()
        self.max_value = max_value
        self.min_value = min_value
        self.percentiles = percentiles or self.DEFAULT_PERCENTILES

        self._buckets: List[Bucket] = []
        self._counts: List[int] = []
        self._values: List[float] = []
        self._sum = 0.0
        self._count = 0
        self._min = float('inf')
        self._max = float('-inf')
        self._lock = threading.RLock()

        self._initialize_buckets()

    def _initialize_buckets(self) -> None:
        """Initialize histogram buckets based on configuration."""
        config = self.bucket_config

        if config.bucket_type == BucketType.LINEAR:
            self._init_linear_buckets()
        elif config.bucket_type == BucketType.EXPONENTIAL:
            self._init_exponential_buckets()
        elif config.bucket_type == BucketType.CUSTOM:
            self._init_custom_buckets()
        elif config.bucket_type == BucketType.PRECISION:
            self._init_precision_buckets()
        else:
            self._init_exponential_buckets()

    def _init_linear_buckets(self) -> None:
        """Initialize linear-spaced buckets."""
        config = self.bucket_config
        num = config.num_buckets

        if self.max_value is not None and self.min_value is not None:
            lower = self.min_value
            upper = self.max_value
        else:
            lower = config.offset
            upper = lower + config.scale * num

        step = (upper - lower) / num
        self._buckets = [
            Bucket(lower + i * step, lower + (i + 1) * step)
            for i in range(num)
        ]

    def _init_exponential_buckets(self) -> None:
        """Initialize exponentially-spaced buckets."""
        config = self.bucket_config
        num = config.num_buckets
        base = config.base
        scale = config.scale
        offset = config.offset

        lower = offset + base
        self._buckets = [Bucket(0, lower)]

        current = lower
        for _ in range(num - 1):
            next_upper = current * scale
            self._buckets.append(Bucket(current, next_upper))
            current = next_upper

        if self.max_value is not None:
            self._buckets.append(Bucket(current, self.max_value))

    def _init_custom_buckets(self) -> None:
        """Initialize buckets from custom bounds."""
        bounds = self.bucket_config.custom_bounds or []
        self._buckets = [
            Bucket(bounds[i], bounds[i + 1])
            for i in range(len(bounds) - 1)
        ]

    def _init_precision_buckets(self) -> None:
        """Initialize buckets based on decimal precision."""
        scale = self.bucket_config.scale
        num = self.bucket_config.num_buckets

        lower = 0.0
        upper = scale
        step = scale / num

        while upper < (self.max_value or float('inf')):
            self._buckets.append(Bucket(lower, upper))
            lower = upper
            upper *= 2

    def record(self, value: float) -> None:
        """
        Record a single value in the histogram.

        Args:
            value: The value to record.
        """
        with self._lock:
            if self.max_value is not None and value > self.max_value:
                value = self.max_value
            if self.min_value is not None and value < self.min_value:
                value = self.min_value

            self._values.append(value)
            self._sum += value
            self._count += 1

            if value < self._min:
                self._min = value
            if value > self._max:
                self._max = value

            for bucket in self._buckets:
                if bucket.contains(value):
                    bucket.count += 1
                    break

    def record_many(self, values: List[float]) -> None:
        """
        Record multiple values in the histogram.

        Args:
            values: List of values to record.
        """
        for value in values:
            self.record(value)

    def get_snapshot(self) -> HistogramSnapshot:
        """
        Get a point-in-time snapshot of the histogram.

        Returns:
            HistogramSnapshot with current statistics.
        """
        with self._lock:
            if self._count == 0:
                return HistogramSnapshot(
                    name=self.name,
                    count=0,
                    sum=0.0,
                    min=0.0,
                    max=0.0,
                    mean=0.0,
                    variance=0.0,
                    std_dev=0.0,
                    percentiles={},
                    buckets=[],
                    timestamp=datetime.now(),
                )

            mean = self._sum / self._count

            variance = 0.0
            if self._count > 1:
                variance = sum((v - mean) ** 2 for v in self._values) / self._count
            std_dev = math.sqrt(variance)

            percentiles = self._calculate_percentiles(mean)

            buckets = [
                {
                    "lower": b.lower,
                    "upper": b.upper,
                    "count": b.count,
                }
                for b in self._buckets
            ]

            return HistogramSnapshot(
                name=self.name,
                count=self._count,
                sum=self._sum,
                min=self._min,
                max=self._max,
                mean=mean,
                variance=variance,
                std_dev=std_dev,
                percentiles=percentiles,
                buckets=buckets,
                timestamp=datetime.now(),
            )

    def _calculate_percentiles(self, mean: float) -> Dict[str, float]:
        """Calculate percentile values."""
        if not self._values:
            return {}

        sorted_values = sorted(self._values)
        n = len(sorted_values)
        result = {}

        for p in self.percentiles:
            if p <= 0:
                result[f"p{int(p * 100)}"] = sorted_values[0]
            elif p >= 1:
                result[f"p{int(p * 100)}"] = sorted_values[-1]
            else:
                idx = int(p * n)
                idx = min(idx, n - 1)
                result[f"p{int(p * 100)}"] = sorted_values[idx]

        return result

    def reset(self) -> None:
        """Reset the histogram to empty state."""
        with self._lock:
            self._buckets = []
            self._counts = []
            self._values = []
            self._sum = 0.0
            self._count = 0
            self._min = float('inf')
            self._max = float('-inf')
            self._initialize_buckets()

    def merge(self, other: Histogram) -> None:
        """
        Merge another histogram into this one.

        Args:
            other: Another histogram to merge.
        """
        with self._lock:
            for i, bucket in enumerate(other._buckets):
                if i < len(self._buckets):
                    self._buckets[i].count += bucket.count

            self._values.extend(other._values)
            self._sum += other._sum
            self._count += other._count
            self._min = min(self._min, other._min)
            self._max = max(self._max, other._max)


class HistogramRegistry:
    """Thread-safe registry of histogram metrics."""

    def __init__(self):
        """Initialize the histogram registry."""
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.RLock()

    def get_or_create(
        self,
        name: str,
        bucket_config: Optional[BucketConfig] = None,
        **kwargs,
    ) -> Histogram:
        """
        Get an existing histogram or create a new one.

        Args:
            name: Name of the histogram.
            bucket_config: Optional bucket configuration.
            **kwargs: Additional arguments for histogram creation.

        Returns:
            The histogram metric.
        """
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, bucket_config, **kwargs)
            return self._histograms[name]

    def get(self, name: str) -> Optional[Histogram]:
        """Get a histogram by name."""
        with self._lock:
            return self._histograms.get(name)

    def list_histograms(self) -> List[str]:
        """List all histogram names."""
        with self._lock:
            return list(self._histograms.keys())

    def reset_all(self) -> None:
        """Reset all histograms in the registry."""
        with self._lock:
            for h in self._histograms.values():
                h.reset()

    def get_snapshots(self) -> List[HistogramSnapshot]:
        """Get snapshots of all histograms."""
        with self._lock:
            return [h.get_snapshot() for h in self._histograms.values()]


# Global default registry
_default_registry = HistogramRegistry()


def histogram_action(
    values: List[float],
    name: str,
    bucket_type: str = "exponential",
    num_buckets: int = 20,
    scale: float = 1.5,
    percentiles: Optional[List[float]] = None,
) -> Dict[str, Any]:
    """
    Action function to record values and get histogram statistics.

    Args:
        values: List of values to record.
        name: Name of the histogram.
        bucket_type: Type of bucket configuration (linear, exponential, custom).
        num_buckets: Number of buckets to use.
        scale: Scale factor for bucket spacing.
        percentiles: List of percentiles to calculate.

    Returns:
        Dictionary with histogram statistics and snapshot.
    """
    bucket_type_map = {
        "linear": BucketType.LINEAR,
        "exponential": BucketType.EXPONENTIAL,
        "custom": BucketType.CUSTOM,
        "precision": BucketType.PRECISION,
    }

    if bucket_type.lower() not in bucket_type_map:
        raise ValueError(f"Unknown bucket type: {bucket_type}")

    config = BucketConfig(
        bucket_type=bucket_type_map[bucket_type.lower()],
        num_buckets=num_buckets,
        scale=scale,
    )

    registry = HistogramRegistry()
    histogram = registry.get_or_create(name, config)

    histogram.record_many(values)
    snapshot = histogram.get_snapshot()

    return snapshot.to_dict()


def get_histogram_snapshot(name: str) -> Optional[Dict[str, Any]]:
    """
    Get a snapshot of a named histogram.

    Args:
        name: Name of the histogram.

    Returns:
        Dictionary with histogram statistics or None if not found.
    """
    registry = HistogramRegistry()
    histogram = registry.get(name)
    if histogram:
        return histogram.get_snapshot().to_dict()
    return None


def reset_histogram(name: str) -> bool:
    """
    Reset a named histogram.

    Args:
        name: Name of the histogram.

    Returns:
        True if histogram was reset, False if not found.
    """
    registry = HistogramRegistry()
    histogram = registry.get(name)
    if histogram:
        histogram.reset()
        return True
    return False
