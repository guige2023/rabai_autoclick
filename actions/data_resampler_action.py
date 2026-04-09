"""
Data Resampler Action Module.

Resample and interpolate data series.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


class ResampleMethod(Enum):
    """Resampling methods."""
    NEAREST = "nearest"
    INTERPOLATE = "interpolate"
    AGGREGATE = "aggregate"
    DECIMATE = "decimate"


class DataResamplerAction:
    """
    Resample data series.

    Supports upsampling, downsampling, and interpolation.
    """

    def __init__(self) -> None:
        self._interpolators: Dict[str, Callable[[float, List[Tuple[float, float]]], float]] = {}

    def downsample(
        self,
        data: List[Tuple[float, Any]],
        target_size: int,
        method: str = "mean",
    ) -> List[Tuple[float, Any]]:
        """
        Downsample data to target size.

        Args:
            data: List of (timestamp, value) tuples
            target_size: Target number of points
            method: Aggregation method (mean, min, max, first, last)

        Returns:
            Downsampled data
        """
        if len(data) <= target_size:
            return data

        chunk_size = len(data) / target_size
        result = []

        for i in range(target_size):
            start = int(i * chunk_size)
            end = int((i + 1) * chunk_size)
            chunk = data[start:end]

            if not chunk:
                continue

            timestamp = chunk[0][0]

            if method == "mean":
                values = [c[1] for c in chunk if c[1] is not None]
                value = sum(values) / len(values) if values else None
            elif method == "min":
                value = min((c[1] for c in chunk if c[1] is not None), default=None)
            elif method == "max":
                value = max((c[1] for c in chunk if c[1] is not None), default=None)
            elif method == "first":
                value = chunk[0][1]
            elif method == "last":
                value = chunk[-1][1]
            else:
                value = chunk[len(chunk) // 2][1]

            result.append((timestamp, value))

        return result

    def upsample(
        self,
        data: List[Tuple[float, Any]],
        target_timestamps: List[float],
        method: str = "linear",
    ) -> List[Tuple[float, Any]]:
        """
        Upsample data to target timestamps.

        Args:
            data: List of (timestamp, value) tuples
            target_timestamps: Target timestamps
            method: Interpolation method

        Returns:
            Upsampled data
        """
        if not data:
            return [(t, None) for t in target_timestamps]

        if method == "nearest":
            return self._upsample_nearest(data, target_timestamps)
        elif method == "linear":
            return self._upsample_linear(data, target_timestamps)
        elif method == "forward":
            return self._upsample_forward(data, target_timestamps)

        return [(t, None) for t in target_timestamps]

    def _upsample_nearest(
        self,
        data: List[Tuple[float, Any]],
        target_timestamps: List[float],
    ) -> List[Tuple[float, Any]]:
        """Nearest neighbor upsampling."""
        sorted_data = sorted(data, key=lambda x: x[0])
        result = []

        for target in target_timestamps:
            nearest = min(sorted_data, key=lambda x: abs(x[0] - target))
            result.append((target, nearest[1]))

        return result

    def _upsample_linear(
        self,
        data: List[Tuple[float, Any]],
        target_timestamps: List[float],
    ) -> List[Tuple[float, Any]]:
        """Linear interpolation upsampling."""
        sorted_data = sorted(data, key=lambda x: x[0])
        result = []

        for target in target_timestamps:
            value = self._linear_interpolate(target, sorted_data)
            result.append((target, value))

        return result

    def _upsample_forward(
        self,
        data: List[Tuple[float, Any]],
        target_timestamps: List[float],
    ) -> List[Tuple[float, Any]]:
        """Forward fill upsampling."""
        sorted_data = sorted(data, key=lambda x: x[0])
        result = []
        last_value = None

        for target in target_timestamps:
            for timestamp, value in sorted_data:
                if timestamp <= target:
                    last_value = value
                else:
                    break

            result.append((target, last_value))

        return result

    def _linear_interpolate(
        self,
        x: float,
        points: List[Tuple[float, Any]],
    ) -> Optional[float]:
        """Linear interpolation between points."""
        if not points:
            return None

        if x <= points[0][0]:
            return points[0][1]

        if x >= points[-1][0]:
            return points[-1][1]

        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]

            if x1 <= x <= x2:
                if y1 is None or y2 is None:
                    return y1 or y2
                t = (x - x1) / (x2 - x1)
                return y1 + t * (y2 - y1)

        return None

    def resample_to_interval(
        self,
        data: List[Tuple[float, Any]],
        interval_seconds: float,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        aggregation: str = "mean",
    ) -> List[Tuple[float, Any]]:
        """
        Resample to fixed interval.

        Args:
            data: List of (timestamp, value) tuples
            interval_seconds: Interval in seconds
            start_time: Start time for bins
            end_time: End time for bins
            aggregation: How to aggregate values

        Returns:
            Resampled data
        """
        if not data:
            return []

        sorted_data = sorted(data, key=lambda x: x[0])

        start = start_time or sorted_data[0][0]
        end = end_time or sorted_data[-1][0]

        bins: Dict[int, List[Any]] = {}

        for timestamp, value in sorted_data:
            if timestamp < start or timestamp > end:
                continue

            bin_key = int((timestamp - start) / interval_seconds)
            if bin_key not in bins:
                bins[bin_key] = []
            bins[bin_key].append(value)

        result = []
        for bin_key in sorted(bins.keys()):
            timestamp = start + bin_key * interval_seconds
            values = bins[bin_key]

            if not values:
                continue

            if aggregation == "mean":
                agg_value = sum(v for v in values if v is not None) / len(values)
            elif aggregation == "sum":
                agg_value = sum(v for v in values if v is not None)
            elif aggregation == "min":
                agg_value = min(v for v in values if v is not None)
            elif aggregation == "max":
                agg_value = max(v for v in values if v is not None)
            elif aggregation == "count":
                agg_value = len(values)
            else:
                agg_value = values[0]

            result.append((timestamp, agg_value))

        return result
