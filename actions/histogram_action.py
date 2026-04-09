"""
Histogram analysis module for data distribution visualization and analysis.

Provides binning strategies, histogram construction, and distribution comparison
for data exploration and statistical analysis workflows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class Bin:
    """Represents a single histogram bin."""
    start: float
    end: float
    count: int
    frequency: float
    density: float


@dataclass
class HistogramResult:
    """Complete histogram analysis result."""
    bins: list[Bin]
    total_count: int
    bin_width: float
    bin_count: int
    min_value: float
    max_value: float
    range_size: float
    most_frequent_bin: Bin
    least_frequent_bin: Bin


class HistogramAnalyzer:
    """
    Constructs and analyzes histograms with multiple binning strategies.
    
    Example:
        analyzer = HistogramAnalyzer()
        data = [random.gauss(0, 1) for _ in range(1000)]
        result = analyzer.compute_histogram(data, bin_count=20)
    """

    def __init__(self) -> None:
        """Initialize histogram analyzer."""
        self._cache: dict[str, object] = {}

    def compute_histogram(
        self,
        data: list[float],
        bin_count: Optional[int] = None,
        bin_width: Optional[float] = None,
        strategy: str = "auto"
    ) -> HistogramResult:
        """
        Compute histogram with specified or auto-determined binning.
        
        Args:
            data: List of numeric values.
            bin_count: Number of bins (mutually exclusive with bin_width).
            bin_width: Width of each bin (mutually exclusive with bin_count).
            strategy: Binning strategy when bin_count is None.
                      Options: 'auto', 'sqrt', 'sturges', 'freedman_diaconis', 'rice'.
            
        Returns:
            HistogramResult with all bins and statistics.
            
        Raises:
            ValueError: If both bin_count and bin_width are specified,
                        or if data is empty.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        if bin_count is not None and bin_width is not None:
            raise ValueError("Specify either bin_count or bin_width, not both")

        min_val = min(data)
        max_val = max(data)
        data_range = max_val - min_val

        if data_range == 0:
            # All values are identical
            single_bin = Bin(
                start=min_val,
                end=max_val if max_val > min_val else min_val + 1.0,
                count=len(data),
                frequency=1.0,
                density=1.0 / len(data)
            )
            return HistogramResult(
                bins=[single_bin],
                total_count=len(data),
                bin_width=1.0,
                bin_count=1,
                min_value=min_val,
                max_value=max_val,
                range_size=data_range,
                most_frequent_bin=single_bin,
                least_frequent_bin=single_bin
            )

        # Determine bin count
        if bin_count is None:
            bin_count = self._auto_bin_count(data, strategy)

        if bin_width is None:
            bin_width = data_range / bin_count

        # Build bins
        bins: list[Bin] = []
        for i in range(bin_count):
            start = min_val + i * bin_width
            end = start + bin_width
            
            # Count values in this bin (include left edge, exclude right except last)
            if i < bin_count - 1:
                count = sum(1 for x in data if start <= x < end)
            else:
                count = sum(1 for x in data if start <= x <= end)

            frequency = count / len(data)
            density = frequency / bin_width

            bins.append(Bin(
                start=round(start, 6),
                end=round(end, 6),
                count=count,
                frequency=round(frequency, 6),
                density=round(density, 6)
            ))

        # Find most/least frequent bins
        most_freq = max(bins, key=lambda b: b.count)
        least_freq = min(bins, key=lambda b: b.count)

        return HistogramResult(
            bins=bins,
            total_count=len(data),
            bin_width=round(bin_width, 6),
            bin_count=bin_count,
            min_value=min_val,
            max_value=max_val,
            range_size=round(data_range, 6),
            most_frequent_bin=most_freq,
            least_frequent_bin=least_freq
        )

    def _auto_bin_count(self, data: list[float], strategy: str) -> int:
        """Determine optimal bin count using various strategies."""
        n = len(data)

        strategies: dict[str, Callable[[list[float]], int]] = {
            'auto': lambda d: int(math.sqrt(len(d))) or 10,
            'sqrt': lambda d: int(math.sqrt(len(d))) or 10,
            'sturges': lambda d: int(math.ceil(math.log2(len(d)))) + 1 if len(d) > 0 else 10,
            'rice': lambda d: int(2 * (len(d) ** (1/3))) or 10,
            'freedman_diaconis': self._freedman_diaconis,
        }

        func = strategies.get(strategy, strategies['auto'])
        return max(1, func(data))

    def _freedman_diaconis(self, data: list[float]) -> int:
        """Freedman-Diaconis rule for bin count selection."""
        if len(data) < 4:
            return 10

        sorted_data = sorted(data)
        n = len(data)

        # Interquartile range
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        iqr = sorted_data[q3_idx] - sorted_data[q1_idx]

        if iqr == 0:
            return int(math.sqrt(len(data))) or 10

        # Bin width = 2 * IQR / n^(1/3)
        h = 2 * iqr / (len(data) ** (1/3))
        data_range = max(data) - min(data)

        if h <= 0:
            return 10

        bin_count = max(1, int(math.ceil(data_range / h)))
        return min(bin_count, 1000)  # Cap at 1000 bins

    def compare_histograms(
        self,
        hist1: HistogramResult,
        hist2: HistogramResult
    ) -> dict[str, float]:
        """
        Compare two histograms using various similarity metrics.
        
        Args:
            hist1: First histogram result.
            hist2: Second histogram result.
            
        Returns:
            Dictionary with comparison metrics.
        """
        # Align bins by finding common range
        common_min = max(hist1.min_value, hist2.min_value)
        common_max = min(hist1.max_value, hist2.max_value)

        if common_min >= common_max:
            return {'overlap_ratio': 0.0, 'chi_square': float('inf'), 'earth_movers_distance': float('inf')}

        # Compute overlap ratio
        total_overlap = 0.0
        for b1 in hist1.bins:
            for b2 in hist2.bins:
                overlap_start = max(b1.start, b2.start)
                overlap_end = min(b1.end, b2.end)
                if overlap_end > overlap_start:
                    overlap_width = overlap_end - overlap_start
                    # Approximate overlap as minimum of densities * overlap width
                    min_density = min(b1.density, b2.density)
                    total_overlap += min_density * overlap_width

        # Chi-square statistic
        chi_square = 0.0
        for b1, b2 in zip(hist1.bins, hist2.bins):
            if b1.count + b2.count > 0:
                chi_square += ((b1.count - b2.count) ** 2) / (b1.count + b2.count)

        return {
            'overlap_ratio': round(min(1.0, total_overlap), 6),
            'chi_square': round(chi_square, 6),
            'total_count_diff': abs(hist1.total_count - hist2.total_count),
        }

    def histogram_to_string(self, hist: HistogramResult, width: int = 60) -> str:
        """
        Render histogram as ASCII art.
        
        Args:
            hist: Histogram to render.
            width: Maximum width of the rendering in characters.
            
        Returns:
            ASCII string representation of the histogram.
        """
        if not hist.bins:
            return "Empty histogram"

        max_count = max(b.count for b in hist.bins)
        lines = [f"Histogram: {hist.bin_count} bins, {hist.total_count} total samples",
                 f"Range: [{hist.min_value}, {hist.max_value}], Width: {hist.bin_width}",
                 "-" * width]

        for bin in hist.bins:
            bar_length = int((bin.count / max_count) * (width - 20)) if max_count > 0 else 0
            bar = '█' * bar_length
            label = f"[{bin.start:>8.3f}, {bin.end:>8.3f}]"
            lines.append(f"{label} {bar} ({bin.count})")

        return "\n".join(lines)
