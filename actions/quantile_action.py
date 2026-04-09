"""
Quantile analysis module for data distribution exploration.

Provides quantile computation, box plot statistics, and percentile
ranking for statistical analysis workflows.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class QuantileSet:
    """Complete quantile set for a dataset."""
    min_val: float
    q01: float   # 1st percentile
    q05: float   # 5th percentile
    q10: float   # 10th percentile
    q25: float   # First quartile (Q1)
    q50: float   # Median (Q2)
    q75: float   # Third quartile (Q3)
    q90: float   # 90th percentile
    q95: float   # 95th percentile
    q99: float   # 99th percentile
    max_val: float
    iqr: float   # Interquartile range (Q3 - Q1)
    range_span: float
    count: int


@dataclass
class BoxPlotStats:
    """Box plot statistics."""
    minimum: float
    q1: float
    median: float
    q3: float
    maximum: float
    iqr: float
    lower_whisker: float
    upper_whisker: float
    outliers_low: list[float]
    outliers_high: list[float]


class QuantileAnalyzer:
    """
    Computes quantiles and related distribution statistics.
    
    Example:
        analyzer = QuantileAnalyzer()
        data = list(range(1, 101))  # 1 to 100
        qs = analyzer.compute_quantiles(data)
    """

    def __init__(self) -> None:
        """Initialize quantile analyzer."""
        self._cache: dict[str, QuantileSet] = {}

    def _linear_interpolate(
        self,
        sorted_data: list[float],
        index: float
    ) -> float:
        """
        Linear interpolation between data points for precise quantile calculation.
        
        Args:
            sorted_data: Pre-sorted data values.
            index: Floating-point index position.
            
        Returns:
            Interpolated value at the given index.
        """
        lower = int(math.floor(index))
        upper = int(math.ceil(index))
        
        if lower == upper:
            return sorted_data[min(lower, len(sorted_data) - 1)]
        
        weight = index - lower
        lower_val = sorted_data[min(lower, len(sorted_data) - 1)]
        upper_val = sorted_data[min(upper, len(sorted_data) - 1)]
        
        return lower_val + weight * (upper_val - lower_val)

    def compute_quantile(
        self,
        data: list[float],
        percentile: float
    ) -> float:
        """
        Compute a single quantile value.
        
        Args:
            data: List of numeric values.
            percentile: Percentile to compute (0-100).
            
        Returns:
            Quantile value at the specified percentile.
            
        Raises:
            ValueError: If data is empty or percentile is out of range.
        """
        if not data:
            raise ValueError("Data cannot be empty")
        if not 0 <= percentile <= 100:
            raise ValueError("Percentile must be between 0 and 100")

        sorted_data = sorted(data)
        n = len(sorted_data)
        
        # Use linear interpolation method
        index = (percentile / 100) * (n - 1)
        return self._linear_interpolate(sorted_data, index)

    def compute_quantiles(
        self,
        data: list[float],
        percentiles: Optional[list[float]] = None
    ) -> QuantileSet:
        """
        Compute a complete set of common quantiles.
        
        Args:
            data: List of numeric values.
            percentiles: Custom percentiles to compute. Defaults to standard set.
            
        Returns:
            QuantileSet with all computed values.
        """
        if not data:
            raise ValueError("Data cannot be empty")

        if percentiles is None:
            percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]

        quantile_values = {p: self.compute_quantile(data, p) for p in percentiles}
        sorted_data = sorted(data)

        q25 = quantile_values.get(25, self.compute_quantile(data, 25))
        q75 = quantile_values.get(75, self.compute_quantile(data, 75))
        iqr = q75 - q25

        return QuantileSet(
            min_val=min(data),
            q01=quantile_values.get(1, self.compute_quantile(data, 1)),
            q05=quantile_values.get(5, self.compute_quantile(data, 5)),
            q10=quantile_values.get(10, self.compute_quantile(data, 10)),
            q25=q25,
            q50=quantile_values.get(50, self.compute_quantile(data, 50)),
            q75=q75,
            q90=quantile_values.get(90, self.compute_quantile(data, 90)),
            q95=quantile_values.get(95, self.compute_quantile(data, 95)),
            q99=quantile_values.get(99, self.compute_quantile(data, 99)),
            max_val=max(data),
            iqr=round(iqr, 6),
            range_span=round(max(data) - min(data), 6),
            count=len(data)
        )

    def compute_boxplot_stats(
        self,
        data: list[float],
        whisker_multiplier: float = 1.5
    ) -> BoxPlotStats:
        """
        Compute box plot statistics including whisker boundaries.
        
        Args:
            data: List of numeric values.
            whisker_multiplier: IQR multiplier for whisker calculation (default 1.5).
            
        Returns:
            BoxPlotStats with all box plot components.
        """
        qs = self.compute_quantiles(data)
        q1, q3, median = qs.q25, qs.q75, qs.q50
        iqr = qs.iqr

        # Whisker boundaries
        lower_bound = q1 - whisker_multiplier * iqr
        upper_bound = q3 + whisker_multiplier * iqr

        # Actual whisker positions (closest data points within bounds)
        sorted_data = sorted(data)
        lower_whisker = min(x for x in sorted_data if x >= lower_bound)
        upper_whisker = max(x for x in sorted_data if x <= upper_bound)

        # Classify outliers
        outliers_low = [x for x in data if x < lower_bound]
        outliers_high = [x for x in data if x > upper_bound]

        return BoxPlotStats(
            minimum=qs.min_val,
            q1=q1,
            median=median,
            q3=q3,
            maximum=qs.max_val,
            iqr=iqr,
            lower_whisker=lower_whisker,
            upper_whisker=upper_whisker,
            outliers_low=outliers_low,
            outliers_high=outliers_high
        )

    def rank_values(
        self,
        data: list[float],
        new_values: list[float]
    ) -> list[int]:
        """
        Compute percentile ranks for new values against a reference dataset.
        
        Args:
            data: Reference dataset for ranking.
            new_values: Values to rank against the reference.
            
        Returns:
            List of percentile ranks (0-100) for each new value.
        """
        sorted_data = sorted(data)
        n = len(sorted_data)
        
        ranks = []
        for v in new_values:
            # Count how many values are strictly less than v
            rank = sum(1 for x in sorted_data if x < v)
            # Percentile: position in distribution
            percentile = (rank / n) * 100
            ranks.append(round(percentile, 2))
        
        return ranks

    def compute_quantile_bins(
        self,
        data: list[float],
        bin_count: int = 10
    ) -> list[tuple[float, float, int]]:
        """
        Create quantile-based bins with equal number of observations per bin.
        
        Args:
            data: List of numeric values.
            bin_count: Number of bins to create.
            
        Returns:
            List of (bin_start, bin_end, count) tuples.
        """
        if not data:
            raise ValueError("Data cannot be empty")
        if bin_count <= 0:
            raise ValueError("bin_count must be positive")

        sorted_data = sorted(data)
        n = len(sorted_data)
        bin_size = n / bin_count

        bins = []
        for i in range(bin_count):
            start_idx = int(i * bin_size)
            end_idx = int((i + 1) * bin_size)
            
            # Handle last bin
            if i == bin_count - 1:
                end_idx = n
            
            start_val = sorted_data[min(start_idx, n - 1)]
            end_val = sorted_data[min(end_idx - 1, n - 1)]
            count = end_idx - start_idx
            
            bins.append((round(start_val, 6), round(end_val, 6), count))

        return bins

    def compute_deciles(
        self,
        data: list[float]
    ) -> list[float]:
        """
        Compute decile boundaries (10 equal-frequency groups).
        
        Args:
            data: List of numeric values.
            
        Returns:
            List of 9 decile values (D1 through D9).
        """
        return [self.compute_quantile(data, p * 10) for p in range(1, 10)]

    def compute_percentiles(
        self,
        data: list[float],
        percentile_list: list[float]
    ) -> list[float]:
        """
        Compute multiple percentiles at once efficiently.
        
        Args:
            data: List of numeric values.
            percentile_list: List of percentile values to compute.
            
        Returns:
            List of computed percentile values in the same order.
        """
        return [self.compute_quantile(data, p) for p in percentile_list]

    def compare_quantiles(
        self,
        data1: list[float],
        data2: list[float]
    ) -> dict[str, float]:
        """
        Compare quantile distributions between two datasets.
        
        Args:
            data1: First dataset.
            data2: Second dataset.
            
        Returns:
            Dictionary of differences at each quantile.
        """
        quantiles = [5, 10, 25, 50, 75, 90, 95]
        q1 = {p: self.compute_quantile(data1, p) for p in quantiles}
        q2 = {p: self.compute_quantile(data2, p) for p in quantiles}

        comparison = {}
        for p in quantiles:
            comparison[f'q{p}_diff'] = round(q1[p] - q2[p], 6)
            comparison[f'q{p}_ratio'] = round(q1[p] / q2[p], 6) if q2[p] != 0 else float('inf')

        comparison['median_diff'] = comparison.get('q50_diff', 0)
        comparison['iqr_diff'] = round((q1[75] - q1[25]) - (q2[75] - q2[25]), 6)

        return comparison
