"""
Data Stats Action - Statistical analysis of data.

This module provides statistical analysis capabilities including
descriptive stats, distributions, and correlation analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DescriptiveStats:
    """Descriptive statistics."""
    count: int = 0
    mean: float = 0.0
    median: float = 0.0
    mode: float = 0.0
    std_dev: float = 0.0
    variance: float = 0.0
    min_val: float = 0.0
    max_val: float = 0.0
    q1: float = 0.0
    q3: float = 0.0


class DataStatsAnalyzer:
    """Analyzes statistical properties of data."""
    
    def __init__(self) -> None:
        pass
    
    def compute_stats(self, values: list[float]) -> DescriptiveStats:
        """Compute descriptive statistics."""
        if not values:
            return DescriptiveStats()
        
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        
        mean_val = sum(sorted_vals) / n
        
        median_val = sorted_vals[n // 2] if n % 2 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
        
        variance = sum((x - mean_val) ** 2 for x in sorted_vals) / n
        std_dev = math.sqrt(variance)
        
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        
        return DescriptiveStats(
            count=n,
            mean=mean_val,
            median=median_val,
            mode=sorted_vals[0],
            std_dev=std_dev,
            variance=variance,
            min_val=min(sorted_vals),
            max_val=max(sorted_vals),
            q1=sorted_vals[q1_idx],
            q3=sorted_vals[q3_idx],
        )
    
    def histogram(self, values: list[float], bins: int = 10) -> list[tuple[float, float]]:
        """Compute histogram."""
        if not values:
            return []
        
        min_val = min(values)
        max_val = max(values)
        bin_width = (max_val - min_val) / bins
        
        histogram = [(min_val + i * bin_width, 0) for i in range(bins)]
        
        for v in values:
            bin_idx = min(int((v - min_val) / bin_width), bins - 1)
            _, count = histogram[bin_idx]
            histogram[bin_idx] = (histogram[bin_idx][0], count + 1)
        
        return histogram


class DataStatsAction:
    """Data stats action for automation workflows."""
    
    def __init__(self) -> None:
        self.analyzer = DataStatsAnalyzer()
    
    async def analyze(self, data: list[dict[str, Any]], field: str) -> DescriptiveStats:
        """Analyze field statistics."""
        values = [float(r[field]) for r in data if field in r and r[field] is not None]
        return self.analyzer.compute_stats(values)
    
    async def histogram(self, data: list[dict[str, Any]], field: str, bins: int = 10) -> list[tuple[float, float]]:
        """Compute histogram for field."""
        values = [float(r[field]) for r in data if field in r and r[field] is not None]
        return self.analyzer.histogram(values, bins)


__all__ = ["DescriptiveStats", "DataStatsAnalyzer", "DataStatsAction"]
