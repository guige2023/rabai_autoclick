"""
Data Normalization Action Module

Provides data normalization and standardization capabilities.
Supports min-max, z-score, log, and custom normalization methods.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Union
from datetime import datetime


class NormalizationMethod(Enum):
    """Normalization method types."""
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    LOG = "log"
    LOG_PLUS_ONE = "log_plus_one"
    ROBUST = "robust"
    UNIT_VECTOR = "unit_vector"
    CUSTOM = "custom"


@dataclass
class NormalizationStats:
    """Statistics for normalization."""
    count: int = 0
    min_val: float = 0.0
    max_val: float = 0.0
    mean: float = 0.0
    median: float = 0.0
    std_dev: float = 0.0
    sum: float = 0.0
    sum_sq: float = 0.0


@dataclass
class NormalizationResult:
    """Result of a normalization operation."""
    original_values: list[Any]
    normalized_values: list[float]
    method: NormalizationMethod
    stats: NormalizationStats
    duration_ms: float = 0.0


@dataclass
class FieldTransform:
    """Transform configuration for a field."""
    field_name: str
    method: NormalizationMethod = NormalizationMethod.MIN_MAX
    custom_fn: Optional[Callable[[float], float]] = None
    handle_outliers: bool = False
    outlier_threshold: float = 3.0


class DataNormalizer:
    """
    Data normalization utility.
    
    Provides various normalization methods for numerical data.
    
    Example:
        normalizer = DataNormalizer()
        
        result = normalizer.normalize(
            values=[1, 2, 3, 4, 5],
            method=NormalizationMethod.MIN_MAX
        )
    """
    
    def __init__(self):
        self._stats_cache: dict[str, NormalizationStats] = {}
        self._fit_data: dict[str, list[float]] = defaultdict(list)
    
    def _compute_stats(self, values: list[float]) -> NormalizationStats:
        """Compute statistics for a list of values."""
        if not values:
            return NormalizationStats()
        
        count = len(values)
        sorted_vals = sorted(values)
        sum_val = sum(values)
        sum_sq = sum(v * v for v in values)
        mean = sum_val / count
        
        median = sorted_vals[count // 2] if count % 2 == 1 else (
            sorted_vals[count // 2 - 1] + sorted_vals[count // 2]
        ) / 2
        
        variance = (sum_sq / count) - (mean * mean)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        
        return NormalizationStats(
            count=count,
            min_val=min(values),
            max_val=max(values),
            mean=mean,
            median=median,
            std_dev=std_dev,
            sum=sum_val,
            sum_sq=sum_sq
        )
    
    def _min_max_normalize(self, value: float, stats: NormalizationStats) -> float:
        """Apply min-max normalization."""
        range_val = stats.max_val - stats.min_val
        if range_val == 0:
            return 0.5
        return (value - stats.min_val) / range_val
    
    def _z_score_normalize(self, value: float, stats: NormalizationStats) -> float:
        """Apply z-score normalization."""
        if stats.std_dev == 0:
            return 0.0
        return (value - stats.mean) / stats.std_dev
    
    def _log_normalize(self, value: float) -> float:
        """Apply log normalization."""
        if value <= 0:
            return 0.0
        return math.log(value)
    
    def _log_plus_one_normalize(self, value: float) -> float:
        """Apply log(x+1) normalization."""
        return math.log(value + 1)
    
    def _robust_normalize(self, value: float, stats: NormalizationStats) -> float:
        """Apply robust normalization using median and IQR."""
        q1 = stats.min_val
        q3 = stats.max_val
        iqr = q3 - q1
        if iqr == 0:
            return 0.0
        return (value - stats.median) / iqr
    
    def _unit_vector_normalize(self, value: float, stats: NormalizationStats) -> float:
        """Apply unit vector normalization."""
        if stats.sum_sq == 0:
            return 0.0
        magnitude = math.sqrt(stats.sum_sq)
        return value / magnitude
    
    def _handle_outliers(self, values: list[float], stats: NormalizationStats,
                         threshold: float) -> list[float]:
        """Cap outliers using z-score threshold."""
        result = []
        for v in values:
            if stats.std_dev > 0:
                z_score = abs((v - stats.mean) / stats.std_dev)
                if z_score > threshold:
                    v = stats.mean + (threshold * stats.std_dev * (1 if v > stats.mean else -1))
            result.append(v)
        return result
    
    def normalize(
        self,
        values: list[float],
        method: NormalizationMethod = NormalizationMethod.MIN_MAX,
        handle_outliers: bool = False,
        outlier_threshold: float = 3.0,
        fit: bool = True,
        cache_key: Optional[str] = None
    ) -> NormalizationResult:
        """
        Normalize a list of values.
        
        Args:
            values: List of numerical values to normalize
            method: Normalization method to use
            handle_outliers: Whether to handle outliers before normalization
            outlier_threshold: Z-score threshold for outlier detection
            fit: Whether to compute new stats or use cached
            cache_key: Key for caching stats
            
        Returns:
            NormalizationResult with normalized values and stats
        """
        start_time = datetime.now()
        
        values_copy = list(values)
        
        if handle_outliers and cache_key and cache_key in self._stats_cache:
            stats = self._stats_cache[cache_key]
            values_copy = self._handle_outliers(values_copy, stats, outlier_threshold)
        elif handle_outliers:
            stats = self._compute_stats(values_copy)
            values_copy = self._handle_outliers(values_copy, stats, outlier_threshold)
        
        if fit or cache_key not in self._stats_cache:
            stats = self._compute_stats(values_copy)
            if cache_key:
                self._stats_cache[cache_key] = stats
        else:
            stats = self._stats_cache[cache_key]
        
        normalized = []
        for v in values_copy:
            if method == NormalizationMethod.MIN_MAX:
                norm_val = self._min_max_normalize(v, stats)
            elif method == NormalizationMethod.Z_SCORE:
                norm_val = self._z_score_normalize(v, stats)
            elif method == NormalizationMethod.LOG:
                norm_val = self._log_normalize(v)
            elif method == NormalizationMethod.LOG_PLUS_ONE:
                norm_val = self._log_plus_one_normalize(v)
            elif method == NormalizationMethod.ROBUST:
                norm_val = self._robust_normalize(v, stats)
            elif method == NormalizationMethod.UNIT_VECTOR:
                norm_val = self._unit_vector_normalize(v, stats)
            elif method == NormalizationMethod.CUSTOM:
                norm_val = v
            else:
                norm_val = v
            normalized.append(norm_val)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return NormalizationResult(
            original_values=values,
            normalized_values=normalized,
            method=method,
            stats=stats,
            duration_ms=duration_ms
        )
    
    def normalize_dict(
        self,
        data: list[dict],
        field_name: str,
        method: NormalizationMethod = NormalizationMethod.MIN_MAX,
        output_field: Optional[str] = None,
        **kwargs
    ) -> list[dict]:
        """
        Normalize a specific field in a list of dictionaries.
        
        Args:
            data: List of dictionaries
            field_name: Field to normalize
            method: Normalization method
            output_field: Output field name (defaults to field_name)
            **kwargs: Additional arguments for normalize
            
        Returns:
            List of dictionaries with normalized field
        """
        output_field = output_field or field_name
        values = [item.get(field_name, 0) for item in data]
        result = self.normalize(values, method, **kwargs)
        
        for i, item in enumerate(data):
            item[output_field] = result.normalized_values[i]
        
        return data
    
    def denormalize(
        self,
        normalized_values: list[float],
        stats: NormalizationStats,
        method: NormalizationMethod = NormalizationMethod.MIN_MAX
    ) -> list[float]:
        """Denormalize values back to original scale."""
        result = []
        for v in normalized_values:
            if method == NormalizationMethod.MIN_MAX:
                orig = v * (stats.max_val - stats.min_val) + stats.min_val
            elif method == NormalizationMethod.Z_SCORE:
                orig = v * stats.std_dev + stats.mean
            else:
                orig = v
            result.append(orig)
        return result
    
    def fit_transform(
        self,
        data: list[dict],
        fields: list[FieldTransform]
    ) -> list[dict]:
        """
        Fit and transform multiple fields.
        
        Args:
            data: List of dictionaries
            fields: List of FieldTransform configurations
            
        Returns:
            Transformed data
        """
        for field_config in fields:
            self.normalize_dict(
                data,
                field_config.field_name,
                field_config.method,
                handle_outliers=field_config.handle_outliers,
                outlier_threshold=field_config.outlier_threshold
            )
        return data
    
    def get_stats(self, cache_key: str) -> Optional[NormalizationStats]:
        """Get cached statistics."""
        return self._stats_cache.get(cache_key)
    
    def clear_cache(self) -> None:
        """Clear statistics cache."""
        self._stats_cache.clear()
