"""
Data Normalization Action - Normalizes and standardizes data from various sources.

This module provides data cleaning, transformation, and normalization
capabilities for automation workflows handling heterogeneous data.
"""

from __future__ import annotations

import re
import math
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar, Generic
from enum import Enum
from datetime import datetime


T = TypeVar("T")


class NormalizationType(Enum):
    """Type of normalization to apply."""
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    LOG = "log"
    SQUARE_ROOT = "square_root"
    ROBUST = "robust"
    PERCENTILE = "percentile"


@dataclass
class NormalizationConfig:
    """Configuration for data normalization."""
    normalization_type: NormalizationType = NormalizationType.MIN_MAX
    clip_outliers: bool = False
    outlier_threshold: float = 3.0
    default_value: Any = None
    handle_nulls: str = "skip"  # skip, zero, mean, median


@dataclass
class DataStats:
    """Statistical summary of a dataset."""
    count: int = 0
    mean: float = 0.0
    median: float = 0.0
    std_dev: float = 0.0
    min_val: float = 0.0
    max_val: float = 0.0
    q1: float = 0.0
    q3: float = 0.0
    null_count: int = 0


@dataclass
class NormalizedResult:
    """Result of normalizing a dataset."""
    data: list[Any]
    stats: DataStats
    transformations_applied: list[str] = field(default_factory=list)
    nulls_handled: int = 0
    outliers_clipped: int = 0


class DataNormalizer:
    """
    Handles normalization of numeric and string data.
    
    Example:
        normalizer = DataNormalizer()
        result = normalizer.normalize([1, 2, 3, 4, 5], NormalizationConfig())
        print(result.data)  # [0.0, 0.25, 0.5, 0.75, 1.0]
    """
    
    def __init__(self, config: NormalizationConfig | None = None) -> None:
        self.config = config or NormalizationConfig()
    
    def compute_stats(self, data: list[float]) -> DataStats:
        """Compute statistical summary of data."""
        clean_data = [x for x in data if x is not None and not math.isnan(x)]
        
        if not clean_data:
            return DataStats()
        
        sorted_data = sorted(clean_data)
        n = len(sorted_data)
        
        mean_val = sum(sorted_data) / n
        median_val = sorted_data[n // 2] if n % 2 else (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2
        
        variance = sum((x - mean_val) ** 2 for x in sorted_data) / n
        std_dev = math.sqrt(variance)
        
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_data[q1_idx]
        q3 = sorted_data[q3_idx]
        
        return DataStats(
            count=n,
            mean=mean_val,
            median=median_val,
            std_dev=std_dev,
            min_val=min(clean_data),
            max_val=max(clean_data),
            q1=q1,
            q3=q3,
            null_count=len(data) - n,
        )
    
    def normalize(
        self,
        data: list[Any],
        config: NormalizationConfig | None = None,
    ) -> NormalizedResult:
        """
        Normalize a list of numeric values.
        
        Args:
            data: List of numeric values
            config: Optional override config
            
        Returns:
            NormalizedResult with normalized data and stats
        """
        cfg = config or self.config
        transformations: list[str] = []
        nulls_handled = 0
        
        numeric_data: list[float] = []
        for val in data:
            if val is None or (isinstance(val, float) and math.isnan(val)):
                if cfg.handle_nulls == "zero":
                    numeric_data.append(0.0)
                    nulls_handled += 1
                elif cfg.handle_nulls == "mean":
                    pass
                elif cfg.handle_nulls == "median":
                    pass
                else:
                    pass
            else:
                try:
                    numeric_data.append(float(val))
                except (ValueError, TypeError):
                    if cfg.default_value is not None:
                        numeric_data.append(float(cfg.default_value))
        
        stats = self.compute_stats(numeric_data)
        
        if cfg.handle_nulls in ("mean", "median"):
            fill_value = stats.median if cfg.handle_nulls == "median" else stats.mean
            nulls_handled = stats.null_count
            numeric_data = [fill_value if (x == 0 and stats.null_count > 0) else x for x in numeric_data]
        
        result_data = self._apply_normalization(numeric_data, stats, cfg, transformations)
        
        outliers_clipped = 0
        if cfg.clip_outliers and cfg.normalization_type == NormalizationType.MIN_MAX:
            outliers_clipped = self._clip_outliers(result_data, cfg.outlier_threshold)
            transformations.append(f"clipped_{outliers_clipped}_outliers")
        
        return NormalizedResult(
            data=result_data,
            stats=stats,
            transformations_applied=transformations,
            nulls_handled=nulls_handled,
            outliers_clipped=outliers_clipped,
        )
    
    def _apply_normalization(
        self,
        data: list[float],
        stats: DataStats,
        config: NormalizationConfig,
        transformations: list[str],
    ) -> list[Any]:
        """Apply the configured normalization."""
        norm_type = config.normalization_type
        
        if norm_type == NormalizationType.MIN_MAX:
            transformations.append("min_max_scaling")
            if stats.max_val == stats.min_val:
                return [0.5] * len(data)
            return [(x - stats.min_val) / (stats.max_val - stats.min_val) for x in data]
        
        elif norm_type == NormalizationType.Z_SCORE:
            transformations.append("z_score_standardization")
            if stats.std_dev == 0:
                return [0.0] * len(data)
            return [(x - stats.mean) / stats.std_dev for x in data]
        
        elif norm_type == NormalizationType.LOG:
            transformations.append("log_transform")
            return [math.log(x + 1) for x in data]
        
        elif norm_type == NormalizationType.SQUARE_ROOT:
            transformations.append("square_root_transform")
            return [math.sqrt(max(0, x)) for x in data]
        
        elif norm_type == NormalizationType.ROBUST:
            transformations.append("robust_scaling")
            iqr = stats.q3 - stats.q1
            if iqr == 0:
                return [0.0] * len(data)
            return [(x - stats.median) / iqr for x in data]
        
        elif norm_type == NormalizationType.PERCENTILE:
            transformations.append("percentile_ranking")
            sorted_unique = sorted(set(data))
            rank_map = {v: i / len(sorted_unique) for i, v in enumerate(sorted_unique)}
            return [rank_map.get(x, 0.5) for x in data]
        
        return data
    
    def _clip_outliers(self, data: list[float], threshold: float) -> int:
        """Clip values beyond threshold."""
        clipped = 0
        for i, val in enumerate(data):
            if abs(val) > threshold:
                data[i] = math.copysign(threshold, val)
                clipped += 1
        return clipped


class StringNormalizer:
    """
    Normalizes and standardizes string data.
    """
    
    def __init__(
        self,
        lowercase: bool = True,
        strip_whitespace: bool = True,
        remove_accents: bool = False,
    ) -> None:
        self.lowercase = lowercase
        self.strip_whitespace = strip_whitespace
        self.remove_accents = remove_accents
    
    def normalize(self, value: str) -> str:
        """Normalize a single string value."""
        if value is None:
            return ""
        
        result = str(value)
        
        if self.strip_whitespace:
            result = result.strip()
        
        if self.lowercase:
            result = result.lower()
        
        if self.remove_accents:
            result = self._remove_accents(result)
        
        return result
    
    def _remove_accents(self, text: str) -> str:
        """Remove accents from text."""
        import unicodedata
        nfd = unicodedata.normalize("NFD", text)
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


class DataNormalizationAction:
    """
    Unified data normalization action for automation workflows.
    
    Example:
        action = DataNormalizationAction()
        result = await action.normalize_dataset(
            {"temperature": [1, 2, 3, 4, 5], "humidity": [10, 20, 30, 40, 50]},
            {"temperature": NormalizationConfig(NormalizationType.MIN_MAX)}
        )
    """
    
    def __init__(
        self,
        numeric_config: NormalizationConfig | None = None,
        string_config: dict[str, Any] | None = None,
    ) -> None:
        self.numeric_normalizer = DataNormalizer(numeric_config)
        self.string_normalizer = StringNormalizer(**(string_config or {}))
    
    async def normalize_dataset(
        self,
        data: dict[str, list[Any]],
        numeric_configs: dict[str, NormalizationConfig] | None = None,
        string_fields: list[str] | None = None,
    ) -> dict[str, list[Any]]:
        """
        Normalize all fields in a dataset.
        
        Args:
            data: Dictionary mapping field names to value lists
            numeric_configs: Config for numeric field normalization
            string_fields: List of fields to normalize as strings
            
        Returns:
            Dictionary with normalized values
        """
        result = {}
        configs = numeric_configs or {}
        string_field_list = string_fields or []
        
        for field_name, values in data.items():
            if field_name in string_field_list:
                result[field_name] = [self.string_normalizer.normalize(v) for v in values]
            elif isinstance(values[0] if values else None, (int, float)):
                config = configs.get(field_name, NormalizationConfig())
                normalized = self.numeric_normalizer.normalize(values, config)
                result[field_name] = normalized.data
            else:
                result[field_name] = values
        
        return result
    
    async def transform_record(
        self,
        record: dict[str, Any],
        transformations: dict[str, Callable[[Any], Any]],
    ) -> dict[str, Any]:
        """Apply field-by-field transformations to a record."""
        result = {}
        for key, value in record.items():
            if key in transformations:
                try:
                    result[key] = transformations[key](value)
                except Exception:
                    result[key] = value
            else:
                result[key] = value
        return result


# Export public API
__all__ = [
    "NormalizationType",
    "NormalizationConfig",
    "DataStats",
    "NormalizedResult",
    "DataNormalizer",
    "StringNormalizer",
    "DataNormalizationAction",
]
