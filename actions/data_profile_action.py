"""
Data Profile Action Module.

Generates comprehensive data profiles with statistics, patterns,
value distributions, and data quality assessments.

Author: RabAi Team
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


@dataclass
class ColumnProfile:
    """Profile statistics for a column."""
    column: str
    dtype: str
    count: int
    null_count: int
    null_percent: float
    unique_count: int
    unique_percent: float
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std: Optional[float] = None
    top_values: List[Tuple[Any, int]] = field(default_factory=list)
    patterns: List[Tuple[str, int]] = field(default_factory=list)
    histogram: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "dtype": self.dtype,
            "count": self.count,
            "null_count": self.null_count,
            "null_percent": self.null_percent,
            "unique_count": self.unique_count,
            "unique_percent": self.unique_percent,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "avg_length": self.avg_length,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean": self.mean,
            "median": self.median,
            "std": self.std,
            "top_values": [(str(k), v) for k, v in self.top_values],
            "patterns": self.patterns,
        }


@dataclass
class DataProfile:
    """Comprehensive data profile."""
    dataset_name: str
    row_count: int
    column_count: int
    byte_size: int
    column_profiles: List[ColumnProfile] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)
    quality_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "byte_size": self.byte_size,
            "quality_score": self.quality_score,
            "column_profiles": [p.to_dict() for p in self.column_profiles],
            "generated_at": self.generated_at.isoformat(),
        }


class DataProfiler:
    """
    Generates comprehensive data profiles.

    Analyzes DataFrames to produce detailed statistics, patterns,
    distributions, and quality metrics.

    Example:
        >>> profiler = DataProfiler()
        >>> profile = profiler.profile(df, "sales_data")
        >>> print(f"Quality score: {profile.quality_score}")
    """

    def __init__(self):
        self._pattern_detectors = {
            "email": r"^[\w.-]+@[\w.-]+\.\w+$",
            "phone": r"^\+?[\d\s\-()]+$",
            "url": r"^https?://[\w./\-?=&]+$",
            "ip_address": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
            "date_iso": r"^\d{4}-\d{2}-\d{2}$",
            "date_us": r"^\d{1,2}/\d{1,2}/\d{4}$",
            "zip_code": r"^\d{5}(-\d{4})?$",
        }

    def profile(
        self,
        df: pd.DataFrame,
        dataset_name: str = "dataset",
    ) -> DataProfile:
        """Generate comprehensive data profile."""
        column_profiles = []

        for col in df.columns:
            profile = self._profile_column(df[col])
            column_profiles.append(profile)

        quality_score = self._calculate_quality_score(column_profiles, len(df))

        byte_size = int(df.memory_usage(deep=True).sum())

        return DataProfile(
            dataset_name=dataset_name,
            row_count=len(df),
            column_count=len(df.columns),
            byte_size=byte_size,
            column_profiles=column_profiles,
            quality_score=quality_score,
        )

    def _profile_column(self, series: pd.Series) -> ColumnProfile:
        """Profile a single column."""
        non_null = series.dropna()
        count = len(series)
        null_count = count - len(non_null)
        null_percent = (null_count / count * 100) if count > 0 else 0

        unique_count = series.nunique()
        unique_percent = (unique_count / count * 100) if count > 0 else 0

        profile = ColumnProfile(
            column=str(series.name),
            dtype=str(series.dtype),
            count=count,
            null_count=null_count,
            null_percent=null_percent,
            unique_count=unique_count,
            unique_percent=unique_percent,
        )

        if len(non_null) > 0:
            if series.dtype in ["object", "string"]:
                lengths = non_null.astype(str).str.len()
                profile.min_length = int(lengths.min())
                profile.max_length = int(lengths.max())
                profile.avg_length = float(lengths.mean())

                value_counts = non_null.value_counts().head(10)
                profile.top_values = [(k, int(v)) for k, v in value_counts.items()]

                profile.patterns = self._detect_patterns(non_null)
            else:
                profile.min_value = non_null.min()
                profile.max_value = non_null.max()
                profile.mean = float(non_null.mean()) if len(non_null) > 0 else None
                profile.median = float(non_null.median()) if len(non_null) > 0 else None
                profile.std = float(non_null.std()) if len(non_null) > 1 else None

        return profile

    def _detect_patterns(self, series: pd.Series) -> List[Tuple[str, int]]:
        """Detect common patterns in string column."""
        patterns_found = []
        for pattern_name, pattern_regex in self._pattern_detectors.items():
            pattern = re.compile(pattern_regex)
            matches = series.astype(str).str.match(pattern, na=False)
            match_count = int(matches.sum())
            if match_count > 0:
                percent = match_count / len(series) * 100
                patterns_found.append((f"{pattern_name}:{percent:.1f}%", match_count))

        patterns_found.sort(key=lambda x: x[1], reverse=True)
        return patterns_found[:5]

    def _calculate_quality_score(
        self,
        profiles: List[ColumnProfile],
        total_rows: int,
    ) -> float:
        """Calculate overall data quality score (0-100)."""
        if not profiles:
            return 0.0

        scores = []
        for profile in profiles:
            completeness_score = (100 - profile.null_percent)
            uniqueness_score = profile.unique_percent if profile.unique_percent < 99 else 100 - (profile.unique_percent - 99) * 10
            col_score = (completeness_score * 0.6 + uniqueness_score * 0.4)
            scores.append(col_score)

        return sum(scores) / len(scores)


def create_profiler() -> DataProfiler:
    """Factory to create a data profiler."""
    return DataProfiler()
