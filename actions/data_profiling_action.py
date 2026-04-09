"""
Data Profiling Action Module

Profiles data to generate statistics, detect patterns, and identify quality issues.
Supports various data types including numbers, strings, dates, and categorical data.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union
from fractions import Fraction


class DataType(Enum):
    """Detected data types."""
    INTEGER = "integer"
    FLOAT = "float"
    STRING = "string"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    NULL = "null"
    MIXED = "mixed"


@dataclass
class NumericStats:
    """Statistics for numeric data."""
    count: int = 0
    null_count: int = 0
    min_val: float = 0.0
    max_val: float = 0.0
    mean: float = 0.0
    median: float = 0.0
    mode: Optional[float] = None
    std_dev: float = 0.0
    variance: float = 0.0
    skewness: float = 0.0
    kurtosis: float = 0.0
    quartiles: tuple[float, float, float, float] = (0, 0, 0, 0)
    percentiles: dict[int, float] = field(default_factory=dict)


@dataclass
class StringStats:
    """Statistics for string data."""
    count: int = 0
    null_count: int = 0
    min_length: int = 0
    max_length: int = 0
    avg_length: float = 0.0
    unique_count: int = 0
    empty_count: int = 0
    pattern_counts: dict[str, int] = field(default_factory=dict)
    top_values: list[tuple[str, int]] = field(default_factory=list)


@dataclass
class DateStats:
    """Statistics for date/time data."""
    count: int = 0
    null_count: int = 0
    min_date: Optional[datetime] = None
    max_date: Optional[datetime] = None
    date_range_days: int = 0
    unique_count: int = 0
    values_by_year: dict[int, int] = field(default_factory=dict)
    values_by_month: dict[str, int] = field(default_factory=dict)


@dataclass
class QualityMetrics:
    """Data quality metrics."""
    completeness: float = 0.0
    validity: float = 0.0
    consistency: float = 0.0
    uniqueness: float = 0.0
    null_count: int = 0
    empty_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0


@dataclass
class DataProfile:
    """Complete data profile."""
    data_type: DataType
    total_count: int
    numeric_stats: Optional[NumericStats] = None
    string_stats: Optional[StringStats] = None
    date_stats: Optional[DateStats] = None
    quality: QualityMetrics = field(default_factory=QualityMetrics)
    generated_at: datetime = field(default_factory=datetime.now)


class DataProfilingAction:
    """
    Data profiling action for analyzing datasets.
    
    Generates comprehensive statistics and quality metrics.
    
    Example:
        profiler = DataProfilingAction()
        
        profile = profiler.profile(
            data=[1, 2, 3, 4, 5, None, 3, 2],
            data_type=DataType.INTEGER
        )
    """
    
    def __init__(self):
        self._history: list[DataProfile] = []
    
    def _compute_numeric_stats(self, values: list[Optional[float]]) -> NumericStats:
        """Compute statistics for numeric data."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)
        
        if not non_null:
            return NumericStats()
        
        count = len(non_null)
        sorted_vals = sorted(non_null)
        sum_val = sum(non_null)
        sum_sq = sum(v * v for v in non_null)
        mean = sum_val / count
        
        variance = (sum_sq / count) - (mean * mean)
        std_dev = math.sqrt(variance) if variance > 0 else 0
        
        median = sorted_vals[count // 2] if count % 2 == 1 else (
            sorted_vals[count // 2 - 1] + sorted_vals[count // 2]
        ) / 2
        
        mode_val = Counter(non_null).most_common(1)[0][0] if non_null else None
        
        q1_idx = count // 4
        q2_idx = count // 2
        q3_idx = (3 * count) // 4
        q4_idx = count - 1
        quartiles = (
            sorted_vals[q1_idx],
            sorted_vals[q2_idx],
            sorted_vals[q3_idx],
            sorted_vals[q4_idx]
        )
        
        percentiles = {}
        for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
            idx = int(count * p / 100)
            if idx >= count:
                idx = count - 1
            percentiles[p] = sorted_vals[idx]
        
        if count >= 3:
            m2 = sum((v - mean) ** 2 for v in non_null) / count
            m3 = sum((v - mean) ** 3 for v in non_null) / count
            skewness = m3 / (m2 ** 1.5) if m2 > 0 else 0
            
            m4 = sum((v - mean) ** 4 for v in non_null) / count
            kurtosis = (m4 / (m2 ** 2)) - 3 if m2 > 0 else 0
        else:
            skewness = 0
            kurtosis = 0
        
        return NumericStats(
            count=count,
            null_count=null_count,
            min_val=min(non_null),
            max_val=max(non_null),
            mean=mean,
            median=median,
            mode=mode_val,
            std_dev=std_dev,
            variance=variance,
            skewness=skewness,
            kurtosis=kurtosis,
            quartiles=quartiles,
            percentiles=percentiles
        )
    
    def _compute_string_stats(self, values: list[Optional[str]]) -> StringStats:
        """Compute statistics for string data."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)
        empty_count = sum(1 for v in non_null if v == "")
        
        if not non_null:
            return StringStats()
        
        lengths = [len(v) for v in non_null]
        unique_vals = set(non_null)
        
        patterns = {
            "email": 0,
            "url": 0,
            "phone": 0,
            "date": 0,
            "number": 0
        }
        
        for v in non_null:
            if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v):
                patterns["email"] += 1
            elif re.match(r'^https?://', v):
                patterns["url"] += 1
            elif re.match(r'^[\d\-\(\)\s]+$', v) and len(v) >= 7:
                patterns["phone"] += 1
            elif re.match(r'^\d{4}[-/]\d{2}[-/]\d{2}', v):
                patterns["date"] += 1
            elif re.match(r'^-?\d+\.?\d*$', v):
                patterns["number"] += 1
        
        top_values = Counter(non_null).most_common(10)
        
        return StringStats(
            count=len(non_null),
            null_count=null_count,
            min_length=min(lengths),
            max_length=max(lengths),
            avg_length=sum(lengths) / len(lengths),
            unique_count=len(unique_vals),
            empty_count=empty_count,
            pattern_counts=patterns,
            top_values=top_values
        )
    
    def _compute_date_stats(self, values: list[Optional[datetime]]) -> DateStats:
        """Compute statistics for date/time data."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)
        
        if not non_null:
            return DateStats()
        
        sorted_dates = sorted(non_null)
        unique_dates = set(non_null)
        
        by_year: dict[int, int] = defaultdict(int)
        by_month: dict[str, int] = defaultdict(int)
        
        for d in non_null:
            by_year[d.year] += 1
            by_month[d.strftime("%Y-%m")] += 1
        
        min_date = sorted_dates[0]
        max_date = sorted_dates[-1]
        range_days = (max_date - min_date).days
        
        return DateStats(
            count=len(non_null),
            null_count=null_count,
            min_date=min_date,
            max_date=max_date,
            date_range_days=range_days,
            unique_count=len(unique_dates),
            values_by_year=dict(by_year),
            values_by_month=dict(by_month)
        )
    
    def _compute_quality_metrics(
        self,
        values: list[Any],
        data_type: DataType
    ) -> QualityMetrics:
        """Compute data quality metrics."""
        total = len(values)
        if total == 0:
            return QualityMetrics()
        
        null_count = sum(1 for v in values if v is None)
        empty_count = sum(1 for v in values if v == "" or v == [])
        
        non_null = [v for v in values if v is not None]
        unique_vals = set(non_null)
        duplicate_count = len(non_null) - len(unique_vals)
        
        completeness = (total - null_count) / total
        uniqueness = len(unique_vals) / len(non_null) if non_null else 0
        
        valid_count = len(non_null)
        validity = valid_count / (total - null_count) if null_count < total else 1.0
        
        consistency = 1.0
        
        return QualityMetrics(
            completeness=completeness,
            validity=validity,
            consistency=consistency,
            uniqueness=uniqueness,
            null_count=null_count,
            empty_count=empty_count,
            duplicate_count=duplicate_count,
            error_count=0
        )
    
    def profile(
        self,
        data: list[Any],
        data_type: Optional[DataType] = None
    ) -> DataProfile:
        """
        Profile a dataset.
        
        Args:
            data: List of values to profile
            data_type: Optional data type hint
            
        Returns:
            DataProfile with statistics and quality metrics
        """
        if not data_type:
            data_type = self._infer_type(data)
        
        numeric_stats = None
        string_stats = None
        date_stats = None
        
        if data_type == DataType.INTEGER or data_type == DataType.FLOAT:
            numeric_data = []
            for v in data:
                if v is not None:
                    try:
                        numeric_data.append(float(v))
                    except (ValueError, TypeError):
                        pass
                else:
                    numeric_data.append(None)
            numeric_stats = self._compute_numeric_stats(numeric_data)
        
        elif data_type == DataType.STRING:
            string_stats = self._compute_string_stats([str(v) if v is not None else None for v in data])
        
        quality = self._compute_quality_metrics(data, data_type)
        
        profile = DataProfile(
            data_type=data_type,
            total_count=len(data),
            numeric_stats=numeric_stats,
            string_stats=string_stats,
            date_stats=date_stats,
            quality=quality
        )
        
        self._history.append(profile)
        return profile
    
    def _infer_type(self, values: list[Any]) -> DataType:
        """Infer the data type of a list of values."""
        non_null = [v for v in values if v is not None]
        if not non_null:
            return DataType.NULL
        
        type_votes = Counter()
        
        for v in non_null:
            if isinstance(v, bool):
                type_votes[DataType.BOOLEAN] += 1
            elif isinstance(v, int):
                type_votes[DataType.INTEGER] += 1
            elif isinstance(v, float):
                type_votes[DataType.FLOAT] += 1
            elif isinstance(v, str):
                type_votes[DataType.STRING] += 1
            elif isinstance(v, datetime):
                type_votes[DataType.DATETIME] += 1
        
        if type_votes:
            return type_votes.most_common(1)[0][0]
        return DataType.MIXED
    
    def profile_dataframe(
        self,
        data: dict[str, list[Any]]
    ) -> dict[str, DataProfile]:
        """Profile all columns of a dataframe-like structure."""
        return {col: self.profile(values) for col, values in data.items()}
    
    def get_history(self) -> list[DataProfile]:
        """Get profiling history."""
        return self._history.copy()
