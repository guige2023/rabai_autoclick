"""Data profiling for comprehensive dataset analysis.

Provides statistical analysis, schema inference, quality assessment,
and data discovery for automation workflows.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from statistics import mean, median, stdev
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import copy
import json
import re


class DataType(Enum):
    """Inferred data types."""
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    STRING = "string"
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    UUID = "uuid"
    EMAIL = "email"
    URL = "url"
    PHONE = "phone"
    IP_ADDRESS = "ip_address"
    JSON = "json"
    LIST = "list"
    DICT = "dict"
    UNKNOWN = "unknown"


@dataclass
class ColumnProfile:
    """Comprehensive profile for a single column."""
    column_id: str
    name: str
    inferred_type: DataType
    total_count: int
    missing_count: int
    missing_pct: float
    unique_count: int
    unique_pct: float
    empty_count: int
    whitespace_count: int
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    min_val: Optional[Any] = None
    max_val: Optional[Any] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    q1: Optional[float] = None
    q3: Optional[float] = None
    iqr: Optional[float] = None
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    value_distribution: Dict[str, int] = field(default_factory=dict)
    pattern_counts: Dict[str, int] = field(default_factory=dict)
    sample_values: List[Any] = field(default_factory=list)
    histogram: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DatasetProfile:
    """Overall profile for a dataset."""
    profile_id: str
    name: str
    created_at: float
    row_count: int
    column_count: int
    total_cells: int
    total_missing: int
    completeness: float
    quality_score: float
    columns: List[ColumnProfile] = field(default_factory=list)
    correlations: Dict[str, Dict[str, float]] = field(default_factory=dict)
    schemas: List[Dict[str, Any]] = field(default_factory=list)


class TypeInferrer:
    """Infers data types from sample values."""

    PATTERNS = {
        DataType.INTEGER: re.compile(r'^-?\d+$'),
        DataType.FLOAT: re.compile(r'^-?\d+\.\d+$'),
        DataType.BOOLEAN: re.compile(r'^(true|false|yes|no|0|1)$', re.IGNORECASE),
        DataType.DATETIME: re.compile(r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'),
        DataType.DATE: re.compile(r'^\d{4}-\d{2}-\d{2}$'),
        DataType.TIME: re.compile(r'^\d{2}:\d{2}:\d{2}$'),
        DataType.UUID: re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
        DataType.EMAIL: re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$'),
        DataType.URL: re.compile(r'^https?://[\w\.-]+\.\w+'),
        DataType.IP_ADDRESS: re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
        DataType.PHONE: re.compile(r'^\+?[\d\s\-\(\)]{7,}$'),
    }

    @classmethod
    def infer(cls, value: Any) -> DataType:
        """Infer the type of a single value."""
        if value is None or value == "":
            return DataType.UNKNOWN

        str_val = str(value).strip()

        if str_val.lower() in ("true", "false", "yes", "no", "0", "1"):
            return DataType.BOOLEAN

        if cls.PATTERNS[DataType.INTEGER].match(str_val):
            return DataType.INTEGER

        if cls.PATTERNS[DataType.FLOAT].match(str_val):
            return DataType.FLOAT

        for dtype, pattern in cls.PATTERNS.items():
            if dtype in (DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN):
                continue
            if pattern.match(str_val):
                return dtype

        if str_val.startswith("{") or str_val.startswith("["):
            try:
                json.loads(str_val)
                return DataType.JSON
            except Exception:
                pass

        return DataType.STRING


class StatisticalAnalyzer:
    """Performs statistical analysis on data."""

    @staticmethod
    def compute_percentiles(values: List[float], percentiles: List[float]) -> Dict[str, float]:
        """Compute percentiles for a sorted list of values."""
        if not values:
            return {}

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        result = {}

        for p in percentiles:
            idx = int((p / 100) * (n - 1))
            idx = max(0, min(idx, n - 1))
            result[f"p{int(p)}"] = sorted_vals[idx]

        return result

    @staticmethod
    def compute_quartiles(values: List[float]) -> Tuple[float, float, float]:
        """Compute Q1, median, Q3."""
        if not values:
            return 0.0, 0.0, 0.0

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        q1_idx = n // 4
        median_idx = n // 2
        q3_idx = (3 * n) // 4

        return sorted_vals[q1_idx], sorted_vals[median_idx], sorted_vals[q3_idx]

    @staticmethod
    def compute_skewness(values: List[float]) -> Optional[float]:
        """Compute skewness of distribution."""
        if len(values) < 3:
            return None

        m = mean(values)
        s = stdev(values)

        if s == 0:
            return None

        n = len(values)
        skew = sum(((x - m) / s) ** 3 for x in values) * n / ((n - 1) * (n - 2))
        return skew

    @staticmethod
    def compute_kurtosis(values: List[float]) -> Optional[float]:
        """Compute kurtosis of distribution."""
        if len(values) < 4:
            return None

        m = mean(values)
        s = stdev(values)

        if s == 0:
            return None

        n = len(values)
        kurt = sum(((x - m) / s) ** 4 for x in values) * n * (n + 1) / ((n - 1) * (n - 2) * (n - 3))
        kurt -= 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))
        return kurt


class DataProfiler:
    """Core data profiling engine."""

    def __init__(self):
        self._profiles: Dict[str, DatasetProfile] = {}
        self._lock = threading.Lock()

    def profile_column(self, name: str, values: List[Any]) -> ColumnProfile:
        """Create a comprehensive profile for a column."""
        column_id = str(uuid.uuid4())[:12]
        total = len(values)

        non_missing = [v for v in values if v is not None and v != ""]
        missing = total - len(non_missing)
        missing_pct = (missing / total * 100) if total > 0 else 0

        empty_count = sum(1 for v in values if v == "")
        whitespace_count = sum(1 for v in values if isinstance(v, str) and v.strip() == "")

        unique_vals = set(non_missing)
        unique_count = len(unique_vals)
        unique_pct = (unique_count / len(non_missing) * 100) if non_missing else 0

        lengths = [len(str(v)) for v in non_missing if hasattr(v, "__len__")]
        min_len = min(lengths) if lengths else None
        max_len = max(lengths) if lengths else None
        avg_len = mean(lengths) if lengths else None

        type_counts: Dict[DataType, int] = defaultdict(int)
        for v in non_missing:
            dtype = TypeInferrer.infer(v)
            type_counts[dtype] += 1

        inferred_type = max(type_counts, key=type_counts.get) if type_counts else DataType.STRING

        numeric_vals: List[float] = []
        for v in non_missing:
            try:
                numeric_vals.append(float(v))
            except (TypeError, ValueError):
                pass

        profile = ColumnProfile(
            column_id=column_id,
            name=name,
            inferred_type=inferred_type,
            total_count=total,
            missing_count=missing,
            missing_pct=missing_pct,
            unique_count=unique_count,
            unique_pct=unique_pct,
            empty_count=empty_count,
            whitespace_count=whitespace_count,
            min_length=min_len,
            max_length=max_len,
            avg_length=avg_len,
            sample_values=list(non_missing[:10]),
        )

        if numeric_vals and len(numeric_vals) > 1:
            numeric_vals_sorted = sorted(numeric_vals)
            profile.min_val = numeric_vals_sorted[0]
            profile.max_val = numeric_vals_sorted[-1]
            profile.mean = mean(numeric_vals)
            profile.median = median(numeric_vals)

            try:
                profile.std_dev = stdev(numeric_vals)
            except Exception:
                pass

            q1, median_val, q3 = StatisticalAnalyzer.compute_quartiles(numeric_vals)
            profile.q1 = q1
            profile.q3 = q3
            profile.iqr = q3 - q1 if q1 and q3 else None

            profile.skewness = StatisticalAnalyzer.compute_skewness(numeric_vals)
            profile.kurtosis = StatisticalAnalyzer.compute_kurtosis(numeric_vals)

            histogram = StatisticalAnalyzer._create_histogram(numeric_vals, 10)
            profile.histogram = histogram

        value_counts = Counter(non_missing)
        profile.value_distribution = dict(value_counts.most_common(20))

        return profile

    @staticmethod
    def _create_histogram(values: List[float], bins: int) -> List[Dict[str, Any]]:
        """Create a histogram with specified number of bins."""
        if not values:
            return []

        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val

        if range_val == 0:
            return [{"range": f"{min_val}", "count": len(values), "percentage": 100}]

        bin_width = range_val / bins
        histogram = []

        for i in range(bins):
            bin_start = min_val + i * bin_width
            bin_end = bin_start + bin_width
            bin_count = sum(1 for v in values if bin_start <= v < bin_end)

            histogram.append({
                "range": f"{bin_start:.2f}-{bin_end:.2f}",
                "count": bin_count,
                "percentage": (bin_count / len(values) * 100) if values else 0,
            })

        return histogram

    def profile_dataset(
        self,
        name: str,
        data: List[Dict[str, Any]],
    ) -> DatasetProfile:
        """Profile an entire dataset."""
        profile_id = str(uuid.uuid4())[:12]

        if not data:
            return DatasetProfile(
                profile_id=profile_id,
                name=name,
                created_at=time.time(),
                row_count=0,
                column_count=0,
                total_cells=0,
                total_missing=0,
                completeness=0.0,
                quality_score=0.0,
            )

        row_count = len(data)
        columns = list(data[0].keys()) if data else []
        column_count = len(columns)

        total_cells = row_count * column_count
        column_profiles = []

        total_missing = 0

        for col_name in columns:
            values = [row.get(col_name) for row in data]
            col_profile = self.profile_column(col_name, values)
            column_profiles.append(col_profile)
            total_missing += col_profile.missing_count

        completeness = ((total_cells - total_missing) / total_cells * 100) if total_cells > 0 else 0

        quality_score = completeness

        profile = DatasetProfile(
            profile_id=profile_id,
            name=name,
            created_at=time.time(),
            row_count=row_count,
            column_count=column_count,
            total_cells=total_cells,
            total_missing=total_missing,
            completeness=completeness,
            quality_score=quality_score,
            columns=column_profiles,
        )

        with self._lock:
            self._profiles[profile_id] = profile

        return profile


class AutomationProfilerAction:
    """Action providing data profiling for automation workflows."""

    def __init__(self, profiler: Optional[DataProfiler] = None):
        self._profiler = profiler or DataProfiler()

    def profile_column(self, name: str, data: List[Any]) -> Dict[str, Any]:
        """Profile a single column."""
        profile = self._profiler.profile_column(name, data)

        return {
            "column_id": profile.column_id,
            "name": profile.name,
            "inferred_type": profile.inferred_type.value,
            "total_count": profile.total_count,
            "missing_count": profile.missing_count,
            "missing_pct": round(profile.missing_pct, 2),
            "unique_count": profile.unique_count,
            "unique_pct": round(profile.unique_pct, 2),
            "min_length": profile.min_length,
            "max_length": profile.max_length,
            "avg_length": round(profile.avg_length, 2) if profile.avg_length else None,
            "min": profile.min_val,
            "max": profile.max_val,
            "mean": round(profile.mean, 4) if profile.mean else None,
            "median": round(profile.median, 4) if profile.median else None,
            "std_dev": round(profile.std_dev, 4) if profile.std_dev else None,
            "q1": round(profile.q1, 4) if profile.q1 else None,
            "q3": round(profile.q3, 4) if profile.q3 else None,
            "iqr": round(profile.iqr, 4) if profile.iqr else None,
            "skewness": round(profile.skewness, 4) if profile.skewness else None,
            "kurtosis": round(profile.kurtosis, 4) if profile.kurtosis else None,
            "top_values": list(profile.value_distribution.items())[:10],
            "sample_values": profile.sample_values[:5],
        }

    def profile_dataset(self, name: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Profile an entire dataset."""
        profile = self._profiler.profile_dataset(name, data)

        return {
            "profile_id": profile.profile_id,
            "name": profile.name,
            "created_at": datetime.fromtimestamp(profile.created_at).isoformat(),
            "row_count": profile.row_count,
            "column_count": profile.column_count,
            "total_cells": profile.total_cells,
            "total_missing": profile.total_missing,
            "completeness": round(profile.completeness, 2),
            "quality_score": round(profile.quality_score, 2),
            "columns": [
                {
                    "name": c.name,
                    "type": c.inferred_type.value,
                    "missing_pct": round(c.missing_pct, 2),
                    "unique_pct": round(c.unique_pct, 2),
                }
                for c in profile.columns
            ],
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a profiling operation.

        Required params:
            operation: str - 'profile_column' or 'profile_dataset'
            data: list - Data to profile

        For profile_column:
            column_name: str - Name of column

        For profile_dataset:
            dataset_name: str - Name of dataset
        """
        operation = params.get("operation")
        data = params.get("data")

        if not data:
            raise ValueError("data is required")

        if operation == "profile_column":
            return self.profile_column(
                name=params.get("column_name", "column"),
                data=data,
            )

        elif operation == "profile_dataset":
            return self.profile_dataset(
                name=params.get("dataset_name", "dataset"),
                data=data,
            )

        else:
            raise ValueError(f"Unknown operation: {operation}")
