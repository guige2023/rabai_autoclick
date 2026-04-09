"""Data Profiling and Statistics.

This module provides data profiling capabilities:
- Column-level statistics
- Data type inference
- Value distribution analysis
- Missing value detection

Example:
    >>> from actions.data_profiling_action import DataProfiler
    >>> profiler = DataProfiler()
    >>> profile = profiler.profile_dataframe(df)
"""

from __future__ import annotations

import logging
import threading
import math
import re
from typing import Any, Callable, Optional
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)


class DataProfiler:
    """Profiles datasets and computes statistics."""

    def __init__(self) -> None:
        """Initialize the data profiler."""
        self._lock = threading.RLock()
        self._profiles: dict[str, dict[str, Any]] = {}

    def profile_records(
        self,
        records: list[dict[str, Any]],
        sample_size: Optional[int] = None,
    ) -> dict[str, Any]:
        """Profile a list of records.

        Args:
            records: List of record dicts.
            sample_size: Optional sample size for large datasets.

        Returns:
            Profile dict with column statistics.
        """
        if not records:
            return {}

        if sample_size and len(records) > sample_size:
            import random
            records = random.sample(records, sample_size)

        columns = set()
        for r in records:
            columns.update(r.keys())

        column_stats = {}
        for col in columns:
            values = [r.get(col) for r in records if col in r]
            column_stats[col] = self._profile_column(values)

        return {
            "total_records": len(records),
            "total_columns": len(columns),
            "columns": column_stats,
        }

    def _profile_column(self, values: list[Any]) -> dict[str, Any]:
        """Profile a single column."""
        non_null = [v for v in values if v is not None]
        null_count = len(values) - len(non_null)
        null_pct = (null_count / len(values) * 100) if values else 0

        if not non_null:
            return {
                "type": "empty",
                "count": len(values),
                "null_count": null_count,
                "null_percent": null_pct,
            }

        inferred_type = self._infer_type(non_null)

        stats = {
            "type": inferred_type,
            "count": len(values),
            "non_null_count": len(non_null),
            "null_count": null_count,
            "null_percent": null_pct,
            "unique_count": len(set(str(v) for v in non_null)),
        }

        if inferred_type == "numeric":
            stats.update(self._numeric_stats(non_null))
        else:
            stats.update(self._categorical_stats(non_null))

        return stats

    def _infer_type(self, values: list[Any]) -> str:
        """Infer the data type of a column."""
        types = Counter()
        for v in values:
            if isinstance(v, bool):
                types["boolean"] += 1
            elif isinstance(v, int):
                types["integer"] += 1
            elif isinstance(v, float):
                types["float"] += 1
            elif isinstance(v, str):
                if self._looks_like_datetime(v):
                    types["datetime"] += 1
                elif self._looks_like_number(v):
                    types["numeric_string"] += 1
                else:
                    types["string"] += 1
            elif isinstance(v, (list, dict)):
                types["object"] += 1
            else:
                types["unknown"] += 1

        if not types:
            return "unknown"

        most_common = types.most_common(1)[0][0]
        if most_common in ("integer", "float", "numeric_string"):
            return "numeric"
        return most_common

    def _looks_like_datetime(self, s: str) -> bool:
        """Check if a string looks like a datetime."""
        patterns = [
            r"\d{4}-\d{2}-\d{2}",
            r"\d{2}/\d{2}/\d{4}",
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",
        ]
        for p in patterns:
            if re.match(p, s):
                return True
        return False

    def _looks_like_number(self, s: str) -> bool:
        """Check if a string looks like a number."""
        try:
            float(s.replace(",", ""))
            return True
        except (ValueError, AttributeError):
            return False

    def _numeric_stats(self, values: list[Any]) -> dict[str, Any]:
        """Calculate numeric statistics."""
        nums = []
        for v in values:
            try:
                if isinstance(v, str):
                    v = float(v.replace(",", ""))
                nums.append(float(v))
            except (ValueError, TypeError):
                continue

        if not nums:
            return {"min": None, "max": None, "mean": None, "median": None}

        sorted_nums = sorted(nums)
        n = len(sorted_nums)

        return {
            "min": min(nums),
            "max": max(nums),
            "mean": sum(nums) / n,
            "median": sorted_nums[n // 2],
            "std": self._std(nums),
            "p25": sorted_nums[n // 4],
            "p75": sorted_nums[3 * n // 4],
            "distinct_values": len(set(nums)),
        }

    def _categorical_stats(self, values: list[Any]) -> dict[str, Any]:
        """Calculate categorical statistics."""
        counter = Counter(str(v) for v in values)
        most_common = counter.most_common(10)

        return {
            "min_length": min(len(str(v)) for v in values),
            "max_length": max(len(str(v)) for v in values),
            "avg_length": sum(len(str(v)) for v in values) / len(values),
            "top_values": [{"value": v, "count": c} for v, c in most_common],
        }

    def _std(self, nums: list[float]) -> float:
        """Calculate standard deviation."""
        if len(nums) < 2:
            return 0.0
        mean = sum(nums) / len(nums)
        variance = sum((x - mean) ** 2 for x in nums) / (len(nums) - 1)
        return math.sqrt(variance)

    def compare_profiles(
        self,
        profile_a: dict[str, Any],
        profile_b: dict[str, Any],
    ) -> dict[str, Any]:
        """Compare two data profiles.

        Args:
            profile_a: First profile.
            profile_b: Second profile.

        Returns:
            Dict describing differences.
        """
        diff = {
            "record_count_diff": profile_b.get("total_records", 0) - profile_a.get("total_records", 0),
            "column_diffs": [],
        }

        cols_a = profile_a.get("columns", {})
        cols_b = profile_b.get("columns", {})

        all_cols = set(cols_a.keys()) | set(cols_b.keys())
        for col in all_cols:
            if col not in cols_a:
                diff["column_diffs"].append({"column": col, "status": "added"})
            elif col not in cols_b:
                diff["column_diffs"].append({"column": col, "status": "removed"})
            else:
                type_a = cols_a[col].get("type")
                type_b = cols_b[col].get("type")
                if type_a != type_b:
                    diff["column_diffs"].append({
                        "column": col,
                        "status": "type_changed",
                        "from": type_a,
                        "to": type_b,
                    })

        return diff
