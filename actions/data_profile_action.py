"""Data profiling action module.

Provides data quality analysis, statistics computation, and schema detection.
Reports on completeness, distribution, cardinality, and data types.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any, List
from collections import Counter, defaultdict
from dataclasses import dataclass, field
import math

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Profile statistics for a single column."""
    name: str
    total_count: int
    null_count: int
    unique_count: int
    data_type: str = "unknown"
    completeness: float = 0.0
    sample_values: List[Any] = field(default_factory=list)
    min_value: Any = None
    max_value: Any = None


@dataclass
class DataProfile:
    """Complete data profile."""
    total_rows: int
    total_columns: int
    columns: Dict[str, ColumnProfile]
    completeness: float = 0.0
    quality_score: float = 0.0


class DataProfileAction:
    """Data profiling engine.

    Analyzes data quality and computes statistics.

    Example:
        profiler = DataProfileAction()
        profile = profiler.profile(data)
        print(profile.quality_score)
    """

    def profile(
        self,
        data: List[Dict[str, Any]],
        sample_size: int = 1000,
    ) -> DataProfile:
        """Compute full data profile.

        Args:
            data: List of dicts to profile.
            sample_size: Max rows to sample for heavy computations.

        Returns:
            DataProfile with statistics.
        """
        if not data:
            return DataProfile(total_rows=0, total_columns=0, columns={})

        sample = data[:sample_size] if len(data) > sample_size else data
        total_rows = len(data)
        total_columns = len(data[0]) if data[0] else 0

        columns: Dict[str, ColumnProfile] = {}
        total_nulls = 0
        total_values = 0

        for col_name in data[0].keys():
            col_profile = self._profile_column(col_name, sample, data)
            columns[col_name] = col_profile
            total_nulls += col_profile.null_count
            total_values += col_profile.total_count

        overall_completeness = (total_values - total_nulls) / total_values if total_values > 0 else 0.0

        return DataProfile(
            total_rows=total_rows,
            total_columns=total_columns,
            columns=columns,
            completeness=overall_completeness,
            quality_score=self._compute_quality_score(columns),
        )

    def _profile_column(
        self,
        name: str,
        sample: List[Dict[str, Any]],
        full_data: List[Dict[str, Any]],
    ) -> ColumnProfile:
        """Profile a single column."""
        values = [row.get(name) for row in full_data]
        non_null = [v for v in values if v is not None and str(v).strip() != ""]

        unique_values = set(non_null)
        sample_non_null = [v for v in sample if v.get(name) is not None and str(v.get(name)).strip() != ""]

        data_type = self._infer_type(sample_non_null)

        return ColumnProfile(
            name=name,
            total_count=len(values),
            null_count=len(values) - len(non_null),
            unique_count=len(unique_values),
            data_type=data_type,
            completeness=len(non_null) / len(values) if values else 0.0,
            sample_values=list(unique_values)[:10],
            min_value=min(non_null) if non_null and data_type in ("int", "float") else None,
            max_value=max(non_null) if non_null and data_type in ("int", "float") else None,
        )

    def _infer_type(self, values: List[Any]) -> str:
        """Infer the data type of a column."""
        if not values:
            return "unknown"

        int_count = 0
        float_count = 0
        bool_count = 0
        str_count = 0

        for v in values[:100]:
            if isinstance(v, bool) or str(v).lower() in ["true", "false"]:
                bool_count += 1
            elif isinstance(v, (int, float)) and "." not in str(v):
                int_count += 1
            elif isinstance(v, (int, float)):
                float_count += 1
            else:
                str_count += 1

        total = len(values[:100])
        threshold = total * 0.8

        if int_count >= threshold:
            return "int"
        elif float_count + int_count >= threshold:
            return "float"
        elif bool_count >= threshold:
            return "bool"
        elif str_count >= threshold:
            return "str"

        return "str"

    def _compute_quality_score(self, columns: Dict[str, ColumnProfile]) -> float:
        """Compute overall quality score (0-100)."""
        if not columns:
            return 0.0

        scores = []
        for col in columns.values():
            score = col.completeness * 100
            scores.append(score)

        return sum(scores) / len(scores) if scores else 0.0

    def get_summary(self, profile: DataProfile) -> str:
        """Get human-readable profile summary."""
        lines = [
            f"Rows: {profile.total_rows}",
            f"Columns: {profile.total_columns}",
            f"Completeness: {profile.completeness:.1%}",
            f"Quality Score: {profile.quality_score:.1f}/100",
            "",
            "Column Details:",
        ]

        for name, col in profile.columns.items():
            lines.append(
                f"  {name}: {col.data_type} | "
                f"complete: {col.completeness:.0%} | "
                f"unique: {col.unique_count}"
            )

        return "\n".join(lines)
