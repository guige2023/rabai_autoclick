"""
Data Explorer Action Module.

Exploratory data analysis: generates summary statistics, distributions,
correlations, and data quality insights with visualizations.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class EDAReport:
    """Exploratory data analysis report."""
    record_count: int
    field_count: int
    fields: dict[str, dict[str, Any]]
    correlations: dict[str, float]
    data_quality: dict[str, Any]


class DataExplorerAction(BaseAction):
    """Perform exploratory data analysis."""

    def __init__(self) -> None:
        super().__init__("data_explorer")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Explore data and generate EDA report.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - sample_size: Max records to analyze
                - compute_correlations: Compute numeric correlations
                - detect_outliers: Detect outliers using IQR

        Returns:
            EDAReport with statistics and insights
        """
        import math

        records = params.get("records", [])
        sample_size = params.get("sample_size", len(records))
        compute_correlations = params.get("compute_correlations", True)
        detect_outliers = params.get("detect_outliers", True)

        if not records:
            return EDAReport(0, 0, {}, {}, {}).__dict__

        sample = records[:sample_size]

        all_fields = set()
        for r in sample:
            if isinstance(r, dict):
                all_fields.update(r.keys())

        fields: dict[str, dict[str, Any]] = {}
        numeric_fields: list[str] = []
        string_fields: list[str] = []

        for field in all_fields:
            values = [r.get(field) for r in sample if isinstance(r, dict)]
            non_null = [v for v in values if v is not None]
            null_count = len(values) - len(non_null)

            field_info: dict[str, Any] = {
                "null_count": null_count,
                "null_percent": null_count / len(values) if values else 0,
                "unique_count": len(set(str(v) for v in non_null)) if non_null else 0
            }

            num_values = [v for v in non_null if isinstance(v, (int, float))]
            if num_values:
                field_info["type"] = "numeric"
                field_info["min"] = min(num_values)
                field_info["max"] = max(num_values)
                field_info["mean"] = sum(num_values) / len(num_values)
                field_info["sum"] = sum(num_values)
                numeric_fields.append(field)
            else:
                field_info["type"] = "string"
                str_values = [str(v) for v in non_null]
                field_info["min_length"] = min(len(s) for s in str_values) if str_values else 0
                field_info["max_length"] = max(len(s) for s in str_values) if str_values else 0
                string_fields.append(field)

            if detect_outliers and num_values:
                q1 = sorted(num_values)[len(num_values) // 4]
                q3 = sorted(num_values)[3 * len(num_values) // 4]
                iqr = q3 - q1
                lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                outliers = [v for v in num_values if v < lower or v > upper]
                field_info["outlier_count"] = len(outliers)

            fields[field] = field_info

        correlations: dict[str, float] = {}
        if compute_correlations and len(numeric_fields) >= 2:
            for i, f1 in enumerate(numeric_fields):
                for f2 in numeric_fields[i + 1:]:
                    v1 = [r.get(f1) for r in sample if isinstance(r.get(f1), (int, float))]
                    v2 = [r.get(f2) for r in sample if isinstance(r.get(f2), (int, float))]
                    if len(v1) >= 3 and len(v2) >= 3:
                        corr = self._pearson(v1, v2)
                        correlations[f"{f1}__{f2}"] = corr

        data_quality = {
            "complete_fields": [f for f, info in fields.items() if info["null_percent"] == 0],
            "incomplete_fields": [f for f, info in fields.items() if info["null_percent"] > 0],
            "high_cardinality": [f for f, info in fields.items() if info["unique_count"] > 100]
        }

        return EDAReport(
            record_count=len(records),
            field_count=len(fields),
            fields=fields,
            correlations=correlations,
            data_quality=data_quality
        ).__dict__

    def _pearson(self, x: list[float], y: list[float]) -> float:
        """Compute Pearson correlation."""
        import math
        n = len(x)
        if n < 2:
            return 0.0
        sum_x, sum_y = sum(x), sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2, sum_y2 = sum(xi ** 2 for xi in x), sum(yi ** 2 for yi in y)
        num = n * sum_xy - sum_x * sum_y
        den = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
        return num / den if den != 0 else 0.0
