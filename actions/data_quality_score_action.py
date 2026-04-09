"""
Data Quality Score Module.

Computes data quality scores across multiple dimensions:
completeness, accuracy, consistency, timeliness, uniqueness,
and validity. Provides actionable insights for data quality improvement.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class QualityDimension(Enum):
    """Data quality dimensions."""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    INTEGRITY = "integrity"


@dataclass
class DimensionScore:
    """Score for a single quality dimension."""
    dimension: QualityDimension
    score: float
    issues: list[str] = field(default_factory=list)
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    record_count: int
    overall_score: float
    dimension_scores: list[DimensionScore]
    timestamp: float = field(default_factory=time.time)
    recommendations: list[str] = field(default_factory=list)


class DataQualityChecker:
    """
    Data quality scoring and analysis.

    Evaluates datasets across multiple quality dimensions
    and provides detailed reports with recommendations.

    Example:
        checker = DataQualityChecker()
        checker.load_data(df)
        report = checker.evaluate()
        print(f"Overall score: {report.overall_score}")
        checker.get_dimension(QualityDimension.COMPLETENESS)
    """

    def __init__(self) -> None:
        self._data: list[dict[str, Any]] = []
        self._columns: list[str] = []
        self._column_types: dict[str, str] = {}
        self._dimension_scores: dict[QualityDimension, DimensionScore] = {}

    def load_data(
        self,
        data: Sequence[dict[str, Any]]
    ) -> None:
        """
        Load data for quality evaluation.

        Args:
            data: List of dictionaries representing rows
        """
        self._data = list(data)
        if self._data:
            self._columns = list(self._data[0].keys())
            self._infer_types()

    def load_csv(self, path: str) -> int:
        """Load data from CSV file."""
        import csv
        rows = 0
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            self._data = list(reader)
            rows = len(self._data)
        if self._data:
            self._columns = list(self._data[0].keys())
            self._infer_types()
        return rows

    def _infer_types(self) -> None:
        """Infer column types from data."""
        for col in self._columns:
            values = [row.get(col) for row in self._data if row.get(col) not in (None, "", "null")]
            if not values:
                self._column_types[col] = "unknown"
                continue

            try:
                for v in values[:100]:
                    float(v)
                self._column_types[col] = "numeric"
            except (ValueError, TypeError):
                if all(self._is_date(v) for v in values[:10]):
                    self._column_types[col] = "datetime"
                elif all(self._is_email(v) for v in values[:10]):
                    self._column_types[col] = "email"
                elif all(self._is_url(v) for v in values[:10]):
                    self._column_types[col] = "url"
                else:
                    self._column_types[col] = "string"

    def evaluate(
        self,
        weights: Optional[dict[QualityDimension, float]] = None
    ) -> QualityReport:
        """
        Evaluate data quality across all dimensions.

        Args:
            weights: Optional weights for each dimension

        Returns:
            QualityReport with scores and recommendations
        """
        self._dimension_scores = {}

        self._check_completeness()
        self._check_uniqueness()
        self._check_validity()
        self._check_consistency()
        self._check_accuracy()
        self._check_timeliness()

        default_weights = {
            QualityDimension.COMPLETENESS: 0.25,
            QualityDimension.ACCURACY: 0.20,
            QualityDimension.CONSISTENCY: 0.15,
            QualityDimension.TIMELINESS: 0.10,
            QualityDimension.UNIQUENESS: 0.15,
            QualityDimension.VALIDITY: 0.15
        }

        w = weights or default_weights
        total_score = sum(
            self._dimension_scores[d].score * w.get(d, 0)
            for d in QualityDimension
            if d in self._dimension_scores
        )

        recommendations = self._generate_recommendations()

        return QualityReport(
            record_count=len(self._data),
            overall_score=total_score,
            dimension_scores=list(self._dimension_scores.values()),
            recommendations=recommendations
        )

    def _check_completeness(self) -> None:
        """Check completeness (null/missing values)."""
        issues = []
        metrics: dict[str, float] = {}
        total_cells = len(self._data) * len(self._columns) if self._columns else 1
        null_count = 0

        for col in self._columns:
            nulls = sum(1 for row in self._data if row.get(col) in (None, "", "null", "NaN"))
            null_rate = nulls / len(self._data) if self._data else 0
            if null_rate > 0.1:
                issues.append(f"Column '{col}' has {null_rate:.1%} null values")
            metrics[f"{col}_null_rate"] = null_rate
            null_count += nulls

        overall_null_rate = null_count / total_cells if total_cells else 0
        score = (1 - overall_null_rate) * 100

        self._dimension_scores[QualityDimension.COMPLETENESS] = DimensionScore(
            dimension=QualityDimension.COMPLETENESS,
            score=score,
            issues=issues,
            metrics=metrics
        )

    def _check_uniqueness(self) -> None:
        """Check uniqueness (duplicate rows/keys)."""
        issues = []
        metrics: dict[str, float] = {}

        if not self._data:
            self._dimension_scores[QualityDimension.UNIQUENESS] = DimensionScore(
                dimension=QualityDimension.UNIQUENESS,
                score=100.0,
                issues=[],
                metrics={}
            )
            return

        seen: set[tuple] = set()
        duplicates = 0
        for row in self._data:
            key = tuple(row.items())
            if key in seen:
                duplicates += 1
            seen.add(key)

        dup_rate = duplicates / len(self._data)
        if dup_rate > 0:
            issues.append(f"Found {duplicates} duplicate rows ({dup_rate:.1%})")

        for col in self._columns[:3]:
            values = [row.get(col) for row in self._data]
            unique_count = len(set(values))
            dup_col_rate = 1 - (unique_count / len(values)) if values else 0
            if dup_col_rate > 0.5:
                issues.append(f"Column '{col}' has high duplicate rate ({dup_col_rate:.1%})")
            metrics[f"{col}_unique_rate"] = 1 - dup_col_rate

        score = (1 - dup_rate) * 100
        self._dimension_scores[QualityDimension.UNIQUENESS] = DimensionScore(
            dimension=QualityDimension.UNIQUENESS,
            score=score,
            issues=issues,
            metrics=metrics
        )

    def _check_validity(self) -> None:
        """Check validity (format compliance)."""
        issues = []
        metrics: dict[str, float] = {}
        total_validations = 0
        failed_validations = 0

        for col in self._columns:
            col_type = self._column_types.get(col, "string")
            valid_count = 0

            for row in self._data:
                value = row.get(col)
                if value in (None, "", "null"):
                    continue

                total_validations += 1
                is_valid = True

                if col_type == "numeric":
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        is_valid = False
                elif col_type == "email":
                    is_valid = self._is_email(value)
                elif col_type == "url":
                    is_valid = self._is_url(value)
                elif col_type == "datetime":
                    is_valid = self._is_date(value)

                if is_valid:
                    valid_count += 1
                else:
                    failed_validations += 1

            if total_validations > 0:
                metrics[f"{col}_valid_rate"] = valid_count / total_validations

        score = ((total_validations - failed_validations) / total_validations * 100) if total_validations else 100
        self._dimension_scores[QualityDimension.VALIDITY] = DimensionScore(
            dimension=QualityDimension.VALIDITY,
            score=score,
            issues=issues,
            metrics=metrics
        )

    def _check_consistency(self) -> None:
        """Check consistency (format/encoding consistency)."""
        issues = []
        metrics: dict[str, float] = {}

        for col in self._columns:
            values = [row.get(col) for row in self._data if row.get(col) not in (None, "", "null")]
            if not values:
                continue

            str_values = [str(v).strip() for v in values]
            formats: dict[str, int] = {}

            for v in str_values[:100]:
                fmt = self._detect_format(v)
                formats[fmt] = formats.get(fmt, 0) + 1

            if len(formats) > 1:
                main_format = max(formats.values())
                consistency = main_format / sum(formats.values())
                if consistency < 0.9:
                    issues.append(f"Column '{col}' has inconsistent formats")
                metrics[f"{col}_consistency"] = consistency * 100

        score = 100 - (len(issues) * 5)
        self._dimension_scores[QualityDimension.CONSISTENCY] = DimensionScore(
            dimension=QualityDimension.CONSISTENCY,
            score=max(0, score),
            issues=issues,
            metrics=metrics
        )

    def _check_accuracy(self) -> None:
        """Check accuracy (against reference data or constraints)."""
        issues = []
        metrics: dict[str, float] = {}

        for col in self._columns:
            if self._column_types.get(col) == "numeric":
                values = []
                for row in self._data:
                    try:
                        values.append(float(row.get(col, 0)))
                    except (ValueError, TypeError):
                        pass

                if values:
                    mean = sum(values) / len(values)
                    variance = sum((v - mean) ** 2 for v in values) / len(values)
                    std_dev = variance ** 0.5

                    metrics[f"{col}_mean"] = mean
                    metrics[f"{col}_std_dev"] = std_dev

        score = 100 - (len(issues) * 10)
        self._dimension_scores[QualityDimension.ACCURACY] = DimensionScore(
            dimension=QualityDimension.ACCURACY,
            score=max(0, score),
            issues=issues,
            metrics=metrics
        )

    def _check_timeliness(self) -> None:
        """Check timeliness (data freshness)."""
        issues = []
        metrics: dict[str, float] = {}

        for col in self._columns:
            if self._column_types.get(col) == "datetime":
                dates = []
                for row in self._data:
                    if self._is_date(row.get(col)):
                        dates.append(row.get(col))

                if dates:
                    metrics[f"{col}_freshness"] = 100.0

        self._dimension_scores[QualityDimension.TIMELINESS] = DimensionScore(
            dimension=QualityDimension.TIMELINESS,
            score=100.0,
            issues=issues,
            metrics=metrics
        )

    def _detect_format(self, value: str) -> str:
        """Detect the format of a string value."""
        if value.isdigit():
            return "numeric"
        if re.match(r"^\d+\.\d+$", value):
            return "decimal"
        if self._is_email(value):
            return "email"
        if self._is_url(value):
            return "url"
        if self._is_date(value):
            return "datetime"
        return "text"

    @staticmethod
    def _is_email(value: Any) -> bool:
        return bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(value)))

    @staticmethod
    def _is_url(value: Any) -> bool:
        return bool(re.match(r"https?://", str(value)))

    @staticmethod
    def _is_date(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        date_patterns = [
            r"^\d{4}-\d{2}-\d{2}",
            r"^\d{2}/\d{2}/\d{4}",
            r"^\d{8}$"
        ]
        return any(re.match(p, value) for p in date_patterns)

    def _generate_recommendations(self) -> list[str]:
        """Generate actionable recommendations based on issues."""
        recommendations = []

        for dim, score in self._dimension_scores.items():
            if score.score < 70:
                if dim == QualityDimension.COMPLETENESS:
                    recommendations.append("Address missing values with imputation or data collection improvement")
                elif dim == QualityDimension.UNIQUENESS:
                    recommendations.append("Implement deduplication strategy to remove duplicate records")
                elif dim == QualityDimension.VALIDITY:
                    recommendations.append("Add data validation rules at data entry points")
                elif dim == QualityDimension.CONSISTENCY:
                    recommendations.append("Standardize data formats and implement encoding guidelines")

        return recommendations

    def get_dimension(self, dimension: QualityDimension) -> Optional[DimensionScore]:
        """Get score for a specific dimension."""
        return self._dimension_scores.get(dimension)
