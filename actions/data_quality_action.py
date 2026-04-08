"""
Data Quality Action Module.

Provides comprehensive data quality assessment and validation capabilities
including completeness checks, accuracy validation, consistency verification,
and quality scoring for datasets and data streams.

Author: RabAi Team
"""

from __future__ import annotations

import json
import re
import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import pandas as pd


class QualityLevel(Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    ACCEPTABLE = "acceptable"
    POOR = "poor"
    CRITICAL = "critical"


class CheckType(Enum):
    """Types of quality checks."""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"


@dataclass
class QualityThreshold:
    """Thresholds for quality checks."""
    min_completeness: float = 0.95
    min_accuracy: float = 0.90
    min_consistency: float = 0.90
    min_uniqueness: float = 0.85
    max_null_ratio: float = 0.05
    max_duplicate_ratio: float = 0.10
    max_age_days: float = 30.0


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""
    check_type: CheckType
    passed: bool
    score: float
    threshold: float
    message: str
    affected_fields: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    overall_score: float
    quality_level: QualityLevel
    total_records: int
    total_fields: int
    checks: List[QualityCheckResult]
    thresholds: QualityThreshold
    recommendations: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary."""
        return {
            "overall_score": self.overall_score,
            "quality_level": self.quality_level.value,
            "total_records": self.total_records,
            "total_fields": self.total_fields,
            "checks": [
                {
                    "check_type": c.check_type.value,
                    "passed": c.passed,
                    "score": c.score,
                    "threshold": c.threshold,
                    "message": c.message,
                    "affected_fields": c.affected_fields,
                    "details": c.details,
                    "timestamp": c.timestamp.isoformat(),
                }
                for c in self.checks
            ],
            "recommendations": self.recommendations,
            "generated_at": self.generated_at.isoformat(),
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Serialize report to JSON."""
        return json.dumps(self.to_dict(), indent=2, default=str)

    def get_failed_checks(self) -> List[QualityCheckResult]:
        """Get all failed quality checks."""
        return [c for c in self.checks if not c.passed]

    def get_warnings(self) -> List[QualityCheckResult]:
        """Get checks that passed but with marginal scores."""
        return [c for c in self.checks if c.passed and c.score < c.threshold * 1.1]


class DataValidator(Protocol):
    """Protocol for custom data validators."""

    def validate(self, value: Any, context: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a value.

        Returns:
            Tuple of (is_valid, message)
        """
        ...


T = TypeVar("T")


class RegexValidator:
    """Validator using regular expressions."""

    def __init__(self, pattern: str, flags: int = 0):
        self.pattern = re.compile(pattern, flags)

    def validate(self, value: Any, context: Dict[str, Any]) -> Tuple[bool, str]:
        if not isinstance(value, str):
            return False, f"Expected string, got {type(value).__name__}"
        if self.pattern.match(value):
            return True, "Valid"
        return False, f"Value does not match pattern {self.pattern.pattern}"


class RangeValidator:
    """Validator for numeric ranges."""

    def __init__(self, min_val: Optional[float] = None, max_val: Optional[float] = None):
        self.min_val = min_val
        self.max_val = max_val

    def validate(self, value: Any, context: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            num_val = float(value)
        except (TypeError, ValueError):
            return False, f"Cannot convert {value} to number"
        if self.min_val is not None and num_val < self.min_val:
            return False, f"Value {num_val} below minimum {self.min_val}"
        if self.max_val is not None and num_val > self.max_val:
            return False, f"Value {num_val} above maximum {self.max_val}"
        return True, "Valid"


class SetValidator:
    """Validator for allowed values."""

    def __init__(self, allowed_values: Set[Any]):
        self.allowed_values = allowed_values

    def validate(self, value: Any, context: Dict[str, Any]) -> Tuple[bool, str]:
        if value in self.allowed_values:
            return True, "Valid"
        return False, f"Value {value} not in allowed set {self.allowed_values}"


class DataQualityEngine:
    """
    Core data quality assessment engine.

    Provides comprehensive quality checks for datasets including
    completeness, accuracy, consistency, uniqueness, and validity.

    Example:
        >>> engine = DataQualityEngine()
        >>> df = pd.DataFrame({"a": [1, 2, None], "b": ["x", "y", "z"]})
        >>> report = engine.assess(df)
        >>> print(report.overall_score)
    """

    def __init__(self, thresholds: Optional[QualityThreshold] = None):
        self.thresholds = thresholds or QualityThreshold()
        self.validators: Dict[str, DataValidator] = {}
        self._custom_checks: List[Callable] = []

    def register_validator(self, field: str, validator: DataValidator) -> None:
        """Register a custom validator for a field."""
        self.validators[field] = validator

    def register_custom_check(self, check: Callable) -> None:
        """Register a custom quality check function."""
        self._custom_checks.append(check)

    def assess(self, data: Union[pd.DataFrame, List[Dict], Dict]) -> QualityReport:
        """
        Perform comprehensive quality assessment on data.

        Args:
            data: Input data (DataFrame, list of dicts, or dict)

        Returns:
            QualityReport with detailed findings
        """
        df = self._to_dataframe(data)
        checks = []

        checks.append(self._check_completeness(df))
        checks.append(self._check_uniqueness(df))
        checks.append(self._check_validity(df))
        checks.append(self._check_consistency(df))

        if self._custom_checks:
            for custom_check in self._custom_checks:
                try:
                    result = custom_check(df)
                    if isinstance(result, QualityCheckResult):
                        checks.append(result)
                except Exception as e:
                    checks.append(
                        QualityCheckResult(
                            check_type=CheckType.VALIDITY,
                            passed=False,
                            score=0.0,
                            threshold=0.0,
                            message=f"Custom check failed: {str(e)}",
                        )
                    )

        overall_score = statistics.mean([c.score for c in checks]) if checks else 0.0
        quality_level = self._score_to_level(overall_score)
        recommendations = self._generate_recommendations(checks)

        return QualityReport(
            overall_score=overall_score,
            quality_level=quality_level,
            total_records=len(df),
            total_fields=len(df.columns),
            checks=checks,
            thresholds=self.thresholds,
            recommendations=recommendations,
        )

    def _to_dataframe(self, data: Union[pd.DataFrame, List[Dict], Dict]) -> pd.DataFrame:
        """Convert input to DataFrame."""
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            return pd.DataFrame([data])
        raise ValueError(f"Unsupported data type: {type(data).__name__}")

    def _check_completeness(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check data completeness (null/missing values)."""
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        null_ratio = null_cells / total_cells if total_cells > 0 else 0.0
        completeness_score = 1.0 - null_ratio

        null_by_column = df.isnull().sum()
        affected_fields = null_by_column[null_by_column > 0].index.tolist()

        passed = completeness_score >= self.thresholds.min_completeness

        return QualityCheckResult(
            check_type=CheckType.COMPLETENESS,
            passed=passed,
            score=completeness_score,
            threshold=self.thresholds.min_completeness,
            message=f"Completeness: {(completeness_score * 100):.2f}%",
            affected_fields=affected_fields,
            details={
                "null_count": int(null_cells),
                "total_cells": int(total_cells),
                "null_ratio": null_ratio,
                "null_by_column": null_by_column.to_dict(),
            },
        )

    def _check_uniqueness(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check data uniqueness (duplicate records)."""
        total_rows = len(df)
        duplicate_rows = df.duplicated().sum()
        duplicate_ratio = duplicate_rows / total_rows if total_rows > 0 else 0.0
        uniqueness_score = 1.0 - duplicate_ratio

        passed = uniqueness_score >= (1.0 - self.thresholds.max_duplicate_ratio)

        duplicate_cols = []
        for col in df.columns:
            col_dupes = df[col].duplicated().sum()
            if col_dupes > 0:
                duplicate_cols.append({"column": col, "duplicates": int(col_dupes)})

        return QualityCheckResult(
            check_type=CheckType.UNIQUENESS,
            passed=passed,
            score=uniqueness_score,
            threshold=1.0 - self.thresholds.max_duplicate_ratio,
            message=f"Uniqueness: {(uniqueness_score * 100):.2f}%",
            details={
                "duplicate_rows": int(duplicate_rows),
                "total_rows": total_rows,
                "duplicate_ratio": duplicate_ratio,
                "duplicate_columns": duplicate_cols,
            },
        )

    def _check_validity(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check data validity using registered validators."""
        total_validations = 0
        passed_validations = 0
        invalid_fields: Dict[str, List[str]] = {}

        for field, validator in self.validators.items():
            if field not in df.columns:
                continue
            for idx, value in df[field].items():
                if pd.isnull(value):
                    continue
                total_validations += 1
                is_valid, _ = validator.validate(value, {"field": field, "index": idx})
                if is_valid:
                    passed_validations += 1
                else:
                    if field not in invalid_fields:
                        invalid_fields[field] = []
                    invalid_fields[field].append(str(value))

        validity_score = (
            passed_validations / total_validations if total_validations > 0 else 1.0
        )
        passed = validity_score >= self.thresholds.min_accuracy

        return QualityCheckResult(
            check_type=CheckType.VALIDITY,
            passed=passed,
            score=validity_score,
            threshold=self.thresholds.min_accuracy,
            message=f"Validity: {(validity_score * 100):.2f}%",
            affected_fields=list(invalid_fields.keys()),
            details={
                "total_validations": total_validations,
                "passed_validations": passed_validations,
                "invalid_fields": invalid_fields,
            },
        )

    def _check_consistency(self, df: pd.DataFrame) -> QualityCheckResult:
        """Check data consistency across related fields."""
        consistency_scores = []

        for col in df.select_dtypes(include=["number"]).columns:
            col_data = df[col].dropna()
            if len(col_data) < 2:
                continue
            mean_val = col_data.mean()
            std_val = col_data.std()
            if std_val == 0:
                consistency_scores.append(1.0)
            else:
                z_scores = ((col_data - mean_val) / std_val).abs()
                consistent_ratio = (z_scores < 3).mean()
                consistency_scores.append(consistent_ratio)

        for col in df.select_dtypes(include=["object"]).columns:
            value_counts = df[col].value_counts(normalize=True)
            top_freq = value_counts.iloc[0] if len(value_counts) > 0 else 1.0
            consistency_scores.append(top_freq)

        overall_consistency = statistics.mean(consistency_scores) if consistency_scores else 1.0
        passed = overall_consistency >= self.thresholds.min_consistency

        return QualityCheckResult(
            check_type=CheckType.CONSISTENCY,
            passed=passed,
            score=overall_consistency,
            threshold=self.thresholds.min_consistency,
            message=f"Consistency: {(overall_consistency * 100):.2f}%",
            details={
                "consistency_scores": consistency_scores,
                "num_columns_checked": len(consistency_scores),
            },
        )

    def _score_to_level(self, score: float) -> QualityLevel:
        """Convert numeric score to quality level."""
        if score >= 0.95:
            return QualityLevel.EXCELLENT
        elif score >= 0.85:
            return QualityLevel.GOOD
        elif score >= 0.70:
            return QualityLevel.ACCEPTABLE
        elif score >= 0.50:
            return QualityLevel.POOR
        else:
            return QualityLevel.CRITICAL

    def _generate_recommendations(self, checks: List[QualityCheckResult]) -> List[str]:
        """Generate actionable recommendations based on failed checks."""
        recommendations = []
        for check in checks:
            if not check.passed:
                if check.check_type == CheckType.COMPLETENESS:
                    recommendations.append(
                        f"Address missing values in fields: {', '.join(check.affected_fields)}"
                    )
                elif check.check_type == CheckType.UNIQUENESS:
                    recommendations.append(
                        "Review and remove duplicate records to improve data uniqueness"
                    )
                elif check.check_type == CheckType.VALIDITY:
                    recommendations.append(
                        f"Fix invalid values in fields: {', '.join(check.affected_fields)}"
                    )
                elif check.check_type == CheckType.CONSISTENCY:
                    recommendations.append(
                        "Standardize data formats and value ranges across related fields"
                    )
        return recommendations


def create_data_quality_action(config: Optional[Dict[str, Any]] = None) -> DataQualityEngine:
    """
    Factory function to create a configured data quality action.

    Args:
        config: Optional configuration dict with thresholds and validators

    Returns:
        Configured DataQualityEngine instance
    """
    if config is None:
        config = {}

    thresholds_config = config.get("thresholds", {})
    thresholds = QualityThreshold(
        min_completeness=thresholds_config.get("min_completeness", 0.95),
        min_accuracy=thresholds_config.get("min_accuracy", 0.90),
        min_consistency=thresholds_config.get("min_consistency", 0.90),
        min_uniqueness=thresholds_config.get("min_uniqueness", 0.85),
        max_null_ratio=thresholds_config.get("max_null_ratio", 0.05),
        max_duplicate_ratio=thresholds_config.get("max_duplicate_ratio", 0.10),
        max_age_days=thresholds_config.get("max_age_days", 30.0),
    )

    engine = DataQualityEngine(thresholds=thresholds)

    validators_config = config.get("validators", {})
    for field, val_config in validators_config.items():
        validator_type = val_config.get("type")
        if validator_type == "regex":
            engine.register_validator(
                field, RegexValidator(val_config["pattern"])
            )
        elif validator_type == "range":
            engine.register_validator(
                field,
                RangeValidator(
                    min_val=val_config.get("min"),
                    max_val=val_config.get("max"),
                ),
            )
        elif validator_type == "set":
            engine.register_validator(
                field, SetValidator(set(val_config["values"]))
            )

    return engine
