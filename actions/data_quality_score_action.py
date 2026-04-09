"""
Data Quality Score Action Module

Calculates data quality scores across multiple dimensions:
completeness, accuracy, consistency, timeliness, uniqueness.

Author: RabAi Team
"""

from __future__ import annotations

import re
import time
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import logging

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Dimensions of data quality."""

    COMPLETENESS = auto()
    ACCURACY = auto()
    CONSISTENCY = auto()
    TIMELINESS = auto()
    UNIQUENESS = auto()
    VALIDITY = auto()
    INTEGRITY = auto()


@dataclass
class DimensionScore:
    """Score for a single quality dimension."""

    dimension: QualityDimension
    score: float
    weight: float
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)

    def weighted_score(self) -> float:
        return self.score * self.weight


@dataclass
class QualityScoreResult:
    """Overall data quality score result."""

    overall_score: float
    dimension_scores: List[DimensionScore]
    total_records: int
    valid_records: int
    timestamp: float = field(default_factory=time.time)
    issues: List[str] = field(default_factory=list)


@dataclass
class QualityRule:
    """A quality rule to validate against."""

    name: str
    dimension: QualityDimension
    check_fn: Callable[[List[Dict[str, Any]]], Tuple[float, str]]
    weight: float = 1.0
    description: str = ""


@dataclass
class SchemaProfile:
    """Profile of a data schema."""

    field_name: str
    field_type: str
    null_count: int = 0
    null_rate: float = 0.0
    unique_count: int = 0
    unique_rate: float = 0.0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    top_values: List[Tuple[Any, int]] = field(default_factory=list)


class DataQualityScorer:
    """Calculates data quality scores."""

    def __init__(self) -> None:
        self._rules: List[QualityRule] = []

    def add_rule(self, rule: QualityRule) -> None:
        """Add a quality validation rule."""
        self._rules.append(rule)

    def calculate_completeness(
        self,
        records: List[Dict[str, Any]],
        required_fields: List[str],
    ) -> DimensionScore:
        """Calculate completeness score."""
        if not records:
            return DimensionScore(QualityDimension.COMPLETENESS, 0.0, 1.0, False)

        total_cells = len(records) * len(required_fields)
        null_cells = 0

        for record in records:
            for field_name in required_fields:
                val = record.get(field_name)
                if val is None or val == "" or val == []:
                    null_cells += 1

        score = ((total_cells - null_cells) / total_cells * 100) if total_cells > 0 else 0.0
        return DimensionScore(
            dimension=QualityDimension.COMPLETENESS,
            score=score,
            weight=1.0,
            passed=score >= 95.0,
            details={
                "total_cells": total_cells,
                "null_cells": null_cells,
                "required_fields": required_fields,
            },
        )

    def calculate_uniqueness(
        self,
        records: List[Dict[str, Any]],
        key_fields: List[str],
    ) -> DimensionScore:
        """Calculate uniqueness score based on key fields."""
        if not records:
            return DimensionScore(QualityDimension.UNIQUENESS, 0.0, 1.0, False)

        key_values: Set[Tuple] = set()
        total_keys = len(records)

        for record in records:
            key = tuple(record.get(f) for f in key_fields)
            key_values.add(key)

        unique_count = len(key_values)
        score = (unique_count / total_keys * 100) if total_keys > 0 else 0.0

        return DimensionScore(
            dimension=QualityDimension.UNIQUENESS,
            score=score,
            weight=1.0,
            passed=score >= 90.0,
            details={
                "total_records": total_keys,
                "unique_keys": unique_count,
                "duplicate_count": total_keys - unique_count,
                "key_fields": key_fields,
            },
        )

    def calculate_validity(
        self,
        records: List[Dict[str, Any]],
        field_validators: Dict[str, Callable[[Any], bool]],
    ) -> DimensionScore:
        """Calculate validity score based on field validators."""
        if not records:
            return DimensionScore(QualityDimension.VALIDITY, 0.0, 1.0, False)

        valid_count = 0
        invalid_details: Dict[str, int] = {}

        for record in records:
            record_valid = True
            for field_name, validator in field_validators.items():
                val = record.get(field_name)
                if val is not None and not validator(val):
                    record_valid = False
                    invalid_details[field_name] = invalid_details.get(field_name, 0) + 1

            if record_valid:
                valid_count += 1

        score = (valid_count / len(records) * 100) if records else 0.0

        return DimensionScore(
            dimension=QualityDimension.VALIDITY,
            score=score,
            weight=1.0,
            passed=score >= 90.0,
            details={
                "valid_records": valid_count,
                "invalid_details": invalid_details,
                "field_validators": list(field_validators.keys()),
            },
        )

    def calculate_consistency(
        self,
        records: List[Dict[str, Any]],
        cross_field_rules: List[Callable[[Dict[str, Any]], bool]],
    ) -> DimensionScore:
        """Calculate consistency score based on cross-field rules."""
        if not records or not cross_field_rules:
            return DimensionScore(QualityDimension.CONSISTENCY, 100.0, 1.0, True)

        consistent_count = 0
        for record in records:
            if all(rule(record) for rule in cross_field_rules):
                consistent_count += 1

        score = (consistent_count / len(records) * 100) if records else 0.0

        return DimensionScore(
            dimension=QualityDimension.CONSISTENCY,
            score=score,
            weight=1.0,
            passed=score >= 90.0,
            details={"consistent_records": consistent_count},
        )

    def calculate_timeliness(
        self,
        records: List[Dict[str, Any]],
        timestamp_field: str,
        max_age_seconds: float = 86400.0,
    ) -> DimensionScore:
        """Calculate timeliness score based on record age."""
        if not records or timestamp_field not in records[0]:
            return DimensionScore(QualityDimension.TIMELINESS, 100.0, 1.0, True)

        current_time = time.time()
        stale_count = 0

        for record in records:
            ts = record.get(timestamp_field)
            if isinstance(ts, (int, float)):
                age = current_time - ts
                if age > max_age_seconds:
                    stale_count += 1

        score = ((len(records) - stale_count) / len(records) * 100) if records else 0.0

        return DimensionScore(
            dimension=QualityDimension.TIMELINESS,
            score=score,
            weight=1.0,
            passed=score >= 80.0,
            details={
                "max_age_seconds": max_age_seconds,
                "stale_records": stale_count,
                "fresh_records": len(records) - stale_count,
            },
        )

    def profile_schema(
        self,
        records: List[Dict[str, Any]],
    ) -> List[SchemaProfile]:
        """Generate schema profiles for all fields."""
        if not records:
            return []

        all_fields = set()
        for record in records:
            all_fields.update(record.keys())

        profiles = []
        for field_name in all_fields:
            values = [r.get(field_name) for r in records]
            non_null = [v for v in values if v is not None]

            type_counter = Counter(type(v).__name__ for v in values)
            inferred_type = type_counter.most_common(1)[0][0] if type_counter else "unknown"

            unique_values = set(non_null)
            value_counts = Counter(non_null)
            top_values = value_counts.most_common(5)

            numeric_values = [v for v in non_null if isinstance(v, (int, float))]
            min_val = min(numeric_values) if numeric_values else None
            max_val = max(numeric_values) if numeric_values else None

            profile = SchemaProfile(
                field_name=field_name,
                field_type=inferred_type,
                null_count=len(values) - len(non_null),
                null_rate=(len(values) - len(non_null)) / len(values) if values else 0.0,
                unique_count=len(unique_values),
                unique_rate=len(unique_values) / len(non_null) if non_null else 0.0,
                min_value=min_val,
                max_value=max_val,
                top_values=top_values,
            )
            profiles.append(profile)

        return profiles


class DataQualityAction:
    """Action class for data quality scoring."""

    def __init__(self) -> None:
        self.scorer = DataQualityScorer()

    def score(
        self,
        records: List[Dict[str, Any]],
        required_fields: Optional[List[str]] = None,
        key_fields: Optional[List[str]] = None,
        timestamp_field: Optional[str] = None,
        max_age_seconds: float = 86400.0,
    ) -> QualityScoreResult:
        """Calculate comprehensive data quality score."""
        if not records:
            return QualityScoreResult(
                overall_score=0.0,
                dimension_scores=[],
                total_records=0,
                valid_records=0,
                issues=["No records to score"],
            )

        all_fields = list(records[0].keys()) if records else []
        required = required_fields or all_fields
        keys = key_fields or all_fields[:1] if all_fields else []

        dimensions: List[DimensionScore] = []

        if required:
            dim = self.scorer.calculate_completeness(records, required)
            dimensions.append(dim)

        if keys:
            dim = self.scorer.calculate_uniqueness(records, keys)
            dimensions.append(dim)

        if timestamp_field:
            dim = self.scorer.calculate_timeliness(records, timestamp_field, max_age_seconds)
            dimensions.append(dim)

        total_weight = sum(d.weight for d in dimensions)
        overall = sum(d.weighted_score() for d in dimensions) / total_weight if total_weight > 0 else 0.0

        issues = []
        for dim in dimensions:
            if not dim.passed:
                issues.append(f"{dim.dimension.name} below threshold: {dim.score:.1f}%")

        return QualityScoreResult(
            overall_score=overall,
            dimension_scores=dimensions,
            total_records=len(records),
            valid_records=sum(1 for d in dimensions if d.passed),
            issues=issues,
        )

    def profile(
        self,
        records: List[Dict[str, Any]],
    ) -> List[SchemaProfile]:
        """Generate schema profiles for records."""
        return self.scorer.profile_schema(records)

    def add_custom_rule(
        self,
        name: str,
        dimension: QualityDimension,
        check_fn: Callable[[List[Dict[str, Any]]], Tuple[float, str]],
        weight: float = 1.0,
        description: str = "",
    ) -> None:
        """Add a custom quality rule."""
        self.scorer.add_rule(QualityRule(name, dimension, check_fn, weight, description))
