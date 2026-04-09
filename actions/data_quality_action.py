"""
Data Quality Assessment and Validation Module.

Validates data quality dimensions: completeness, consistency,
accuracy, timeliness, and uniqueness. Provides quality scores and reports.

Author: AutoGen
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    COMPLETENESS = auto()
    CONSISTENCY = auto()
    ACCURACY = auto()
    TIMELINESS = auto()
    UNIQUENESS = auto()
    VALIDITY = auto()


@dataclass
class ValidationRule:
    rule_id: str
    field: str
    rule_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    severity: str = "error"


@dataclass
class ValidationResult:
    rule_id: str
    field: str
    passed: bool
    value: Any = None
    error_message: str = ""


@dataclass
class FieldQuality:
    field_name: str
    completeness: float = 1.0
    uniqueness: float = 1.0
    validity: float = 1.0
    consistency: float = 1.0
    accuracy: float = 1.0
    quality_score: float = 1.0
    issues: List[str] = field(default_factory=list)


@dataclass
class DataQualityReport:
    dataset_name: str
    total_records: int = 0
    overall_score: float = 1.0
    dimension_scores: Dict[str, float] = field(default_factory=dict)
    field_quality: List[FieldQuality] = field(default_factory=list)
    validation_results: List[ValidationResult] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)


class ValidationRuleEngine:
    """Executes validation rules against data records."""

    BUILT_IN_RULES = {
        "required": lambda v: v is not None and str(v).strip() != "",
        "email": lambda v: bool(re.match(r"^[\w\.\+\-]+@[\w\.\-]+\.\w+$", str(v))) if v else False,
        "phone": lambda v: bool(re.match(r"^\+?[\d\s\-]{10,}$", str(v))) if v else False,
        "url": lambda v: bool(re.match(r"^https?://", str(v))) if v else False,
        "numeric": lambda v: str(v).isdigit() if v else False,
        "min_length": lambda v, p: len(str(v)) >= int(p) if v else False,
        "max_length": lambda v, p: len(str(v)) <= int(p) if v else False,
        "min_value": lambda v, p: float(v) >= float(p) if v else False,
        "max_value": lambda v, p: float(v) <= float(p) if v else False,
        "pattern": lambda v, p: bool(re.match(p, str(v))) if v else False,
        "in_list": lambda v, p: str(v) in p.split(",") if v else False,
    }

    def __init__(self):
        self._rules: List[ValidationRule] = []

    def add_rule(
        self,
        field: str,
        rule_type: str,
        params: Optional[Dict[str, Any]] = None,
        error_message: str = "",
        severity: str = "error",
    ) -> str:
        import uuid
        rule_id = str(uuid.uuid4())[:8]
        rule = ValidationRule(
            rule_id=rule_id,
            field=field,
            rule_type=rule_type,
            params=params or {},
            error_message=error_message or f"Validation failed for {field} ({rule_type})",
            severity=severity,
        )
        self._rules.append(rule)
        return rule_id

    def validate_record(self, record: Dict[str, Any]) -> List[ValidationResult]:
        results = []
        for rule in self._rules:
            value = record.get(rule.field)
            result = self._apply_rule(rule, value)
            results.append(result)
        return results

    def _apply_rule(self, rule: ValidationRule, value: Any) -> ValidationResult:
        rule_func = self.BUILT_IN_RULES.get(rule.rule_type)
        if not rule_func:
            return ValidationResult(
                rule_id=rule.rule_id,
                field=rule.field,
                passed=False,
                value=value,
                error_message=f"Unknown rule type: {rule.rule_type}",
            )

        try:
            if rule.params:
                passed = rule_func(value, *rule.params.values())
            else:
                passed = rule_func(value)
        except Exception as exc:
            passed = False

        return ValidationResult(
            rule_id=rule.rule_id,
            field=rule.field,
            passed=bool(passed),
            value=value,
            error_message=rule.error_message if not passed else "",
        )


class DataQualityChecker:
    """
    Assesses data quality across multiple dimensions.
    """

    def __init__(self):
        self.rule_engine = ValidationRuleEngine()

    def assess_field_quality(self, field_name: str, values: List[Any]) -> FieldQuality:
        non_null = [v for v in values if v is not None and str(v).strip() != ""]
        completeness = len(non_null) / max(len(values), 1)

        unique_values = set(non_null)
        uniqueness = len(unique_values) / max(len(non_null), 1)

        valid_count = sum(1 for v in non_null if self._is_valid_scalar(v))
        validity = valid_count / max(len(non_null), 1)

        consistency = 1.0
        if len(non_null) > 1:
            types = Counter(type(v).__name__ for v in non_null)
            consistency = types.most_common(1)[0][1] / len(non_null)

        quality_score = (
            completeness * 0.25 +
            uniqueness * 0.25 +
            validity * 0.25 +
            consistency * 0.25
        )

        issues = []
        if completeness < 0.5:
            issues.append(f"Low completeness: {completeness:.1%}")
        if uniqueness < 0.1 and len(non_null) > 10:
            issues.append(f"Low uniqueness: {uniqueness:.1%} (possible duplicates)")
        if validity < 0.8:
            issues.append(f"Validity concerns: {validity:.1%}")

        return FieldQuality(
            field_name=field_name,
            completeness=completeness,
            uniqueness=uniqueness,
            validity=validity,
            consistency=consistency,
            quality_score=quality_score,
            issues=issues,
        )

    def _is_valid_scalar(self, value: Any) -> bool:
        if isinstance(value, (int, float, str, bool)):
            return True
        return False

    def assess_dataset(
        self,
        dataset_name: str,
        records: List[Dict[str, Any]],
        rules: Optional[List[ValidationRule]] = None,
    ) -> DataQualityReport:
        if not records:
            return DataQualityReport(
                dataset_name=dataset_name,
                total_records=0,
                overall_score=0.0,
                issues=["Empty dataset"],
            )

        field_names = list(records[0].keys()) if records else []
        field_values: Dict[str, List[Any]] = {f: [] for f in field_names}

        for record in records:
            for field_name in field_names:
                field_values[field_name].append(record.get(field_name))

        field_quality_list = []
        dimension_totals = {d.name.lower(): 0.0 for d in QualityDimension}

        for field_name, values in field_values.items():
            fq = self.assess_field_quality(field_name, values)
            field_quality_list.append(fq)
            dimension_totals["completeness"] += fq.completeness
            dimension_totals["uniqueness"] += fq.uniqueness
            dimension_totals["validity"] += fq.validity
            dimension_totals["consistency"] += fq.consistency

        dimension_scores = {
            name: score / max(len(field_names), 1)
            for name, score in dimension_totals.items()
        }

        all_results = []
        if rules:
            self.rule_engine._rules = rules
        for record in records:
            all_results.extend(self.rule_engine.validate_record(record))

        overall_score = sum(dimension_scores.values()) / len(dimension_scores) if dimension_scores else 0.0

        issues = []
        if overall_score < 0.7:
            issues.append(f"Overall quality score is below threshold: {overall_score:.1%}")
        failed_validations = [r for r in all_results if not r.passed]
        if failed_validations:
            issues.append(f"{len(failed_validations)} validation rules failed")

        return DataQualityReport(
            dataset_name=dataset_name,
            total_records=len(records),
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            field_quality=field_quality_list,
            validation_results=all_results,
            issues=issues,
        )

    def to_json(self, report: DataQualityReport) -> str:
        return json.dumps({
            "dataset_name": report.dataset_name,
            "total_records": report.total_records,
            "overall_score": report.overall_score,
            "dimension_scores": report.dimension_scores,
            "field_quality": [
                {
                    "field_name": fq.field_name,
                    "quality_score": fq.quality_score,
                    "completeness": fq.completeness,
                    "uniqueness": fq.uniqueness,
                    "validity": fq.validity,
                    "issues": fq.issues,
                }
                for fq in report.field_quality
            ],
            "validation_summary": {
                "total": len(report.validation_results),
                "passed": sum(1 for r in report.validation_results if r.passed),
                "failed": sum(1 for r in report.validation_results if not r.passed),
            },
            "issues": report.issues,
            "generated_at": report.generated_at.isoformat(),
        }, indent=2)
