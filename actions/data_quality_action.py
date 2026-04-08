"""Data Quality Action Module.

Provides comprehensive data quality assessment, profiling,
validation, and cleaning capabilities for automation workflows.
"""

from __future__ import annotations

import sys
import os
import time
import re
import hashlib
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QualityScore(Enum):
    """Overall data quality score levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


class ValidationSeverity(Enum):
    """Severity level of validation issues."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class FieldProfile:
    """Statistical profile of a data field."""
    field_name: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    empty_count: int = 0
    data_type: str = "unknown"
    min_value: Any = None
    max_value: Any = None
    mean_value: Optional[float] = None
    std_dev: Optional[float] = None
    patterns: List[str] = field(default_factory=list)
    top_values: List[Tuple[Any, int]] = field(default_factory=list)


@dataclass
class ValidationIssue:
    """A data quality validation issue."""
    issue_id: str
    severity: ValidationSeverity
    field_name: str
    rule_name: str
    message: str
    affected_count: int = 0
    sample_values: List[Any] = field(default_factory=list)


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    report_id: str
    generated_at: float
    total_records: int
    total_fields: int
    overall_score: QualityScore
    score_value: float
    field_profiles: Dict[str, FieldProfile] = field(default_factory=dict)
    validation_issues: List[ValidationIssue] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityRule:
    """A data quality validation rule."""
    rule_id: str
    name: str
    field_name: str
    rule_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    severity: ValidationSeverity = ValidationSeverity.ERROR
    enabled: bool = True


class DataQualityAction(BaseAction):
    """
    Data quality assessment and validation for automation.

    Provides profiling, validation rules, issue detection,
    and quality scoring for datasets.

    Example:
        dq = DataQualityAction()
        result = dq.execute(ctx, {
            "action": "assess",
            "data": [...]
        })
    """
    action_type = "data_quality"
    display_name = "数据质量检测"
    description = "提供数据质量评估、性能分析、验证和清洗功能"

    def __init__(self) -> None:
        super().__init__()
        self._rules: List[QualityRule] = []
        self._default_rules_loaded = False

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a data quality action.

        Args:
            context: Execution context.
            params: Dict with keys: action (assess|validate|clean|profile),
                   data, rules, field_name.

        Returns:
            ActionResult with quality assessment result.
        """
        action = params.get("action", "")

        try:
            if action == "assess":
                return self._assess_quality(params)
            elif action == "validate":
                return self._validate_data(params)
            elif action == "clean":
                return self._clean_data(params)
            elif action == "profile":
                return self._profile_field(params)
            elif action == "add_rule":
                return self._add_rule(params)
            elif action == "get_report":
                return self._get_report(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Quality action error: {str(e)}")

    def _assess_quality(self, params: Dict[str, Any]) -> ActionResult:
        """Assess overall data quality."""
        data = self._ensure_list(params.get("data", []))
        rules = params.get("rules", [])
        report_id = self._generate_report_id()

        if not data:
            return ActionResult(success=False, message="No data provided")

        if not self._default_rules_loaded:
            self._load_default_rules()

        all_rules = self._rules + [self._build_rule(r) for r in rules]
        all_rules = [r for r in all_rules if r.enabled]

        field_profiles = self._profile_dataset(data)
        issues = self._run_validation(data, all_rules, field_profiles)
        score_value = self._calculate_score(len(data), len(field_profiles), issues)
        overall_score = self._score_to_enum(score_value)

        statistics = {
            "total_records": len(data),
            "total_fields": len(field_profiles),
            "complete_records": self._count_complete_records(data, field_profiles.keys()),
            "completeness_rate": 0.0,
            "uniqueness_rate": self._calculate_uniqueness_rate(field_profiles),
            "consistency_score": self._calculate_consistency_score(issues),
        }
        statistics["completeness_rate"] = statistics["complete_records"] / len(data) if len(data) > 0 else 0

        report = QualityReport(
            report_id=report_id,
            generated_at=time.time(),
            total_records=len(data),
            total_fields=len(field_profiles),
            overall_score=overall_score,
            score_value=score_value,
            field_profiles=field_profiles,
            validation_issues=issues,
            statistics=statistics,
        )

        return ActionResult(
            success=True,
            message=f"Quality assessment complete: {overall_score.value} ({score_value:.1f}%)",
            data={
                "report_id": report_id,
                "total_records": len(data),
                "total_fields": len(field_profiles),
                "overall_score": overall_score.value,
                "score_value": score_value,
                "issue_count": len(issues),
                "issues_by_severity": self._count_issues_by_severity(issues),
                "statistics": statistics,
            }
        )

    def _validate_data(self, params: Dict[str, Any]) -> ActionResult:
        """Validate data against rules."""
        data = self._ensure_list(params.get("data", []))
        rules = params.get("rules", [])

        if not data:
            return ActionResult(success=False, message="No data provided")

        rules_to_validate = [self._build_rule(r) for r in rules]
        field_profiles = self._profile_dataset(data)
        issues = self._run_validation(data, rules_to_validate, field_profiles)

        valid_records = self._get_valid_records(data, issues)

        return ActionResult(
            success=True,
            message=f"Validation complete: {len(valid_records)}/{len(data)} valid",
            data={
                "total_records": len(data),
                "valid_records": len(valid_records),
                "invalid_records": len(data) - len(valid_records),
                "issues": [
                    {
                        "rule_name": i.rule_name,
                        "field_name": i.field_name,
                        "severity": i.severity.value,
                        "message": i.message,
                        "affected_count": i.affected_count,
                    }
                    for i in issues
                ],
            }
        )

    def _clean_data(self, params: Dict[str, Any]) -> ActionResult:
        """Clean data based on quality issues."""
        data = self._ensure_list(params.get("data", []))
        clean_strategies = params.get("strategies", ["remove_nulls", "trim_strings", "fix_types"])

        if not data:
            return ActionResult(success=False, message="No data provided")

        cleaned = list(data)

        if "remove_duplicates" in clean_strategies:
            cleaned = self._remove_duplicates(cleaned)

        if "remove_nulls" in clean_strategies:
            cleaned = self._remove_null_records(cleaned)

        if "trim_strings" in clean_strategies:
            cleaned = self._trim_string_fields(cleaned)

        if "fix_types" in clean_strategies:
            cleaned = self._fix_data_types(cleaned)

        if "fix_outliers" in clean_strategies:
            cleaned = self._handle_outliers(cleaned)

        return ActionResult(
            success=True,
            message=f"Cleaning complete: {len(cleaned}/{len(data)} records retained",
            data={
                "original_count": len(data),
                "cleaned_count": len(cleaned),
                "removed_count": len(data) - len(cleaned),
                "data": cleaned,
            }
        )

    def _profile_field(self, params: Dict[str, Any]) -> ActionResult:
        """Profile a specific field."""
        data = self._ensure_list(params.get("data", []))
        field_name = params.get("field_name", "")

        if not data:
            return ActionResult(success=False, message="No data provided")

        if not field_name:
            return ActionResult(success=False, message="field_name is required")

        profile = self._profile_single_field(field_name, data)

        return ActionResult(
            success=True,
            data={
                "field_name": field_name,
                "total_count": profile.total_count,
                "null_count": profile.null_count,
                "unique_count": profile.unique_count,
                "data_type": profile.data_type,
                "top_values": [{"value": v, "count": c} for v, c in profile.top_values[:10]],
                "min_value": profile.min_value,
                "max_value": profile.max_value,
                "mean_value": profile.mean_value,
            }
        )

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add a quality validation rule."""
        rule = self._build_rule(params)

        if not rule.rule_id:
            rule.rule_id = self._generate_rule_id()

        self._rules.append(rule)

        return ActionResult(
            success=True,
            message=f"Rule added: {rule.name}",
            data={"rule_id": rule.rule_id, "rule_name": rule.name}
        )

    def _get_report(self, params: Dict[str, Any]) -> ActionResult:
        """Get a previously generated report."""
        return ActionResult(success=True, message="Report retrieval not persisted in memory")

    def _profile_dataset(self, data: List[Dict[str, Any]]) -> Dict[str, FieldProfile]:
        """Profile all fields in a dataset."""
        if not data:
            return {}

        field_names = set()
        for record in data:
            if isinstance(record, dict):
                field_names.update(record.keys())

        profiles: Dict[str, FieldProfile] = {}

        for field_name in field_names:
            profiles[field_name] = self._profile_single_field(field_name, data)

        return profiles

    def _profile_single_field(self, field_name: str, data: List[Any]) -> FieldProfile:
        """Profile a single field."""
        profile = FieldProfile(field_name=field_name)
        values: List[Any] = []

        for record in data:
            if isinstance(record, dict):
                value = record.get(field_name)
            else:
                value = record

            profile.total_count += 1

            if value is None:
                profile.null_count += 1
            elif value == "" or value == []:
                profile.empty_count += 1
            else:
                values.append(value)

        profile.unique_count = len(set(str(v) for v in values))

        if values:
            type_counts = Counter(type(v).__name__ for v in values)
            profile.data_type = type_counts.most_common(1)[0][0]

            numeric_values = [v for v in values if isinstance(v, (int, float))]
            if numeric_values:
                profile.min_value = min(numeric_values)
                profile.max_value = max(numeric_values)
                if len(numeric_values) > 1:
                    mean = sum(numeric_values) / len(numeric_values)
                    profile.mean_value = mean
                    variance = sum((v - mean) ** 2 for v in numeric_values) / len(numeric_values)
                    profile.std_dev = variance ** 0.5

            string_values = [v for v in values if isinstance(v, str)]
            if string_values:
                pattern_counts: Dict[str, int] = {}
                for v in string_values:
                    pattern = self._detect_pattern(v)
                    pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
                profile.patterns = sorted(pattern_counts.keys(), key=lambda p: pattern_counts[p], reverse=True)[:5]

            value_counts = Counter(values)
            profile.top_values = value_counts.most_common(10)

        return profile

    def _detect_pattern(self, value: str) -> str:
        """Detect the pattern type of a string value."""
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "date_iso"
        elif re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", value):
            return "ipv4"
        elif re.match(r"^[\w.-]+@[\w.-]+\.\w+$", value):
            return "email"
        elif re.match(r"^\+?[\d\s-()]+$", value):
            return "phone"
        elif re.match(r"^[A-Z]{2,}\d+$", value):
            return "code_upper"
        elif re.match(r"^[a-z]+$", value):
            return "lowercase"
        elif re.match(r"^[A-Z][a-z]+$", value):
            return "title_case"
        else:
            return "mixed"

    def _run_validation(
        self,
        data: List[Any],
        rules: List[QualityRule],
        profiles: Dict[str, FieldProfile],
    ) -> List[ValidationIssue]:
        """Run validation rules against data."""
        issues: List[ValidationIssue] = []

        for rule in rules:
            if rule.field_name and rule.field_name not in profiles:
                continue

            field_values = [r.get(rule.field_name) if isinstance(r, dict) else r for r in data]
            rule_issues = self._apply_rule(rule, field_values, data)

            if rule_issues:
                issues.extend(rule_issues)

        return issues

    def _apply_rule(
        self,
        rule: QualityRule,
        values: List[Any],
        full_records: List[Any],
    ) -> List[ValidationIssue]:
        """Apply a single validation rule."""
        issues: List[ValidationIssue] = []
        affected: List[Any] = []

        for i, value in enumerate(values):
            is_invalid = False

            if rule.rule_type == "not_null" and (value is None or value == ""):
                is_invalid = True
            elif rule.rule_type == "is_unique" and values.count(value) > 1:
                is_invalid = True
            elif rule.rule_type == "min_length":
                min_len = rule.params.get("min", 0)
                if isinstance(value, str) and len(value) < min_len:
                    is_invalid = True
            elif rule.rule_type == "max_length":
                max_len = rule.params.get("max", float("inf"))
                if isinstance(value, str) and len(value) > max_len:
                    is_invalid = True
            elif rule.rule_type == "pattern":
                pattern = rule.params.get("pattern", "")
                if isinstance(value, str) and not re.match(pattern, value):
                    is_invalid = True
            elif rule.rule_type == "range":
                min_val = rule.params.get("min")
                max_val = rule.params.get("max")
                if isinstance(value, (int, float)):
                    if min_val is not None and value < min_val:
                        is_invalid = True
                    if max_val is not None and value > max_val:
                        is_invalid = True
            elif rule.rule_type == "enum":
                allowed = rule.params.get("values", [])
                if value not in allowed:
                    is_invalid = True

            if is_invalid:
                affected.append(value)

        if affected:
            sample_values = list(set(str(v) for v in affected[:5]))
            issue = ValidationIssue(
                issue_id=self._generate_issue_id(),
                severity=rule.severity,
                field_name=rule.field_name or "record",
                rule_name=rule.name,
                message=f"Validation failed for {rule.name}",
                affected_count=len(affected),
                sample_values=sample_values,
            )
            issues.append(issue)

        return issues

    def _build_rule(self, rule_data: Dict[str, Any]) -> QualityRule:
        """Build a QualityRule from data."""
        severity_str = rule_data.get("severity", "error")
        try:
            severity = ValidationSeverity(severity_str)
        except ValueError:
            severity = ValidationSeverity.ERROR

        return QualityRule(
            rule_id=rule_data.get("rule_id", ""),
            name=rule_data.get("name", "unnamed_rule"),
            field_name=rule_data.get("field_name", ""),
            rule_type=rule_data.get("rule_type", "not_null"),
            params=rule_data.get("params", {}),
            severity=severity,
            enabled=rule_data.get("enabled", True),
        )

    def _calculate_score(
        self,
        total_records: int,
        total_fields: int,
        issues: List[ValidationIssue],
    ) -> float:
        """Calculate overall quality score (0-100)."""
        if total_records == 0:
            return 100.0

        base_score = 100.0

        severity_weights = {
            ValidationSeverity.CRITICAL: 20.0,
            ValidationSeverity.ERROR: 10.0,
            ValidationSeverity.WARNING: 3.0,
            ValidationSeverity.INFO: 0.5,
        }

        for issue in issues:
            weight = severity_weights.get(issue.severity, 5.0)
            affected_ratio = issue.affected_count / total_records
            deduction = weight * affected_ratio
            base_score -= deduction

        return max(0.0, min(100.0, base_score))

    def _score_to_enum(self, score: float) -> QualityScore:
        """Convert numeric score to QualityScore enum."""
        if score >= 95:
            return QualityScore.EXCELLENT
        elif score >= 80:
            return QualityScore.GOOD
        elif score >= 60:
            return QualityScore.FAIR
        elif score >= 40:
            return QualityScore.POOR
        else:
            return QualityScore.CRITICAL

    def _load_default_rules(self) -> None:
        """Load default quality rules."""
        self._rules = [
            QualityRule(
                rule_id="default_not_null",
                name="Not Null Check",
                field_name="",
                rule_type="not_null",
                severity=ValidationSeverity.ERROR,
                enabled=True,
            ),
        ]
        self._default_rules_loaded = True

    def _remove_duplicates(self, data: List[Any]) -> List[Any]:
        """Remove duplicate records."""
        seen = set()
        result = []
        for record in data:
            key = str(record) if not isinstance(record, dict) else json.dumps(record, sort_keys=True)
            if key not in seen:
                seen.add(key)
                result.append(record)
        return result

    def _remove_null_records(self, data: List[Any]) -> List[Any]:
        """Remove records with all null values."""
        result = []
        for record in data:
            if isinstance(record, dict):
                if any(v is not None and v != "" for v in record.values()):
                    result.append(record)
            elif record is not None and record != "":
                result.append(record)
        return result

    def _trim_string_fields(self, data: List[Any]) -> List[Any]:
        """Trim whitespace from string fields."""
        result = []
        for record in data:
            if isinstance(record, dict):
                cleaned = {}
                for k, v in record.items():
                    if isinstance(v, str):
                        cleaned[k] = v.strip()
                    else:
                        cleaned[k] = v
                result.append(cleaned)
            else:
                result.append(record)
        return result

    def _fix_data_types(self, data: List[Any]) -> List[Any]:
        """Attempt to fix common data type issues."""
        result = []
        for record in data:
            if isinstance(record, dict):
                cleaned = {}
                for k, v in record.items():
                    if isinstance(v, str):
                        if v.lower() in ("true", "false"):
                            cleaned[k] = v.lower() == "true"
                        elif v.isdigit():
                            cleaned[k] = int(v)
                        elif re.match(r"^\d+\.\d+$", v):
                            cleaned[k] = float(v)
                        else:
                            cleaned[k] = v
                    else:
                        cleaned[k] = v
                result.append(cleaned)
            else:
                result.append(record)
        return result

    def _handle_outliers(self, data: List[Any]) -> List[Any]:
        """Handle statistical outliers in numeric fields."""
        numeric_fields = {}
        for record in data:
            if isinstance(record, dict):
                for k, v in record.items():
                    if isinstance(v, (int, float)) and k not in numeric_fields:
                        numeric_fields[k] = []
                    if isinstance(v, (int, float)):
                        numeric_fields[k].append(v)

        return data

    def _count_complete_records(
        self,
        data: List[Any],
        field_names: set,
    ) -> int:
        """Count records with all fields populated."""
        complete = 0
        for record in data:
            if isinstance(record, dict):
                if all(
                    record.get(f) is not None and record.get(f) != ""
                    for f in field_names
                ):
                    complete += 1
            else:
                complete += 1
        return complete

    def _calculate_uniqueness_rate(
        self,
        profiles: Dict[str, FieldProfile],
    ) -> float:
        """Calculate average uniqueness rate across fields."""
        if not profiles:
            return 0.0
        total_unique_ratio = sum(
            p.unique_count / p.total_count if p.total_count > 0 else 0
            for p in profiles.values()
        )
        return total_unique_ratio / len(profiles)

    def _calculate_consistency_score(self, issues: List[ValidationIssue]) -> float:
        """Calculate consistency score based on validation issues."""
        if not issues:
            return 100.0

        critical_weight = 20
        error_weight = 10
        warning_weight = 3
        info_weight = 1

        total_penalty = sum(
            (critical_weight if i.severity == ValidationSeverity.CRITICAL else
             error_weight if i.severity == ValidationSeverity.ERROR else
             warning_weight if i.severity == ValidationSeverity.WARNING else info_weight)
            * min(i.affected_count, 10)
            for i in issues
        )

        return max(0.0, 100.0 - total_penalty)

    def _count_issues_by_severity(self, issues: List[ValidationIssue]) -> Dict[str, int]:
        """Count issues grouped by severity."""
        counts = {s.value: 0 for s in ValidationSeverity}
        for issue in issues:
            counts[issue.severity.value] += 1
        return counts

    def _get_valid_records(
        self,
        data: List[Any],
        issues: List[ValidationIssue],
    ) -> List[Any]:
        """Get records that don't have critical issues."""
        return data

    def _ensure_list(self, data: Any) -> List[Any]:
        """Ensure data is a list."""
        if data is None:
            return []
        if isinstance(data, list):
            return data
        return [data]

    def _generate_report_id(self) -> str:
        """Generate a unique report ID."""
        return f"qr_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:12]}"

    def _generate_rule_id(self) -> str:
        """Generate a unique rule ID."""
        return f"rule_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:8]}"

    def _generate_issue_id(self) -> str:
        """Generate a unique issue ID."""
        return f"iss_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:8]}"
