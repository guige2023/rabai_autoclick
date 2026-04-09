"""
Data Quality Action Module.

Provides data quality checking and validation including completeness,
consistency, validity checks, and anomaly detection for data pipelines.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
from collections import Counter
from datetime import datetime


class QualityDimension(Enum):
    """Data quality dimensions."""
    COMPLETENESS = "completeness"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    ACCURACY = "accuracy"


@dataclass
class QualityRule:
    """Represents a data quality rule."""
    name: str
    dimension: QualityDimension
    check_fn: Callable[[List[Dict]], Dict[str, Any]]
    description: str = ""
    severity: str = "error"  # error, warning, info


@dataclass
class QualityResult:
    """Result of a quality check."""
    rule_name: str
    passed: bool
    dimension: QualityDimension
    passed_count: int
    failed_count: int
    total_count: int
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)


@dataclass
class DataQualityReport:
    """Complete data quality report."""
    dataset_name: str
    total_records: int
    checked_at: datetime
    results: List[QualityResult]
    overall_score: float
    dimensions: Dict[str, float]
    issues: List[Dict[str, Any]]


class CompletenessChecker:
    """
    Checks data completeness - presence of required values.
    
    Example:
        checker = CompletenessChecker()
        checker.add_required_field("user_id")
        checker.add_required_field("email")
        
        result = checker.check(data_records)
    """
    
    def __init__(self):
        self.required_fields: Set[str] = set()
        self.nullable_fields: Set[str] = set()
    
    def add_required_field(self, field: str) -> "CompletenessChecker":
        """Add a required field."""
        self.required_fields.add(field)
        return self
    
    def add_nullable_field(self, field: str) -> "CompletenessChecker":
        """Add a nullable field (not required)."""
        self.nullable_fields.add(field)
        return self
    
    def check(self, data: List[Dict]) -> QualityResult:
        """Check completeness of data."""
        total = len(data)
        failed_checks = []
        field_stats = {}
        
        for field in self.required_fields:
            null_count = sum(1 for row in data if row.get(field) is None)
            empty_count = sum(1 for row in data if row.get(field) in ("", []))
            
            failed = null_count + empty_count
            field_stats[field] = {
                "null_count": null_count,
                "empty_count": empty_count,
                "completeness": 1.0 - (failed / max(1, total))
            }
            
            if failed > 0:
                failed_checks.append(f"Field '{field}' has {failed} incomplete values")
        
        passed = total - len(set(f for f in failed_checks for _ in range(1)))
        
        return QualityResult(
            rule_name="completeness_check",
            passed=len(failed_checks) == 0,
            dimension=QualityDimension.COMPLETENESS,
            passed_count=total,
            failed_count=len(failed_checks),
            total_count=total,
            details={"field_stats": field_stats},
            errors=failed_checks
        )


class ConsistencyChecker:
    """
    Checks data consistency across records.
    
    Example:
        checker = ConsistencyChecker()
        checker.add_rule("status", ["pending", "processing", "completed"])
        
        result = checker.check(data_records)
    """
    
    def __init__(self):
        self.field_values: Dict[str, Set[Any]] = {}
        self.patterns: Dict[str, str] = {}
    
    def add_allowed_values(self, field: str, values: List[Any]) -> "ConsistencyChecker":
        """Set allowed values for a field."""
        self.field_values[field] = set(values)
        return self
    
    def add_pattern(self, field: str, pattern: str) -> "ConsistencyChecker":
        """Add regex pattern for field validation."""
        self.patterns[field] = pattern
        return self
    
    def check(self, data: List[Dict]) -> QualityResult:
        """Check consistency of data."""
        total = len(data)
        errors = []
        field_violations = {}
        
        for field, allowed in self.field_values.items():
            violations = []
            for i, row in enumerate(data):
                value = row.get(field)
                if value is not None and value not in allowed:
                    violations.append(i)
            
            if violations:
                field_violations[field] = {
                    "violation_count": len(violations),
                    "allowed_values": list(allowed),
                    "sample_violations": violations[:5]
                }
                errors.append(f"Field '{field}' has {len(violations)} invalid values")
        
        for field, pattern in self.patterns.items():
            regex = re.compile(pattern)
            violations = []
            for i, row in enumerate(data):
                value = row.get(field)
                if value is not None and not regex.match(str(value)):
                    violations.append(i)
            
            if violations:
                field_violations[field] = {
                    "violation_count": len(violations),
                    "pattern": pattern,
                    "sample_violations": violations[:5]
                }
                errors.append(f"Field '{field}' has {len(violations)} values not matching pattern")
        
        return QualityResult(
            rule_name="consistency_check",
            passed=len(errors) == 0,
            dimension=QualityDimension.CONSISTENCY,
            passed_count=total - len(errors),
            failed_count=len(errors),
            total_count=total,
            details={"field_violations": field_violations},
            errors=errors
        )


class UniquenessChecker:
    """
    Checks data uniqueness constraints.
    
    Example:
        checker = UniquenessChecker()
        checker.add_unique_field("user_id")
        checker.add_composite_key(["email", "tenant_id"])
        
        result = checker.check(data_records)
    """
    
    def __init__(self):
        self.unique_fields: Set[str] = set()
        self.composite_keys: List[List[str]] = []
    
    def add_unique_field(self, field: str) -> "UniquenessChecker":
        """Add a unique field constraint."""
        self.unique_fields.add(field)
        return self
    
    def add_composite_key(self, fields: List[str]) -> "UniquenessChecker":
        """Add a composite key for uniqueness."""
        self.composite_keys.append(fields)
        return self
    
    def check(self, data: List[Dict]) -> QualityResult:
        """Check uniqueness constraints."""
        total = len(data)
        errors = []
        duplicate_info = {}
        
        # Check single fields
        for field in self.unique_fields:
            values = [row.get(field) for row in data]
            duplicates = self._find_duplicates(values)
            
            if duplicates:
                duplicate_info[field] = {
                    "duplicate_count": len(duplicates),
                    "duplicate_values": list(duplicates)[:10]
                }
                errors.append(f"Field '{field}' has {len(duplicates)} duplicate values")
        
        # Check composite keys
        for i, key_fields in enumerate(self.composite_keys):
            key_values = [tuple(row.get(f) for f in key_fields) for row in data]
            duplicates = self._find_duplicates(key_values)
            
            if duplicates:
                duplicate_info[f"composite_{i}"] = {
                    "fields": key_fields,
                    "duplicate_count": len(duplicates)
                }
                errors.append(f"Composite key {key_fields} has {len(duplicates)} duplicates")
        
        return QualityResult(
            rule_name="uniqueness_check",
            passed=len(errors) == 0,
            dimension=QualityDimension.UNIQUENESS,
            passed_count=total - len(errors),
            failed_count=len(errors),
            total_count=total,
            details={"duplicate_info": duplicate_info},
            errors=errors
        )
    
    def _find_duplicates(self, values: List[Any]) -> Set[Any]:
        """Find duplicate values."""
        seen: Set[Any] = set()
        duplicates: Set[Any] = set()
        
        for v in values:
            if v in seen:
                duplicates.add(v)
            seen.add(v)
        
        return duplicates


class ValidityChecker:
    """
    Checks data validity with type and format validation.
    
    Example:
        checker = ValidityChecker()
        checker.add_type_check("age", int, min_value=0, max_value=150)
        checker.add_format_check("email", r"^[^@]+@[^@]+$")
        
        result = checker.check(data_records)
    """
    
    def __init__(self):
        self.type_checks: List[Tuple[str, type, Dict]] = []
        self.format_checks: Dict[str, str] = {}
    
    def add_type_check(
        self,
        field: str,
        expected_type: type,
        **constraints
    ) -> "ValidityChecker":
        """Add type-based validation."""
        self.type_checks.append((field, expected_type, constraints))
        return self
    
    def add_format_check(self, field: str, pattern: str) -> "ValidityChecker":
        """Add regex format validation."""
        self.format_checks[field] = pattern
        return self
    
    def check(self, data: List[Dict]) -> QualityResult:
        """Check data validity."""
        total = len(data)
        errors = []
        violation_details = {}
        
        for field, expected_type, constraints in self.type_checks:
            violations = []
            for i, row in enumerate(data):
                value = row.get(field)
                
                if value is None:
                    continue
                
                # Type check
                if not isinstance(value, expected_type):
                    violations.append(i)
                    continue
                
                # Constraint checks
                if "min_value" in constraints and value < constraints["min_value"]:
                    violations.append(i)
                if "max_value" in constraints and value > constraints["max_value"]:
                    violations.append(i)
                if "min_length" in constraints and len(str(value)) < constraints["min_length"]:
                    violations.append(i)
                if "max_length" in constraints and len(str(value)) > constraints["max_length"]:
                    violations.append(i)
            
            if violations:
                violation_details[field] = {
                    "expected_type": expected_type.__name__,
                    "violation_count": len(violations),
                    "sample_indices": violations[:5]
                }
                errors.append(f"Field '{field}' has {len(violations)} type/constraint violations")
        
        for field, pattern in self.format_checks.items():
            regex = re.compile(pattern)
            violations = []
            for i, row in enumerate(data):
                value = row.get(field)
                if value is not None and not regex.match(str(value)):
                    violations.append(i)
            
            if violations:
                violation_details[f"{field}_format"] = {
                    "pattern": pattern,
                    "violation_count": len(violations),
                    "sample_indices": violations[:5]
                }
                errors.append(f"Field '{field}' has {len(violations)} format violations")
        
        return QualityResult(
            rule_name="validity_check",
            passed=len(errors) == 0,
            dimension=QualityDimension.VALIDITY,
            passed_count=total - len(errors),
            failed_count=len(errors),
            total_count=total,
            details={"violation_details": violation_details},
            errors=errors
        )


class DataQualityChecker:
    """
    Main data quality checking interface.
    
    Example:
        checker = DataQualityChecker()
        checker.add_rule(CompletenessChecker().add_required_field("id"))
        checker.add_rule(UniquenessChecker().add_unique_field("id"))
        
        report = checker.check_quality(data_records)
    """
    
    def __init__(self, dataset_name: str = "dataset"):
        self.dataset_name = dataset_name
        self.checkers: Dict[QualityDimension, Any] = {}
        self.custom_rules: List[QualityRule] = []
    
    def add_checker(
        self,
        dimension: QualityDimension,
        checker: Any
    ) -> "DataQualityChecker":
        """Add a dimension checker."""
        self.checkers[dimension] = checker
        return self
    
    def add_custom_rule(self, rule: QualityRule) -> "DataQualityChecker":
        """Add a custom quality rule."""
        self.custom_rules.append(rule)
        return self
    
    def check_quality(self, data: List[Dict]) -> DataQualityReport:
        """Run all quality checks."""
        results = []
        dimension_scores: Dict[str, List[float]] = defaultdict(list)
        all_issues = []
        
        # Run dimension checkers
        for dimension, checker in self.checkers.items():
            result = checker.check(data)
            results.append(result)
            
            score = result.passed_count / max(1, result.total_count)
            dimension_scores[dimension.value].append(score)
            
            if not result.passed:
                all_issues.append({
                    "dimension": dimension.value,
                    "rule": result.rule_name,
                    "errors": result.errors,
                    "severity": "error"
                })
        
        # Run custom rules
        for rule in self.custom_rules:
            result = rule.check_fn(data)
            result.rule_name = rule.name
            result.dimension = rule.dimension
            results.append(result)
            
            score = result.passed_count / max(1, result.total_count)
            dimension_scores[rule.dimension.value].append(score)
            
            if not result.passed:
                all_issues.append({
                    "dimension": rule.dimension.value,
                    "rule": rule.name,
                    "errors": result.errors,
                    "severity": rule.severity
                })
        
        # Calculate overall scores
        dimension_avg = {
            dim: sum(scores) / len(scores) if scores else 0
            for dim, scores in dimension_scores.items()
        }
        
        overall = sum(dimension_avg.values()) / len(dimension_avg) if dimension_avg else 0
        
        return DataQualityReport(
            dataset_name=self.dataset_name,
            total_records=len(data),
            checked_at=datetime.now(),
            results=results,
            overall_score=overall,
            dimensions=dimension_avg,
            issues=all_issues
        )


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataQualityAction(BaseAction):
    """
    Data quality checking action for data pipelines.
    
    Parameters:
        data: List of records to check
        dataset_name: Name of the dataset
        checks: List of check types to perform
    
    Example:
        action = DataQualityAction()
        result = action.execute({}, {
            "data": [{"id": 1, "email": "a@b.com"}],
            "dataset_name": "users",
            "checks": ["completeness", "uniqueness"]
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data quality checks."""
        data = params.get("data", [])
        dataset_name = params.get("dataset_name", "dataset")
        checks = params.get("checks", ["completeness", "uniqueness", "validity"])
        
        checker = DataQualityChecker(dataset_name=dataset_name)
        
        if "completeness" in checks:
            completeness = CompletenessChecker()
            if "required_fields" in params:
                for field in params["required_fields"]:
                    completeness.add_required_field(field)
            checker.add_checker(QualityDimension.COMPLETENESS, completeness)
        
        if "uniqueness" in checks:
            uniqueness = UniquenessChecker()
            if "unique_fields" in params:
                for field in params["unique_fields"]:
                    uniqueness.add_unique_field(field)
            checker.add_checker(QualityDimension.UNIQUENESS, uniqueness)
        
        if "validity" in checks:
            validity = ValidityChecker()
            checker.add_checker(QualityDimension.VALIDITY, validity)
        
        if "consistency" in checks:
            consistency = ConsistencyChecker()
            checker.add_checker(QualityDimension.CONSISTENCY, consistency)
        
        report = checker.check_quality(data)
        
        return {
            "success": True,
            "dataset_name": dataset_name,
            "total_records": report.total_records,
            "overall_score": report.overall_score,
            "dimensions": report.dimensions,
            "issue_count": len(report.issues),
            "issues": report.issues[:10],  # Top 10 issues
            "checked_at": report.checked_at.isoformat()
        }
