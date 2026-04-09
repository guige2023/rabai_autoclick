"""
Data Quality Framework Module.

Provides comprehensive data quality assessment including
completeness, accuracy, consistency, timeliness, and validity
checks with configurable rules and scoring.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, TypeVar, Generic
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
import re
from collections import Counter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class QualityDimension(Enum):
    """Data quality dimensions."""
    COMPLETENESS = auto()
    ACCURACY = auto()
    CONSISTENCY = auto()
    TIMELINESS = auto()
    VALIDITY = auto()
    UNIQUENESS = auto()
    INTEGRITY = auto()


class QualityRuleType(Enum):
    """Types of quality rules."""
    NOT_NULL = auto()
    UNIQUE = auto()
    RANGE = auto()
    PATTERN = auto()
    TYPE = auto()
    REFERENCE = auto()
    FRESHNESS = auto()
    CUSTOM = auto()


@dataclass
class QualityRule:
    """Data quality rule definition."""
    name: str
    field: str
    rule_type: QualityRuleType
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = "error"  # error, warning, info
    description: Optional[str] = None
    enabled: bool = True
    
    def validate(self, value: Any, context: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate a value against this rule.
        
        Returns:
            Tuple of (passed, error_message)
        """
        if self.rule_type == QualityRuleType.NOT_NULL:
            return self._validate_not_null(value)
        elif self.rule_type == QualityRuleType.UNIQUE:
            return self._validate_unique(value, context)
        elif self.rule_type == QualityRuleType.RANGE:
            return self._validate_range(value)
        elif self.rule_type == QualityRuleType.PATTERN:
            return self._validate_pattern(value)
        elif self.rule_type == QualityRuleType.TYPE:
            return self._validate_type(value)
        elif self.rule_type == QualityRuleType.FRESHNESS:
            return self._validate_freshness(context)
        
        return True, None
    
    def _validate_not_null(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate not null."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return False, f"Value cannot be null or empty"
        return True, None
    
    def _validate_unique(self, value: Any, context: Dict) -> Tuple[bool, Optional[str]]:
        """Validate uniqueness."""
        seen = context.get("_seen_values", {}).get(self.field, set())
        if value in seen:
            return False, f"Duplicate value: {value}"
        seen.add(value)
        context.setdefault("_seen_values", {})[self.field] = seen
        return True, None
    
    def _validate_range(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate value is within range."""
        if value is None:
            return True, None
        
        try:
            num_value = float(value)
            min_val = self.params.get("min")
            max_val = self.params.get("max")
            
            if min_val is not None and num_value < min_val:
                return False, f"Value {num_value} below minimum {min_val}"
            if max_val is not None and num_value > max_val:
                return False, f"Value {num_value} above maximum {max_val}"
        except (ValueError, TypeError):
            return False, f"Value {value} is not numeric"
        
        return True, None
    
    def _validate_pattern(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate value matches pattern."""
        if value is None:
            return True, None
        
        pattern = self.params.get("pattern")
        if not pattern:
            return True, None
        
        if not re.match(pattern, str(value)):
            return False, f"Value does not match pattern: {pattern}"
        
        return True, None
    
    def _validate_type(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate value type."""
        if value is None:
            return True, None
        
        expected_type = self.params.get("type")
        if not expected_type:
            return True, None
        
        type_mapping = {
            "string": str,
            "integer": int,
            "float": (int, float),
            "boolean": bool,
            "datetime": datetime
        }
        
        expected = type_mapping.get(expected_type)
        if expected and not isinstance(value, expected):
            return False, f"Expected type {expected_type}, got {type(value).__name__}"
        
        return True, None
    
    def _validate_freshness(self, context: Dict) -> Tuple[bool, Optional[str]]:
        """Validate data freshness."""
        timestamp_field = self.params.get("timestamp_field")
        max_age_seconds = self.params.get("max_age_seconds")
        
        if not timestamp_field or max_age_seconds is None:
            return True, None
        
        timestamp = context.get(timestamp_field)
        if not timestamp:
            return True, None
        
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp)
            except ValueError:
                return False, "Invalid timestamp format"
        
        age = (datetime.now() - timestamp).total_seconds()
        if age > max_age_seconds:
            return False, f"Data is {age}s old, exceeds max {max_age_seconds}s"
        
        return True, None


@dataclass
class QualityViolation:
    """Quality rule violation."""
    rule_name: str
    field: str
    value: Any
    message: str
    severity: str
    record_id: Optional[str] = None
    row_index: Optional[int] = None


@dataclass
class QualityScore:
    """Overall quality score for a dimension."""
    dimension: QualityDimension
    score: float  # 0-100
    passed_rules: int
    failed_rules: int
    total_records: int
    violation_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "dimension": self.dimension.name,
            "score": round(self.score, 2),
            "passed": self.passed_rules,
            "failed": self.failed_rules,
            "violations": self.violation_count
        }


@dataclass
class QualityReport:
    """Complete data quality report."""
    timestamp: datetime
    total_records: int
    total_fields: int
    overall_score: float
    dimension_scores: List[QualityScore]
    violations: List[QualityViolation]
    summary: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "total_records": self.total_records,
            "total_fields": self.total_fields,
            "overall_score": round(self.overall_score, 2),
            "dimensions": [d.to_dict() for d in self.dimension_scores],
            "violation_count": len(self.violations),
            "summary": self.summary
        }


class DataQualityFramework:
    """
    Comprehensive data quality assessment framework.
    
    Provides configurable rules, multi-dimensional scoring,
    and detailed reporting for data quality monitoring.
    """
    
    def __init__(self) -> None:
        self.rules: List[QualityRule] = []
        self._dimension_rules: Dict[QualityDimension, List[QualityRule]] = {}
    
    def add_rule(self, rule: QualityRule) -> "DataQualityFramework":
        """Add a quality rule."""
        self.rules.append(rule)
        
        if rule.rule_type == QualityRuleType.NOT_NULL:
            self._dimension_rules.setdefault(QualityDimension.COMPLETENESS, []).append(rule)
        elif rule.rule_type == QualityRuleType.UNIQUE:
            self._dimension_rules.setdefault(QualityDimension.UNIQUENESS, []).append(rule)
        elif rule.rule_type == QualityRuleType.RANGE:
            self._dimension_rules.setdefault(QualityDimension.VALIDITY, []).append(rule)
        elif rule.rule_type == QualityRuleType.PATTERN:
            self._dimension_rules.setdefault(QualityDimension.VALIDITY, []).append(rule)
        elif rule.rule_type == QualityRuleType.TYPE:
            self._dimension_rules.setdefault(QualityDimension.ACCURACY, []).append(rule)
        elif rule.rule_type == QualityRuleType.FRESHNESS:
            self._dimension_rules.setdefault(QualityDimension.TIMELINESS, []).append(rule)
        
        return self
    
    def add_not_null_rule(
        self,
        name: str,
        field: str,
        severity: str = "error"
    ) -> "DataQualityFramework":
        """Add not-null rule."""
        rule = QualityRule(
            name=name,
            field=field,
            rule_type=QualityRuleType.NOT_NULL,
            severity=severity
        )
        return self.add_rule(rule)
    
    def add_unique_rule(
        self,
        name: str,
        field: str,
        severity: str = "error"
    ) -> "DataQualityFramework":
        """Add uniqueness rule."""
        rule = QualityRule(
            name=name,
            field=field,
            rule_type=QualityRuleType.UNIQUE,
            severity=severity
        )
        return self.add_rule(rule)
    
    def add_range_rule(
        self,
        name: str,
        field: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        severity: str = "error"
    ) -> "DataQualityFramework":
        """Add range validation rule."""
        rule = QualityRule(
            name=name,
            field=field,
            rule_type=QualityRuleType.RANGE,
            params={"min": min_val, "max": max_val},
            severity=severity
        )
        return self.add_rule(rule)
    
    def add_pattern_rule(
        self,
        name: str,
        field: str,
        pattern: str,
        severity: str = "error"
    ) -> "DataQualityFramework":
        """Add pattern validation rule."""
        rule = QualityRule(
            name=name,
            field=field,
            rule_type=QualityRuleType.PATTERN,
            params={"pattern": pattern},
            severity=severity
        )
        return self.add_rule(rule)
    
    def assess(
        self,
        records: List[Dict[str, Any]],
        record_ids: Optional[List[str]] = None
    ) -> QualityReport:
        """
        Assess data quality.
        
        Args:
            records: List of records to assess
            record_ids: Optional record identifiers
            
        Returns:
            QualityReport
        """
        if not records:
            return QualityReport(
                timestamp=datetime.now(),
                total_records=0,
                total_fields=0,
                overall_score=0,
                dimension_scores=[],
                violations=[]
            )
        
        record_ids = record_ids or [str(i) for i in range(len(records))]
        
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        violations: List[QualityViolation] = []
        dimension_violations: Dict[QualityDimension, List[QualityViolation]] = {
            d: [] for d in QualityDimension
        }
        
        # Group rules by field for efficiency
        field_rules: Dict[str, List[QualityRule]] = {}
        for rule in self.rules:
            if rule.enabled:
                field_rules.setdefault(rule.field, []).append(rule)
        
        # Evaluate each record
        for idx, record in enumerate(records):
            record_id = record_ids[idx]
            context = {"_seen_values": {}}
            
            # Add all fields to context for cross-field rules
            context.update(record)
            
            for field_name, rules in field_rules.items():
                value = record.get(field_name)
                
                for rule in rules:
                    passed, error_msg = rule.validate(value, context)
                    
                    if not passed:
                        violation = QualityViolation(
                            rule_name=rule.name,
                            field=field_name,
                            value=value,
                            message=error_msg or "Validation failed",
                            severity=rule.severity,
                            record_id=record_id,
                            row_index=idx
                        )
                        violations.append(violation)
                        
                        # Assign to dimension
                        for dim, dim_rules in self._dimension_rules.items():
                            if rule in dim_rules:
                                dimension_violations[dim].append(violation)
        
        # Calculate dimension scores
        dimension_scores = []
        total_passes = 0
        total_fails = 0
        
        for dimension, dim_violations in dimension_violations.items():
            enabled_rules = [r for r in self.rules if r.enabled and r in self._dimension_rules.get(dimension, [])]
            
            if not enabled_rules:
                continue
            
            passed = len(enabled_rules) * len(records) - len(dim_violations)
            total = len(enabled_rules) * len(records)
            score = (passed / total * 100) if total > 0 else 100
            
            total_passes += passed
            total_fails += len(dim_violations)
            
            dimension_scores.append(QualityScore(
                dimension=dimension,
                score=score,
                passed_rules=len(enabled_rules),
                failed_rules=len(dim_violations),
                total_records=len(records),
                violation_count=len(dim_violations)
            ))
        
        # Overall score
        total_checks = total_passes + total_fails
        overall_score = (total_passes / total_checks * 100) if total_checks > 0 else 100
        
        return QualityReport(
            timestamp=datetime.now(),
            total_records=len(records),
            total_fields=len(all_fields),
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            violations=violations,
            summary={
                "total_checks": total_checks,
                "passed_checks": total_passes,
                "failed_checks": total_fails
            }
        )


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data with quality issues
    records = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25},
        {"id": 3, "name": "", "email": "invalid-email", "age": 150},  # Issues
        {"id": 4, "name": "Carol", "email": "carol@example.com", "age": None},  # Issues
        {"id": 5, "name": "Dave", "email": "dave@example.com", "age": 35},
    ]
    
    # Build quality framework
    framework = DataQualityFramework()
    
    framework.add_not_null_rule("name_required", "name")
    framework.add_pattern_rule("email_pattern", "email", r"^[^@]+@[^@]+\.[^@]+$")
    framework.add_range_rule("age_range", "age", min_val=0, max_val=120)
    framework.add_unique_rule("id_unique", "id")
    
    print("=== Data Quality Assessment ===\n")
    
    report = framework.assess(records)
    
    print(f"Overall Quality Score: {report.overall_score:.1f}%")
    print(f"Total Records: {report.total_records}")
    print(f"Total Violations: {len(report.violations)}")
    
    print("\nDimension Scores:")
    for dim_score in report.dimension_scores:
        print(f"  {dim_score.dimension.name}: {dim_score.score:.1f}% ({dim_score.violation_count} violations)")
    
    print("\nViolations:")
    for v in report.violations[:5]:
        print(f"  [{v.severity}] {v.rule_name} - {v.field}: {v.message}")
