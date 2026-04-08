"""
Data Quality Action.

Provides data quality checking and validation.
Supports:
- Schema validation
- Data type checking
- Range and constraint validation
- Completeness checks
- Custom quality rules
"""

from typing import Dict, List, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
import re
import json

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    """Data quality level."""
    EXCELLENT = "excellent"
    GOOD = "good"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    field: str
    passed: bool
    rule: str
    message: Optional[str] = None
    value: Any = None
    expected: Optional[Any] = None
    severity: str = "error"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field": self.field,
            "passed": self.passed,
            "rule": self.rule,
            "message": self.message,
            "value": str(self.value) if self.value is not None else None,
            "expected": str(self.expected) if self.expected is not None else None,
            "severity": self.severity
        }


@dataclass
class QualityReport:
    """Data quality report."""
    dataset_name: str
    timestamp: datetime
    total_records: int
    total_fields: int
    valid_records: int
    invalid_records: int
    validation_results: List[ValidationResult] = field(default_factory=list)
    field_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    quality_score: float = 0.0
    quality_level: QualityLevel = QualityLevel.GOOD
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset_name": self.dataset_name,
            "timestamp": self.timestamp.isoformat(),
            "total_records": self.total_records,
            "total_fields": self.total_fields,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "quality_score": self.quality_score,
            "quality_level": self.quality_level.value,
            "field_stats": self.field_stats,
            "validation_results": [r.to_dict() for r in self.validation_results]
        }


class DataQualityRule:
    """Base class for data quality rules."""
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Validate a value against this rule."""
        raise NotImplementedError


class NotNullRule(DataQualityRule):
    """Rule to check for non-null values."""
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Check if value is not null."""
        passed = value is not None and str(value).strip() != ""
        return ValidationResult(
            field=field,
            passed=passed,
            rule="not_null",
            message=None if passed else f"Field '{field}' is null or empty",
            value=value,
            severity="error"
        )


class DataTypeRule(DataQualityRule):
    """Rule to check data type."""
    
    def __init__(self, expected_type: str):
        """
        Initialize data type rule.
        
        Args:
            expected_type: Expected type (string, integer, float, boolean, date, datetime)
        """
        self.expected_type = expected_type.lower()
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Check if value matches expected type."""
        if value is None:
            return ValidationResult(
                field=field,
                passed=False,
                rule=f"data_type_{self.expected_type}",
                message=f"Field '{field}' is null",
                value=value,
                severity="error"
            )
        
        passed = self._check_type(value)
        return ValidationResult(
            field=field,
            passed=passed,
            rule=f"data_type_{self.expected_type}",
            message=None if passed else f"Field '{field}' expected {self.expected_type}",
            value=value,
            expected=self.expected_type,
            severity="error"
        )
    
    def _check_type(self, value: Any) -> bool:
        """Check if value matches expected type."""
        if self.expected_type == "string":
            return isinstance(value, str)
        elif self.expected_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        elif self.expected_type == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif self.expected_type == "boolean":
            return isinstance(value, bool)
        elif self.expected_type == "date":
            return isinstance(value, datetime) or self._is_date_string(value)
        elif self.expected_type == "datetime":
            return isinstance(value, datetime) or self._is_datetime_string(value)
        return False
    
    def _is_date_string(self, value: str) -> bool:
        """Check if value is a date string."""
        if not isinstance(value, str):
            return False
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", value))
    
    def _is_datetime_string(self, value: str) -> bool:
        """Check if value is a datetime string."""
        if not isinstance(value, str):
            return False
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", value))


class RangeRule(DataQualityRule):
    """Rule to check value is within range."""
    
    def __init__(self, min_value: Optional[Union[int, float]] = None, max_value: Optional[Union[int, float]] = None):
        """
        Initialize range rule.
        
        Args:
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
        """
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Check if value is within range."""
        if value is None:
            return ValidationResult(
                field=field,
                passed=False,
                rule="range",
                message=f"Field '{field}' is null",
                value=value,
                severity="error"
            )
        
        try:
            num_value = float(value)
            
            if self.min_value is not None and num_value < self.min_value:
                return ValidationResult(
                    field=field,
                    passed=False,
                    rule="range",
                    message=f"Field '{field}' value {num_value} below minimum {self.min_value}",
                    value=value,
                    expected=f"[{self.min_value}, {self.max_value}]",
                    severity="error"
                )
            
            if self.max_value is not None and num_value > self.max_value:
                return ValidationResult(
                    field=field,
                    passed=False,
                    rule="range",
                    message=f"Field '{field}' value {num_value} above maximum {self.max_value}",
                    value=value,
                    expected=f"[{self.min_value}, {self.max_value}]",
                    severity="error"
                )
            
            return ValidationResult(
                field=field,
                passed=True,
                rule="range",
                value=value
            )
        
        except (ValueError, TypeError):
            return ValidationResult(
                field=field,
                passed=False,
                rule="range",
                message=f"Field '{field}' value '{value}' is not numeric",
                value=value,
                expected=f"[{self.min_value}, {self.max_value}]",
                severity="error"
            )


class PatternRule(DataQualityRule):
    """Rule to check value matches a regex pattern."""
    
    def __init__(self, pattern: str, message: Optional[str] = None):
        """
        Initialize pattern rule.
        
        Args:
            pattern: Regex pattern
            message: Custom error message
        """
        self.pattern = re.compile(pattern)
        self.message = message
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Check if value matches pattern."""
        if value is None:
            return ValidationResult(
                field=field,
                passed=False,
                rule="pattern",
                message=f"Field '{field}' is null",
                value=value,
                severity="error"
            )
        
        passed = bool(self.pattern.match(str(value)))
        return ValidationResult(
            field=field,
            passed=passed,
            rule="pattern",
            message=self.message if not passed else None,
            value=value,
            expected=str(self.pattern.pattern),
            severity="error" if not passed else "info"
        )


class UniqueRule(DataQualityRule):
    """Rule to check values are unique within a dataset."""
    
    def __init__(self, values: List[Any]):
        """
        Initialize unique rule.
        
        Args:
            values: All values in the field
        """
        self.values = values
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Check if value is unique."""
        if value is None:
            return ValidationResult(
                field=field,
                passed=False,
                rule="unique",
                message=f"Field '{field}' contains null",
                value=value,
                severity="warning"
            )
        
        count = sum(1 for v in self.values if v == value)
        passed = count == 1
        
        return ValidationResult(
            field=field,
            passed=passed,
            rule="unique",
            message=None if passed else f"Field '{field}' has duplicate value: {value}",
            value=value,
            severity="warning"
        )


class CompletenessRule(DataQualityRule):
    """Rule to check data completeness."""
    
    def __init__(self, threshold: float = 0.95):
        """
        Initialize completeness rule.
        
        Args:
            threshold: Minimum completeness ratio (0-1)
        """
        self.threshold = threshold
    
    def validate(self, field: str, value: Any) -> ValidationResult:
        """Calculate completeness for a field."""
        # This is a placeholder - completeness is calculated at dataset level
        return ValidationResult(
            field=field,
            passed=True,
            rule="completeness",
            value=value
        )


class DataQualityAction:
    """
    Data Quality Action.
    
    Provides comprehensive data quality checking with support for:
    - Multiple validation rules
    - Custom rule definitions
    - Quality scoring
    - Detailed reporting
    """
    
    def __init__(self, dataset_name: str):
        """
        Initialize the Data Quality Action.
        
        Args:
            dataset_name: Name of the dataset to check
        """
        self.dataset_name = dataset_name
        self.rules: Dict[str, List[DataQualityRule]] = {}
        self._field_values: Dict[str, List[Any]] = {}
    
    def add_rule(self, field: str, rule: DataQualityRule) -> "DataQualityAction":
        """
        Add a validation rule for a field.
        
        Args:
            field: Field name
            rule: Validation rule
        
        Returns:
            Self for chaining
        """
        if field not in self.rules:
            self.rules[field] = []
        self.rules[field].append(rule)
        return self
    
    def not_null(self, field: str) -> "DataQualityAction":
        """Add not null rule for a field."""
        return self.add_rule(field, NotNullRule())
    
    def data_type(self, field: str, dtype: str) -> "DataQualityAction":
        """Add data type rule for a field."""
        return self.add_rule(field, DataTypeRule(dtype))
    
    def range(
        self,
        field: str,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None
    ) -> "DataQualityAction":
        """Add range rule for a field."""
        return self.add_rule(field, RangeRule(min_value, max_value))
    
    def pattern(self, field: str, pattern: str, message: Optional[str] = None) -> "DataQualityAction":
        """Add pattern rule for a field."""
        return self.add_rule(field, PatternRule(pattern, message))
    
    def unique(self, field: str) -> "DataQualityAction":
        """Add unique rule for a field."""
        return self.add_rule(field, UniqueRule(self._field_values.get(field, [])))
    
    def completeness(self, field: str, threshold: float = 0.95) -> "DataQualityAction":
        """Add completeness rule for a field."""
        return self.add_rule(field, CompletenessRule(threshold))
    
    def validate_record(self, record: Dict[str, Any]) -> List[ValidationResult]:
        """
        Validate a single record.
        
        Args:
            record: Record to validate
        
        Returns:
            List of validation results
        """
        results = []
        
        for field_name, rules in self.rules.items():
            value = record.get(field_name)
            
            # Track values for unique checks
            if field_name not in self._field_values:
                self._field_values[field_name] = []
            self._field_values[field_name].append(value)
            
            for rule in rules:
                result = rule.validate(field_name, value)
                results.append(result)
        
        return results
    
    def validate_dataset(
        self,
        records: List[Dict[str, Any]]
    ) -> QualityReport:
        """
        Validate an entire dataset.
        
        Args:
            records: List of records to validate
        
        Returns:
            QualityReport with validation results
        """
        # Reset field values
        self._field_values = {}
        
        all_results: List[ValidationResult] = []
        valid_count = 0
        
        for record in records:
            results = self.validate_record(record)
            
            # Record is valid if all rules pass
            record_valid = all(r.passed for r in results)
            if record_valid:
                valid_count += 1
            
            all_results.extend(results)
        
        # Calculate quality score
        if all_results:
            passed = sum(1 for r in all_results if r.passed)
            total = len(all_results)
            quality_score = (passed / total * 100) if total > 0 else 0
        else:
            quality_score = 100.0
        
        # Determine quality level
        if quality_score >= 95:
            level = QualityLevel.EXCELLENT
        elif quality_score >= 80:
            level = QualityLevel.GOOD
        elif quality_score >= 60:
            level = QualityLevel.WARNING
        else:
            level = QualityLevel.CRITICAL
        
        # Calculate field stats
        field_stats = self._calculate_field_stats(records)
        
        report = QualityReport(
            dataset_name=self.dataset_name,
            timestamp=datetime.utcnow(),
            total_records=len(records),
            total_fields=len(records[0]) if records else 0,
            valid_records=valid_count,
            invalid_records=len(records) - valid_count,
            validation_results=all_results,
            field_stats=field_stats,
            quality_score=quality_score,
            quality_level=level
        )
        
        logger.info(
            f"Quality report for '{self.dataset_name}': "
            f"{quality_score:.1f}% ({level.value})"
        )
        
        return report
    
    def _calculate_field_stats(
        self,
        records: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate statistics for each field."""
        if not records:
            return {}
        
        field_names = set()
        for record in records:
            field_names.update(record.keys())
        
        stats = {}
        
        for field in field_names:
            values = [r.get(field) for r in records]
            non_null = [v for v in values if v is not None and str(v).strip() != ""]
            
            stats[field] = {
                "total_count": len(values),
                "null_count": len(values) - len(non_null),
                "completeness": len(non_null) / len(values) if values else 0,
                "unique_count": len(set(str(v) for v in non_null))
            }
        
        return stats


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    records = [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "", "age": 35, "email": "invalid-email"},
        {"id": 4, "name": "Diana", "age": 150, "email": "diana@example.com"},
        {"id": 5, "name": "Eve", "age": 28, "email": "eve@example.com"},
    ]
    
    # Create quality checker
    dq = DataQualityAction("users")
    
    # Add rules
    dq.not_null("name").data_type("name", "string")
    dq.not_null("email").pattern("email", r"^[^@]+@[^@]+\.[^@]+$", "Invalid email format")
    dq.range("age", min_value=0, max_value=120)
    dq.data_type("age", "integer")
    
    # Validate
    report = dq.validate_dataset(records)
    
    print(f"Quality Score: {report.quality_score:.1f}%")
    print(f"Quality Level: {report.quality_level.value}")
    print(f"Valid Records: {report.valid_records}/{report.total_records}")
    print(f"\nField Stats: {json.dumps(report.field_stats, indent=2)}")
    print(f"\nValidation Results: {len(report.validation_results)}")
    
    for result in report.validation_results:
        if not result.passed:
            print(f"  - {result.field}: {result.message}")
