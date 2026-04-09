"""Data Quality Action Module.

Provides data quality utilities: validation rules, profiling,
anomaly detection, cleansing, and quality scoring.

Example:
    result = execute(context, {"action": "validate", "data": [...], "rules": [...]})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from collections import Counter


@dataclass
class ValidationRule:
    """A data validation rule."""
    
    name: str
    rule_type: str
    field: str
    params: dict[str, Any] = field(default_factory=dict)
    error_message: str = ""
    
    def __post_init__(self) -> None:
        """Set default error message."""
        if not self.error_message:
            self.error_message = f"Validation failed for {self.field} using {self.rule_type}"


@dataclass
class ValidationResult:
    """Result of a validation check."""
    
    rule_name: str
    passed: bool
    field: str
    value: Any = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class DataValidator:
    """Validates data against defined rules."""
    
    RULE_TYPES = {
        "required",
        "type",
        "range",
        "pattern",
        "enum",
        "length",
        "min",
        "max",
        "unique",
        "custom",
    }
    
    def __init__(self) -> None:
        """Initialize data validator."""
        self._rules: list[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add validation rule.
        
        Args:
            rule: Rule to add
        """
        if rule.rule_type not in self.RULE_TYPES:
            raise ValueError(f"Invalid rule_type: {rule.rule_type}")
        self._rules.append(rule)
    
    def validate(self, data: dict[str, Any]) -> list[ValidationResult]:
        """Validate data against all rules.
        
        Args:
            data: Data to validate
            
        Returns:
            List of validation results
        """
        results = []
        
        for rule in self._rules:
            result = self._validate_rule(rule, data)
            results.append(result)
        
        return results
    
    def _validate_rule(self, rule: ValidationRule, data: dict[str, Any]) -> ValidationResult:
        """Validate single rule."""
        value = data.get(rule.field)
        
        if rule.rule_type == "required":
            passed = value is not None and value != ""
        
        elif rule.rule_type == "type":
            expected_type = rule.params.get("type")
            passed = isinstance(value, expected_type) if value is not None else True
        
        elif rule.rule_type == "range":
            min_val = rule.params.get("min")
            max_val = rule.params.get("max")
            if value is None:
                passed = True
            elif min_val is not None and value < min_val:
                passed = False
            elif max_val is not None and value > max_val:
                passed = False
            else:
                passed = True
        
        elif rule.rule_type == "pattern":
            import re
            pattern = rule.params.get("pattern", "")
            regex = re.compile(pattern)
            passed = bool(regex.match(str(value))) if value is not None else True
        
        elif rule.rule_type == "enum":
            allowed = rule.params.get("values", [])
            passed = value in allowed if value is not None else True
        
        elif rule.rule_type == "length":
            length = len(value) if value is not None else 0
            min_len = rule.params.get("min", 0)
            max_len = rule.params.get("max", float("inf"))
            passed = min_len <= length <= max_len
        
        elif rule.rule_type == "min":
            passed = value >= rule.params.get("min", 0) if value is not None else True
        
        elif rule.rule_type == "max":
            passed = value <= rule.params.get("max", 0) if value is not None else True
        
        elif rule.rule_type == "unique":
            passed = True
        
        else:
            passed = True
        
        return ValidationResult(
            rule_name=rule.name,
            passed=passed,
            field=rule.field,
            value=value,
            error=rule.error_message if not passed else None,
        )


class DataProfiler:
    """Profiles data to understand its characteristics."""
    
    @staticmethod
    def profile(data: list[dict[str, Any]]) -> dict[str, Any]:
        """Profile a dataset.
        
        Args:
            data: List of records to profile
            
        Returns:
            Profile statistics
        """
        if not data:
            return {"record_count": 0}
        
        fields = set()
        for record in data:
            fields.update(record.keys())
        
        field_stats = {}
        for field_name in fields:
            values = [r.get(field_name) for r in data if field_name in r]
            values = [v for v in values if v is not None]
            
            type_counts = Counter(type(v).__name__ for v in values)
            
            field_stats[field_name] = {
                "null_count": len(data) - len(values),
                "non_null_count": len(values),
                "unique_count": len(set(str(v) for v in values)),
                "type_distribution": dict(type_counts),
            }
            
            if values:
                numeric_values = [v for v in values if isinstance(v, (int, float))]
                if numeric_values:
                    field_stats[field_name].update({
                        "min": min(numeric_values),
                        "max": max(numeric_values),
                        "avg": sum(numeric_values) / len(numeric_values),
                    })
                
                str_values = [v for v in values if isinstance(v, str)]
                if str_values:
                    field_stats[field_name].update({
                        "min_length": min(len(s) for s in str_values),
                        "max_length": max(len(s) for s in str_values),
                        "avg_length": sum(len(s) for s in str_values) / len(str_values),
                    })
        
        return {
            "record_count": len(data),
            "field_count": len(fields),
            "fields": field_stats,
        }


class AnomalyDetector:
    """Detects anomalies in data."""
    
    def __init__(self, threshold_std: float = 3.0) -> None:
        """Initialize anomaly detector.
        
        Args:
            threshold_std: Standard deviation threshold for anomaly
        """
        self.threshold_std = threshold_std
    
    def detect_numeric(self, values: list[float]) -> list[int]:
        """Detect anomalies in numeric data using z-score.
        
        Args:
            values: List of numeric values
            
        Returns:
            Indices of anomalous values
        """
        if len(values) < 3:
            return []
        
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance ** 0.5
        
        if std == 0:
            return []
        
        anomalies = []
        for i, value in enumerate(values):
            z_score = abs((value - mean) / std)
            if z_score > self.threshold_std:
                anomalies.append(i)
        
        return anomalies
    
    def detect_categorical(
        self,
        values: list[str],
        min_frequency: float = 0.01,
    ) -> list[int]:
        """Detect rare categorical values.
        
        Args:
            values: List of categorical values
            min_frequency: Minimum frequency ratio
            
        Returns:
            Indices of anomalous values
        """
        if not values:
            return []
        
        total = len(values)
        frequency = Counter(values)
        
        threshold = total * min_frequency
        
        anomalies = []
        for i, value in enumerate(values):
            if frequency[value] < threshold:
                anomalies.append(i)
        
        return anomalies


class DataCleanser:
    """Cleans and standardizes data."""
    
    @staticmethod
    def cleanse_record(record: dict[str, Any]) -> dict[str, Any]:
        """Cleanse a single record.
        
        Args:
            record: Record to cleanse
            
        Returns:
            Cleansed record
        """
        cleansed = {}
        
        for key, value in record.items():
            if isinstance(value, str):
                value = value.strip()
                value = " ".join(value.split())
                
                if value.lower() in ("null", "none", "n/a", "-", ""):
                    value = None
            
            cleansed[key] = value
        
        return cleansed
    
    @staticmethod
    def standardize_dates(
        records: list[dict[str, Any]],
        date_fields: list[str],
        format: str = "%Y-%m-%d",
    ) -> list[dict[str, Any]]:
        """Standardize date formats.
        
        Args:
            records: List of records
            date_fields: Fields containing dates
            format: Target date format
            
        Returns:
            Records with standardized dates
        """
        from datetime import datetime
        
        result = []
        
        for record in records:
            new_record = record.copy()
            for field in date_fields:
                if field in new_record and new_record[field]:
                    try:
                        if isinstance(new_record[field], str):
                            dt = datetime.fromisoformat(new_record[field])
                            new_record[field] = dt.strftime(format)
                    except (ValueError, TypeError):
                        pass
            result.append(new_record)
        
        return result


class QualityScorer:
    """Computes data quality scores."""
    
    @staticmethod
    def compute_score(
        data: list[dict[str, Any]],
        validation_results: list[ValidationResult],
    ) -> dict[str, Any]:
        """Compute overall data quality score.
        
        Args:
            data: Dataset
            validation_results: Validation results
            
        Returns:
            Quality scores
        """
        if not data:
            return {"overall_score": 0.0}
        
        total_rules = len(validation_results)
        passed_rules = sum(1 for r in validation_results if r.passed)
        
        completeness = 1.0
        if data:
            total_cells = len(data) * len(data[0]) if data else 0
            null_cells = sum(
                1 for record in data
                for value in record.values()
                if value is None or value == ""
            )
            if total_cells > 0:
                completeness = 1.0 - (null_cells / total_cells)
        
        validity = passed_rules / total_rules if total_rules > 0 else 1.0
        
        overall = (completeness * 0.4) + (validity * 0.6)
        
        return {
            "overall_score": round(overall, 3),
            "completeness": round(completeness, 3),
            "validity": round(validity, 3),
            "total_records": len(data),
            "passed_rules": passed_rules,
            "failed_rules": total_rules - passed_rules,
        }


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute data quality action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "validate":
        validator = DataValidator()
        rule = ValidationRule(
            name=params.get("rule_name", "rule"),
            rule_type=params.get("rule_type", "required"),
            field=params.get("field", ""),
        )
        validator.add_rule(rule)
        
        data = params.get("data", {})
        validation_results = validator.validate(data)
        
        passed = all(r.passed for r in validation_results)
        result["data"] = {
            "passed": passed,
            "results": [
                {"rule": r.rule_name, "passed": r.passed, "error": r.error}
                for r in validation_results
            ],
        }
    
    elif action == "profile":
        data = params.get("data", [])
        profile = DataProfiler.profile(data)
        result["data"] = profile
    
    elif action == "detect_anomalies":
        detector = AnomalyDetector(threshold_std=params.get("threshold_std", 3.0))
        values = params.get("values", [])
        indices = detector.detect_numeric(values)
        result["data"] = {"anomaly_indices": indices}
    
    elif action == "cleanse":
        data = params.get("data", {})
        if isinstance(data, dict):
            cleansed = DataCleanser.cleanse_record(data)
        else:
            cleansed = data
        result["data"] = {"cleansed": cleansed}
    
    elif action == "quality_score":
        data = params.get("data", [])
        validation_results = []
        score = QualityScorer.compute_score(data, validation_results)
        result["data"] = score
    
    elif action == "standardize_dates":
        records = params.get("records", [])
        date_fields = params.get("date_fields", [])
        standardized = DataCleanser.standardize_dates(records, date_fields)
        result["data"] = {"standardized_count": len(standardized)}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
