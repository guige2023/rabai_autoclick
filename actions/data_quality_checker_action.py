"""
Data quality checker action for validation and anomaly detection.

Provides schema validation, completeness checks, and consistency verification.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import re


class DataQualityCheckerAction:
    """Data quality validation and checking."""

    def __init__(
        self,
        strict_mode: bool = False,
        max_errors: int = 100,
    ) -> None:
        """
        Initialize data quality checker.

        Args:
            strict_mode: Fail on first error
            max_errors: Maximum errors to collect
        """
        self.strict_mode = strict_mode
        self.max_errors = max_errors
        self._schemas: Dict[str, Dict[str, Any]] = {}
        self._validation_history: List[Dict[str, Any]] = []

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute data quality check.

        Args:
            params: Dictionary containing:
                - operation: 'check', 'register_schema', 'validate', 'report'
                - data: Data to check
                - schema_name: Schema to validate against
                - checks: List of checks to perform

        Returns:
            Dictionary with validation result
        """
        operation = params.get("operation", "check")

        if operation == "check":
            return self._check_data(params)
        elif operation == "register_schema":
            return self._register_schema(params)
        elif operation == "validate":
            return self._validate_against_schema(params)
        elif operation == "report":
            return self._get_report(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _check_data(self, params: dict[str, Any]) -> dict[str, Any]:
        """Run quality checks on data."""
        data = params.get("data", {})
        checks = params.get("checks", ["completeness", "validity", "consistency"])

        errors = []
        warnings = []
        passed = []

        for check in checks:
            if check == "completeness":
                result = self._check_completeness(data)
            elif check == "validity":
                result = self._check_validity(data)
            elif check == "consistency":
                result = self._check_consistency(data)
            elif check == "uniqueness":
                result = self._check_uniqueness(data)
            elif check == "freshness":
                result = self._check_freshness(data)
            else:
                result = {"passed": False, "error": f"Unknown check: {check}"}

            if result.get("passed", False):
                passed.append(check)
            elif result.get("warning"):
                warnings.append({check: result["warning"]})
            else:
                errors.append({check: result.get("error", "Check failed")})

            if self.strict_mode and errors:
                break

        quality_score = len(passed) / len(checks) * 100 if checks else 100

        return {
            "success": True,
            "quality_score": round(quality_score, 2),
            "checks_passed": len(passed),
            "checks_failed": len(errors),
            "warnings": warnings,
            "errors": errors,
        }

    def _check_completeness(self, data: dict[str, Any]) -> dict[str, Any]:
        """Check data completeness."""
        total_fields = len(data)
        null_fields = sum(1 for v in data.values() if v is None or v == "")
        missing_fields = [k for k, v in data.items() if v is None or v == ""]

        completeness = (total_fields - null_fields) / total_fields * 100 if total_fields > 0 else 100

        if completeness < 80:
            return {
                "passed": False,
                "error": f"Completeness {completeness:.1f}% is below threshold",
                "missing_fields": missing_fields,
            }

        return {"passed": True, "completeness": completeness}

    def _check_validity(self, data: dict[str, Any]) -> dict[str, Any]:
        """Check data validity."""
        type_checks = {
            "email": lambda x: bool(re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", str(x))),
            "phone": lambda x: bool(re.match(r"^\+?[\d\s-]{10,}$", str(x))),
            "url": lambda x: str(x).startswith(("http://", "https://")),
            "date": lambda x: bool(re.match(r"^\d{4}-\d{2}-\d{2}$", str(x))),
        }

        invalid_fields = []
        for field, value in data.items():
            field_type = data.get(f"{field}_type")
            if field_type in type_checks and value:
                if not type_checks[field_type](value):
                    invalid_fields.append(field)

        if invalid_fields:
            return {
                "passed": False,
                "error": f"Invalid fields: {invalid_fields}",
            }

        return {"passed": True}

    def _check_consistency(self, data: dict[str, Any]) -> dict[str, Any]:
        """Check data consistency."""
        inconsistencies = []

        if "start_date" in data and "end_date" in data:
            start = data.get("start_date", "")
            end = data.get("end_date", "")
            if start and end and start > end:
                inconsistencies.append("start_date is after end_date")

        if "quantity" in data and "unit_price" in data:
            if data.get("quantity", 0) < 0 or data.get("unit_price", 0) < 0:
                inconsistencies.append("Negative quantity or price")

        if inconsistencies:
            return {"passed": False, "error": "; ".join(inconsistencies)}

        return {"passed": True}

    def _check_uniqueness(self, data: dict[str, Any]) -> dict[str, Any]:
        """Check uniqueness of data records."""
        if not isinstance(data, list):
            return {"passed": True, "warning": "Uniqueness check requires list data"}

        seen = set()
        duplicates = []
        for item in data:
            key = str(item.get("id", item))
            if key in seen:
                duplicates.append(key)
            seen.add(key)

        if duplicates:
            return {
                "passed": False,
                "error": f"Found {len(duplicates)} duplicate records",
            }

        return {"passed": True}

    def _check_freshness(self, data: dict[str, Any]) -> dict[str, Any]:
        """Check data freshness."""
        timestamp_field = data.get("timestamp_field", "updated_at")
        max_age_seconds = data.get("max_age", 86400)

        if timestamp_field not in data:
            return {"passed": True, "warning": "No timestamp field found"}

        try:
            record_time = data[timestamp_field]
            age = time.time() - record_time
            if age > max_age_seconds:
                return {
                    "passed": False,
                    "error": f"Data is {age:.0f}s old, exceeds {max_age_seconds}s threshold",
                }
        except Exception as e:
            return {"passed": False, "error": f"Freshness check failed: {e}"}

        return {"passed": True}

    def _register_schema(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register data schema."""
        schema_name = params.get("schema_name", "")
        schema_def = params.get("schema", {})

        if not schema_name:
            return {"success": False, "error": "Schema name is required"}

        self._schemas[schema_name] = {
            "definition": schema_def,
            "registered_at": time.time(),
        }

        return {"success": True, "schema_name": schema_name}

    def _validate_against_schema(self, params: dict[str, Any]) -> dict[str, Any]:
        """Validate data against registered schema."""
        schema_name = params.get("schema_name", "")
        data = params.get("data", {})

        if schema_name not in self._schemas:
            return {"success": False, "error": f"Schema '{schema_name}' not found"}

        schema = self._schemas[schema_name]["definition"]
        errors = []

        for field, field_spec in schema.items():
            required = field_spec.get("required", False)
            field_type = field_spec.get("type")

            if required and field not in data:
                errors.append(f"Required field '{field}' is missing")
                continue

            if field in data and field_type:
                value = data[field]
                if not self._validate_type(value, field_type):
                    errors.append(f"Field '{field}' has invalid type, expected {field_type}")

        return {
            "success": len(errors) == 0,
            "errors": errors,
        }

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate value against expected type."""
        type_map = {
            "string": str,
            "integer": int,
            "float": (int, float),
            "boolean": bool,
            "list": list,
            "dict": dict,
        }
        return isinstance(value, type_map.get(expected_type, object))

    def _get_report(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get quality report."""
        return {
            "success": True,
            "schemas_registered": len(self._schemas),
            "validations_performed": len(self._validation_history),
            "schemas": list(self._schemas.keys()),
        }
