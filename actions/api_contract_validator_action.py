"""
API Contract Validator Module.

Validates API responses against OpenAPI/RAML specifications,
checks schema conformance, and ensures contract compatibility
for API testing and monitoring.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, Pattern, Match
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import json
import logging
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation strictness levels."""
    STRICT = auto()
    MODERATE = auto()
    LAX = auto()


class ValidationResult(Enum):
    """Validation result status."""
    VALID = auto()
    INVALID = auto()
    WARNING = auto()
    SKIPPED = auto()


@dataclass
class ContractViolation:
    """Represents a contract validation error."""
    field_path: str
    expected: Any
    actual: Any
    message: str
    severity: str = "error"
    schema_path: Optional[str] = None


@dataclass
class ValidationReport:
    """Complete validation report."""
    status: ValidationResult
    response_code: int
    content_type: str
    violations: List[ContractViolation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration_ms: float
    validated_at: datetime = field(default_factory=datetime.now)
    schema_version: Optional[str] = None
    
    @property
    def is_valid(self) -> bool:
        return self.status == ValidationResult.VALID
    
    @property
    def error_count(self) -> int:
        return len([v for v in self.violations if v.severity == "error"])
    
    @property
    def warning_count(self) -> int:
        return len(self.warnings)


class JsonSchemaValidator:
    """Validates JSON data against JSON Schema."""
    
    TYPE_MAPPING = {
        "string": str,
        "integer": int,
        "number": (int, float),
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None)
    }
    
    def __init__(self, schema: Dict[str, Any]) -> None:
        self.schema = schema
    
    def validate(self, data: Any) -> List[ContractViolation]:
        """Validate data against schema."""
        violations = []
        self._validate_value(data, self.schema, "", violations)
        return violations
    
    def _validate_value(
        self,
        value: Any,
        schema: Dict[str, Any],
        path: str,
        violations: List[ContractViolation]
    ) -> None:
        """Recursively validate value against schema."""
        if schema.get("type"):
            expected_type = schema["type"]
            if not self._check_type(value, expected_type):
                violations.append(ContractViolation(
                    field_path=path or "$",
                    expected=f"type: {expected_type}",
                    actual=type(value).__name__,
                    message=f"Type mismatch at {path or '$'}",
                    schema_path=path or "$"
                ))
                return
        
        if schema.get("enum"):
            if value not in schema["enum"]:
                violations.append(ContractViolation(
                    field_path=path,
                    expected=schema["enum"],
                    actual=value,
                    message=f"Value not in enum at {path}"
                ))
        
        if schema.get("minimum") is not None:
            if isinstance(value, (int, float)) and value < schema["minimum"]:
                violations.append(ContractViolation(
                    field_path=path,
                    expected=f">= {schema['minimum']}",
                    actual=value,
                    message=f"Value below minimum at {path}"
                ))
        
        if schema.get("maximum") is not None:
            if isinstance(value, (int, float)) and value > schema["maximum"]:
                violations.append(ContractViolation(
                    field_path=path,
                    expected=f"<= {schema['maximum']}",
                    actual=value,
                    message=f"Value above maximum at {path}"
                ))
        
        if schema.get("minLength") is not None:
            if isinstance(value, str) and len(value) < schema["minLength"]:
                violations.append(ContractViolation(
                    field_path=path,
                    expected=f"length >= {schema['minLength']}",
                    actual=len(value),
                    message=f"String too short at {path}"
                ))
        
        if schema.get("pattern"):
            if isinstance(value, str) and not re.match(schema["pattern"], value):
                violations.append(ContractViolation(
                    field_path=path,
                    expected=f"pattern: {schema['pattern']}",
                    actual=value,
                    message=f"Pattern mismatch at {path}"
                ))
        
        if schema.get("properties"):
            for prop_name, prop_schema in schema["properties"].items():
                if prop_name in (value or {}):
                    new_path = f"{path}.{prop_name}" if path else prop_name
                    self._validate_value(value[prop_name], prop_schema, new_path, violations)
        
        if schema.get("required"):
            for req_field in schema["required"]:
                if req_field not in (value or {}):
                    violations.append(ContractViolation(
                        field_path=f"{path}.{req_field}" if path else req_field,
                        expected="present",
                        actual="missing",
                        message=f"Required field missing at {path}.{req_field}"
                    ))
        
        if schema.get("items") and isinstance(value, list):
            for i, item in enumerate(value):
                self._validate_value(item, schema["items"], f"{path}[{i}]", violations)
    
    def _check_type(self, value: Any, expected_type: Union[str, List[str]]) -> bool:
        """Check if value matches expected type."""
        if value is None:
            return True
        
        if isinstance(expected_type, list):
            return any(self._check_type(value, t) for t in expected_type)
        
        type_class = self.TYPE_MAPPING.get(expected_type)
        if type_class is None:
            return True
        
        return isinstance(value, type_class)


class ApiContractValidator:
    """
    Validates API responses against contract specifications.
    
    Supports OpenAPI 3.x schemas, custom validation rules,
    and provides detailed violation reports.
    """
    
    def __init__(
        self,
        spec: Optional[Dict[str, Any]] = None,
        validation_level: ValidationLevel = ValidationLevel.MODERATE
    ) -> None:
        self.spec = spec
        self.validation_level = validation_level
        self._schema_validators: Dict[str, JsonSchemaValidator] = {}
        
        if spec:
            self._load_schemas_from_spec()
    
    def _load_schemas_from_spec(self) -> None:
        """Load schemas from OpenAPI spec."""
        if not self.spec:
            return
        
        schemas = self.spec.get("components", {}).get("schemas", {})
        for name, schema in schemas.items():
            self._schema_validators[name] = JsonSchemaValidator(schema)
    
    def validate_response(
        self,
        status_code: int,
        headers: Dict[str, str],
        body: Any,
        endpoint: Optional[str] = None,
        method: Optional[str] = None
    ) -> ValidationReport:
        """
        Validate complete API response.
        
        Args:
            status_code: HTTP status code
            headers: Response headers
            body: Response body
            endpoint: API endpoint path
            method: HTTP method
            
        Returns:
            ValidationReport with results
        """
        start_time = datetime.now()
        violations = []
        warnings = []
        
        content_type = headers.get("Content-Type", "").split(";")[0].strip()
        
        # Check if response has content
        if status_code >= 400 and body:
            warnings.append(f"Error response has body: {str(body)[:100]}")
        
        # Find matching schema
        if endpoint and method and self.spec:
            schema = self._find_response_schema(endpoint, method, status_code)
            if schema:
                violations.extend(self._validate_against_schema(body, schema))
        
        # Validate content type
        expected_content_types = self._get_expected_content_types(endpoint, method)
        if expected_content_types and content_type not in expected_content_types:
            violations.append(ContractViolation(
                field_path="$",
                expected=expected_content_types,
                actual=content_type,
                message="Content-Type mismatch"
            ))
        
        # Check required headers
        if endpoint:
            required_headers = self._get_required_headers(endpoint, method)
            for header in required_headers:
                if header.lower() not in {k.lower(): v for k, v in headers.items()}:
                    if self.validation_level == ValidationLevel.STRICT:
                        violations.append(ContractViolation(
                            field_path=f"headers.{header}",
                            expected="present",
                            actual="missing",
                            message=f"Required header missing: {header}"
                        ))
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        if violations:
            status = ValidationResult.INVALID
        elif warnings:
            status = ValidationResult.WARNING
        else:
            status = ValidationResult.VALID
        
        return ValidationReport(
            status=status,
            response_code=status_code,
            content_type=content_type,
            violations=violations,
            warnings=warnings,
            duration_ms=duration_ms,
            schema_version=self.spec.get("openapi", self.spec.get("swagger")) if self.spec else None
        )
    
    def _find_response_schema(
        self,
        endpoint: str,
        method: str,
        status_code: int
    ) -> Optional[Dict[str, Any]]:
        """Find schema for response."""
        if not self.spec:
            return None
        
        paths = self.spec.get("paths", {})
        path_item = paths.get(endpoint)
        if not path_item:
            return None
        
        operation = path_item.get(method.lower())
        if not operation:
            return None
        
        responses = operation.get("responses", {})
        response = responses.get(str(status_code)) or responses.get("default")
        if not response:
            return None
        
        content = response.get("content", {})
        for media_type in content.values():
            schema = media_type.get("schema")
            if schema:
                return schema
        
        return None
    
    def _validate_against_schema(
        self,
        body: Any,
        schema: Dict[str, Any]
    ) -> List[ContractViolation]:
        """Validate body against schema."""
        validator = JsonSchemaValidator(schema)
        return validator.validate(body)
    
    def _get_expected_content_types(
        self,
        endpoint: Optional[str],
        method: Optional[str]
    ) -> List[str]:
        """Get expected content types for endpoint."""
        if not (endpoint and method and self.spec):
            return []
        
        path_item = self.spec.get("paths", {}).get(endpoint, {})
        operation = path_item.get(method.lower(), {})
        
        produces = operation.get("produces") or self.spec.get("produces") or []
        return produces
    
    def _get_required_headers(
        self,
        endpoint: Optional[str],
        method: Optional[str]
    ) -> List[str]:
        """Get required response headers for endpoint."""
        return []  # Override to add header requirements
    
    def add_custom_validation(
        self,
        name: str,
        validator: Callable[[Any], List[ContractViolation]]
    ) -> None:
        """Add custom validation function."""
        setattr(self, f"_custom_{name}", validator)
    
    def validate_against_custom_rules(
        self,
        body: Any,
        rules: List[Dict[str, Any]]
    ) -> List[ContractViolation]:
        """Validate against custom rules."""
        violations = []
        
        for rule in rules:
            field_path = rule.get("field")
            rule_type = rule.get("type")
            
            if rule_type == "required":
                value = self._get_nested_value(body, field_path)
                if value is None:
                    violations.append(ContractViolation(
                        field_path=field_path,
                        expected="present",
                        actual="missing",
                        message=f"Required field missing: {field_path}"
                    ))
            
            elif rule_type == "range":
                value = self._get_nested_value(body, field_path)
                min_val = rule.get("min")
                max_val = rule.get("max")
                
                if min_val is not None and value < min_val:
                    violations.append(ContractViolation(
                        field_path=field_path,
                        expected=f">= {min_val}",
                        actual=value,
                        message=f"Value below minimum at {field_path}"
                    ))
                
                if max_val is not None and value > max_val:
                    violations.append(ContractViolation(
                        field_path=field_path,
                        expected=f"<= {max_val}",
                        actual=value,
                        message=f"Value above maximum at {field_path}"
                    ))
        
        return violations
    
    def _get_nested_value(self, data: Any, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split(".")
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    idx = int(part)
                    current = current[idx]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        
        return current


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample OpenAPI spec
    spec = {
        "openapi": "3.0.0",
        "components": {
            "schemas": {
                "User": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "integer"},
                        "name": {"type": "string", "minLength": 1},
                        "email": {"type": "string", "pattern": "^[^@]+@[^@]+$"},
                        "age": {"type": "integer", "minimum": 0, "maximum": 150}
                    },
                    "required": ["id", "name", "email"]
                }
            }
        }
    }
    
    validator = ApiContractValidator(spec=spec)
    
    # Valid response
    valid_user = {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    }
    
    # Invalid response
    invalid_user = {
        "id": "not_an_int",  # Wrong type
        "name": "",  # Too short
        "email": "invalid_email",  # Pattern mismatch
        "age": 200  # Above maximum
    }
    
    print("=== API Contract Validation ===")
    
    result = validator.validate_response(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body=valid_user
    )
    
    print(f"\nValid user: {result.is_valid}")
    print(f"Violations: {result.error_count}")
    
    result = validator.validate_response(
        status_code=200,
        headers={"Content-Type": "application/json"},
        body=invalid_user
    )
    
    print(f"\nInvalid user: {result.is_valid}")
    print(f"Violations: {result.error_count}")
    for v in result.violations:
        print(f"  - {v.field_path}: {v.message}")
