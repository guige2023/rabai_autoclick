"""Data Validator Action Module.

Validates data against schemas and rules with:
- Schema validation
- Custom rule validation
- Cross-field validation
- Error reporting
- Auto-correction

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class ValidationErrorType(Enum):
    """Types of validation errors."""
    REQUIRED = auto()
    TYPE_MISMATCH = auto()
    RANGE_ERROR = auto()
    PATTERN_ERROR = auto()
    CUSTOM = auto()
    CROSS_FIELD = auto()


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    error_type: ValidationErrorType
    message: str
    value: Any = None
    expected: Any = None
    row_index: Optional[int] = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    records_checked: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duration_ms: float = 0.0


class SchemaValidator:
    """Validates data against a schema definition.
    
    Features:
    - Type validation
    - Range checking
    - Pattern matching
    - Required field validation
    - Custom validator functions
    """
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self._validators: Dict[str, List[Callable]] = {}
    
    def add_validator(
        self,
        field_name: str,
        validator: Callable[[Any], Tuple[bool, Optional[str]]]
    ) -> None:
        """Add a custom validator for a field.
        
        Args:
            field_name: Field to validate
            validator: Function returning (is_valid, error_message)
        """
        if field_name not in self._validators:
            self._validators[field_name] = []
        self._validators[field_name].append(validator)
    
    async def validate(
        self,
        records: List[Dict[str, Any]],
        continue_on_error: bool = True
    ) -> ValidationResult:
        """Validate records against schema.
        
        Args:
            records: Records to validate
            continue_on_error: Whether to continue after first error
            
        Returns:
            Validation result
        """
        import time
        start_time = time.time()
        
        errors: List[ValidationError] = []
        warnings: List[str] = []
        valid_count = 0
        invalid_count = 0
        
        required_fields = self.schema.get("required", [])
        field_schemas = self.schema.get("fields", {})
        
        for i, record in enumerate(records):
            record_errors = []
            
            for field_name in required_fields:
                if field_name not in record or record[field_name] is None:
                    record_errors.append(ValidationError(
                        field=field_name,
                        error_type=ValidationErrorType.REQUIRED,
                        message=f"Required field '{field_name}' is missing or null",
                        row_index=i
                    ))
            
            for field_name, value in record.items():
                field_errors = await self._validate_field(
                    field_name, value, field_schemas.get(field_name, {}), i
                )
                record_errors.extend(field_errors)
            
            if record_errors:
                errors.extend(record_errors)
                invalid_count += 1
            else:
                valid_count += 1
        
        duration_ms = (time.time() - start_time) * 1000
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            records_checked=len(records),
            valid_records=valid_count,
            invalid_records=invalid_count,
            duration_ms=duration_ms
        )
    
    async def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_schema: Dict[str, Any],
        row_index: int
    ) -> List[ValidationError]:
        """Validate a single field.
        
        Args:
            field_name: Field name
            value: Field value
            field_schema: Field schema definition
            row_index: Row index for error reporting
            
        Returns:
            List of validation errors
        """
        errors = []
        
        if value is None:
            return errors
        
        expected_type = field_schema.get("type")
        if expected_type:
            type_error = self._validate_type(value, expected_type)
            if type_error:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationErrorType.TYPE_MISMATCH,
                    message=type_error,
                    value=type(value).__name__,
                    expected=expected_type,
                    row_index=row_index
                ))
        
        min_val = field_schema.get("min")
        if min_val is not None and isinstance(value, (int, float)):
            if value < min_val:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message=f"Value {value} is less than minimum {min_val}",
                    value=value,
                    expected=f">={min_val}",
                    row_index=row_index
                ))
        
        max_val = field_schema.get("max")
        if max_val is not None and isinstance(value, (int, float)):
            if value > max_val:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationErrorType.RANGE_ERROR,
                    message=f"Value {value} exceeds maximum {max_val}",
                    value=value,
                    expected=f"<={max_val}",
                    row_index=row_index
                ))
        
        pattern = field_schema.get("pattern")
        if pattern and isinstance(value, str):
            if not re.match(pattern, value):
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationErrorType.PATTERN_ERROR,
                    message=f"Value does not match pattern '{pattern}'",
                    value=value,
                    expected=pattern,
                    row_index=row_index
                ))
        
        allowed_values = field_schema.get("enum")
        if allowed_values and value not in allowed_values:
            errors.append(ValidationError(
                field=field_name,
                error_type=ValidationErrorType.CUSTOM,
                message=f"Value must be one of {allowed_values}",
                value=value,
                expected=allowed_values,
                row_index=row_index
            ))
        
        custom_validators = self._validators.get(field_name, [])
        for validator in custom_validators:
            try:
                is_valid, error_msg = validator(value)
                if not is_valid:
                    errors.append(ValidationError(
                        field=field_name,
                        error_type=ValidationErrorType.CUSTOM,
                        message=error_msg or "Custom validation failed",
                        value=value,
                        row_index=row_index
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field=field_name,
                    error_type=ValidationErrorType.CUSTOM,
                    message=f"Validator error: {e}",
                    value=value,
                    row_index=row_index
                ))
        
        return errors
    
    def _validate_type(self, value: Any, expected_type: str) -> Optional[str]:
        """Validate value type.
        
        Args:
            value: Value to check
            expected_type: Expected type name
            
        Returns:
            Error message or None
        """
        type_map = {
            "string": str,
            "integer": int,
            "float": (int, float),
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict
        }
        
        expected = type_map.get(expected_type)
        if expected and not isinstance(value, expected):
            return f"Expected type '{expected_type}', got '{type(value).__name__}'"
        
        return None


class CrossFieldValidator:
    """Validates relationships between fields."""
    
    def __init__(self):
        self._rules: List[Dict[str, Any]] = []
    
    def add_rule(
        self,
        name: str,
        condition: Callable[[Dict[str, Any]], bool],
        error_message: str,
        fields: Optional[List[str]] = None
    ) -> None:
        """Add a cross-field validation rule.
        
        Args:
            name: Rule name
            condition: Function returning True if valid
            error_message: Error message when invalid
            fields: Fields involved in validation
        """
        self._rules.append({
            "name": name,
            "condition": condition,
            "error_message": error_message,
            "fields": fields or []
        })
    
    def add_comparison_rule(
        self,
        field1: str,
        field2: str,
        operator: str,
        error_message: Optional[str] = None
    ) -> None:
        """Add a comparison rule between two fields.
        
        Args:
            field1: First field name
            field2: Second field name
            operator: Comparison operator (eq, ne, gt, lt, gte, lte)
            error_message: Optional custom error message
        """
        operators = {
            "eq": lambda a, b: a == b,
            "ne": lambda a, b: a != b,
            "gt": lambda a, b: a > b,
            "lt": lambda a, b: a < b,
            "gte": lambda a, b: a >= b,
            "lte": lambda a, b: a <= b
        }
        
        if operator not in operators:
            raise ValueError(f"Unknown operator: {operator}")
        
        def condition(record: Dict[str, Any]) -> bool:
            if field1 not in record or field2 not in record:
                return True
            return operators[operator](record[field1], record[field2])
        
        msg = error_message or f"'{field1}' must be {operator} '{field2}'"
        
        self.add_rule(f"{field1}_{operator}_{field2}", condition, msg, [field1, field2])
    
    async def validate(
        self,
        records: List[Dict[str, Any]]
    ) -> List[ValidationError]:
        """Validate records against cross-field rules.
        
        Args:
            records: Records to validate
            
        Returns:
            List of validation errors
        """
        errors = []
        
        for i, record in enumerate(records):
            for rule in self._rules:
                try:
                    if not rule["condition"](record):
                        errors.append(ValidationError(
                            field=",".join(rule["fields"]),
                            error_type=ValidationErrorType.CROSS_FIELD,
                            message=rule["error_message"],
                            row_index=i
                        ))
                except Exception as e:
                    logger.debug(f"Cross-field validation error: {e}")
        
        return errors


class DataValidator:
    """High-level data validation orchestrator."""
    
    def __init__(self):
        self._schema_validators: Dict[str, SchemaValidator] = {}
        self._cross_field_validators: Dict[str, CrossFieldValidator] = {}
    
    def add_schema(
        self,
        schema_name: str,
        schema: Dict[str, Any]
    ) -> SchemaValidator:
        """Add a named schema.
        
        Args:
            schema_name: Schema name
            schema: Schema definition
            
        Returns:
            Schema validator
        """
        validator = SchemaValidator(schema)
        self._schema_validators[schema_name] = validator
        return validator
    
    def get_schema_validator(self, schema_name: str) -> Optional[SchemaValidator]:
        """Get a schema validator by name."""
        return self._schema_validators.get(schema_name)
    
    async def validate(
        self,
        records: List[Dict[str, Any]],
        schema_name: Optional[str] = None,
        use_cross_field: bool = True
    ) -> ValidationResult:
        """Validate records.
        
        Args:
            records: Records to validate
            schema_name: Optional schema name to use
            use_cross_field: Whether to run cross-field validation
            
        Returns:
            Combined validation result
        """
        all_errors: List[ValidationError] = []
        warnings: List[str] = []
        
        if schema_name and schema_name in self._schema_validators:
            result = await self._schema_validators[schema_name].validate(records)
            all_errors.extend(result.errors)
            warnings.extend(result.warnings)
        
        if use_cross_field:
            for name, cfv in self._cross_field_validators.items():
                cross_errors = await cfv.validate(records)
                all_errors.extend(cross_errors)
        
        return ValidationResult(
            valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=warnings,
            records_checked=len(records),
            valid_records=len(records) - len(set(e.row_index for e in all_errors if e.row_index is not None)),
            invalid_records=len(set(e.row_index for e in all_errors if e.row_index is not None))
        )
