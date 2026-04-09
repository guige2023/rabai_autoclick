"""
Data Contract Validator Action.

Validates data against structural contracts (schemas) with support
for versioning, backward compatibility checks, and migration guidance.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self


class ContractViolationType(Enum):
    """Types of contract violations."""
    MISSING_FIELD = auto()
    EXTRA_FIELD = auto()
    TYPE_MISMATCH = auto()
    VALUE_CONSTRAINT = auto()
    SCHEMA_VERSION_MISMATCH = auto()
    REQUIRED_FIELD = auto()
    PATTERN_MISMATCH = auto()
    RANGE_VIOLATION = auto()
    ENUM_VIOLATION = auto()


@dataclass
class ContractViolation:
    """A single contract violation."""
    violation_type: ContractViolationType
    path: str  # dot notation path to field
    message: str
    expected: Any
    actual: Any
    schema_version: Optional[str] = None
    is_breaking: bool = True
    suggestion: Optional[str] = None

    def __str__(self) -> str:
        severity = "BREAKING" if self.is_breaking else "WARNING"
        return f"[{severity}] {self.path}: {self.message}"


@dataclass
class MigrationGuidance:
    """Guidance for migrating data from one schema version to another."""
    from_version: str
    to_version: str
    breaking_changes: List[str] = field(default_factory=list)
    backward_compatible: List[str] = field(default_factory=list)
    migration_steps: List[str] = field(default_factory=list)


@dataclass
class ContractValidationResult:
    """Complete result of contract validation."""
    valid: bool
    violations: List[ContractViolation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: Optional[str] = None
    data_snapshot: Optional[Dict[str, Any]] = None

    def __bool__(self) -> bool:
        return self.valid

    @property
    def breaking_violations(self) -> List[ContractViolation]:
        return [v for v in self.violations if v.is_breaking]

    @property
    def non_breaking_violations(self) -> List[ContractViolation]:
        return [v for v in self.violations if not v.is_breaking]

    def summary(self) -> str:
        breaking = len(self.breaking_violations)
        warnings = len(self.non_breaking_violations)
        return (f"[{'PASS' if self.valid else 'FAIL'}] "
                f"{breaking} breaking, {warnings} warnings, "
                f"{len(self.warnings)} notices")


@dataclass
class FieldContract:
    """Contract definition for a single field."""
    name: str
    field_type: type
    required: bool = True
    default: Any = None
    pattern: Optional[str] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    allowed_values: Optional[List[Any]] = None
    nested_schema: Optional[Dict[str, Any]] = None  # For complex types
    custom_validator: Optional[Callable[[Any], Tuple[bool, str]]] = None
    deprecation_message: Optional[str] = None
    since_version: Optional[str] = None
    removed_in_version: Optional[str] = None


class SchemaContract:
    """A complete schema contract."""

    def __init__(
        self,
        name: str,
        version: str,
        fields: List[FieldContract],
        description: str = "",
    ) -> None:
        self.name = name
        self.version = version
        self.fields = {f.name: f for f in fields}
        self.description = description

    def get_field(self, name: str) -> Optional[FieldContract]:
        return self.fields.get(name)

    def all_field_names(self) -> Set[str]:
        return set(self.fields.keys())


class DataContractValidator:
    """
    Validates data against structural contracts.

    Example:
        schema = SchemaContract("user_contract", "1.0", [
            FieldContract("user_id", int, required=True),
            FieldContract("email", str, required=True,
                          custom_validator=lambda v: (bool(re.match(r"^[^@]+@[^@]+$", v)), "Invalid email")),
            FieldContract("age", int, required=False, min_value=0, max_value=150),
        ])
        validator = DataContractValidator()
        result = validator.validate(schema, {"user_id": 1, "email": "alice@example.com"})
    """

    def __init__(self) -> None:
        self._custom_type_validators: Dict[type, Callable[[Any], bool]] = {}

    def register_type_validator(self, type_: type,
                                 validator: Callable[[Any], bool]) -> None:
        """Register a custom type validator."""
        self._custom_type_validators[type_] = validator

    def validate(
        self,
        schema: SchemaContract,
        data: Dict[str, Any],
        strict: bool = False,
    ) -> ContractValidationResult:
        """
        Validate data against a schema contract.

        Args:
            schema: The schema contract to validate against
            data: The data to validate
            strict: If True, extra fields are also violations

        Returns:
            ContractValidationResult with all violations
        """
        violations: List[ContractViolation] = []
        warnings: List[str] = []

        if not isinstance(data, dict):
            violations.append(ContractViolation(
                violation_type=ContractViolationType.TYPE_MISMATCH,
                path="$",
                message="Data must be a dictionary",
                expected="dict",
                actual=type(data).__name__,
            ))
            return ContractValidationResult(valid=False, violations=violations)

        data_keys = set(data.keys())
        schema_keys = schema.all_field_names()

        # Check for missing required fields
        for field_name, field_def in schema.fields.items():
            if field_def.required and field_name not in data_keys:
                # Check if there's a default
                if field_def.default is None and field_def.since_version is None:
                    violations.append(ContractViolation(
                        violation_type=ContractViolationType.MISSING_FIELD,
                        path=field_name,
                        message=f"Required field '{field_name}' is missing",
                        expected=field_def.field_type.__name__,
                        actual=None,
                        schema_version=schema.version,
                        is_breaking=True,
                    ))

        # Check for extra fields (only in strict mode)
        if strict:
            extra = data_keys - schema_keys
            for field_name in extra:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.EXTRA_FIELD,
                    path=field_name,
                    message=f"Extra field '{field_name}' not in schema",
                    expected="<schema fields>",
                    actual=type(data[field_name]).__name__,
                    schema_version=schema.version,
                    is_breaking=False,
                    suggestion=f"Remove '{field_name}' or update schema",
                ))

        # Validate each present field
        for field_name, field_def in schema.fields.items():
            if field_name not in data_keys:
                continue

            value = data[field_name]
            field_violations = self._validate_field(field_def, value, schema.version)
            violations.extend(field_violations)

            # Check deprecation
            if field_def.deprecation_message and field_name in data_keys:
                warnings.append(f"Field '{field_name}' is deprecated: {field_def.deprecation_message}")

        valid = len([v for v in violations if v.is_breaking]) == 0
        return ContractValidationResult(
            valid=valid,
            violations=violations,
            warnings=warnings,
            schema_version=schema.version,
            data_snapshot=data if len(data) < 100 else None,
        )

    def _validate_field(
        self,
        field_def: FieldContract,
        value: Any,
        schema_version: str,
    ) -> List[ContractViolation]:
        """Validate a single field against its contract."""
        violations: List[ContractViolation] = []
        path = field_def.name

        # None handling
        if value is None:
            if field_def.required:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.REQUIRED_FIELD,
                    path=path,
                    message=f"Field '{field_def.name}' cannot be null",
                    expected=field_def.field_type.__name__,
                    actual=None,
                    schema_version=schema_version,
                    is_breaking=True,
                ))
            return violations

        # Type checking
        expected_type = field_def.field_type
        if not self._is_valid_type(value, expected_type):
            violations.append(ContractViolation(
                violation_type=ContractViolationType.TYPE_MISMATCH,
                path=path,
                message=f"Field '{field_def.name}' type mismatch",
                expected=expected_type.__name__,
                actual=type(value).__name__,
                schema_version=schema_version,
                is_breaking=True,
            ))
            return violations

        # Pattern validation
        if field_def.pattern and isinstance(value, str):
            if not re.match(field_def.pattern, value):
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.PATTERN_MISMATCH,
                    path=path,
                    message=f"Field '{field_def.name}' does not match pattern '{field_def.pattern}'",
                    expected=field_def.pattern,
                    actual=value,
                    schema_version=schema_version,
                    is_breaking=False,
                ))

        # Range validation
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if field_def.min_value is not None and value < field_def.min_value:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.RANGE_VIOLATION,
                    path=path,
                    message=f"Field '{field_def.name}' below minimum value",
                    expected=f">= {field_def.min_value}",
                    actual=value,
                    schema_version=schema_version,
                    is_breaking=False,
                ))
            if field_def.max_value is not None and value > field_def.max_value:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.RANGE_VIOLATION,
                    path=path,
                    message=f"Field '{field_def.name}' exceeds maximum value",
                    expected=f"<= {field_def.max_value}",
                    actual=value,
                    schema_version=schema_version,
                    is_breaking=False,
                ))

        # Enum validation (allowed values)
        if field_def.allowed_values is not None:
            if value not in field_def.allowed_values:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.ENUM_VIOLATION,
                    path=path,
                    message=f"Field '{field_def.name}' value not in allowed set",
                    expected=str(field_def.allowed_values),
                    actual=value,
                    schema_version=schema_version,
                    is_breaking=True,
                ))

        # Custom validator
        if field_def.custom_validator:
            try:
                valid, msg = field_def.custom_validator(value)
                if not valid:
                    violations.append(ContractViolation(
                        violation_type=ContractViolationType.VALUE_CONSTRAINT,
                        path=path,
                        message=f"Field '{field_def.name}': {msg}",
                        expected="custom",
                        actual=value,
                        schema_version=schema_version,
                        is_breaking=False,
                    ))
            except Exception as exc:
                violations.append(ContractViolation(
                    violation_type=ContractViolationType.VALUE_CONSTRAINT,
                    path=path,
                    message=f"Custom validator error: {exc}",
                    expected="valid",
                    actual=str(exc),
                    schema_version=schema_version,
                    is_breaking=False,
                ))

        return violations

    def _is_valid_type(self, value: Any, expected_type: type) -> bool:
        """Check if value matches expected type."""
        if expected_type is Any:
            return True
        if expected_type in self._custom_type_validators:
            return self._custom_type_validators[expected_type](value)
        try:
            return isinstance(value, expected_type)
        except TypeError:
            return type(value) == expected_type


def compare_schemas(old: SchemaContract, new: SchemaContract) -> MigrationGuidance:
    """
    Compare two schema versions and generate migration guidance.

    Detects breaking vs non-breaking changes.
    """
    old_fields = old.all_field_names()
    new_fields = new.all_field_names()

    breaking: List[str] = []
    backward_compatible: List[str] = []

    removed = old_fields - new_fields
    added = new_fields - old_fields
    common = old_fields & new_fields

    for field_name in removed:
        breaking.append(f"Field '{field_name}' was removed")

    for field_name in added:
        backward_compatible.append(f"Field '{field_name}' was added (optional)")

    for field_name in common:
        old_field = old.get_field(field_name)
        new_field = new.get_field(field_name)
        if old_field and new_field:
            if old_field.required and not new_field.required:
                backward_compatible.append(f"Field '{field_name}' made optional")
            elif not old_field.required and new_field.required:
                breaking.append(f"Field '{field_name}' made required")

    return MigrationGuidance(
        from_version=old.version,
        to_version=new.version,
        breaking_changes=breaking,
        backward_compatible=backward_compatible,
        migration_steps=[],
    )
