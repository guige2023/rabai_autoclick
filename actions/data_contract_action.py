"""Data Contract Action Module.

Provides data contract validation and schema enforcement
for data quality and compliance.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ContractLevel(Enum):
    """Data contract enforcement level."""
    STRICT = "strict"
    MODERATE = "moderate"
    LAX = "lax"


class ViolationSeverity(Enum):
    """Violation severity level."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class FieldConstraint:
    """Constraint for a data field."""
    field_name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable] = None


@dataclass
class DataContract:
    """Data contract definition."""
    contract_id: str
    name: str
    version: str
    description: Optional[str] = None
    level: ContractLevel = ContractLevel.MODERATE
    fields: List[FieldConstraint] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class ContractViolation:
    """A contract violation."""
    field_name: str
    severity: ViolationSeverity
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    constraint_type: Optional[str] = None


@dataclass
class ContractValidationResult:
    """Result of contract validation."""
    contract_id: str
    passed: bool
    record_count: int
    violations: List[ContractViolation]
    timestamp: float = field(default_factory=time.time)
    duration_ms: float = 0.0


class DataContractValidator:
    """Validates data against contracts."""

    def __init__(self):
        self._contracts: Dict[str, DataContract] = {}
        self._validation_history: List[ContractValidationResult] = []

    def create_contract(
        self,
        contract_id: str,
        name: str,
        version: str = "1.0.0",
        description: Optional[str] = None,
        level: ContractLevel = ContractLevel.MODERATE
    ) -> DataContract:
        """Create a new data contract."""
        contract = DataContract(
            contract_id=contract_id,
            name=name,
            version=version,
            description=description,
            level=level
        )
        self._contracts[contract_id] = contract
        return contract

    def add_field_constraint(
        self,
        contract_id: str,
        constraint: FieldConstraint
    ) -> bool:
        """Add a field constraint to contract."""
        if contract_id not in self._contracts:
            return False

        self._contracts[contract_id].fields.append(constraint)
        self._contracts[contract_id].updated_at = time.time()
        return True

    def get_contract(self, contract_id: str) -> Optional[DataContract]:
        """Get contract by ID."""
        return self._contracts.get(contract_id)

    def validate_record(
        self,
        contract_id: str,
        record: Dict[str, Any]
    ) -> List[ContractViolation]:
        """Validate a single record against contract."""
        violations = []
        contract = self._contracts.get(contract_id)

        if not contract:
            violations.append(ContractViolation(
                field_name="_contract",
                severity=ViolationSeverity.CRITICAL,
                message=f"Contract not found: {contract_id}"
            ))
            return violations

        field_names = {c.field_name for c in contract.fields}

        for constraint in contract.fields:
            value = record.get(constraint.field_name)
            field_violations = self._validate_field(constraint, value)
            violations.extend(field_violations)

        for field_name in record:
            if field_name not in field_names and contract.level == ContractLevel.STRICT:
                violations.append(ContractViolation(
                    field_name=field_name,
                    severity=ViolationSeverity.MEDIUM,
                    message=f"Unexpected field in strict mode: {field_name}"
                ))

        return violations

    def _validate_field(
        self,
        constraint: FieldConstraint,
        value: Any
    ) -> List[ContractViolation]:
        """Validate a single field against constraint."""
        violations = []

        if value is None:
            if constraint.required and not constraint.nullable:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.HIGH,
                    message=f"Required field is null: {constraint.field_name}",
                    constraint_type="required"
                ))
            return violations

        if constraint.field_type == "string" and not isinstance(value, str):
            violations.append(ContractViolation(
                field_name=constraint.field_name,
                severity=ViolationSeverity.HIGH,
                message=f"Expected string, got {type(value).__name__}",
                expected="string",
                actual=type(value).__name__,
                constraint_type="type"
            ))
            return violations

        if constraint.field_type == "integer" and not isinstance(value, int):
            violations.append(ContractViolation(
                field_name=constraint.field_name,
                severity=ViolationSeverity.HIGH,
                message=f"Expected integer, got {type(value).__name__}",
                expected="integer",
                actual=type(value).__name__,
                constraint_type="type"
            ))
            return violations

        if constraint.field_type == "number" and not isinstance(value, (int, float)):
            violations.append(ContractViolation(
                field_name=constraint.field_name,
                severity=ViolationSeverity.HIGH,
                message=f"Expected number, got {type(value).__name__}",
                expected="number",
                actual=type(value).__name__,
                constraint_type="type"
            ))
            return violations

        if isinstance(value, (int, float)):
            if constraint.min_value is not None and value < constraint.min_value:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.MEDIUM,
                    message=f"Value {value} below minimum {constraint.min_value}",
                    expected=f">={constraint.min_value}",
                    actual=value,
                    constraint_type="min_value"
                ))

            if constraint.max_value is not None and value > constraint.max_value:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.MEDIUM,
                    message=f"Value {value} above maximum {constraint.max_value}",
                    expected=f"<={constraint.max_value}",
                    actual=value,
                    constraint_type="max_value"
                ))

        if isinstance(value, str):
            if constraint.min_length is not None and len(value) < constraint.min_length:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.LOW,
                    message=f"String length {len(value)} below minimum {constraint.min_length}",
                    constraint_type="min_length"
                ))

            if constraint.max_length is not None and len(value) > constraint.max_length:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.MEDIUM,
                    message=f"String length {len(value)} exceeds maximum {constraint.max_length}",
                    constraint_type="max_length"
                ))

            if constraint.pattern:
                import re
                if not re.match(constraint.pattern, value):
                    violations.append(ContractViolation(
                        field_name=constraint.field_name,
                        severity=ViolationSeverity.MEDIUM,
                        message=f"String does not match pattern: {constraint.pattern}",
                        expected=constraint.pattern,
                        actual=value,
                        constraint_type="pattern"
                    ))

        if constraint.allowed_values and value not in constraint.allowed_values:
            violations.append(ContractViolation(
                field_name=constraint.field_name,
                severity=ViolationSeverity.MEDIUM,
                message=f"Value not in allowed list: {value}",
                expected=constraint.allowed_values,
                actual=value,
                constraint_type="allowed_values"
            ))

        if constraint.custom_validator:
            try:
                if not constraint.custom_validator(value):
                    violations.append(ContractViolation(
                        field_name=constraint.field_name,
                        severity=ViolationSeverity.HIGH,
                        message="Custom validation failed",
                        constraint_type="custom"
                    ))
            except Exception as e:
                violations.append(ContractViolation(
                    field_name=constraint.field_name,
                    severity=ViolationSeverity.HIGH,
                    message=f"Custom validator error: {str(e)}",
                    constraint_type="custom"
                ))

        return violations

    def validate_batch(
        self,
        contract_id: str,
        records: List[Dict[str, Any]]
    ) -> ContractValidationResult:
        """Validate a batch of records."""
        start_time = time.time()
        all_violations = []

        for record in records:
            violations = self.validate_record(contract_id, record)
            all_violations.extend(violations)

        duration = (time.time() - start_time) * 1000

        result = ContractValidationResult(
            contract_id=contract_id,
            passed=len(all_violations) == 0,
            record_count=len(records),
            violations=all_violations,
            duration_ms=duration
        )

        self._validation_history.append(result)
        return result

    def get_history(
        self,
        contract_id: Optional[str] = None,
        limit: int = 100
    ) -> List[ContractValidationResult]:
        """Get validation history."""
        history = self._validation_history

        if contract_id:
            history = [h for h in history if h.contract_id == contract_id]

        return history[-limit:]

    def get_contract_stats(
        self,
        contract_id: str
    ) -> Dict[str, Any]:
        """Get contract statistics."""
        history = [
            h for h in self._validation_history
            if h.contract_id == contract_id
        ]

        if not history:
            return {
                "total_validations": 0,
                "passed_count": 0,
                "failed_count": 0,
                "pass_rate": 0.0
            }

        passed = sum(1 for h in history if h.passed)
        total = len(history)

        return {
            "total_validations": total,
            "passed_count": passed,
            "failed_count": total - passed,
            "pass_rate": passed / total if total > 0 else 0.0
        }


class DataContractAction(BaseAction):
    """Action for data contract operations."""

    def __init__(self):
        super().__init__("data_contract")
        self._validator = DataContractValidator()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute data contract action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_contract(params)
            elif operation == "add_field":
                return self._add_field(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "validate_batch":
                return self._validate_batch(params)
            elif operation == "get_contract":
                return self._get_contract(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "stats":
                return self._get_stats(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_contract(self, params: Dict[str, Any]) -> ActionResult:
        """Create a data contract."""
        contract_id = params.get("contract_id")
        name = params.get("name", "Unnamed Contract")

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        contract = self._validator.create_contract(
            contract_id=contract_id,
            name=name,
            version=params.get("version", "1.0.0"),
            description=params.get("description"),
            level=ContractLevel(params.get("level", "moderate"))
        )

        return ActionResult(
            success=True,
            message=f"Contract created: {contract_id}"
        )

    def _add_field(self, params: Dict[str, Any]) -> ActionResult:
        """Add field constraint to contract."""
        contract_id = params.get("contract_id")

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        constraint = FieldConstraint(
            field_name=params.get("field_name", ""),
            field_type=params.get("field_type", "string"),
            required=params.get("required", False),
            nullable=params.get("nullable", True),
            min_value=params.get("min_value"),
            max_value=params.get("max_value"),
            min_length=params.get("min_length"),
            max_length=params.get("max_length"),
            pattern=params.get("pattern"),
            allowed_values=params.get("allowed_values")
        )

        success = self._validator.add_field_constraint(contract_id, constraint)

        return ActionResult(
            success=success,
            message="Field constraint added" if success else "Contract not found"
        )

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a single record."""
        contract_id = params.get("contract_id")
        record = params.get("record", {})

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        violations = self._validator.validate_record(contract_id, record)

        return ActionResult(
            success=len(violations) == 0,
            data={
                "passed": len(violations) == 0,
                "violation_count": len(violations),
                "violations": [
                    {
                        "field": v.field_name,
                        "severity": v.severity.value,
                        "message": v.message,
                        "constraint_type": v.constraint_type
                    }
                    for v in violations
                ]
            }
        )

    def _validate_batch(self, params: Dict[str, Any]) -> ActionResult:
        """Validate batch of records."""
        contract_id = params.get("contract_id")
        records = params.get("records", [])

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        result = self._validator.validate_batch(contract_id, records)

        return ActionResult(
            success=result.passed,
            data={
                "passed": result.passed,
                "record_count": result.record_count,
                "violation_count": len(result.violations),
                "duration_ms": result.duration_ms,
                "violations": [
                    {
                        "field": v.field_name,
                        "severity": v.severity.value,
                        "message": v.message
                    }
                    for v in result.violations
                ]
            }
        )

    def _get_contract(self, params: Dict[str, Any]) -> ActionResult:
        """Get contract details."""
        contract_id = params.get("contract_id")

        if not contract_id:
            return ActionResult(
                success=True,
                data={
                    "contracts": [
                        {"contract_id": c.contract_id, "name": c.name}
                        for c in self._validator._contracts.values()
                    ]
                }
            )

        contract = self._validator.get_contract(contract_id)
        if not contract:
            return ActionResult(success=False, message="Contract not found")

        return ActionResult(
            success=True,
            data={
                "contract_id": contract.contract_id,
                "name": contract.name,
                "version": contract.version,
                "description": contract.description,
                "level": contract.level.value,
                "field_count": len(contract.fields)
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get validation history."""
        contract_id = params.get("contract_id")
        limit = params.get("limit", 100)

        history = self._validator.get_history(contract_id, limit)

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "contract_id": h.contract_id,
                        "passed": h.passed,
                        "record_count": h.record_count,
                        "violation_count": len(h.violations),
                        "timestamp": h.timestamp
                    }
                    for h in history
                ]
            }
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get contract statistics."""
        contract_id = params.get("contract_id")

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        stats = self._validator.get_contract_stats(contract_id)

        return ActionResult(success=True, data=stats)
