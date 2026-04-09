"""
API Contract Action Module.

Provides API contract validation and testing, ensuring API responses
conform to expected schemas and contracts.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import logging
import re

logger = logging.getLogger(__name__)


class ContractStatus(Enum):
    """Status of contract validation."""
    PASSED = auto()
    FAILED = auto()
    SKIPPED = auto()
    WARNING = auto()


@dataclass
class ContractField:
    """Defines a field in a contract."""
    name: str
    field_type: str
    required: bool = True
    nullable: bool = False
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    allowed_values: Optional[Set[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None
    description: str = ""

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate a value against this field definition."""
        if value is None:
            if self.required and not self.nullable:
                return False, f"Field '{self.name}' is required"
            return True, None

        type_checks = {
            "string": lambda v: isinstance(v, str),
            "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
            "number": lambda v: isinstance(v, (int, float)),
            "boolean": lambda v: isinstance(v, bool),
            "array": lambda v: isinstance(v, list),
            "object": lambda v: isinstance(v, dict),
        }

        if self.field_type in type_checks:
            if not type_checks[self.field_type](value):
                return False, f"Field '{self.name}' must be of type {self.field_type}"

        if self.pattern:
            if not isinstance(value, str):
                return False, f"Field '{self.name}' must be a string for pattern matching"
            if not re.match(self.pattern, value):
                return False, f"Field '{self.name}' does not match pattern {self.pattern}"

        if self.min_value is not None:
            if isinstance(value, (int, float)) and value < self.min_value:
                return False, f"Field '{self.name}' must be >= {self.min_value}"

        if self.max_value is not None:
            if isinstance(value, (int, float)) and value > self.max_value:
                return False, f"Field '{self.name}' must be <= {self.max_value}"

        if self.min_length is not None:
            if isinstance(value, (str, list)) and len(value) < self.min_length:
                return False, f"Field '{self.name}' must have length >= {self.min_length}"

        if self.max_length is not None:
            if isinstance(value, (str, list)) and len(value) > self.max_length:
                return False, f"Field '{self.name}' must have length <= {self.max_length}"

        if self.allowed_values is not None:
            if value not in self.allowed_values:
                return False, f"Field '{self.name}' must be one of {self.allowed_values}"

        if self.custom_validator:
            try:
                if not self.custom_validator(value):
                    return False, f"Field '{self.name}' failed custom validation"
            except Exception as e:
                return False, f"Field '{self.name}' custom validation error: {e}"

        return True, None


@dataclass
class ContractDefinition:
    """Defines an API contract."""
    name: str
    version: str
    fields: Dict[str, ContractField]
    required_fields: List[str] = field(default_factory=list)
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_field(self, field: ContractField) -> None:
        """Add a field to the contract."""
        self.fields[field.name] = field
        if field.required:
            self.required_fields.append(field.name)


@dataclass
class ContractViolation:
    """Represents a contract violation."""
    field_name: str
    expected: str
    actual: Any
    message: str
    severity: str = "error"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field": self.field_name,
            "expected": self.expected,
            "actual": str(self.actual),
            "message": self.message,
            "severity": self.severity,
        }


@dataclass
class ContractResult:
    """Result of contract validation."""
    status: ContractStatus
    contract_name: str
    violations: List[ContractViolation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)
    duration_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def passed(self) -> bool:
        """Check if validation passed."""
        return self.status == ContractStatus.PASSED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status": self.status.name,
            "contract": self.contract_name,
            "violation_count": len(self.violations),
            "warning_count": len(self.warnings),
            "validated_at": self.validated_at.isoformat(),
            "duration_ms": self.duration_ms,
            "violations": [v.to_dict() for v in self.violations],
            "warnings": self.warnings,
        }


class ContractRegistry:
    """Registry for API contracts."""

    def __init__(self):
        """Initialize the contract registry."""
        self._contracts: Dict[str, ContractDefinition] = {}

    def register(self, contract: ContractDefinition) -> None:
        """Register a contract."""
        self._contracts[contract.name] = contract
        logger.info(f"Registered contract: {contract.name} v{contract.version}")

    def get(self, name: str) -> Optional[ContractDefinition]:
        """Get a contract by name."""
        return self._contracts.get(name)

    def list_contracts(self) -> List[str]:
        """List all registered contract names."""
        return list(self._contracts.keys())

    def unregister(self, name: str) -> bool:
        """Unregister a contract."""
        if name in self._contracts:
            del self._contracts[name]
            return True
        return False


class ApiContractAction:
    """
    Validates API responses against defined contracts.

    This action provides contract-based validation for API responses,
    ensuring that data conforms to expected schemas and business rules.

    Example:
        >>> action = ApiContractAction()
        >>> contract = ContractDefinition(name="user", version="1.0")
        >>> contract.add_field(ContractField("id", "integer", required=True))
        >>> action.register(contract)
        >>> result = action.validate("user", {"id": 123, "name": "Alice"})
        >>> print(result.passed())
        True
    """

    def __init__(self):
        """Initialize the API Contract Action."""
        self.registry = ContractRegistry()
        self._validation_cache: Dict[str, ContractResult] = {}
        self._cache_ttl_seconds: float = 300.0

    def create_contract(
        self,
        name: str,
        version: str,
        description: str = "",
    ) -> ContractDefinition:
        """
        Create a new contract definition.

        Args:
            name: Contract name.
            version: Contract version.
            description: Optional description.

        Returns:
            Created ContractDefinition.
        """
        contract = ContractDefinition(
            name=name,
            version=version,
            fields={},
            description=description,
        )
        self.registry.register(contract)
        return contract

    def add_field(
        self,
        contract_name: str,
        name: str,
        field_type: str,
        required: bool = True,
        **kwargs,
    ) -> bool:
        """
        Add a field to a contract.

        Args:
            contract_name: Name of the contract.
            name: Field name.
            field_type: Field type.
            required: Whether field is required.
            **kwargs: Additional field parameters.

        Returns:
            True if added successfully.
        """
        contract = self.registry.get(contract_name)
        if not contract:
            return False

        field = ContractField(name=name, field_type=field_type, required=required, **kwargs)
        contract.add_field(field)
        return True

    def validate(
        self,
        contract_name: str,
        data: Dict[str, Any],
        strict: bool = True,
        use_cache: bool = False,
    ) -> ContractResult:
        """
        Validate data against a contract.

        Args:
            contract_name: Name of the contract.
            data: Data to validate.
            strict: Whether to fail on unexpected fields.
            use_cache: Whether to use cached results.

        Returns:
            ContractResult with validation outcome.
        """
        import time
        start = time.perf_counter()

        contract = self.registry.get(contract_name)
        if not contract:
            return ContractResult(
                status=ContractStatus.FAILED,
                contract_name=contract_name,
                violations=[
                    ContractViolation(
                        field_name="",
                        expected="contract",
                        actual="none",
                        message=f"Contract '{contract_name}' not found",
                    )
                ],
                duration_ms=(time.perf_counter() - start) * 1000,
            )

        violations: List[ContractViolation] = []
        warnings: List[str] = []

        required_fields = set(contract.required_fields)
        provided_fields = set(data.keys())

        missing_required = required_fields - provided_fields
        if missing_required:
            for field_name in missing_required:
                violations.append(ContractViolation(
                    field_name=field_name,
                    expected="present",
                    actual="missing",
                    message=f"Required field '{field_name}' is missing",
                ))

        if strict:
            unexpected = provided_fields - set(contract.fields.keys())
            if unexpected:
                for field_name in unexpected:
                    warnings.append(f"Unexpected field '{field_name}' present in data")

        for field_name, field_def in contract.fields.items():
            if field_name not in data:
                continue

            value = data[field_name]
            valid, message = field_def.validate(value)

            if not valid:
                violations.append(ContractViolation(
                    field_name=field_name,
                    expected=field_def.field_type,
                    actual=type(value).__name__,
                    message=message or "Validation failed",
                ))

        status = ContractStatus.PASSED
        if violations:
            status = ContractStatus.FAILED
        elif warnings:
            status = ContractStatus.WARNING

        duration = (time.perf_counter() - start) * 1000

        result = ContractResult(
            status=status,
            contract_name=contract_name,
            violations=violations,
            warnings=warnings,
            duration_ms=duration,
        )

        return result

    def validate_many(
        self,
        contract_name: str,
        items: List[Dict[str, Any]],
        stop_on_first_failure: bool = False,
    ) -> List[ContractResult]:
        """
        Validate multiple items against a contract.

        Args:
            contract_name: Name of the contract.
            items: List of items to validate.
            stop_on_first_failure: Whether to stop on first failure.

        Returns:
            List of ContractResults.
        """
        results = []
        for item in items:
            result = self.validate(contract_name, item)
            results.append(result)

            if stop_on_first_failure and not result.passed():
                break

        return results

    def compare_contracts(
        self,
        contract_name1: str,
        contract_name2: str,
    ) -> Dict[str, Any]:
        """
        Compare two contracts for compatibility.

        Args:
            contract_name1: First contract name.
            contract_name2: Second contract name.

        Returns:
            Dictionary describing differences.
        """
        contract1 = self.registry.get(contract_name1)
        contract2 = self.registry.get(contract_name2)

        if not contract1 or not contract2:
            return {"error": "Contract not found"}

        fields1 = set(contract1.fields.keys())
        fields2 = set(contract2.fields.keys())

        return {
            "added": list(fields2 - fields1),
            "removed": list(fields1 - fields2),
            "common": list(fields1 & fields2),
            "breaking_changes": self._find_breaking_changes(contract1, contract2),
        }

    def _find_breaking_changes(
        self,
        old_contract: ContractDefinition,
        new_contract: ContractDefinition,
    ) -> List[str]:
        """Find breaking changes between contracts."""
        breaking = []

        for field_name in old_contract.required_fields:
            if field_name not in new_contract.fields:
                breaking.append(f"Required field '{field_name}' was removed")
            elif not new_contract.fields[field_name].required:
                breaking.append(f"Required field '{field_name}' is now optional")

        return breaking

    def get_contract_schema(self, contract_name: str) -> Optional[Dict[str, Any]]:
        """Get JSON schema for a contract."""
        contract = self.registry.get(contract_name)
        if not contract:
            return None

        required = []
        properties = {}

        for field_name, field_def in contract.fields.items():
            type_mapping = {
                "string": "string",
                "integer": "integer",
                "number": "number",
                "boolean": "boolean",
                "array": "array",
                "object": "object",
            }

            schema_type = type_mapping.get(field_def.field_type, "string")

            prop = {
                "type": schema_type,
                "description": field_def.description,
            }

            if field_def.pattern:
                prop["pattern"] = field_def.pattern
            if field_def.min_length is not None:
                prop["minLength"] = field_def.min_length
            if field_def.max_length is not None:
                prop["maxLength"] = field_def.max_length
            if field_def.allowed_values is not None:
                prop["enum"] = list(field_def.allowed_values)

            properties[field_name] = prop

            if field_def.required:
                required.append(field_name)

        return {
            "title": contract.name,
            "version": contract.version,
            "type": "object",
            "properties": properties,
            "required": required,
        }


def create_contract_action() -> ApiContractAction:
    """Factory function to create an ApiContractAction."""
    return ApiContractAction()
