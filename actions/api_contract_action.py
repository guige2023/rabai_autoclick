"""
API Contract Action Module.

Provides API contract testing, schema validation,
and contract enforcement capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import re
import uuid

logger = logging.getLogger(__name__)


class ContractType(Enum):
    """Contract types."""
    CONSUMER = "consumer"
    PROVIDER = "provider"


class ViolationSeverity(Enum):
    """Severity of contract violations."""
    BLOCKING = "blocking"
    WARNING = "warning"
    INFO = "info"


@dataclass
class SchemaField:
    """Schema field definition."""
    name: str
    field_type: type
    required: bool = False
    pattern: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    enum_values: Optional[List[Any]] = None
    items: Optional["SchemaField"] = None


@dataclass
class APIEndpoint:
    """API endpoint contract."""
    path: str
    method: str
    request_schema: Optional["Schema"] = None
    response_schema: Optional["Schema"] = None
    headers_schema: Optional[Dict[str, Any]] = None


@dataclass
class Schema:
    """JSON Schema definition."""
    name: str
    fields: Dict[str, SchemaField]
    strict: bool = False

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate data against schema."""
        errors = []

        for field_name, field_def in self.fields.items():
            if field_name not in data:
                if field_def.required:
                    errors.append(f"Missing required field: {field_name}")
                continue

            value = data[field_name]
            field_errors = self._validate_field(field_def, value)
            errors.extend(field_errors)

        if self.strict:
            extra = set(data.keys()) - set(self.fields.keys())
            if extra:
                errors.append(f"Extra fields not allowed: {extra}")

        return len(errors) == 0, errors

    def _validate_field(self, field_def: SchemaField, value: Any) -> List[str]:
        """Validate single field."""
        errors = []

        if value is None:
            return errors

        if not isinstance(value, field_def.field_type):
            errors.append(f"Field {field_def.name}: expected {field_def.field_type}, got {type(value)}")
            return errors

        if field_def.pattern:
            if not re.match(field_def.pattern, str(value)):
                errors.append(f"Field {field_def.name}: pattern mismatch")

        if field_def.min_value is not None:
            if isinstance(value, (int, float)) and value < field_def.min_value:
                errors.append(f"Field {field_def.name}: below minimum {field_def.min_value}")

        if field_def.max_value is not None:
            if isinstance(value, (int, float)) and value > field_def.max_value:
                errors.append(f"Field {field_def.name}: above maximum {field_def.max_value}")

        if field_def.enum_values:
            if value not in field_def.enum_values:
                errors.append(f"Field {field_def.name}: not in enum {field_def.enum_values}")

        return errors


@dataclass
class ContractViolation:
    """Contract violation record."""
    contract_id: str
    endpoint: str
    severity: ViolationSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ContractTestResult:
    """Result of contract test."""
    test_id: str
    contract_id: str
    passed: bool
    violations: List[ContractViolation]
    execution_time: float


class ContractEnforcer:
    """Enforces API contracts."""

    def __init__(self):
        self.contracts: Dict[str, APIEndpoint] = {}
        self.violations: List[ContractViolation] = []

    def register_contract(self, contract: APIEndpoint):
        """Register API contract."""
        key = f"{contract.method}:{contract.path}"
        self.contracts[key] = contract

    async def validate_request(
        self,
        method: str,
        path: str,
        body: Any,
        headers: Dict[str, str]
    ) -> List[ContractViolation]:
        """Validate request against contract."""
        key = f"{method}:{path}"
        contract = self.contracts.get(key)

        if not contract:
            return []

        violations = []
        data = body if isinstance(body, dict) else {}

        if contract.request_schema:
            valid, errors = contract.request_schema.validate(data)
            if not valid:
                for error in errors:
                    violations.append(ContractViolation(
                        contract_id=key,
                        endpoint=path,
                        severity=ViolationSeverity.BLOCKING,
                        message=f"Request validation failed: {error}"
                    ))

        return violations

    async def validate_response(
        self,
        method: str,
        path: str,
        status_code: int,
        body: Any,
        headers: Dict[str, str]
    ) -> List[ContractViolation]:
        """Validate response against contract."""
        key = f"{method}:{path}"
        contract = self.contracts.get(key)

        if not contract:
            return []

        violations = []
        data = body if isinstance(body, dict) else {}

        if contract.response_schema:
            valid, errors = contract.response_schema.validate(data)
            if not valid:
                for error in errors:
                    violations.append(ContractViolation(
                        contract_id=key,
                        endpoint=path,
                        severity=ViolationSeverity.BLOCKING,
                        message=f"Response validation failed: {error}"
                    ))

        return violations


class ContractTester:
    """Tests API contracts."""

    def __init__(self, enforcer: ContractEnforcer):
        self.enforcer = enforcer
        self.test_cases: Dict[str, List[Dict[str, Any]]] = {}

    def add_test_case(
        self,
        contract_id: str,
        test_case: Dict[str, Any]
    ):
        """Add test case for contract."""
        if contract_id not in self.test_cases:
            self.test_cases[contract_id] = []
        self.test_cases[contract_id].append(test_case)

    async def run_tests(
        self,
        contract_id: str,
        executor: Callable
    ) -> ContractTestResult:
        """Run contract tests."""
        start_time = datetime.now()
        test_cases = self.test_cases.get(contract_id, [])
        violations = []

        for case in test_cases:
            method = case.get("method", "GET")
            path = case.get("path", "/")
            expected_status = case.get("expected_status", 200)

            result = await executor(method, path, case.get("body"))

            if result["status_code"] != expected_status:
                violations.append(ContractViolation(
                    contract_id=contract_id,
                    endpoint=path,
                    severity=ViolationSeverity.BLOCKING,
                    message=f"Expected status {expected_status}, got {result['status_code']}"
                ))

            response_violations = await self.enforcer.validate_response(
                method, path, result["status_code"], result["body"], {}
            )
            violations.extend(response_violations)

        execution_time = (datetime.now() - start_time).total_seconds()

        return ContractTestResult(
            test_id=str(uuid.uuid4()),
            contract_id=contract_id,
            passed=len(violations) == 0,
            violations=violations,
            execution_time=execution_time
        )


async def main():
    """Demonstrate contract testing."""
    enforcer = ContractEnforcer()

    schema = Schema(
        name="UserSchema",
        fields={
            "id": SchemaField(name="id", field_type=int, required=True),
            "name": SchemaField(name="name", field_type=str, required=True),
            "email": SchemaField(name="email", field_type=str, pattern=r"^[\w\.]+@[\w\.]+$")
        }
    )

    contract = APIEndpoint(
        path="/users",
        method="POST",
        request_schema=schema
    )

    enforcer.register_contract(contract)

    violations = await enforcer.validate_request(
        "POST", "/users",
        {"id": 1, "name": "John", "email": "john@example.com"},
        {}
    )

    print(f"Violations: {len(violations)}")


if __name__ == "__main__":
    asyncio.run(main())
