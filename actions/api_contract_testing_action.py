"""
API Contract Testing Action Module

Provides contract testing capabilities for API services with producer and consumer
contracts, schema validation, and backward compatibility checking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class ContractStatus(Enum):
    """Contract validation status."""

    PASSED = "passed"
    FAILED = "failed"
    PENDING = "pending"
    BROKEN = "broken"


class ContractType(Enum):
    """Contract type."""

    CONSUMER = "consumer"
    PRODUCER = "producer"


@dataclass
class ContractRequest:
    """A request specification in a contract."""

    method: str
    path: str
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, Any] = field(default_factory=dict)
    body_schema: Optional[Dict[str, Any]] = None
    body_example: Optional[Dict[str, Any]] = None


@dataclass
class ContractResponse:
    """A response specification in a contract."""

    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body_schema: Optional[Dict[str, Any]] = None
    body_example: Optional[Dict[str, Any]] = None
    response_time_ms: Optional[float] = None


@dataclass
class Contract:
    """A contract between consumer and producer."""

    contract_id: str
    name: str
    version: str
    contract_type: ContractType
    provider: str
    consumer: str
    request: ContractRequest
    response: ContractResponse
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[float] = None
    updated_at: Optional[float] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class ContractViolation:
    """A contract violation."""

    violation_id: str
    contract_id: str
    violation_type: str
    message: str
    expected: Any = None
    actual: Any = None
    severity: str = "error"
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class ContractResult:
    """Result of contract testing."""

    contract_id: str
    status: ContractStatus
    violations: List[ContractViolation] = field(default_factory=list)
    tested_at: Optional[float] = None
    duration_ms: float = 0.0

    def __post_init__(self):
        if self.tested_at is None:
            self.tested_at = time.time()


@dataclass
class SchemaValidator:
    """Schema validation utilities."""

    @staticmethod
    def validate_type(value: Any, expected_type: str) -> bool:
        """Validate value type."""
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }
        return isinstance(value, type_map.get(expected_type, str))

    @staticmethod
    def validate_schema(
        data: Dict[str, Any],
        schema: Dict[str, Any],
    ) -> List[str]:
        """Validate data against a JSON schema."""
        errors = []

        required = schema.get("required", [])
        properties = schema.get("properties", {})

        for field_name in required:
            if field_name not in data:
                errors.append(f"Missing required field: {field_name}")

        for field_name, field_schema in properties.items():
            if field_name not in data:
                continue

            value = data[field_name]
            field_type = field_schema.get("type")

            if field_type and not SchemaValidator.validate_type(value, field_type):
                errors.append(
                    f"Field '{field_name}' expected type {field_type}, "
                    f"got {type(value).__name__}"
                )

            if field_type == "string" and "minLength" in field_schema:
                if len(value) < field_schema["minLength"]:
                    errors.append(
                        f"Field '{field_name}' length {len(value)} "
                        f"below minLength {field_schema['minLength']}"
                    )

            if field_type == "string" and "maxLength" in field_schema:
                if len(value) > field_schema["maxLength"]:
                    errors.append(
                        f"Field '{field_name}' length {len(value)} "
                        f"above maxLength {field_schema['maxLength']}"
                    )

        return errors


@dataclass
class ContractConfig:
    """Configuration for contract testing."""

    strict_mode: bool = True
    backward_compatibility_check: bool = True
    timeout_seconds: float = 30.0
    schema_validation_enabled: bool = True


class APIContractTestingAction:
    """
    Contract testing action for API services.

    Features:
    - Consumer and producer contract definition
    - Schema-based request/response validation
    - Backward compatibility checking
    - Contract versioning
    - Mock response generation
    - Contract verification reports

    Usage:
        testing = APIContractTestingAction(config)
        
        # Define contract
        contract = testing.create_contract(
            name="user-service",
            provider="user-service",
            consumer="order-service",
            request=ContractRequest(method="GET", path="/users/{id}"),
            response=ContractResponse(status_code=200, body_schema=schema),
        )
        
        # Verify contract
        result = await testing.verify_contract(contract, actual_response)
    """

    def __init__(self, config: Optional[ContractConfig] = None):
        self.config = config or ContractConfig()
        self._contracts: Dict[str, Contract] = {}
        self._contract_results: Dict[str, ContractResult] = {}
        self._stats = {
            "contracts_defined": 0,
            "contracts_tested": 0,
            "contracts_passed": 0,
            "contracts_failed": 0,
            "violations_found": 0,
        }

    def create_contract(
        self,
        name: str,
        provider: str,
        consumer: str,
        request: ContractRequest,
        response: ContractResponse,
        version: str = "1.0.0",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Contract:
        """Create a new contract."""
        contract_id = f"contract_{uuid.uuid4().hex[:12]}"
        contract = Contract(
            contract_id=contract_id,
            name=name,
            version=version,
            contract_type=ContractType.CONSUMER,
            provider=provider,
            consumer=consumer,
            request=request,
            response=response,
            metadata=metadata or {},
        )
        self._contracts[contract_id] = contract
        self._stats["contracts_defined"] += 1
        return contract

    def get_contract(self, contract_id: str) -> Optional[Contract]:
        """Get a contract by ID."""
        return self._contracts.get(contract_id)

    async def verify_contract(
        self,
        contract: Contract,
        actual_response: Optional[Dict[str, Any]] = None,
        actual_status_code: Optional[int] = None,
    ) -> ContractResult:
        """
        Verify a contract against actual API behavior.

        Args:
            contract: Contract to verify
            actual_response: Actual response body from API
            actual_status_code: Actual HTTP status code

        Returns:
            ContractResult with violations if any
        """
        result = ContractResult(contract_id=contract.contract_id)
        start_time = time.time()

        violations: List[ContractViolation] = []

        # Validate status code
        if actual_status_code is not None:
            if actual_status_code != contract.response.status_code:
                violations.append(ContractViolation(
                    violation_id=f"violation_{uuid.uuid4().hex[:8]}",
                    contract_id=contract.contract_id,
                    violation_type="status_code",
                    message=f"Expected status {contract.response.status_code}, got {actual_status_code}",
                    expected=contract.response.status_code,
                    actual=actual_status_code,
                ))

        # Validate response schema
        if actual_response is not None and contract.response.body_schema:
            schema_errors = SchemaValidator.validate_schema(
                actual_response,
                contract.response.body_schema,
            )
            for error in schema_errors:
                violations.append(ContractViolation(
                    violation_id=f"violation_{uuid.uuid4().hex[:8]}",
                    contract_id=contract.contract_id,
                    violation_type="schema",
                    message=error,
                    expected=contract.response.body_schema,
                    actual=actual_response,
                ))

        result.duration_ms = (time.time() - start_time) * 1000
        result.violations = violations

        if violations:
            result.status = ContractStatus.FAILED
            self._stats["contracts_failed"] += 1
        else:
            result.status = ContractStatus.PASSED
            self._stats["contracts_passed"] += 1

        self._stats["contracts_tested"] += 1
        self._stats["violations_found"] += len(violations)
        self._contract_results[contract.contract_id] = result

        return result

    async def verify_against_provider(
        self,
        contract: Contract,
        request_executor: Callable[..., Any],
    ) -> ContractResult:
        """
        Verify a contract by executing requests against the provider.

        Args:
            contract: Contract to verify
            request_executor: Function that executes HTTP requests

        Returns:
            ContractResult with test results
        """
        try:
            result = await request_executor(contract)
            return result
        except Exception as e:
            result = ContractResult(
                contract_id=contract.contract_id,
                status=ContractStatus.BROKEN,
            )
            result.violations.append(ContractViolation(
                violation_id=f"violation_{uuid.uuid4().hex[:8]}",
                contract_id=contract.contract_id,
                violation_type="execution",
                message=f"Contract test execution failed: {str(e)}",
                severity="critical",
            ))
            return result

    def check_backward_compatibility(
        self,
        old_contract: Contract,
        new_contract: Contract,
    ) -> List[ContractViolation]:
        """Check backward compatibility between contract versions."""
        violations = []

        if not self.config.backward_compatibility_check:
            return violations

        # Check required fields are still required
        old_required = old_contract.response.body_schema.get("required", [])
        new_required = new_contract.response.body_schema.get("required", [])

        for field_name in old_required:
            if field_name not in new_required:
                violations.append(ContractViolation(
                    violation_id=f"violation_{uuid.uuid4().hex[:8]}",
                    contract_id=new_contract.contract_id,
                    violation_type="backward_compatibility",
                    message=f"Field '{field_name}' was required in old version but removed",
                    expected=field_name,
                    actual="field removed",
                    severity="critical",
                ))

        # Check field types haven't changed
        old_props = old_contract.response.body_schema.get("properties", {})
        new_props = new_contract.response.body_schema.get("properties", {})

        for field_name, old_schema in old_props.items():
            if field_name in new_props:
                if old_schema.get("type") != new_props[field_name].get("type"):
                    violations.append(ContractViolation(
                        violation_id=f"violation_{uuid.uuid4().hex[:8]}",
                        contract_id=new_contract.contract_id,
                        violation_type="backward_compatibility",
                        message=f"Field '{field_name}' type changed from {old_schema.get('type')} to {new_props[field_name].get('type')}",
                        expected=old_schema.get("type"),
                        actual=new_props[field_name].get("type"),
                        severity="critical",
                    ))

        return violations

    def get_contract_results(
        self,
        contract_id: str,
    ) -> Optional[ContractResult]:
        """Get test results for a contract."""
        return self._contract_results.get(contract_id)

    def get_all_results(self) -> Dict[str, ContractResult]:
        """Get all contract test results."""
        return self._contract_results.copy()

    def generate_report(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Generate a contract testing report."""
        contract = self._contracts.get(contract_id)
        result = self._contract_results.get(contract_id)

        if not contract or not result:
            return None

        return {
            "contract_id": contract_id,
            "contract_name": contract.name,
            "provider": contract.provider,
            "consumer": contract.consumer,
            "version": contract.version,
            "status": result.status.value,
            "violations": [
                {
                    "violation_id": v.violation_id,
                    "type": v.violation_type,
                    "message": v.message,
                    "severity": v.severity,
                    "timestamp": v.timestamp,
                }
                for v in result.violations
            ],
            "tested_at": result.tested_at,
            "duration_ms": result.duration_ms,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get contract testing statistics."""
        return self._stats.copy()


async def demo_contract_testing():
    """Demonstrate contract testing."""
    config = ContractConfig()
    testing = APIContractTestingAction(config)

    request = ContractRequest(
        method="GET",
        path="/users/{id}",
    )

    response_schema = {
        "type": "object",
        "required": ["id", "name", "email"],
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string", "minLength": 1},
            "email": {"type": "string"},
        },
    }

    response = ContractResponse(
        status_code=200,
        body_schema=response_schema,
    )

    contract = testing.create_contract(
        name="get-user",
        provider="user-service",
        consumer="order-service",
        request=request,
        response=response,
    )

    # Verify with actual response
    actual = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    result = await testing.verify_contract(contract, actual_response=actual, actual_status_code=200)

    print(f"Contract status: {result.status.value}")
    print(f"Violations: {len(result.violations)}")
    print(f"Stats: {testing.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_contract_testing())
