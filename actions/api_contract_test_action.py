"""API Contract Test Action.

Implements contract testing for API endpoints including
schema validation, response matching, and compatibility checks.
"""
from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union


class ContractStatus(Enum):
    """Status of a contract test."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class FieldContract:
    """Contract for a single field."""
    name: str
    expected_type: Optional[str] = None
    required: bool = False
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None
    enum_values: Optional[Set[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ResponseContract:
    """Contract for an API response."""
    status_code: int
    schema: Optional[Dict[str, Any]] = None
    fields: Dict[str, FieldContract] = field(default_factory=dict)
    required_fields: Set[str] = field(default_factory=set)
    additional_properties: bool = False
    headers_contract: Optional[Dict[str, str]] = None


@dataclass
class ContractTestResult:
    """Result of a contract test."""
    test_name: str
    endpoint: str
    method: str
    status: ContractStatus
    contract: str
    actual_status_code: Optional[int] = None
    field_results: Dict[str, bool] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class ContractTestReport:
    """Summary report of contract tests."""
    total_tests: int
    passed: int
    failed: int
    skipped: int
    errors: int
    results: List[ContractTestResult] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)


class APIContractTestAction:
    """Contract testing for API endpoints."""

    def __init__(self) -> None:
        self._contracts: Dict[str, ResponseContract] = {}
        self._test_history: List[ContractTestResult] = []
        self._max_history = 500

    def register_contract(
        self,
        contract_name: str,
        status_code: int,
        schema: Optional[Dict[str, Any]] = None,
        fields: Optional[Dict[str, FieldContract]] = None,
        required_fields: Optional[List[str]] = None,
        additional_properties: bool = False,
        headers_contract: Optional[Dict[str, str]] = None,
    ) -> ResponseContract:
        """Register a response contract."""
        contract = ResponseContract(
            status_code=status_code,
            schema=schema,
            fields=fields or {},
            required_fields=set(required_fields or []),
            additional_properties=additional_properties,
            headers_contract=headers_contract,
        )
        self._contracts[contract_name] = contract
        return contract

    def add_field_contract(
        self,
        contract_name: str,
        field_name: str,
        expected_type: Optional[str] = None,
        required: bool = False,
        nullable: bool = True,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        pattern: Optional[str] = None,
        enum_values: Optional[List[Any]] = None,
        custom_validator: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        """Add a field contract to an existing contract."""
        if contract_name not in self._contracts:
            raise ValueError(f"Contract '{contract_name}' not found")

        field_contract = FieldContract(
            name=field_name,
            expected_type=expected_type,
            required=required,
            nullable=nullable,
            min_value=min_value,
            max_value=max_value,
            pattern=pattern,
            enum_values=set(enum_values) if enum_values else None,
            custom_validator=custom_validator,
        )

        self._contracts[contract_name].fields[field_name] = field_contract
        if required:
            self._contracts[contract_name].required_fields.add(field_name)

    def test_response(
        self,
        test_name: str,
        endpoint: str,
        method: str,
        contract_name: str,
        actual_response: Any,
        actual_status_code: int,
        actual_headers: Optional[Dict[str, str]] = None,
    ) -> ContractTestResult:
        """Test a response against a contract."""
        start_time = time.time()
        result = ContractTestResult(
            test_name=test_name,
            endpoint=endpoint,
            method=method,
            status=ContractStatus.PASSED,
            contract=contract_name,
            actual_status_code=actual_status_code,
        )

        if contract_name not in self._contracts:
            result.status = ContractStatus.ERROR
            result.errors.append(f"Contract '{contract_name}' not found")
            result.duration_ms = (time.time() - start_time) * 1000
            return result

        contract = self._contracts[contract_name]

        if actual_status_code != contract.status_code:
            result.status = ContractStatus.FAILED
            result.errors.append(
                f"Status code mismatch: expected {contract.status_code}, got {actual_status_code}"
            )

        if actual_headers and contract.headers_contract:
            for header_name, expected_pattern in contract.headers_contract.items():
                actual_value = actual_headers.get(header_name, "")
                if not re.match(expected_pattern, actual_value):
                    result.warnings.append(
                        f"Header '{header_name}' mismatch: pattern '{expected_pattern}'"
                    )

        response_data = actual_response
        if isinstance(actual_response, str):
            try:
                response_data = json.loads(actual_response)
            except json.JSONDecodeError:
                pass

        if isinstance(response_data, dict):
            result.field_results = self._validate_fields(contract, response_data, result)

            for field_name in contract.required_fields:
                if field_name not in response_data:
                    result.status = ContractStatus.FAILED
                    result.errors.append(f"Required field '{field_name}' missing")

            if not contract.additional_properties:
                expected_fields = set(contract.fields.keys())
                actual_fields = set(response_data.keys())
                extra = actual_fields - expected_fields
                if extra:
                    result.warnings.append(f"Unexpected fields: {extra}")

        result.duration_ms = (time.time() - start_time) * 1000
        self._add_result(result)

        return result

    def _validate_fields(
        self,
        contract: ResponseContract,
        data: Dict[str, Any],
        result: ContractTestResult,
    ) -> Dict[str, bool]:
        """Validate response fields against contract."""
        field_results: Dict[str, bool] = {}

        for field_name, field_contract in contract.fields.items():
            value = data.get(field_name)
            field_valid = self._validate_field(field_name, value, field_contract, result)
            field_results[field_name] = field_valid

        if not all(field_results.values()):
            result.status = ContractStatus.FAILED

        return field_results

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        contract: FieldContract,
        result: ContractTestResult,
    ) -> bool:
        """Validate a single field against its contract."""
        if value is None:
            if not contract.nullable:
                result.errors.append(f"Field '{field_name}' is not nullable")
                return False
            return True

        if contract.expected_type:
            type_valid = self._check_type(value, contract.expected_type)
            if not type_valid:
                result.errors.append(
                    f"Field '{field_name}' type mismatch: expected {contract.expected_type}"
                )
                return False

        if contract.enum_values is not None and value not in contract.enum_values:
            result.errors.append(
                f"Field '{field_name}' value '{value}' not in allowed values"
            )
            return False

        if contract.min_value is not None:
            try:
                if float(value) < contract.min_value:
                    result.errors.append(
                        f"Field '{field_name}' value {value} below minimum {contract.min_value}"
                    )
                    return False
            except (TypeError, ValueError):
                pass

        if contract.max_value is not None:
            try:
                if float(value) > contract.max_value:
                    result.errors.append(
                        f"Field '{field_name}' value {value} above maximum {contract.max_value}"
                    )
                    return False
            except (TypeError, ValueError):
                pass

        if contract.pattern:
            str_value = str(value)
            if not re.match(contract.pattern, str_value):
                result.errors.append(
                    f"Field '{field_name}' value '{str_value}' does not match pattern '{contract.pattern}'"
                )
                return False

        if contract.custom_validator:
            try:
                if not contract.custom_validator(value):
                    result.errors.append(f"Field '{field_name}' failed custom validation")
                    return False
            except Exception as e:
                result.errors.append(f"Field '{field_name}' custom validator error: {e}")
                return False

        return True

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            "string": str,
            "integer": int,
            "int": int,
            "number": (int, float),
            "float": (int, float),
            "boolean": bool,
            "bool": bool,
            "array": list,
            "list": list,
            "object": dict,
            "dict": dict,
            "null": type(None),
        }

        expected = type_map.get(expected_type.lower())
        if expected is None:
            return True

        return isinstance(value, expected)

    def _add_result(self, result: ContractTestResult) -> None:
        """Add a test result to history."""
        self._test_history.append(result)
        if len(self._test_history) > self._max_history:
            self._test_history = self._test_history[-self._max_history // 2:]

    def generate_report(self) -> ContractTestReport:
        """Generate a summary report of all tests."""
        passed = sum(1 for r in self._test_history if r.status == ContractStatus.PASSED)
        failed = sum(1 for r in self._test_history if r.status == ContractStatus.FAILED)
        skipped = sum(1 for r in self._test_history if r.status == ContractStatus.SKIPPED)
        errors = sum(1 for r in self._test_history if r.status == ContractStatus.ERROR)

        return ContractTestReport(
            total_tests=len(self._test_history),
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            results=list(self._test_history),
        )

    def get_contract(self, contract_name: str) -> Optional[ResponseContract]:
        """Get a registered contract."""
        return self._contracts.get(contract_name)
