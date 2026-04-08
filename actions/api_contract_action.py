"""
API Contract Action Module.

Validates API responses against OpenAPI contracts, checks schema compliance,
and generates contract test reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ContractViolation:
    """A contract violation found."""
    path: str
    field: str
    expected_type: str
    actual_value: Any
    severity: str  # error, warning


@dataclass
class ContractTestResult:
    """Result of contract testing."""
    passed: bool
    violations: list[ContractViolation]
    warnings: list[str]


class APIContractAction(BaseAction):
    """Validate API responses against OpenAPI contracts."""

    def __init__(self) -> None:
        super().__init__("api_contract")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Validate API response against contract.

        Args:
            context: Execution context
            params: Parameters:
                - response: API response to validate
                - contract: OpenAPI contract dict or path
                - strict: Enable strict type checking

        Returns:
            ContractTestResult with violations
        """
        response = params.get("response")
        contract = params.get("contract", {})
        strict = params.get("strict", False)

        if not response:
            return ContractTestResult(False, [], ["No response provided"]).__dict__

        violations: list[ContractViolation] = []
        warnings: list[str] = []

        if isinstance(response, dict) and "data" in contract:
            schema = contract.get("data", {})
            violations.extend(self._validate_schema(response, schema, "", strict))

        if isinstance(response, dict):
            for key, value in response.items():
                if value is None:
                    violations.append(ContractViolation(
                        path=key,
                        field=key,
                        expected_type="non-null",
                        actual_value=None,
                        severity="error"
                    ))
                elif isinstance(value, str) and not value.strip():
                    warnings.append(f"Empty string at {key}")

        return ContractTestResult(
            passed=len(violations) == 0,
            violations=violations,
            warnings=warnings
        ).__dict__

    def _validate_schema(self, data: Any, schema: dict, path: str, strict: bool) -> list[ContractViolation]:
        """Validate data against JSON schema."""
        violations = []
        expected_type = schema.get("type", "object")

        if expected_type == "object":
            if not isinstance(data, dict):
                violations.append(ContractViolation(path=path, field=path, expected_type="object", actual_value=type(data).__name__, severity="error"))
            else:
                required = schema.get("required", [])
                for req_field in required:
                    if req_field not in data:
                        violations.append(ContractViolation(path=f"{path}.{req_field}", field=req_field, expected_type="required", actual_value=None, severity="error"))

        elif expected_type == "string":
            if not isinstance(data, str):
                violations.append(ContractViolation(path=path, field=path, expected_type="string", actual_value=type(data).__name__, severity="error"))

        elif expected_type == "number":
            if not isinstance(data, (int, float)):
                violations.append(ContractViolation(path=path, field=path, expected_type="number", actual_value=type(data).__name__, severity="error"))

        elif expected_type == "array":
            if not isinstance(data, list):
                violations.append(ContractViolation(path=path, field=path, expected_type="array", actual_value=type(data).__name__, severity="error"))

        return violations
