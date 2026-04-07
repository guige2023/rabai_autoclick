"""Contract testing utilities: consumer-driven contracts, provider verification, and schema validation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Contract",
    "ContractTest",
    "ConsumerContract",
    "ProviderContract",
    "ContractVerifier",
    "SchemaValidator",
    "validate_contract",
]


@dataclass
class Contract:
    """Base contract definition."""

    name: str
    provider: str
    consumer: str
    interactions: list["Interaction"] = field(default_factory=list)


@dataclass
class Interaction:
    """A single interaction in a contract (request/response pair)."""

    description: str
    request: "Request"
    response: "Response"
    enabled: bool = True


@dataclass
class Request:
    """HTTP request definition."""

    method: str = "GET"
    path: str = "/"
    headers: dict[str, str] = field(default_factory=dict)
    query: dict[str, str] | None = None
    body: Any = None
    body_schema: dict[str, Any] | None = None

    def matches(self, other: "Request") -> bool:
        """Check if this request matches another."""
        if self.method != other.method:
            return False
        if not self._path_matches(self.path, other.path):
            return False
        return True

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Match path with variable placeholders."""
        pattern_parts = pattern.strip("/").split("/")
        path_parts = path.strip("/").split("/")
        if len(pattern_parts) != len(path_parts):
            return False
        for p, a in zip(pattern_parts, path_parts):
            if p.startswith("{"):
                continue
            if p != a:
                return False
        return True


@dataclass
class Response:
    """HTTP response definition."""

    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    body_schema: dict[str, Any] | None = None
    matchers: dict[str, str] | None = None


@dataclass
class ContractTest:
    """A contract test case."""

    name: str
    contract: Contract
    interaction_index: int

    def run(
        self,
        send_request: Callable[[Request], Response],
    ) -> "ContractTestResult":
        """Run this contract test against a provider."""
        interaction = self.contract.interactions[self.interaction_index]
        try:
            resp = send_request(interaction.request)
            match_result = self._verify_response(interaction.response, resp)
            return ContractTestResult(
                name=self.name,
                passed=match_result["matched"],
                errors=match_result["errors"],
            )
        except Exception as e:
            return ContractTestResult(
                name=self.name,
                passed=False,
                errors=[str(e)],
            )

    def _verify_response(
        self,
        expected: Response,
        actual: Response,
    ) -> dict[str, Any]:
        """Verify response matches expected."""
        errors: list[str] = []
        matched = True

        if expected.status_code != actual.status_code:
            errors.append(f"Status mismatch: expected {expected.status_code}, got {actual.status_code}")
            matched = False

        for key, val in expected.headers.items():
            if key not in actual.headers or actual.headers[key] != val:
                errors.append(f"Header '{key}' mismatch")
                matched = False

        if expected.body_schema:
            schema_errors = SchemaValidator.validate(expected.body_schema, actual.body)
            if schema_errors:
                errors.extend(schema_errors)
                matched = False

        return {"matched": matched, "errors": errors}


@dataclass
class ContractTestResult:
    """Result of a contract test run."""

    name: str
    passed: bool
    errors: list[str] = field(default_factory=list)
    duration_ms: float = 0.0


class ConsumerContract:
    """Consumer-side contract definition."""

    def __init__(self, consumer_name: str) -> None:
        self.consumer_name = consumer_name
        self._interactions: list[Interaction] = []

    def given(self, provider: str) -> "ConsumerContractBuilder":
        return ConsumerContractBuilder(self, provider)

    def add_interaction(self, interaction: Interaction) -> None:
        self._interactions.append(interaction)

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer": {"name": self.consumer_name},
            "interactions": [
                {
                    "description": i.description,
                    "request": {
                        "method": i.request.method,
                        "path": i.request.path,
                        "headers": i.request.headers,
                    },
                    "response": {
                        "status": i.response.status_code,
                        "headers": i.response.headers,
                    },
                }
                for i in self._interactions
            ],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class ConsumerContractBuilder:
    """Builder for consumer contract interactions."""

    def __init__(self, contract: ConsumerContract, provider: str) -> None:
        self.contract = contract
        self.provider = provider
        self._request = Request()
        self._response = Response()
        self._description = ""

    def upon_receiving(self, description: str) -> "ConsumerContractBuilder":
        self._description = description
        return self

    def with_request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> "ConsumerContractBuilder":
        self._request = Request(method=method, path=path, **kwargs)
        return self

    def will_respond_with(
        self,
        status_code: int = 200,
        **kwargs,
    ) -> "ConsumerContractBuilder":
        self._response = Response(status_code=status_code, **kwargs)
        return self

    def build(self) -> Interaction:
        interaction = Interaction(
            description=self._description,
            request=self._request,
            response=self._response,
        )
        self.contract.add_interaction(interaction)
        return interaction


class ProviderContract:
    """Provider-side contract definition."""

    def __init__(self, provider_name: str) -> None:
        self.provider_name = provider_name
        self._interactions: list[Interaction] = []

    def add_interaction(self, interaction: Interaction) -> None:
        self._interactions.append(interaction)

    def verify(
        self,
        interaction: Interaction,
        send_request: Callable[[Request], Response],
    ) -> ContractTestResult:
        """Verify a single interaction against the provider."""
        test = ContractTest(
            name=interaction.description,
            contract=Contract(
                name="",
                provider=self.provider_name,
                consumer="",
                interactions=[interaction],
            ),
            interaction_index=0,
        )
        return test.run(send_request)


class SchemaValidator:
    """JSON schema validator for contract body validation."""

    @staticmethod
    def validate(schema: dict[str, Any], data: Any, path: str = "") -> list[str]:
        """Validate data against a simple JSON-like schema."""
        errors: list[str] = []

        if not isinstance(data, dict):
            errors.append(f"{path}: expected object, got {type(data).__name__}")
            return errors

        required = schema.get("required", [])
        properties = schema.get("properties", {})

        for field_name in required:
            if field_name not in data:
                errors.append(f"{path}.{field_name}: required field missing")

        for field_name, field_schema in properties.items():
            if field_name not in data:
                continue

            value = data[field_name]
            field_path = f"{path}.{field_name}" if path else field_name

            if "type" in field_schema:
                expected_type = field_schema["type"]
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(f"{field_path}: expected string")
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    errors.append(f"{field_path}: expected number")
                elif expected_type == "boolean" and not isinstance(value, bool):
                    errors.append(f"{field_path}: expected boolean")
                elif expected_type == "array" and not isinstance(value, list):
                    errors.append(f"{field_path}: expected array")

            if "enum" in field_schema and value not in field_schema["enum"]:
                errors.append(f"{field_path}: value must be one of {field_schema['enum']}")

        return errors


class ContractVerifier:
    """Verifies contracts between consumer and provider."""

    def __init__(self) -> None:
        self._contracts: list[Contract] = []

    def add_contract(self, contract: Contract) -> None:
        self._contracts.append(contract)

    def verify_all(
        self,
        send_request: Callable[[Request], Response],
    ) -> dict[str, ContractTestResult]:
        """Verify all contracts."""
        results: dict[str, ContractTestResult] = {}
        for contract in self._contracts:
            for i, interaction in enumerate(contract.interactions):
                if not interaction.enabled:
                    continue
                test = ContractTest(
                    name=f"{contract.consumer}-{contract.provider}: {interaction.description}",
                    contract=contract,
                    interaction_index=i,
                )
                result = test.run(send_request)
                results[test.name] = result
        return results


def validate_contract(contract: Contract) -> list[str]:
    """Validate a contract definition for common issues."""
    errors: list[str] = []

    if not contract.name:
        errors.append("Contract name is required")
    if not contract.provider:
        errors.append("Contract provider is required")
    if not contract.consumer:
        errors.append("Contract consumer is required")
    if not contract.interactions:
        errors.append("Contract must have at least one interaction")

    for i, interaction in enumerate(contract.interactions):
        if not interaction.request.path:
            errors.append(f"Interaction {i}: request path is required")
        if interaction.response.status_code == 0:
            errors.append(f"Interaction {i}: response status code is required")

    return errors
