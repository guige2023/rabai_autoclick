"""Contract testing utilities for API consumer/provider relationships."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Contract",
    "ContractAssertion",
    "ContractTest",
    "ContractRunner",
    "SchemaValidator",
]


@dataclass
class ContractAssertion:
    """A single assertion within a contract."""
    path: str
    operator: str
    expected: Any
    description: str = ""

    def check(self, data: Any) -> tuple[bool, str]:
        parts = self.path.lstrip("/").split("/")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return False, f"Path not found: {self.path}"
            else:
                return False, f"Cannot traverse: {part}"

        ops = {
            "eq": lambda a, e: a == e,
            "ne": lambda a, e: a != e,
            "gt": lambda a, e: a > e,
            "gte": lambda a, e: a >= e,
            "lt": lambda a, e: a < e,
            "lte": lambda a, e: a <= e,
            "contains": lambda a, e: e in a,
            "matches": lambda a, e: bool(re.match(e, str(a))),
            "exists": lambda a, e: a is not None,
            "in_": lambda a, e: a in e,
            "type": lambda a, e: isinstance(a, e),
        }
        op_fn = ops.get(self.operator)
        if not op_fn:
            return False, f"Unknown operator: {self.operator}"

        try:
            ok = op_fn(current, self.expected)
            msg = f"{self.path} {self.operator} {self.expected!r} -> {ok} (actual: {current!r})"
            return ok, msg
        except Exception as e:
            return False, f"Assertion error: {e}"


@dataclass
class Contract:
    """A contract defining expected API behavior."""
    name: str
    provider: str
    consumer: str
    interactions: list[ContractInteraction] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ContractInteraction:
    """A single request/response interaction in a contract."""
    description: str
    request: dict[str, Any]
    response: dict[str, Any]
    assertions: list[ContractAssertion] = field(default_factory=list)
    status_code: int = 200


@dataclass
class ContractTest:
    """A contract test case."""
    contract: Contract
    interaction: ContractInteraction
    response_data: dict[str, Any]
    passed: bool
    failed_assertions: list[str] = field(default_factory=list)
    duration_ms: float = 0.0


class SchemaValidator:
    """JSON Schema-like validator for API responses."""

    def __init__(self) -> None:
        self._errors: list[str] = []

    def validate(
        self,
        data: Any,
        schema: dict[str, Any],
        path: str = "",
    ) -> tuple[bool, list[str]]:
        self._errors = []
        self._validate(data, schema, path)
        return len(self._errors) == 0, self._errors

    def _validate(self, data: Any, schema: dict[str, Any], path: str) -> None:
        if "type" in schema:
            expected_type = schema["type"]
            type_map = {
                "string": str, "integer": int, "number": (int, float),
                "boolean": bool, "array": list, "object": dict, "null": type(None),
            }
            expected = type_map.get(expected_type)
            if expected and not isinstance(data, expected):
                self._errors.append(f"{path}: expected {expected_type}, got {type(data).__name__}")

        if "enum" in schema and data not in schema["enum"]:
            self._errors.append(f"{path}: value {data!r} not in {schema['enum']}")

        if "pattern" in schema and isinstance(data, str):
            if not re.match(schema["pattern"], data):
                self._errors.append(f"{path}: pattern mismatch")

        if "minimum" in schema and data < schema["minimum"]:
            self._errors.append(f"{path}: {data} < minimum {schema['minimum']}")

        if "maximum" in schema and data > schema["maximum"]:
            self._errors.append(f"{path}: {data} > maximum {schema['maximum']}")

        if "minLength" in schema and isinstance(data, str) and len(data) < schema["minLength"]:
            self._errors.append(f"{path}: length {len(data)} < minLength {schema['minLength']}")

        if "required" in schema and isinstance(data, dict):
            for req in schema["required"]:
                if req not in data:
                    self._errors.append(f"{path}: missing required field '{req}'")

        if "properties" in schema and isinstance(data, dict):
            for prop, prop_schema in schema["properties"].items():
                if prop in data:
                    self._validate(data[prop], prop_schema, f"{path}/{prop}")

        if "items" in schema and isinstance(data, list):
            for i, item in enumerate(data):
                self._validate(item, schema["items"], f"{path}[{i}]")


class ContractRunner:
    """Runs contract tests against a provider."""

    def __init__(self, http_client: Any | None = None) -> None:
        self._client = http_client

    def run_interaction(
        self,
        contract: Contract,
        interaction: ContractInteraction,
    ) -> ContractTest:
        start = time.perf_counter()
        failed: list[str] = []

        for assertion in interaction.assertions:
            ok, msg = assertion.check(interaction.response)
            if not ok:
                failed.append(msg)

        duration = (time.perf_counter() - start) * 1000
        return ContractTest(
            contract=contract,
            interaction=interaction,
            response_data=interaction.response,
            passed=len(failed) == 0,
            failed_assertions=failed,
            duration_ms=duration,
        )

    def run_contract(self, contract: Contract) -> list[ContractTest]:
        return [
            self.run_interaction(contract, interaction)
            for interaction in contract.interactions
        ]

    def run_all(self, contracts: list[Contract]) -> dict[str, list[ContractTest]]:
        results: dict[str, list[ContractTest]] = {}
        for c in contracts:
            results[c.name] = self.run_contract(c)
        return results

    def print_results(self, results: dict[str, list[ContractTest]]) -> None:
        for name, tests in results.items():
            passed = sum(1 for t in tests if t.passed)
            total = len(tests)
            status = "PASS" if passed == total else "FAIL"
            print(f"[{status}] {name}: {passed}/{total}")
            for t in tests:
                if not t.passed:
                    for err in t.failed_assertions:
                        print(f"    - {err}")
