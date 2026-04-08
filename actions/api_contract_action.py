"""API Contract Action Module.

Provides API contract validation and testing capabilities
using OpenAPI specs and custom contract definitions.
"""

import json
import time
import hashlib
import re
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ContractType(Enum):
    """Type of API contract."""
    OPENAPI = "openapi"
    SWAGGER = "swagger"
    RAML = "ram"
    GRAPHQL = "graphql"
    CUSTOM = "custom"


class ValidationLevel(Enum):
    """Contract validation level."""
    STRICT = "strict"
    MODERATE = "moderate"
    LAX = "lax"


@dataclass
class ContractEndpoint:
    """Defines an API endpoint in a contract."""
    path: str
    method: str
    summary: Optional[str] = None
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    security: List[Dict[str, Any]] = field(default_factory=list)
    deprecated: bool = False


@dataclass
class ContractViolation:
    """Represents a contract violation."""
    severity: str
    location: str
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    rule: Optional[str] = None


@dataclass
class ContractTestResult:
    """Result of a contract test."""
    endpoint: str
    method: str
    passed: bool
    violations: List[ContractViolation]
    duration_ms: float
    timestamp: float = field(default_factory=time.time)


class ContractValidator:
    """Validates API requests against contract."""

    def __init__(self, validation_level: ValidationLevel = ValidationLevel.MODERATE):
        self._contracts: Dict[str, Dict[str, ContractEndpoint]] = {}
        self._validation_level = validation_level
        self._custom_rules: Dict[str, Callable] = {}

    def load_contract(
        self,
        contract_id: str,
        spec: Dict[str, Any],
        contract_type: ContractType = ContractType.OPENAPI
    ) -> int:
        """Load a contract specification."""
        endpoints = self._parse_contract_spec(spec, contract_type)
        self._contracts[contract_id] = endpoints
        return len(endpoints)

    def _parse_contract_spec(
        self,
        spec: Dict[str, Any],
        contract_type: ContractType
    ) -> Dict[str, ContractEndpoint]:
        """Parse contract specification into endpoints."""
        endpoints = {}

        if contract_type in (ContractType.OPENAPI, ContractType.SWAGGER):
            paths = spec.get("paths", {})
            for path, path_item in paths.items():
                for method, operation in path_item.items():
                    if method in ("get", "post", "put", "delete", "patch", "options", "head"):
                        endpoint = ContractEndpoint(
                            path=path,
                            method=method.upper(),
                            summary=operation.get("summary"),
                            description=operation.get("description"),
                            parameters=operation.get("parameters", []),
                            request_body=operation.get("requestBody"),
                            responses=operation.get("responses", {}),
                            security=operation.get("security", []),
                            deprecated=operation.get("deprecated", False)
                        )
                        key = f"{method.upper()}:{path}"
                        endpoints[key] = endpoint

        elif contract_type == ContractType.CUSTOM:
            for endpoint_data in spec.get("endpoints", []):
                endpoint = ContractEndpoint(
                    path=endpoint_data.get("path", "/"),
                    method=endpoint_data.get("method", "GET").upper(),
                    summary=endpoint_data.get("summary"),
                    description=endpoint_data.get("description"),
                    parameters=endpoint_data.get("parameters", []),
                    request_body=endpoint_data.get("requestBody"),
                    responses=endpoint_data.get("responses", {}),
                    deprecated=endpoint_data.get("deprecated", False)
                )
                key = f"{endpoint.method}:{endpoint.path}"
                endpoints[key] = endpoint

        return endpoints

    def validate_request(
        self,
        contract_id: str,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        query_params: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None
    ) -> List[ContractViolation]:
        """Validate a request against contract."""
        violations = []
        headers = headers or {}
        query_params = query_params or {}

        if contract_id not in self._contracts:
            violations.append(ContractViolation(
                severity="error",
                location="contract",
                message=f"Contract not found: {contract_id}"
            ))
            return violations

        key = f"{method.upper()}:{path}"
        endpoint = self._contracts[contract_id].get(key)

        if not endpoint:
            violations.append(ContractViolation(
                severity="error",
                location="endpoint",
                message=f"Endpoint not defined in contract: {method} {path}"
            ))
            return violations

        if endpoint.deprecated:
            violations.append(ContractViolation(
                severity="warning",
                location="endpoint",
                message=f"Endpoint is deprecated: {method} {path}"
            ))

        violations.extend(self._validate_parameters(endpoint, query_params, headers))
        violations.extend(self._validate_body(endpoint, body))
        violations.extend(self._validate_headers(endpoint, headers))

        return violations

    def _validate_parameters(
        self,
        endpoint: ContractEndpoint,
        query_params: Dict[str, str],
        headers: Dict[str, str]
    ) -> List[ContractViolation]:
        """Validate request parameters."""
        violations = []

        for param in endpoint.parameters:
            param_name = param.get("name")
            param_in = param.get("in")
            required = param.get("required", False)

            if param_in == "query":
                if required and param_name not in query_params:
                    violations.append(ContractViolation(
                        severity="error",
                        location=f"query.{param_name}",
                        message=f"Required query parameter missing: {param_name}"
                    ))
            elif param_in == "header":
                if required and param_name.lower() not in [h.lower() for h in headers]:
                    violations.append(ContractViolation(
                        severity="error",
                        location=f"header.{param_name}",
                        message=f"Required header missing: {param_name}"
                    ))

            if param_in == "query" and param_name in query_params:
                violations.extend(self._validate_param_type(
                    param, query_params[param_name], f"query.{param_name}"
                ))

        return violations

    def _validate_param_type(
        self,
        param: Dict[str, Any],
        value: str,
        location: str
    ) -> List[ContractViolation]:
        """Validate parameter type and format."""
        violations = []
        param_schema = param.get("schema", {})
        param_type = param_schema.get("type")

        if param_type == "integer":
            if not value.isdigit() and not (value.startswith('-') and value[1:].isdigit()):
                violations.append(ContractViolation(
                    severity="error",
                    location=location,
                    message=f"Parameter must be integer: {value}"
                ))
        elif param_type == "number":
            try:
                float(value)
            except ValueError:
                violations.append(ContractViolation(
                    severity="error",
                    location=location,
                    message=f"Parameter must be number: {value}"
                ))
        elif param_type == "boolean":
            if value.lower() not in ("true", "false", "1", "0"):
                violations.append(ContractViolation(
                    severity="error",
                    location=location,
                    message=f"Parameter must be boolean: {value}"
                ))

        return violations

    def _validate_body(
        self,
        endpoint: ContractEndpoint,
        body: Optional[Any]
    ) -> List[ContractViolation]:
        """Validate request body."""
        violations = []

        if endpoint.request_body:
            required = endpoint.request_body.get("required", False)
            if required and body is None:
                violations.append(ContractViolation(
                    severity="error",
                    location="body",
                    message="Request body is required but missing"
                ))

            if body is not None:
                content = endpoint.request_body.get("content", {})
                json_content = content.get("application/json", {})
                schema = json_content.get("schema")

                if schema and self._validation_level == ValidationLevel.STRICT:
                    violations.extend(self._validate_body_schema(body, schema, "body"))

        return violations

    def _validate_body_schema(
        self,
        body: Any,
        schema: Dict[str, Any],
        location: str
    ) -> List[ContractViolation]:
        """Validate body against JSON schema."""
        violations = []
        body_type = schema.get("type")

        if body_type == "object":
            if not isinstance(body, dict):
                violations.append(ContractViolation(
                    severity="error",
                    location=location,
                    message="Body must be object"
                ))
            else:
                properties = schema.get("properties", {})
                required = schema.get("required", [])

                for req_field in required:
                    if req_field not in body:
                        violations.append(ContractViolation(
                            severity="error",
                            location=f"{location}.{req_field}",
                            message=f"Required field missing: {req_field}"
                        ))

                for field_name, field_schema in properties.items():
                    if field_name in body:
                        violations.extend(self._validate_body_schema(
                            body[field_name],
                            field_schema,
                            f"{location}.{field_name}"
                        ))

        elif body_type == "array":
            if not isinstance(body, list):
                violations.append(ContractViolation(
                    severity="error",
                    location=location,
                    message="Body must be array"
                ))

        return violations

    def _validate_headers(
        self,
        endpoint: ContractEndpoint,
        headers: Dict[str, str]
    ) -> List[ContractViolation]:
        """Validate request headers."""
        violations = []

        for sec in endpoint.security:
            if "apiKey" in sec:
                api_key_header = sec["apiKey"].get("name", "X-API-Key")
                if api_key_header.lower() not in [h.lower() for h in headers]:
                    violations.append(ContractViolation(
                        severity="error",
                        location=f"header.{api_key_header}",
                        message=f"API key header required: {api_key_header}"
                    ))

        return violations

    def add_custom_rule(
        self,
        rule_name: str,
        validator: Callable[[Dict[str, Any]], List[ContractViolation]]
    ) -> None:
        """Add a custom validation rule."""
        self._custom_rules[rule_name] = validator

    def get_contract_endpoints(
        self,
        contract_id: str
    ) -> List[Dict[str, Any]]:
        """Get all endpoints in a contract."""
        if contract_id not in self._contracts:
            return []

        return [
            {
                "path": e.path,
                "method": e.method,
                "summary": e.summary,
                "deprecated": e.deprecated
            }
            for e in self._contracts[contract_id].values()
        ]


class APIContractAction(BaseAction):
    """Action for API contract operations."""

    def __init__(self):
        super().__init__("api_contract")
        self._validator = ContractValidator()
        self._test_history: List[ContractTestResult] = []

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute API contract action."""
        try:
            operation = params.get("operation", "validate")

            if operation == "load":
                return self._load_contract(params)
            elif operation == "validate":
                return self._validate_request(params)
            elif operation == "test":
                return self._run_contract_test(params)
            elif operation == "endpoints":
                return self._get_endpoints(params)
            elif operation == "history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _load_contract(self, params: Dict[str, Any]) -> ActionResult:
        """Load a contract specification."""
        contract_id = params.get("contract_id")
        spec = params.get("spec", {})
        contract_type = ContractType(params.get("contract_type", "openapi"))

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        count = self._validator.load_contract(contract_id, spec, contract_type)
        return ActionResult(
            success=True,
            message=f"Loaded {count} endpoints for contract: {contract_id}"
        )

    def _validate_request(self, params: Dict[str, Any]) -> ActionResult:
        """Validate a request against contract."""
        contract_id = params.get("contract_id")
        method = params.get("method", "GET")
        path = params.get("path", "/")

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        violations = self._validator.validate_request(
            contract_id=contract_id,
            method=method,
            path=path,
            headers=params.get("headers"),
            query_params=params.get("query_params"),
            body=params.get("body")
        )

        return ActionResult(
            success=len(violations) == 0,
            data={
                "passed": len(violations) == 0,
                "violation_count": len(violations),
                "violations": [
                    {
                        "severity": v.severity,
                        "location": v.location,
                        "message": v.message
                    }
                    for v in violations
                ]
            }
        )

    def _run_contract_test(self, params: Dict[str, Any]) -> ActionResult:
        """Run contract tests."""
        contract_id = params.get("contract_id")
        test_requests = params.get("requests", [])

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        results = []
        for req in test_requests:
            start = time.time()
            violations = self._validator.validate_request(
                contract_id=contract_id,
                method=req.get("method", "GET"),
                path=req.get("path", "/"),
                headers=req.get("headers"),
                query_params=req.get("query_params"),
                body=req.get("body")
            )
            duration = (time.time() - start) * 1000

            result = ContractTestResult(
                endpoint=req.get("path", "/"),
                method=req.get("method", "GET"),
                passed=len(violations) == 0,
                violations=violations,
                duration_ms=duration
            )
            results.append(result)

        self._test_history.extend(results)

        passed = sum(1 for r in results if r.passed)
        return ActionResult(
            success=passed == len(results),
            data={
                "total": len(results),
                "passed": passed,
                "failed": len(results) - passed,
                "results": [
                    {
                        "endpoint": r.endpoint,
                        "method": r.method,
                        "passed": r.passed,
                        "violations": len(r.violations),
                        "duration_ms": r.duration_ms
                    }
                    for r in results
                ]
            }
        )

    def _get_endpoints(self, params: Dict[str, Any]) -> ActionResult:
        """Get contract endpoints."""
        contract_id = params.get("contract_id")

        if not contract_id:
            return ActionResult(success=False, message="contract_id required")

        endpoints = self._validator.get_contract_endpoints(contract_id)
        return ActionResult(success=True, data={"endpoints": endpoints})

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get test history."""
        limit = params.get("limit", 100)
        history = self._test_history[-limit:]

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "endpoint": r.endpoint,
                        "method": r.method,
                        "passed": r.passed,
                        "violations": len(r.violations),
                        "timestamp": r.timestamp
                    }
                    for r in history
                ]
            }
        )
