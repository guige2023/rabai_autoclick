"""
API Contract Action Module

Provides API contract testing, validation, and specification management.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import re
import json


class ContractType(Enum):
    """API contract type."""
    OPENAPI = "openapi"
    SWAGGER = "swagger"
    GRAPHQL = "graphql"
    GRPC = "grpc"
    CUSTOM = "custom"


class ViolationSeverity(Enum):
    """Severity of contract violation."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ContractEndpoint:
    """An API endpoint in the contract."""
    path: str
    method: str
    summary: str
    parameters: list[dict] = field(default_factory=list)
    request_body: Optional[dict] = None
    responses: dict[str, dict] = field(default_factory=dict)
    security: list[str] = field(default_factory=list)
    deprecated: bool = False


@dataclass
class ContractSchema:
    """Schema definition for contract."""
    name: str
    schema_type: str  # object, array, primitive
    properties: dict[str, dict] = field(default_factory=list)
    required: list[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class ContractViolation:
    """A contract violation."""
    severity: ViolationSeverity
    endpoint: str
    method: str
    rule: str
    message: str
    actual_value: Any = None
    expected_value: Any = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ContractTestResult:
    """Result of contract test."""
    passed: bool
    violations: list[ContractViolation]
    tested_endpoints: int
    coverage_percent: float
    duration_ms: float


@dataclass
class ContractDefinition:
    """Full API contract definition."""
    name: str
    version: str
    contract_type: ContractType
    description: str
    base_url: str
    endpoints: dict[str, ContractEndpoint] = field(default_factory=dict)
    schemas: dict[str, ContractSchema] = field(default_factory=dict)
    security_schemes: dict[str, dict] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class ApiContractAction:
    """Main API contract action handler."""
    
    def __init__(self):
        self._contracts: dict[str, ContractDefinition] = {}
        self._validators: dict[str, Callable] = {}
        self._contract_stats: dict[str, dict] = {}
    
    def register_contract(
        self,
        contract: ContractDefinition
    ) -> "ApiContractAction":
        """Register an API contract."""
        self._contracts[contract.name] = contract
        return self
    
    def register_validator(
        self,
        name: str,
        validator: Callable[[dict], bool]
    ) -> "ApiContractAction":
        """Register a custom validator function."""
        self._validators[name] = validator
        return self
    
    async def load_from_openapi(
        self,
        name: str,
        spec: dict
    ) -> ContractDefinition:
        """Load contract from OpenAPI specification."""
        info = spec.get("info", {})
        servers = spec.get("servers", [{"url": "/"}])
        
        endpoints = {}
        schemas = {}
        
        # Parse paths
        for path, path_item in spec.get("paths", {}).items():
            for method, operation in path_item.items():
                if method in ["get", "post", "put", "delete", "patch", "options", "head"]:
                    endpoint = ContractEndpoint(
                        path=path,
                        method=method.upper(),
                        summary=operation.get("summary", ""),
                        parameters=operation.get("parameters", []),
                        request_body=operation.get("requestBody"),
                        responses=operation.get("responses", {}),
                        security=operation.get("security", []),
                        deprecated=operation.get("deprecated", False)
                    )
                    
                    key = f"{method.upper()}:{path}"
                    endpoints[key] = endpoint
        
        # Parse schemas
        for schema_name, schema_def in spec.get("components", {}).get("schemas", {}).items():
            schema = ContractSchema(
                name=schema_name,
                schema_type=schema_def.get("type", "object"),
                properties=schema_def.get("properties", {}),
                required=schema_def.get("required", []),
                description=schema_def.get("description")
            )
            schemas[schema_name] = schema
        
        contract = ContractDefinition(
            name=name,
            version=info.get("version", "1.0.0"),
            contract_type=ContractType.OPENAPI,
            description=info.get("description", ""),
            base_url=servers[0].get("url", "/") if servers else "/",
            endpoints=endpoints,
            schemas=schemas,
            security_schemes=spec.get("components", {}).get("securitySchemes", {}),
            metadata={"title": info.get("title")}
        )
        
        self._contracts[name] = contract
        return contract
    
    async def validate_request(
        self,
        contract_name: str,
        endpoint: str,
        method: str,
        request_data: dict[str, Any]
    ) -> list[ContractViolation]:
        """
        Validate a request against the contract.
        
        Args:
            contract_name: Name of registered contract
            endpoint: API endpoint path
            method: HTTP method
            request_data: Request data to validate
            
        Returns:
            List of violations found
        """
        violations = []
        
        if contract_name not in self._contracts:
            violations.append(ContractViolation(
                severity=ViolationSeverity.ERROR,
                endpoint=endpoint,
                method=method,
                rule="contract_not_found",
                message=f"Contract '{contract_name}' not found"
            ))
            return violations
        
        contract = self._contracts[contract_name]
        key = f"{method.upper()}:{endpoint}"
        
        if key not in contract.endpoints:
            violations.append(ContractViolation(
                severity=ViolationSeverity.ERROR,
                endpoint=endpoint,
                method=method,
                rule="endpoint_not_found",
                message=f"Endpoint '{key}' not defined in contract"
            ))
            return violations
        
        endpoint_def = contract.endpoints[key]
        
        # Check deprecated
        if endpoint_def.deprecated:
            violations.append(ContractViolation(
                severity=ViolationSeverity.WARNING,
                endpoint=endpoint,
                method=method,
                rule="deprecated",
                message=f"Endpoint '{key}' is deprecated"
            ))
        
        # Validate parameters
        param_violations = self._validate_parameters(
            endpoint, method,
            request_data.get("params", {}),
            endpoint_def.parameters
        )
        violations.extend(param_violations)
        
        # Validate request body
        if endpoint_def.request_body and "body" in request_data:
            body_violations = self._validate_request_body(
                endpoint, method,
                request_data["body"],
                endpoint_def.request_body,
                contract
            )
            violations.extend(body_violations)
        
        return violations
    
    def _validate_parameters(
        self,
        endpoint: str,
        method: str,
        provided_params: dict,
        expected_params: list[dict]
    ) -> list[ContractViolation]:
        """Validate request parameters."""
        violations = []
        
        # Check required parameters
        for param in expected_params:
            if param.get("required") and param["name"] not in provided_params:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    endpoint=endpoint,
                    method=method,
                    rule="missing_required_param",
                    message=f"Required parameter '{param['name']}' is missing",
                    expected_value=param["name"]
                ))
        
        # Validate parameter types
        for param_name, param_value in provided_params.items():
            expected_param = next(
                (p for p in expected_params if p["name"] == param_name),
                None
            )
            
            if expected_param:
                expected_type = expected_param.get("schema", {}).get("type")
                if expected_type and not self._check_type(param_value, expected_type):
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        endpoint=endpoint,
                        method=method,
                        rule="invalid_param_type",
                        message=f"Parameter '{param_name}' should be {expected_type}",
                        actual_value=type(param_value).__name__,
                        expected_value=expected_type
                    ))
        
        return violations
    
    def _validate_request_body(
        self,
        endpoint: str,
        method: str,
        body: Any,
        body_def: dict,
        contract: ContractDefinition
    ) -> list[ContractViolation]:
        """Validate request body against contract."""
        violations = []
        
        content = body_def.get("content", {})
        json_content = content.get("application/json", {})
        schema_ref = json_content.get("schema", {}).get("$ref", "")
        
        if schema_ref:
            schema_name = schema_ref.split("/")[-1]
            if schema_name in contract.schemas:
                schema = contract.schemas[schema_name]
                violations.extend(
                    self._validate_with_schema(endpoint, method, body, schema)
                )
        
        return violations
    
    def _validate_with_schema(
        self,
        endpoint: str,
        method: str,
        data: Any,
        schema: ContractSchema
    ) -> list[ContractViolation]:
        """Validate data against a schema."""
        violations = []
        
        if not isinstance(data, dict):
            violations.append(ContractViolation(
                severity=ViolationSeverity.ERROR,
                endpoint=endpoint,
                method=method,
                rule="invalid_type",
                message=f"Expected object, got {type(data).__name__}"
            ))
            return violations
        
        # Check required fields
        for required_field in schema.required:
            if required_field not in data:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    endpoint=endpoint,
                    method=method,
                    rule="missing_required_field",
                    message=f"Required field '{required_field}' is missing",
                    expected_value=required_field
                ))
        
        # Validate field types
        for field_name, field_def in schema.properties.items():
            if field_name in data:
                expected_type = field_def.get("type")
                actual_value = data[field_name]
                
                if expected_type and not self._check_type(actual_value, expected_type):
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        endpoint=endpoint,
                        method=method,
                        rule="invalid_field_type",
                        message=f"Field '{field_name}' should be {expected_type}",
                        actual_value=type(actual_value).__name__,
                        expected_value=expected_type
                    ))
        
        return violations
    
    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            "string": str,
            "number": (int, float),
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }
        
        expected = type_map.get(expected_type)
        if expected:
            return isinstance(value, expected)
        return True
    
    async def validate_response(
        self,
        contract_name: str,
        endpoint: str,
        method: str,
        status_code: int,
        response_data: Any
    ) -> list[ContractViolation]:
        """Validate a response against the contract."""
        violations = []
        
        if contract_name not in self._contracts:
            return [ContractViolation(
                severity=ViolationSeverity.ERROR,
                endpoint=endpoint,
                method=method,
                rule="contract_not_found",
                message=f"Contract '{contract_name}' not found"
            )]
        
        contract = self._contracts[contract_name]
        key = f"{method.upper()}:{endpoint}"
        
        if key not in contract.endpoints:
            return [ContractViolation(
                severity=ViolationSeverity.ERROR,
                endpoint=endpoint,
                method=method,
                rule="endpoint_not_found",
                message=f"Endpoint '{key}' not defined in contract"
            )]
        
        endpoint_def = contract.endpoints[key]
        responses = endpoint_def.responses
        
        # Check if status code is defined
        status_str = str(status_code)
        if status_str not in responses:
            # Check for default response
            if "default" not in responses:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.WARNING,
                    endpoint=endpoint,
                    method=method,
                    rule="undefined_status_code",
                    message=f"Status code {status_code} not defined in contract",
                    actual_value=status_code
                ))
        
        return violations
    
    async def test_contract(
        self,
        contract_name: str,
        test_cases: list[dict[str, Any]]
    ) -> ContractTestResult:
        """Run contract tests against test cases."""
        start_time = datetime.now()
        violations = []
        tested = 0
        
        for test_case in test_cases:
            endpoint = test_case.get("endpoint")
            method = test_case.get("method", "GET")
            request_data = test_case.get("request", {})
            expected_status = test_case.get("expected_status", 200)
            
            violations.extend(
                await self.validate_request(
                    contract_name, endpoint, method, request_data
                )
            )
            
            tested += 1
        
        # Calculate coverage
        contract = self._contracts.get(contract_name)
        total_endpoints = len(contract.endpoints) if contract else 0
        coverage = (tested / max(1, total_endpoints)) * 100
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return ContractTestResult(
            passed=len([v for v in violations if v.severity == ViolationSeverity.ERROR]) == 0,
            violations=violations,
            tested_endpoints=tested,
            coverage_percent=coverage,
            duration_ms=duration_ms
        )
    
    def get_contract_info(self, name: str) -> Optional[dict[str, Any]]:
        """Get contract information."""
        if name not in self._contracts:
            return None
        
        contract = self._contracts[name]
        return {
            "name": contract.name,
            "version": contract.version,
            "type": contract.contract_type.value,
            "description": contract.description,
            "base_url": contract.base_url,
            "endpoint_count": len(contract.endpoints),
            "schema_count": len(contract.schemas)
        }
    
    def list_endpoints(self, contract_name: str) -> list[dict[str, Any]]:
        """List all endpoints in a contract."""
        if contract_name not in self._contracts:
            return []
        
        contract = self._contracts[contract_name]
        return [
            {
                "path": ep.path,
                "method": ep.method,
                "summary": ep.summary,
                "deprecated": ep.deprecated
            }
            for ep in contract.endpoints.values()
        ]
