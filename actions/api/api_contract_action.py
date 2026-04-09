"""API Contract Testing Action Module.

Provides contract testing capabilities for API integrations,
validating that providers and consumers adhere to agreed specifications.

Example:
    >>> from actions.api.api_contract_action import APIContractTester
    >>> tester = APIContractTester()
    >>> result = await tester.verify_contract(contract, provider_response)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
import threading


class ContractType(Enum):
    """Type of contract test."""
    PROVIDER = "provider"
    CONSUMER = "consumer"
    PACT = "pact"
    OPEN_API = "openapi"
    GRAPHQL = "graphql"


class VerificationStatus(Enum):
    """Result of contract verification."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


@dataclass
class SchemaField:
    """Schema field definition.
    
    Attributes:
        name: Field name
        field_type: Expected type (string, number, boolean, object, array)
        required: Whether field is required
        pattern: Optional regex pattern
        enum_values: Optional set of allowed values
        min_value: Optional minimum for numbers
        max_value: Optional maximum for numbers
        min_length: Optional minimum length for strings
        max_length: Optional maximum length for strings
        items_schema: Schema for array items
        properties: Schema for object properties
    """
    name: str
    field_type: str = "string"
    required: bool = False
    pattern: Optional[str] = None
    enum_values: Optional[Set[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    items_schema: Optional[SchemaField] = None
    properties: Dict[str, SchemaField] = field(default_factory=dict)


@dataclass
class ContractEndpoint:
    """Contract definition for an API endpoint.
    
    Attributes:
        path: API path (e.g., '/users/{id}')
        method: HTTP method
        request_schema: Schema for request body
        response_schema: Schema for response body by status code
        headers: Required headers
        description: Endpoint description
    """
    path: str
    method: str
    request_schema: Optional[SchemaField] = None
    response_schema: Dict[int, SchemaField] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    description: str = ""


@dataclass
class Contract:
    """API contract definition.
    
    Attributes:
        name: Contract name
        version: Contract version
        contract_type: Type of contract
        base_url: Base URL for the API
        endpoints: List of endpoint definitions
        metadata: Additional contract metadata
    """
    name: str
    version: str
    contract_type: ContractType = ContractType.PROVIDER
    base_url: str = ""
    endpoints: List[ContractEndpoint] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class VerificationResult:
    """Result of a contract verification.
    
    Attributes:
        status: Overall verification status
        endpoint: Endpoint that was verified
        status_code: HTTP status code of response
        errors: List of verification errors
        warnings: List of warnings
        duration: Time taken for verification
        timestamp: When verification occurred
    """
    status: VerificationStatus
    endpoint: str
    method: str
    status_code: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    duration: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ContractTestResult:
    """Result of full contract test suite.
    
    Attributes:
        contract_name: Name of tested contract
        total_tests: Total number of tests
        passed: Number of passed tests
        failed: Number of failed tests
        skipped: Number of skipped tests
        results: Individual verification results
        start_time: Test start time
        end_time: Test end time
    """
    contract_name: str
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    results: List[VerificationResult] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class ContractConfig:
    """Configuration for contract testing.
    
    Attributes:
        strict_mode: Whether to fail on warnings
        validate_headers: Whether to validate headers
        validate_schema: Whether to validate response schema
        ignore_fields: Fields to ignore in validation
        custom_validators: Custom validation functions
    """
    strict_mode: bool = False
    validate_headers: bool = True
    validate_schema: bool = True
    ignore_fields: Set[str] = field(default_factory=set)
    custom_validators: Dict[str, Callable] = field(default_factory=dict)


class APIContractTester:
    """Handles API contract testing and verification.
    
    Validates API responses against contract specifications,
    ensuring compatibility between providers and consumers.
    
    Attributes:
        config: Contract testing configuration
    
    Example:
        >>> tester = APIContractTester()
        >>> result = await tester.verify_contract(contract, response)
    """
    
    def __init__(self, config: Optional[ContractConfig] = None):
        """Initialize the contract tester.
        
        Args:
            config: Contract testing configuration
        """
        self.config = config or ContractConfig()
        self._contracts: Dict[str, Contract] = {}
        self._lock = threading.RLock()
        self._test_history: List[ContractTestResult] = []
    
    def register_contract(self, contract: Contract) -> None:
        """Register a contract for testing.
        
        Args:
            contract: Contract to register
        """
        with self._lock:
            self._contracts[contract.name] = contract
    
    def get_contract(self, name: str) -> Optional[Contract]:
        """Get a registered contract.
        
        Args:
            name: Contract name
        
        Returns:
            Contract or None
        """
        with self._lock:
            return self._contracts.get(name)
    
    def create_contract(
        self,
        name: str,
        version: str,
        base_url: str,
        contract_type: ContractType = ContractType.PROVIDER
    ) -> Contract:
        """Create and register a new contract.
        
        Args:
            name: Contract name
            version: Contract version
            base_url: Base URL for the API
            contract_type: Type of contract
        
        Returns:
            Created contract
        """
        contract = Contract(
            name=name,
            version=version,
            base_url=base_url,
            contract_type=contract_type
        )
        self.register_contract(contract)
        return contract
    
    def add_endpoint(
        self,
        contract_name: str,
        path: str,
        method: str,
        response_schema: Optional[Dict[int, SchemaField]] = None,
        request_schema: Optional[SchemaField] = None
    ) -> ContractEndpoint:
        """Add an endpoint to a contract.
        
        Args:
            contract_name: Name of the contract
            path: API path
            method: HTTP method
            response_schema: Response schemas by status code
            request_schema: Request body schema
        
        Returns:
            Created endpoint
        
        Raises:
            ValueError: If contract not found
        """
        with self._lock:
            contract = self._contracts.get(contract_name)
            if not contract:
                raise ValueError(f"Contract not found: {contract_name}")
        
        endpoint = ContractEndpoint(
            path=path,
            method=method.upper(),
            request_schema=request_schema,
            response_schema=response_schema or {}
        )
        
        with self._lock:
            contract.endpoints.append(endpoint)
        
        return endpoint
    
    async def verify_response(
        self,
        contract_name: str,
        endpoint: str,
        method: str,
        status_code: int,
        response_body: Any,
        response_headers: Optional[Dict[str, str]] = None
    ) -> VerificationResult:
        """Verify a response against its contract.
        
        Args:
            contract_name: Contract name
            endpoint: API endpoint path
            method: HTTP method
            status_code: Response status code
            response_body: Response body
            response_headers: Response headers
        
        Returns:
            VerificationResult
        """
        start_time = datetime.now()
        result = VerificationResult(
            status=VerificationStatus.PASSED,
            endpoint=endpoint,
            method=method.upper(),
            status_code=status_code,
            timestamp=start_time
        )
        
        with self._lock:
            contract = self._contracts.get(contract_name)
        
        if not contract:
            result.status = VerificationStatus.ERROR
            result.errors.append(f"Contract not found: {contract_name}")
            return result
        
        # Find matching endpoint
        matched_endpoint = None
        for ep in contract.endpoints:
            if self._match_path(ep.path, endpoint) and ep.method == method.upper():
                matched_endpoint = ep
                break
        
        if not matched_endpoint:
            result.status = VerificationStatus.SKIPPED
            result.warnings.append(f"No contract definition for {method} {endpoint}")
            return result
        
        # Verify status code
        if status_code not in matched_endpoint.response_schema:
            result.warnings.append(f"No schema defined for status code {status_code}")
            if self.config.strict_mode:
                result.status = VerificationStatus.FAILED
        
        # Verify schema if available
        if status_code in matched_endpoint.response_schema and self.config.validate_schema:
            schema = matched_endpoint.response_schema[status_code]
            schema_errors = self._validate_schema(response_body, schema)
            result.errors.extend(schema_errors)
            if schema_errors:
                result.status = VerificationStatus.FAILED
        
        # Verify headers
        if self.config.validate_headers and matched_endpoint.headers:
            header_errors = self._validate_headers(response_headers or {}, matched_endpoint.headers)
            result.errors.extend(header_errors)
            if header_errors:
                result.status = VerificationStatus.FAILED
        
        result.duration = (datetime.now() - start_time).total_seconds()
        return result
    
    def _match_path(self, contract_path: str, actual_path: str) -> bool:
        """Match a contract path pattern against an actual path.
        
        Args:
            contract_path: Contract path with {param} placeholders
            actual_path: Actual request path
        
        Returns:
            True if paths match
        """
        # Convert {param} to regex
        pattern = re.escape(contract_path)
        pattern = re.sub(r'\\{[^}]+\\}', r'[^/]+', pattern)
        pattern = f"^{pattern}$"
        
        return bool(re.match(pattern, actual_path))
    
    def _validate_schema(self, data: Any, schema: SchemaField, path: str = "") -> List[str]:
        """Validate data against a schema.
        
        Args:
            data: Data to validate
            schema: Schema to validate against
            path: Current path for error messages
        
        Returns:
            List of validation errors
        """
        errors = []
        current_path = f"{path}.{schema.name}" if path else schema.name
        
        # Check if data is None
        if data is None:
            if schema.required:
                errors.append(f"{current_path}: required field is missing")
            return errors
        
        # Type validation
        type_map = {
            "string": (str,),
            "number": (int, float),
            "integer": (int,),
            "boolean": (bool,),
            "object": (dict,),
            "array": (list, tuple),
            "null": (type(None),)
        }
        
        expected_types = type_map.get(schema.field_type, (str,))
        
        # Allow int for number type
        if schema.field_type == "number" and isinstance(data, int):
            expected_types = (int, float)
        
        if not isinstance(data, expected_types):
            errors.append(f"{current_path}: expected {schema.field_type}, got {type(data).__name__}")
            return errors  # Can't validate further if wrong type
        
        # String validations
        if schema.field_type == "string" and isinstance(data, str):
            if schema.pattern:
                if not re.match(schema.pattern, data):
                    errors.append(f"{current_path}: does not match pattern {schema.pattern}")
            
            if schema.min_length is not None and len(data) < schema.min_length:
                errors.append(f"{current_path}: length {len(data)} below minimum {schema.min_length}")
            
            if schema.max_length is not None and len(data) > schema.max_length:
                errors.append(f"{current_path}: length {len(data)} above maximum {schema.max_length}")
            
            if schema.enum_values and data not in schema.enum_values:
                errors.append(f"{current_path}: value '{data}' not in allowed values")
        
        # Number validations
        if schema.field_type in ("number", "integer") and isinstance(data, (int, float)):
            if schema.min_value is not None and data < schema.min_value:
                errors.append(f"{current_path}: value {data} below minimum {schema.min_value}")
            
            if schema.max_value is not None and data > schema.max_value:
                errors.append(f"{current_path}: value {data} above maximum {schema.max_value}")
        
        # Object validations
        if schema.field_type == "object" and isinstance(data, dict):
            # Check required properties
            for prop_name, prop_schema in schema.properties.items():
                if prop_schema.required and prop_name not in data:
                    errors.append(f"{current_path}.{prop_name}: required property is missing")
                elif prop_name in data:
                    prop_errors = self._validate_schema(data[prop_name], prop_schema, current_path)
                    errors.extend(prop_errors)
        
        # Array validations
        if schema.field_type == "array" and isinstance(data, (list, tuple)):
            if schema.items_schema:
                for i, item in enumerate(data):
                    item_errors = self._validate_schema(item, schema.items_schema, f"{current_path}[{i}]")
                    errors.extend(item_errors)
        
        return errors
    
    def _validate_headers(
        self,
        actual: Dict[str, str],
        expected: Dict[str, str]
    ) -> List[str]:
        """Validate response headers.
        
        Args:
            actual: Actual response headers
            expected: Expected headers
        
        Returns:
            List of validation errors
        """
        errors = []
        
        for header_name, expected_value in expected.items():
            if header_name not in actual:
                errors.append(f"Header '{header_name}': missing (expected: {expected_value})")
            elif actual[header_name] != expected_value:
                errors.append(
                    f"Header '{header_name}': expected '{expected_value}', got '{actual[header_name]}'"
                )
        
        return errors
    
    async def run_contract_tests(
        self,
        contract_name: str,
        test_data: List[Dict[str, Any]]
    ) -> ContractTestResult:
        """Run a full contract test suite.
        
        Args:
            contract_name: Contract to test
            test_data: List of test cases with response data
        
        Returns:
            ContractTestResult
        """
        start_time = datetime.now()
        result = ContractTestResult(
            contract_name=contract_name,
            start_time=start_time
        )
        
        for test_case in test_data:
            verification = await self.verify_response(
                contract_name=contract_name,
                endpoint=test_case.get("endpoint", "/"),
                method=test_case.get("method", "GET"),
                status_code=test_case.get("status_code", 200),
                response_body=test_case.get("body"),
                response_headers=test_case.get("headers")
            )
            
            result.results.append(verification)
            result.total_tests += 1
            
            if verification.status == VerificationStatus.PASSED:
                result.passed += 1
            elif verification.status == VerificationStatus.FAILED:
                result.failed += 1
            else:
                result.skipped += 1
        
        result.end_time = datetime.now()
        
        with self._lock:
            self._test_history.append(result)
        
        return result
    
    def get_test_history(self, contract_name: Optional[str] = None) -> List[ContractTestResult]:
        """Get test execution history.
        
        Args:
            contract_name: Optional filter by contract name
        
        Returns:
            List of test results
        """
        with self._lock:
            if contract_name:
                return [r for r in self._test_history if r.contract_name == contract_name]
            return list(self._test_history)
    
    def generate_pact_file(self, contract: Contract) -> Dict[str, Any]:
        """Generate a Pact-compatible contract file.
        
        Args:
            contract: Contract to export
        
        Returns:
            Pact-formatted contract dictionary
        """
        interactions = []
        
        for endpoint in contract.endpoints:
            interaction = {
                "description": f"{endpoint.method} {endpoint.path}",
                "request": {
                    "method": endpoint.method,
                    "path": endpoint.path
                },
                "response": []
            }
            
            for status_code, schema in endpoint.response_schema.items():
                response = {
                    "status": status_code,
                    "body": self._schema_to_example(schema)
                }
                interaction["response"].append(response)
            
            interactions.append(interaction)
        
        return {
            "consumer": {"name": contract.name},
            "provider": {"name": contract.metadata.get("provider", "unknown")},
            "interactions": interactions
        }
    
    def _schema_to_example(self, schema: SchemaField) -> Dict[str, Any]:
        """Generate an example from a schema.
        
        Args:
            schema: Schema definition
        
        Returns:
            Example data matching the schema
        """
        if schema.field_type == "string":
            if schema.enum_values:
                return list(schema.enum_values)[0]
            return "example_string"
        elif schema.field_type == "number":
            return 0.0
        elif schema.field_type == "integer":
            return 0
        elif schema.field_type == "boolean":
            return True
        elif schema.field_type == "object":
            result = {}
            for prop_name, prop_schema in schema.properties.items():
                result[prop_name] = self._schema_to_example(prop_schema)
            return result
        elif schema.field_type == "array":
            if schema.items_schema:
                return [self._schema_to_example(schema.items_schema)]
            return []
        
        return None
