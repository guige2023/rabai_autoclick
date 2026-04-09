"""API Testing Action Module.

Provides API testing utilities: mock responses, contract testing,
load testing helpers, and assertion helpers.

Example:
    result = execute(context, {"action": "mock_response", "status": 200})
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json


@dataclass
class MockResponse:
    """Mock HTTP response for testing."""
    
    status_code: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: Any = None
    delay_ms: int = 0
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "status_code": self.status_code,
            "headers": self.headers,
            "body": self.body,
            "delay_ms": self.delay_ms,
        }


@dataclass
class TestCase:
    """API test case definition."""
    
    name: str
    request: dict[str, Any]
    expected_response: dict[str, Any]
    expected_status: int = 200
    validators: list[dict[str, Any]] = field(default_factory=list)


class MockServer:
    """In-memory mock API server for testing."""
    
    def __init__(self) -> None:
        """Initialize mock server."""
        self._responses: dict[str, MockResponse] = {}
        self._request_log: list[dict[str, Any]] = []
    
    def register(
        self,
        path: str,
        status_code: int = 200,
        body: Any = None,
        headers: Optional[dict[str, str]] = None,
        delay_ms: int = 0,
    ) -> None:
        """Register a mock response.
        
        Args:
            path: API endpoint path
            status_code: HTTP status code
            body: Response body
            headers: Response headers
            delay_ms: Simulated delay in milliseconds
        """
        self._responses[path] = MockResponse(
            status_code=status_code,
            headers=headers or {"Content-Type": "application/json"},
            body=body,
            delay_ms=delay_ms,
        )
    
    def get_response(self, path: str) -> Optional[MockResponse]:
        """Get mock response for path."""
        return self._responses.get(path)
    
    def record_request(self, path: str, method: str, headers: dict[str, str]) -> None:
        """Record incoming request."""
        self._request_log.append({
            "path": path,
            "method": method,
            "headers": headers,
            "timestamp": datetime.now().isoformat(),
        })
    
    def get_log(self) -> list[dict[str, Any]]:
        """Get request log."""
        return self._request_log.copy()
    
    def clear_log(self) -> None:
        """Clear request log."""
        self._request_log.clear()


class ContractValidator:
    """Validates API responses against contract/schema."""
    
    @staticmethod
    def validate_response(
        response: dict[str, Any],
        schema: dict[str, Any],
    ) -> tuple[bool, list[str]]:
        """Validate response against JSON schema.
        
        Args:
            response: API response to validate
            schema: JSON schema definition
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        for field_name, field_schema in schema.items():
            if field_schema.get("required", False) and field_name not in response:
                errors.append(f"Required field missing: {field_name}")
                continue
            
            if field_name in response:
                value = response[field_name]
                expected_type = field_schema.get("type")
                
                if expected_type and not ContractValidator._check_type(value, expected_type):
                    errors.append(
                        f"Field '{field_name}' type mismatch: "
                        f"expected {expected_type}, got {type(value).__name__}"
                    )
                
                if "enum" in field_schema:
                    if value not in field_schema["enum"]:
                        errors.append(
                            f"Field '{field_name}' value not in allowed values: {value}"
                        )
                
                if "min" in field_schema and isinstance(value, (int, float)):
                    if value < field_schema["min"]:
                        errors.append(f"Field '{field_name}' below minimum: {value}")
                
                if "max" in field_schema and isinstance(value, (int, float)):
                    if value > field_schema["max"]:
                        errors.append(f"Field '{field_name}' above maximum: {value}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def _check_type(value: Any, expected: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None),
        }
        
        expected_type = type_map.get(expected)
        if expected_type is None:
            return True
        
        return isinstance(value, expected_type)


class LoadTestRunner:
    """Load testing helper for API endpoints."""
    
    def __init__(self, concurrency: int = 10, iterations: int = 100) -> None:
        """Initialize load test runner.
        
        Args:
            concurrency: Number of concurrent requests
            iterations: Total number of iterations
        """
        self.concurrency = concurrency
        self.iterations = iterations
        self._results: list[dict[str, Any]] = []
    
    def record_latency(self, endpoint: str, latency_ms: float, success: bool) -> None:
        """Record a request result."""
        self._results.append({
            "endpoint": endpoint,
            "latency_ms": latency_ms,
            "success": success,
            "timestamp": datetime.now().isoformat(),
        })
    
    def get_stats(self) -> dict[str, Any]:
        """Get load test statistics."""
        if not self._results:
            return {
                "total_requests": 0,
                "success_rate": 0.0,
                "avg_latency_ms": 0.0,
            }
        
        successful = sum(1 for r in self._results if r["success"])
        latencies = [r["latency_ms"] for r in self._results]
        
        return {
            "total_requests": len(self._results),
            "successful_requests": successful,
            "failed_requests": len(self._results) - successful,
            "success_rate": successful / len(self._results),
            "avg_latency_ms": sum(latencies) / len(latencies),
            "min_latency_ms": min(latencies),
            "max_latency_ms": max(latencies),
        }
    
    def clear(self) -> None:
        """Clear results."""
        self._results.clear()


class AssertionHelper:
    """Assertion helpers for API testing."""
    
    @staticmethod
    def assert_status(response: dict[str, Any], expected: int) -> tuple[bool, str]:
        """Assert response status code."""
        actual = response.get("status_code", response.get("status", 0))
        if actual == expected:
            return True, f"Status {expected} OK"
        return False, f"Expected status {expected}, got {actual}"
    
    @staticmethod
    def assert_header(
        response: dict[str, Any],
        header: str,
        expected: str,
    ) -> tuple[bool, str]:
        """Assert response header value."""
        headers = response.get("headers", {})
        actual = headers.get(header, "")
        if actual == expected:
            return True, f"Header {header} OK"
        return False, f"Expected header {header}={expected}, got {actual}"
    
    @staticmethod
    def assert_bodyContains(
        response: dict[str, Any],
        field: str,
        expected: str,
    ) -> tuple[bool, str]:
        """Assert body contains expected value."""
        body = response.get("body", {})
        
        if isinstance(body, dict):
            value = body.get(field, "")
        elif isinstance(body, str):
            value = body
        else:
            value = str(body)
        
        if expected in str(value):
            return True, f"Body contains '{expected}'"
        return False, f"Expected body to contain '{expected}'"
    
    @staticmethod
    def assert_jsonSchema(
        response: dict[str, Any],
        schema: dict[str, Any],
    ) -> tuple[bool, list[str]]:
        """Assert response matches JSON schema."""
        return ContractValidator.validate_response(response, schema)


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute API testing action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "mock_response":
        mock = MockResponse(
            status_code=params.get("status_code", 200),
            headers=params.get("headers", {"Content-Type": "application/json"}),
            body=params.get("body"),
            delay_ms=params.get("delay_ms", 0),
        )
        result["data"] = mock.to_dict()
    
    elif action == "validate_contract":
        response = params.get("response", {})
        schema = params.get("schema", {})
        is_valid, errors = ContractValidator.validate_response(response, schema)
        result["data"] = {"valid": is_valid, "errors": errors}
    
    elif action == "assert_status":
        response = params.get("response", {})
        expected = params.get("expected_status", 200)
        passed, message = AssertionHelper.assert_status(response, expected)
        result["data"] = {"passed": passed, "message": message}
    
    elif action == "assert_header":
        response = params.get("response", {})
        header = params.get("header", "")
        expected = params.get("expected", "")
        passed, message = AssertionHelper.assert_header(response, header, expected)
        result["data"] = {"passed": passed, "message": message}
    
    elif action == "assert_contains":
        response = params.get("response", {})
        field = params.get("field", "body")
        expected = params.get("expected", "")
        passed, message = AssertionHelper.assert_bodyContains(response, field, expected)
        result["data"] = {"passed": passed, "message": message}
    
    elif action == "load_stats":
        runner = LoadTestRunner()
        result["data"] = runner.get_stats()
    
    elif action == "record_latency":
        runner = LoadTestRunner()
        runner.record_latency(
            params.get("endpoint", ""),
            params.get("latency_ms", 0),
            params.get("success", True),
        )
        result["data"] = {"recorded": True}
    
    elif action == "test_case_validate":
        test_case = TestCase(
            name=params.get("name", ""),
            request=params.get("request", {}),
            expected_response=params.get("expected_response", {}),
            expected_status=params.get("expected_status", 200),
        )
        result["data"] = {
            "name": test_case.name,
            "request": test_case.request,
            "expected_status": test_case.expected_status,
        }
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
