"""
API Mock Server Module.

Creates mock API servers for testing with configurable responses,
dynamic behavior, response delays, and error simulation.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, Union, Pattern
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
import random
import time
import json
import re

logger = logging.getLogger(__name__)


class MockResponseType(Enum):
    """Type of mock response."""
    STATIC = auto()
    DYNAMIC = auto()
    TEMPLATED = auto()
    RANDOM = auto()
    SEQUENTIAL = auto()
    ERROR = auto()


@dataclass
class MockResponse:
    """Mock response definition."""
    status_code: int = 200
    headers: Dict[str, str] = field(default_factory=lambda: {"Content-Type": "application/json"})
    body: Any = None
    delay_ms: int = 0
    response_type: MockResponseType = MockResponseType.STATIC
    
    def get_body(self) -> Any:
        """Get response body based on type."""
        if isinstance(self.body, Callable):
            return self.body()
        return self.body


@dataclass
class MockEndpoint:
    """Mock API endpoint definition."""
    path: str
    method: str
    response: MockResponse
    description: Optional[str] = None
    conditions: Optional[List[Callable[[Dict], bool]]] = None
    usage_count: int = 0


@dataclass
class MockScenario:
    """Mock scenario with multiple endpoints."""
    name: str
    endpoints: List[MockEndpoint]
    priority: int = 0
    active: bool = True


class ResponseTemplate:
    """Template engine for dynamic responses."""
    
    @staticmethod
    def render(template: str, context: Dict[str, Any]) -> Any:
        """Render template with context."""
        if isinstance(template, dict):
            return {k: ResponseTemplate.render(v, context) for k, v in template.items()}
        elif isinstance(template, list):
            return [ResponseTemplate.render(item, context) for item in template]
        elif isinstance(template, str):
            return ResponseTemplate._render_string(template, context)
        return template
    
    @staticmethod
    def _render_string(template: str, context: Dict[str, Any]) -> str:
        """Render string template."""
        result = template
        
        for key, value in context.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in result:
                result = result.replace(placeholder, str(value))
        
        return result


class DynamicResponseBuilder:
    """Builds dynamic mock responses."""
    
    @staticmethod
    def generate_paginated_response(
        items: List[Any],
        page: int = 1,
        page_size: int = 10
    ) -> Dict[str, Any]:
        """Generate paginated response."""
        start = (page - 1) * page_size
        end = start + page_size
        
        return {
            "data": items[start:end],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": len(items),
                "has_next": end < len(items),
                "has_prev": page > 1
            }
        }
    
    @staticmethod
    def generate_error_response(
        code: str,
        message: str,
        details: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Generate error response."""
        response = {
            "error": {
                "code": code,
                "message": message
            }
        }
        if details:
            response["error"]["details"] = details
        return response
    
    @staticmethod
    def generate_id_object(prefix: str = "obj") -> Dict[str, Any]:
        """Generate object with unique ID."""
        import uuid
        return {
            "id": str(uuid.uuid4()),
            "type": prefix,
            "created_at": datetime.now().isoformat()
        }


class MockServer:
    """
    Mock API server for testing.
    
    Provides configurable endpoints, response templates,
    dynamic behavior, and error simulation.
    """
    
    def __init__(self, base_url: str = "http://localhost:8080") -> None:
        self.base_url = base_url
        self._endpoints: Dict[str, MockEndpoint] = {}
        self._scenarios: Dict[str, MockScenario] = {}
        self._active_scenario: Optional[str] = None
        self._request_log: List[Dict] = []
        self._request_count = 0
    
    def add_endpoint(
        self,
        path: str,
        method: str,
        response: MockResponse,
        description: Optional[str] = None
    ) -> "MockServer":
        """Add mock endpoint."""
        key = f"{method.upper()}:{path}"
        
        endpoint = MockEndpoint(
            path=path,
            method=method.upper(),
            response=response,
            description=description
        )
        
        self._endpoints[key] = endpoint
        return self
    
    def get(
        self,
        path: str,
        body: Any = None,
        status_code: int = 200,
        **kwargs
    ) -> "MockServer":
        """Add GET endpoint."""
        response = MockResponse(
            status_code=status_code,
            body=body,
            **kwargs
        )
        return self.add_endpoint(path, "GET", response)
    
    def post(
        self,
        path: str,
        body: Any = None,
        status_code: int = 201,
        **kwargs
    ) -> "MockServer":
        """Add POST endpoint."""
        response = MockResponse(
            status_code=status_code,
            body=body,
            **kwargs
        )
        return self.add_endpoint(path, "POST", response)
    
    def put(
        self,
        path: str,
        body: Any = None,
        status_code: int = 200,
        **kwargs
    ) -> "MockServer":
        """Add PUT endpoint."""
        response = MockResponse(
            status_code=status_code,
            body=body,
            **kwargs
        )
        return self.add_endpoint(path, "PUT", response)
    
    def delete(
        self,
        path: str,
        body: Any = None,
        status_code: int = 204,
        **kwargs
    ) -> "MockServer":
        """Add DELETE endpoint."""
        response = MockResponse(
            status_code=status_code,
            body=body,
            **kwargs
        )
        return self.add_endpoint(path, "DELETE", response)
    
    def add_scenario(self, scenario: MockScenario) -> "MockServer":
        """Add mock scenario."""
        self._scenarios[scenario.name] = scenario
        return self
    
    def use_scenario(self, scenario_name: str) -> "MockServer":
        """Activate a scenario."""
        if scenario_name in self._scenarios:
            self._active_scenario = scenario_name
            scenario = self._scenarios[scenario_name]
            self._endpoints = {e.path: e for e in scenario.endpoints}
        return self
    
    def add_delay(self, delay_ms: int) -> "MockServer":
        """Add delay to last added endpoint."""
        if self._endpoints:
            key = list(self._endpoints.keys())[-1]
            self._endpoints[key].response.delay_ms = delay_ms
        return self
    
    def simulate_error(
        self,
        path: str,
        method: str,
        error_code: int,
        error_message: str
    ) -> "MockServer":
        """Add error simulation endpoint."""
        response = MockResponse(
            status_code=error_code,
            body=DynamicResponseBuilder.generate_error_response(
                code=f"ERR_{error_code}",
                message=error_message
            ),
            response_type=MockResponseType.ERROR
        )
        return self.add_endpoint(path, method, response)
    
    def handle_request(
        self,
        method: str,
        path: str,
        headers: Dict[str, str],
        query_params: Dict[str, str],
        body: Optional[Any] = None
    ) -> Tuple[MockResponse, Dict[str, Any]]:
        """
        Handle incoming request.
        
        Returns:
            Tuple of (MockResponse, request_info)
        """
        self._request_count += 1
        
        key = f"{method.upper()}:{path}"
        endpoint = self._endpoints.get(key)
        
        request_info = {
            "timestamp": datetime.now().isoformat(),
            "method": method,
            "path": path,
            "request_number": self._request_count
        }
        
        self._request_log.append(request_info)
        
        if endpoint:
            endpoint.usage_count += 1
            
            if endpoint.conditions:
                for condition in endpoint.conditions:
                    if not condition({"body": body, "params": query_params, "headers": headers}):
                        # Return 400 if condition not met
                        return MockResponse(
                            status_code=400,
                            body={"error": "Condition not met"}
                        ), request_info
            
            response = endpoint.response
            
            if response.delay_ms > 0:
                time.sleep(response.delay_ms / 1000)
            
            return response, request_info
        
        return MockResponse(
            status_code=404,
            body={"error": "Not Found", "path": path}
        ), request_info
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return {
            "total_endpoints": len(self._endpoints),
            "total_requests": self._request_count,
            "active_scenario": self._active_scenario,
            "endpoint_usage": {
                f"{e.method}:{e.path}": e.usage_count
                for e in self._endpoints.values()
            }
        }
    
    def reset(self) -> "MockServer":
        """Reset server state."""
        for endpoint in self._endpoints.values():
            endpoint.usage_count = 0
        self._request_log.clear()
        self._request_count = 0
        return self


class MockServerBuilder:
    """Builder for creating mock servers with common patterns."""
    
    def __init__(self, base_url: str = "http://localhost:8080") -> None:
        self.server = MockServer(base_url)
    
    def with_user_endpoints(self) -> "MockServerBuilder":
        """Add common user management endpoints."""
        self.server.get(
            "/users",
            body={
                "users": [
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"}
                ]
            }
        )
        
        self.server.get(
            "/users/{id}",
            body={"id": 1, "name": "Alice", "email": "alice@example.com"}
        )
        
        self.server.post(
            "/users",
            body={"id": 3, "name": "New User", "email": "new@example.com"},
            status_code=201
        )
        
        self.server.put(
            "/users/{id}",
            body={"id": 1, "name": "Alice Updated", "email": "alice@example.com"}
        )
        
        self.server.delete("/users/{id}", status_code=204)
        
        return self
    
    def with_pagination(self) -> "MockServerBuilder":
        """Add paginated list endpoint."""
        items = [{"id": i, "name": f"Item {i}"} for i in range(100)]
        
        def paginated_response(**kwargs) -> Dict:
            params = kwargs.get("params", {})
            page = int(params.get("page", 1))
            page_size = int(params.get("page_size", 10))
            return DynamicResponseBuilder.generate_paginated_response(
                items, page, page_size
            )
        
        self.server.get(
            "/items",
            body=paginated_response,
            response_type=MockResponseType.DYNAMIC
        )
        
        return self
    
    def with_auth(self) -> "MockServerBuilder":
        """Add authentication endpoints."""
        self.server.post(
            "/auth/login",
            body={
                "access_token": "mock_token_12345",
                "refresh_token": "mock_refresh_67890",
                "expires_in": 3600
            }
        )
        
        self.server.post(
            "/auth/refresh",
            body={
                "access_token": "mock_token_refreshed",
                "expires_in": 3600
            }
        )
        
        self.server.post("/auth/logout", status_code=204)
        
        return self
    
    def with_delay(self, delay_ms: int) -> "MockServerBuilder":
        """Add delay to all endpoints."""
        self.server.add_delay(delay_ms)
        return self
    
    def with_errors(self) -> "MockServerBuilder":
        """Add error simulation endpoints."""
        self.server.simulate_error("/users", "GET", 500, "Internal Server Error")
        self.server.simulate_error("/users", "POST", 400, "Invalid request body")
        return self
    
    def build(self) -> MockServer:
        """Build and return mock server."""
        return self.server


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Build mock server
    builder = MockServerBuilder()
    
    server = (
        builder
        .with_user_endpoints()
        .with_pagination()
        .with_auth()
        .with_delay(100)
        .build()
    )
    
    print("=== Mock Server Demo ===")
    print(f"Server URL: {server.base_url}")
    print(f"Endpoints: {len(server._endpoints)}")
    
    # Simulate requests
    print("\n--- Simulating Requests ---")
    
    # GET users
    response, info = server.handle_request(
        "GET", "/users", {}, {}, None
    )
    print(f"GET /users -> {response.status_code}")
    
    # GET paginated
    response, info = server.handle_request(
        "GET", "/items", {}, {"page": "1", "page_size": "5"}, None
    )
    print(f"GET /items?page=1&page_size=5 -> {response.status_code}")
    
    # POST user
    response, info = server.handle_request(
        "POST", "/users", {}, {},
        {"name": "New User", "email": "new@example.com"}
    )
    print(f"POST /users -> {response.status_code}")
    
    # Stats
    print("\n--- Server Stats ---")
    stats = server.get_stats()
    print(f"Total requests: {stats['total_requests']}")
    print(f"Endpoint usage: {stats['endpoint_usage']}")
