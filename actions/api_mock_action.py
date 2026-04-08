"""
API Mock Action - Mocks API responses for testing.

This module provides API mocking capabilities for
testing without real backend dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class MockEndpoint:
    """A mock endpoint configuration."""
    path: str
    method: str = "GET"
    status_code: int = 200
    response: Any = None
    delay_ms: float = 0.0


@dataclass
class MockResult:
    """Result of mock API call."""
    status_code: int
    data: Any
    delay_ms: float


class APIMocker:
    """Mocks API responses."""
    
    def __init__(self) -> None:
        self._endpoints: dict[str, MockEndpoint] = {}
    
    def add_endpoint(self, endpoint: MockEndpoint) -> None:
        """Add a mock endpoint."""
        key = f"{endpoint.method}:{endpoint.path}"
        self._endpoints[key] = endpoint
    
    def get_response(self, method: str, path: str) -> MockResult | None:
        """Get mock response."""
        import time
        key = f"{method}:{path}"
        endpoint = self._endpoints.get(key)
        if endpoint:
            delay_ms = endpoint.delay_ms
            return MockResult(
                status_code=endpoint.status_code,
                data=endpoint.response,
                delay_ms=delay_ms,
            )
        return None


class APIMockAction:
    """API mock action for automation workflows."""
    
    def __init__(self) -> None:
        self.mocker = APIMocker()
    
    def add_mock(
        self,
        path: str,
        method: str = "GET",
        status_code: int = 200,
        response: Any = None,
    ) -> None:
        """Add a mock endpoint."""
        self.mocker.add_endpoint(MockEndpoint(
            path=path,
            method=method,
            status_code=status_code,
            response=response,
        ))
    
    async def get_response(self, method: str, path: str) -> MockResult | None:
        """Get mock response."""
        return self.mocker.get_response(method, path)


__all__ = ["MockEndpoint", "MockResult", "APIMocker", "APIMockAction"]
