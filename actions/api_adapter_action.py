"""
API Adapter Action Module.

Provides API adapter pattern for integrating with different
API styles (REST, GraphQL, gRPC, etc.) with unified interface.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class APIProtocol(Enum):
    """Supported API protocols."""
    REST = "rest"
    GRAPHQL = "graphql"
    GRPC = "grpc"
    WEBSOCKET = "websocket"
    WEBHOOK = "webhook"


@dataclass
class APIRequest:
    """API request representation."""
    method: str
    path: str
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0


@dataclass
class APIResponse:
    """API response representation."""
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def is_client_error(self) -> bool:
        return 400 <= self.status_code < 500

    @property
    def is_server_error(self) -> bool:
        return 500 <= self.status_code < 600


class APIAdapter(ABC):
    """Abstract base class for API adapters."""

    @abstractmethod
    async def request(self, req: APIRequest) -> APIResponse:
        """Execute API request."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close adapter connections."""
        pass


class RESTAdapter(APIAdapter):
    """REST API adapter using aiohttp."""

    def __init__(self, base_url: str, default_headers: Optional[Dict] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.default_headers = default_headers or {}
        self._session = None

    async def _get_session(self):
        """Get or create aiohttp session."""
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession(
                headers=self.default_headers,
            )
        return self._session

    async def request(self, req: APIRequest) -> APIResponse:
        """Execute REST API request."""
        import aiohttp

        url = f"{self.base_url}{req.path}"
        headers = {**self.default_headers, **req.headers}

        try:
            session = await self._get_session()
            async with session.request(
                method=req.method,
                url=url,
                headers=headers,
                params=req.query_params,
                json=req.body,
                timeout=aiohttp.ClientTimeout(total=req.timeout),
            ) as response:
                try:
                    body = await response.json()
                except Exception:
                    body = await response.text()

                return APIResponse(
                    status_code=response.status,
                    headers=dict(response.headers),
                    body=body,
                )
        except asyncio.TimeoutError:
            return APIResponse(
                status_code=408,
                error="Request timeout",
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                error=str(e),
            )

    async def close(self) -> None:
        """Close the session."""
        if self._session:
            await self._session.close()
            self._session = None


class GraphQLAdapter(APIAdapter):
    """GraphQL API adapter."""

    def __init__(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.endpoint = endpoint
        self.headers = headers or {}

    async def request(self, req: APIRequest) -> APIResponse:
        """Execute GraphQL request."""
        import aiohttp

        query = req.body.get("query", "")
        variables = req.body.get("variables", {})
        operation_name = req.body.get("operationName")

        payload = {
            "query": query,
            "variables": variables,
        }
        if operation_name:
            payload["operationName"] = operation_name

        headers = {**self.headers, **req.headers}
        headers["Content-Type"] = "application/json"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=req.timeout),
                ) as response:
                    body = await response.json()
                    return APIResponse(
                        status_code=response.status,
                        headers=dict(response.headers),
                        body=body,
                    )
        except asyncio.TimeoutError:
            return APIResponse(status_code=408, error="Request timeout")
        except Exception as e:
            return APIResponse(status_code=500, error=str(e))

    async def query(
        self,
        query_str: str,
        variables: Optional[Dict] = None,
    ) -> APIResponse:
        """Execute a GraphQL query."""
        req = APIRequest(
            method="POST",
            path="",
            body={"query": query_str, "variables": variables or {}},
        )
        return await self.request(req)

    async def mutate(
        self,
        mutation_str: str,
        variables: Optional[Dict] = None,
    ) -> APIResponse:
        """Execute a GraphQL mutation."""
        return await self.query(mutation_str, variables)

    async def close(self) -> None:
        """No-op for GraphQL."""
        pass


class APIGateway:
    """Unified API gateway for multiple adapters."""

    def __init__(self) -> None:
        self.adapters: Dict[str, APIAdapter] = {}

    def register_adapter(
        self,
        name: str,
        adapter: APIAdapter,
    ) -> None:
        """Register an API adapter."""
        self.adapters[name] = adapter

    def get_adapter(self, name: str) -> APIAdapter:
        """Get registered adapter."""
        if name not in self.adapters:
            raise KeyError(f"Adapter not found: {name}")
        return self.adapters[name]

    async def call(
        self,
        adapter_name: str,
        method: str,
        path: str,
        **kwargs,
    ) -> APIResponse:
        """Call API through registered adapter."""
        adapter = self.get_adapter(adapter_name)
        req = APIRequest(method=method, path=path, **kwargs)
        return await adapter.request(req)

    async def batch_request(
        self,
        requests: List[Tuple[str, APIRequest]],
    ) -> List[APIResponse]:
        """Execute batch requests in parallel."""
        tasks = []
        for adapter_name, req in requests:
            adapter = self.get_adapter(adapter_name)
            tasks.append(adapter.request(req))
        return await asyncio.gather(*tasks)

    async def close_all(self) -> None:
        """Close all adapter connections."""
        for adapter in self.adapters.values():
            await adapter.close()


@dataclass
class RequestBuilder:
    """Builder for API requests."""
    method: str = "GET"
    path: str = "/"
    headers: Dict[str, str] = field(default_factory=dict)
    query_params: Dict[str, str] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0

    def with_header(self, key: str, value: str) -> "RequestBuilder":
        """Add header."""
        self.headers[key] = value
        return self

    def with_param(self, key: str, value: str) -> "RequestBuilder":
        """Add query param."""
        self.query_params[key] = value
        return self

    def with_json(self, data: Any) -> "RequestBuilder":
        """Set JSON body."""
        self.body = data
        self.headers["Content-Type"] = "application/json"
        return self

    def with_auth(self, token: str, scheme: str = "Bearer") -> "RequestBuilder":
        """Add authorization header."""
        self.headers["Authorization"] = f"{scheme} {token}"
        return self

    def build(self) -> APIRequest:
        """Build the request."""
        return APIRequest(
            method=self.method,
            path=self.path,
            headers=self.headers,
            query_params=self.query_params,
            body=self.body,
            timeout=self.timeout,
        )
