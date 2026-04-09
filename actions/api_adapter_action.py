"""
API Adapter Action Module.

Unified API adapter with protocol translation, request/response
transformation, and multi-backend routing.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class Protocol(Enum):
    """Supported API protocols."""
    REST = "rest"
    GRAPHQL = "graphql"
    WEBSOCKET = "websocket"
    GRPC = "grpc"
    WEBHOOK = "webhook"


@dataclass
class Endpoint:
    """API endpoint definition."""
    path: str
    method: str
    protocol: Protocol
    handler: Optional[Callable] = None
    auth_required: bool = False
    rate_limit: Optional[float] = None


@dataclass
class RequestTransform:
    """Request transformation rules."""
    field_mapping: dict[str, str] = field(default_factory=dict)
    default_values: dict[str, Any] = field(default_factory=dict)
    value_transformers: dict[str, Callable] = field(default_factory=dict)


@dataclass
class ResponseTransform:
    """Response transformation rules."""
    field_mapping: dict[str, str] = field(default_factory=dict)
    exclude_fields: list[str] = field(default_factory=list)
    value_transformers: dict[str, Callable] = field(default_factory=dict)
    wrapper_key: Optional[str] = None


@dataclass
class BackendConfig:
    """Backend server configuration."""
    name: str
    base_url: str
    protocol: Protocol
    headers: dict = field(default_factory=dict)
    timeout: float = 30.0


class APIAdapterAction:
    """
    Unified API adapter with protocol translation and transformation.

    Example:
        adapter = APIAdapterAction()
        adapter.add_backend("main", "https://api.example.com", Protocol.REST)
        adapter.add_endpoint("/users", "GET", Protocol.REST)
        adapter.transform_request(field_mapping={"user_id": "id"})
        result = await adapter.request("GET", "/users")
    """

    def __init__(self):
        """Initialize API adapter."""
        self.backends: dict[str, BackendConfig] = {}
        self.endpoints: dict[str, Endpoint] = {}
        self.request_transform = RequestTransform()
        self.response_transform = ResponseTransform()
        self._active_backend: Optional[str] = None

    def add_backend(
        self,
        name: str,
        base_url: str,
        protocol: Protocol = Protocol.REST,
        headers: Optional[dict] = None,
        timeout: float = 30.0
    ) -> BackendConfig:
        """
        Add a backend configuration.

        Args:
            name: Backend identifier.
            base_url: Base URL for backend.
            protocol: API protocol.
            headers: Default headers.
            timeout: Request timeout.

        Returns:
            Created BackendConfig.
        """
        backend = BackendConfig(
            name=name,
            base_url=base_url.rstrip("/"),
            protocol=protocol,
            headers=headers or {},
            timeout=timeout
        )

        self.backends[name] = backend
        if self._active_backend is None:
            self._active_backend = name

        logger.info(f"Added backend: {name} ({base_url})")
        return backend

    def set_active_backend(self, name: str) -> bool:
        """Set active backend for requests."""
        if name in self.backends:
            self._active_backend = name
            return True
        return False

    def add_endpoint(
        self,
        path: str,
        method: str,
        protocol: Protocol = Protocol.REST,
        handler: Optional[Callable] = None,
        auth_required: bool = False,
        rate_limit: Optional[float] = None
    ) -> Endpoint:
        """
        Add an API endpoint.

        Args:
            path: Endpoint path.
            method: HTTP method.
            protocol: API protocol.
            handler: Request handler function.
            auth_required: Whether auth is required.
            rate_limit: Optional rate limit.

        Returns:
            Created Endpoint.
        """
        endpoint = Endpoint(
            path=path,
            method=method.upper(),
            protocol=protocol,
            handler=handler,
            auth_required=auth_required,
            rate_limit=rate_limit
        )

        key = f"{method.upper()}:{path}"
        self.endpoints[key] = endpoint
        logger.debug(f"Added endpoint: {method.upper()} {path}")
        return endpoint

    def transform_request(
        self,
        field_mapping: Optional[dict[str, str]] = None,
        default_values: Optional[dict[str, Any]] = None,
        value_transformers: Optional[dict[str, Callable]] = None
    ) -> None:
        """Configure request transformation rules."""
        if field_mapping:
            self.request_transform.field_mapping.update(field_mapping)
        if default_values:
            self.request_transform.default_values.update(default_values)
        if value_transformers:
            self.request_transform.value_transformers.update(value_transformers)

    def transform_response(
        self,
        field_mapping: Optional[dict[str, str]] = None,
        exclude_fields: Optional[list[str]] = None,
        value_transformers: Optional[dict[str, Callable]] = None,
        wrapper_key: Optional[str] = None
    ) -> None:
        """Configure response transformation rules."""
        if field_mapping:
            self.response_transform.field_mapping.update(field_mapping)
        if exclude_fields:
            self.response_transform.exclude_fields.extend(exclude_fields)
        if value_transformers:
            self.response_transform.value_transformers.update(value_transformers)
        if wrapper_key:
            self.response_transform.wrapper_key = wrapper_key

    async def request(
        self,
        method: str,
        path: str,
        data: Optional[Any] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
        backend: Optional[str] = None
    ) -> Any:
        """
        Make an API request.

        Args:
            method: HTTP method.
            path: Request path.
            data: Request body data.
            headers: Additional headers.
            params: Query parameters.
            backend: Backend name (uses active if None).

        Returns:
            Response data.

        Raises:
            ValueError: If no backend configured or endpoint not found.
        """
        backend_name = backend or self._active_backend
        if not backend_name or backend_name not in self.backends:
            raise ValueError(f"No backend configured: {backend_name}")

        backend_config = self.backends[backend_name]
        key = f"{method.upper()}:{path}"

        endpoint = self.endpoints.get(key)

        if endpoint and endpoint.auth_required:
            if headers and "Authorization" not in headers:
                raise ValueError("Authentication required for this endpoint")

        request_headers = backend_config.headers.copy()
        if headers:
            request_headers.update(headers)

        transformed_data = self._transform_request_data(data) if data else None

        url = f"{backend_config.base_url}{path}"

        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method.upper(),
                    url=url,
                    json=transformed_data,
                    headers=request_headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=backend_config.timeout)
                ) as response:
                    response_data = await response.json() if response.content_type == "application/json" else await response.text()

                    if not response.ok:
                        raise Exception(f"Request failed: {response.status} {response_data}")

                    return self._transform_response_data(response_data)

        except ImportError:
            raise ImportError("aiohttp is required for API requests. Install with: pip install aiohttp")

    async def graphql_query(
        self,
        query: str,
        variables: Optional[dict] = None,
        backend: Optional[str] = None
    ) -> dict:
        """
        Execute a GraphQL query.

        Args:
            query: GraphQL query string.
            variables: Query variables.
            backend: Backend name.

        Returns:
            GraphQL response data.
        """
        return await self.request(
            method="POST",
            path="/graphql",
            data={"query": query, "variables": variables or {}},
            backend=backend
        )

    def _transform_request_data(self, data: Any) -> Any:
        """Apply request transformation to data."""
        if isinstance(data, dict):
            result = {}

            for key, value in data.items():
                mapped_key = self.request_transform.field_mapping.get(key, key)

                if key in self.request_transform.value_transformers:
                    value = self.request_transform.value_transformers[key](value)

                result[mapped_key] = self._transform_request_data(value)

            for key, value in self.request_transform.default_values.items():
                if key not in result:
                    result[key] = value

            return result

        elif isinstance(data, list):
            return [self._transform_request_data(item) for item in data]

        return data

    def _transform_response_data(self, data: Any) -> Any:
        """Apply response transformation to data."""
        wrapper = self.response_transform.wrapper_key

        if wrapper and isinstance(data, dict) and wrapper in data:
            data = data[wrapper]

        if isinstance(data, dict):
            result = {}

            for key, value in data.items():
                if key in self.response_transform.exclude_fields:
                    continue

                mapped_key = self.response_transform.field_mapping.get(key, key)

                if key in self.response_transform.value_transformers:
                    value = self.response_transform.value_transformers[key](value)

                result[mapped_key] = self._transform_response_data(value)

            return result

        elif isinstance(data, list):
            return [self._transform_response_data(item) for item in data]

        return data

    def get_endpoints(self) -> list[dict]:
        """Get all registered endpoints."""
        return [
            {
                "path": ep.path,
                "method": ep.method,
                "protocol": ep.protocol.value,
                "auth_required": ep.auth_required
            }
            for ep in self.endpoints.values()
        ]

    def get_backends(self) -> list[dict]:
        """Get all configured backends."""
        return [
            {
                "name": b.name,
                "base_url": b.base_url,
                "protocol": b.protocol.value,
                "is_active": b.name == self._active_backend
            }
            for b in self.backends.values()
        ]
