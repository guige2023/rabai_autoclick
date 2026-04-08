"""
API Discovery Action Module.

Auto-discovers and documents API endpoints from OpenAPI specs,
 service registries, or by introspecting running services.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DiscoverySource(Enum):
    """Source of API discovery."""
    OPENAPI = "openapi"
    SWAGGER = "swagger"
    GRAPHQL = "graphql"
    GRPC = "grpc"
    SERVICE_REGISTRY = "service_registry"
    NETWORK_SCAN = "network_scan"


@dataclass
class DiscoveredEndpoint:
    """A discovered API endpoint."""
    path: str
    method: str
    summary: Optional[str] = None
    description: Optional[str] = None
    parameters: list[dict[str, Any]] = field(default_factory=list)
    request_body: Optional[dict[str, Any]] = None
    responses: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    auth_required: bool = False
    source: DiscoverySource = DiscoverySource.OPENAPI


@dataclass
class DiscoveredAPI:
    """A discovered API service."""
    name: str
    version: str
    base_url: str
    description: Optional[str] = None
    endpoints: list[DiscoveredEndpoint] = field(default_factory=list)
    source: DiscoverySource = DiscoverySource.OPENAPI
    raw_spec: Optional[dict[str, Any]] = None


@dataclass
class DiscoveryResult:
    """Result of API discovery operation."""
    apis: list[DiscoveredAPI]
    total_endpoints: int
    duration_ms: float = 0.0
    errors: list[str] = field(default_factory=list)


class APIDiscoveryAction:
    """
    API endpoint discovery from various sources.

    Discovers APIs from OpenAPI/Swagger specs, service registries,
    or by scanning network endpoints.

    Example:
        discoverer = APIDiscoveryAction()
        result = await discoverer.discover_from_openapi("https://api.example.com/openapi.json")
        for api in result.apis:
            print(f"Found {len(api.endpoints)} endpoints")
    """

    def __init__(
        self,
        auth_token: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.auth_token = auth_token
        self.timeout = timeout
        self._registered_handlers: dict[DiscoverySource, Callable] = {}

    async def discover_from_openapi(
        self,
        spec_url: str,
        base_url: Optional[str] = None,
    ) -> DiscoveryResult:
        """Discover API from an OpenAPI/Swagger specification."""
        import time
        import aiohttp
        start_time = time.monotonic()
        errors: list[str] = []

        try:
            async with aiohttp.ClientSession() as session:
                headers = {}
                if self.auth_token:
                    headers["Authorization"] = f"Bearer {self.auth_token}"

                async with session.get(spec_url, headers=headers, timeout=self.timeout) as resp:
                    spec = await resp.json()

            api = self._parse_openapi_spec(spec, base_url or spec_url)
            return DiscoveryResult(
                apis=[api],
                total_endpoints=len(api.endpoints),
                duration_ms=(time.monotonic() - start_time) * 1000,
            )

        except Exception as e:
            errors.append(f"OpenAPI discovery failed: {e}")
            return DiscoveryResult(
                apis=[],
                total_endpoints=0,
                duration_ms=(time.monotonic() - start_time) * 1000,
                errors=errors,
            )

    def _parse_openapi_spec(
        self,
        spec: dict[str, Any],
        base_url: str,
    ) -> DiscoveredAPI:
        """Parse an OpenAPI specification into structured data."""
        info = spec.get("info", {})
        paths = spec.get("paths", {})

        api = DiscoveredAPI(
            name=info.get("title", "Unknown API"),
            version=info.get("version", "1.0.0"),
            base_url=base_url,
            description=info.get("description"),
            source=DiscoverySource.OPENAPI,
            raw_spec=spec,
        )

        for path, path_item in paths.items():
            for method in ["get", "post", "put", "patch", "delete", "options", "head"]:
                if method not in path_item:
                    continue

                operation = path_item[method]
                endpoint = DiscoveredEndpoint(
                    path=path,
                    method=method.upper(),
                    summary=operation.get("summary"),
                    description=operation.get("description"),
                    parameters=operation.get("parameters", []),
                    request_body=operation.get("requestBody"),
                    responses=operation.get("responses", {}),
                    tags=operation.get("tags", []),
                    auth_required=self._has_auth(operation),
                    source=DiscoverySource.OPENAPI,
                )
                api.endpoints.append(endpoint)

        return api

    def _has_auth(self, operation: dict[str, Any]) -> bool:
        """Check if operation requires authentication."""
        security = operation.get("security", [])
        if not security:
            return False
        return len(security) > 0

    async def discover_batch(
        self,
        sources: list[tuple[DiscoverySource, str]],
    ) -> DiscoveryResult:
        """Discover from multiple sources in parallel."""
        import asyncio
        import time
        start_time = time.monotonic()

        tasks = []
        for source_type, source_url in sources:
            if source_type == DiscoverySource.OPENAPI:
                tasks.append(self.discover_from_openapi(source_url))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_apis: list[DiscoveredAPI] = []
        total_endpoints = 0
        all_errors: list[str] = []

        for result in results:
            if isinstance(result, DiscoveryResult):
                all_apis.extend(result.apis)
                total_endpoints += result.total_endpoints
                all_errors.extend(result.errors)
            elif isinstance(result, Exception):
                all_errors.append(str(result))

        return DiscoveryResult(
            apis=all_apis,
            total_endpoints=total_endpoints,
            duration_ms=(time.monotonic() - start_time) * 1000,
            errors=all_errors,
        )

    def generate_markdown(self, api: DiscoveredAPI) -> str:
        """Generate Markdown documentation for a discovered API."""
        lines = [
            f"# {api.name}",
            f"",
            f"**Version:** {api.version}",
            f"**Base URL:** {api.base_url}",
            f"",
        ]

        if api.description:
            lines.append(f"{api.description}")
            lines.append("")

        lines.append("## Endpoints")
        lines.append("")

        for endpoint in api.endpoints:
            lines.append(f"### `{endpoint.method} {endpoint.path}`")
            lines.append("")
            if endpoint.summary:
                lines.append(f"**Summary:** {endpoint.summary}")
            if endpoint.description:
                lines.append(f"{endpoint.description}")
            lines.append("")

        return "\n".join(lines)
