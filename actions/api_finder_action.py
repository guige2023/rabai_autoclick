"""
API Finder Action Module.

Discovers and catalogs API endpoints automatically,
builds endpoint indexes and route maps.
"""

from __future__ import annotations

from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging
import re
import time
import httpx

logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """HTTP method types."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class APIEndpoint:
    """Discovered API endpoint."""
    path: str
    method: HTTPMethod
    summary: str = ""
    parameters: list[dict[str, Any]] = field(default_factory=list)
    responses: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    discovered_at: float = field(default_factory=time.time)


@dataclass
class APICatalog:
    """Complete API catalog."""
    base_url: str
    version: str = ""
    title: str = ""
    endpoints: list[APIEndpoint] = field(default_factory=list)
    schemas: dict[str, Any] = field(default_factory=dict)


class APIFinderAction:
    """
    Discovers and catalogs API endpoints.

    Supports OpenAPI/Swagger detection, endpoint scanning,
    and route map generation.

    Example:
        finder = APIFinderAction()
        catalog = await finder.discover("https://api.example.com")
        for endpoint in catalog.endpoints:
            print(endpoint.path, endpoint.method)
    """

    def __init__(
        self,
        timeout: float = 10.0,
        headers: Optional[dict[str, str]] = None,
    ) -> None:
        self.timeout = timeout
        self.headers = headers or {}
        self._catalog: Optional[APICatalog] = None

    async def discover(
        self,
        base_url: str,
        discover_schemas: bool = True,
    ) -> APICatalog:
        """Discover API endpoints from a base URL."""
        catalog = APICatalog(base_url=base_url)

        async with httpx.AsyncClient(
            timeout=self.timeout,
            headers=self.headers,
        ) as client:
            openapi_doc = await self._try_openapi(client, base_url)

            if openapi_doc:
                catalog = self._parse_openapi(openapi_doc, base_url)
            else:
                catalog = await self._probe_endpoints(client, base_url)

        self._catalog = catalog
        return catalog

    async def _try_openapi(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> Optional[dict[str, Any]]:
        """Try to fetch OpenAPI document."""
        candidates = [
            f"{base_url.rstrip('/')}/openapi.json",
            f"{base_url.rstrip('/')}/api/openapi.json",
            f"{base_url.rstrip('/')}/swagger.json",
            f"{base_url.rstrip('/')}/api/docs.json",
        ]

        for url in candidates:
            try:
                response = await client.get(url)
                if response.status_code == 200:
                    logger.info("Found OpenAPI doc at %s", url)
                    return response.json()
            except Exception:
                continue

        return None

    def _parse_openapi(
        self,
        doc: dict[str, Any],
        base_url: str,
    ) -> APICatalog:
        """Parse OpenAPI document into catalog."""
        catalog = APICatalog(
            base_url=base_url,
            version=doc.get("openapi", doc.get("swagger", "")),
            title=doc.get("info", {}).get("title", ""),
        )

        paths = doc.get("paths", {})
        components = doc.get("components", {})
        schemas = components.get("schemas", {})

        for path, methods in paths.items():
            for method_str, details in methods.items():
                if method_str.upper() in [m.value for m in HTTPMethod]:
                    endpoint = APIEndpoint(
                        path=path,
                        method=HTTPMethod(method_str.upper()),
                        summary=details.get("summary", ""),
                        parameters=details.get("parameters", []),
                        responses=details.get("responses", {}),
                        tags=details.get("tags", []),
                    )
                    catalog.endpoints.append(endpoint)

        catalog.schemas = schemas
        return catalog

    async def _probe_endpoints(
        self,
        client: httpx.AsyncClient,
        base_url: str,
    ) -> APICatalog:
        """Probe common endpoints to discover API."""
        catalog = APICatalog(base_url=base_url)

        common_paths = [
            "/api", "/api/v1", "/api/v2",
            "/users", "/products", "/data",
            "/health", "/status", "/info",
        ]

        for path in common_paths:
            url = f"{base_url.rstrip('/')}{path}"
            for method in [HTTPMethod.GET, HTTPMethod.POST]:
                try:
                    response = await client.request(method.value, url)
                    if response.status_code < 500:
                        endpoint = APIEndpoint(
                            path=path,
                            method=method,
                            summary=f"Discovered at {url}",
                        )
                        catalog.endpoints.append(endpoint)
                except Exception:
                    continue

        return catalog

    def find_by_path(self, path_pattern: str) -> list[APIEndpoint]:
        """Find endpoints matching a path pattern."""
        if not self._catalog:
            return []

        regex = re.compile(path_pattern)
        return [
            e for e in self._catalog.endpoints
            if regex.search(e.path)
        ]

    def find_by_method(self, method: HTTPMethod) -> list[APIEndpoint]:
        """Find all endpoints for a specific HTTP method."""
        if not self._catalog:
            return []
        return [e for e in self._catalog.endpoints if e.method == method]

    def find_by_tag(self, tag: str) -> list[APIEndpoint]:
        """Find all endpoints with a specific tag."""
        if not self._catalog:
            return []
        return [e for e in self._catalog.endpoints if tag in e.tags]

    def generate_route_map(self) -> dict[str, list[str]]:
        """Generate a route map grouped by path."""
        if not self._catalog:
            return {}

        route_map: dict[str, list[str]] = {}

        for endpoint in self._catalog.endpoints:
            if endpoint.path in route_map:
                route_map[endpoint.path].append(endpoint.method.value)
            else:
                route_map[endpoint.path] = [endpoint.method.value]

        return route_map
