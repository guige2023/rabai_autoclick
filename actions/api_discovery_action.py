"""
API Discovery Action - Discovers and documents API endpoints.

This module provides API discovery capabilities including endpoint
detection, documentation generation, and schema inference.
"""

from __future__ import annotations

import asyncio
import time
import re
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum
from collections import defaultdict


class EndpointType(Enum):
    """Types of API endpoints."""
    REST = "rest"
    GRAPHQL = "graphql"
    WEBSOCKET = "websocket"
    WEBHOOK = "webhook"


@dataclass
class DiscoveredEndpoint:
    """A discovered API endpoint."""
    path: str
    method: str | None = None
    endpoint_type: EndpointType = EndpointType.REST
    parameters: list[dict[str, Any]] = field(default_factory=list)
    request_body: dict[str, Any] | None = None
    responses: dict[str, Any] = field(default_factory=dict)
    security: list[str] = field(default_factory=list)
    deprecated: bool = False
    tags: list[str] = field(default_factory=list)


@dataclass
class APISchema:
    """Discovered API schema."""
    title: str
    version: str = "1.0.0"
    base_url: str = ""
    endpoints: list[DiscoveredEndpoint] = field(default_factory=list)
    schemas: dict[str, Any] = field(default_factory=dict)
    security_schemes: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryConfig:
    """Configuration for API discovery."""
    base_url: str
    include_internal: bool = False
    follow_redirects: bool = True
    timeout: float = 30.0
    auth_token: str | None = None


class SchemaInferrer:
    """Infers schema from API responses."""
    
    def __init__(self) -> None:
        self._type_patterns = {
            r"^\d{4}-\d{2}-\d{2}$": "date",
            r"^\d{4}-\d{2}-\d{2}T.*": "datetime",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$": "email",
            r"^https?://": "url",
            r"^\d+$": "integer",
            r"^\d+\.\d+$": "number",
            r"^(true|false)$": "boolean",
        }
    
    def infer_type(self, value: Any) -> str:
        """Infer type from value."""
        if value is None:
            return "null"
        
        if isinstance(value, bool):
            return "boolean"
        
        if isinstance(value, int):
            return "integer"
        
        if isinstance(value, float):
            return "number"
        
        if isinstance(value, (list, dict)):
            return "object" if isinstance(value, dict) else "array"
        
        if isinstance(value, str):
            for pattern, type_name in self._type_patterns.items():
                if re.match(pattern, value):
                    return type_name
            return "string"
        
        return "unknown"
    
    def infer_schema(self, data: Any) -> dict[str, Any]:
        """Infer schema from data sample."""
        if isinstance(data, dict):
            properties = {}
            required = []
            
            for key, value in data.items():
                prop_type = self.infer_type(value)
                properties[key] = {"type": prop_type}
                
                if value is not None:
                    required.append(key)
                
                if prop_type == "object":
                    properties[key] = self.infer_schema(value)
                elif prop_type == "array" and value:
                    properties[key] = {
                        "type": "array",
                        "items": {"type": self.infer_type(value[0])},
                    }
            
            return {
                "type": "object",
                "properties": properties,
                "required": required,
            }
        
        return {"type": self.infer_type(data)}


class APIEndpointsScanner:
    """Scans for API endpoints."""
    
    def __init__(self, config: DiscoveryConfig) -> None:
        self.config = config
        self._inferrer = SchemaInferrer()
        self._discovered: list[DiscoveredEndpoint] = []
    
    async def scan(self) -> APISchema:
        """Scan for API endpoints."""
        schema = APISchema(
            title="Discovered API",
            base_url=self.config.base_url,
        )
        
        try:
            await self._check_openapi(schema)
        except Exception:
            pass
        
        try:
            await self._probe_endpoints(schema)
        except Exception:
            pass
        
        return schema
    
    async def _check_openapi(self, schema: APISchema) -> None:
        """Check for OpenAPI/Swagger specification."""
        openapi_paths = [
            "/openapi.json",
            "/swagger.json",
            "/api/docs.json",
            "/api/openapi.json",
        ]
        
        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        
        for path in openapi_paths:
            try:
                import aiohttp
                timeout = aiohttp.ClientTimeout(total=self.config.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    url = f"{self.config.base_url.rstrip('/')}{path}"
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            spec = await response.json()
                            self._parse_openapi(spec, schema)
                            break
            except Exception:
                continue
    
    def _parse_openapi(self, spec: dict[str, Any], schema: APISchema) -> None:
        """Parse OpenAPI spec into schema."""
        schema.title = spec.get("info", {}).get("title", schema.title)
        schema.version = spec.get("info", {}).get("version", schema.version)
        
        for path, methods in spec.get("paths", {}).items():
            for method, details in methods.items():
                if method.upper() in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                    endpoint = DiscoveredEndpoint(
                        path=path,
                        method=method.upper(),
                        endpoint_type=EndpointType.REST,
                        parameters=details.get("parameters", []),
                        deprecated=details.get("deprecated", False),
                        tags=details.get("tags", []),
                    )
                    schema.endpoints.append(endpoint)
        
        if "components" in spec:
            schema.schemas = spec["components"].get("schemas", {})
            schema.security_schemes = spec["components"].get("securitySchemes", {})
    
    async def _probe_endpoints(self, schema: APISchema) -> None:
        """Probe common API endpoints."""
        common_paths = [
            "/api/users",
            "/api/products",
            "/api/data",
            "/api/v1/resources",
            "/api/health",
        ]
        
        headers = {}
        if self.config.auth_token:
            headers["Authorization"] = f"Bearer {self.config.auth_token}"
        
        for path in common_paths:
            try:
                import aiohttp
                timeout = aiohttp.ClientTimeout(total=5.0)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    url = f"{self.config.base_url.rstrip('/')}{path}"
                    async with session.get(url, headers=headers) as response:
                        if response.status < 500:
                            endpoint = DiscoveredEndpoint(
                                path=path,
                                method="GET",
                                endpoint_type=EndpointType.REST,
                            )
                            schema.endpoints.append(endpoint)
            except Exception:
                continue


class APIDiscoveryAction:
    """
    API discovery action for automation workflows.
    
    Example:
        action = APIDiscoveryAction()
        schema = await action.discover("https://api.example.com")
        for endpoint in schema.endpoints:
            print(f"{endpoint.method} {endpoint.path}")
    """
    
    def __init__(self) -> None:
        self._schemas: dict[str, APISchema] = {}
    
    async def discover(
        self,
        base_url: str,
        **kwargs,
    ) -> APISchema:
        """Discover API endpoints."""
        config = DiscoveryConfig(base_url=base_url, **kwargs)
        scanner = APIEndpointsScanner(config)
        schema = await scanner.scan()
        self._schemas[base_url] = schema
        return schema
    
    def get_schema(self, base_url: str) -> APISchema | None:
        """Get cached schema."""
        return self._schemas.get(base_url)
    
    async def generate_docs(
        self,
        base_url: str,
        format: str = "markdown",
    ) -> str:
        """Generate documentation from discovered schema."""
        schema = self._schemas.get(base_url)
        if not schema:
            schema = await self.discover(base_url)
        
        if format == "markdown":
            return self._generate_markdown(schema)
        elif format == "openapi":
            return self._generate_openapi(schema)
        
        return str(schema)
    
    def _generate_markdown(self, schema: APISchema) -> str:
        """Generate Markdown documentation."""
        lines = [f"# {schema.title}", f"**Version:** {schema.version}", ""]
        lines.append(f"**Base URL:** {schema.base_url}")
        lines.append("")
        lines.append("## Endpoints")
        lines.append("")
        
        for endpoint in schema.endpoints:
            deprecated = " **[DEPRECATED]**" if endpoint.deprecated else ""
            lines.append(f"### {endpoint.method} {endpoint.path}{deprecated}")
            
            if endpoint.tags:
                lines.append(f"**Tags:** {', '.join(endpoint.tags)}")
            
            if endpoint.parameters:
                lines.append("**Parameters:**")
                lines.append("| Name | Type | Location | Required |")
                lines.append("|------|------|----------|----------|")
                for param in endpoint.parameters:
                    lines.append(
                        f"| {param.get('name')} | {param.get('schema', {}).get('type')} "
                        f"| {param.get('in')} | {param.get('required', False)} |"
                    )
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _generate_openapi(self, schema: APISchema) -> str:
        """Generate OpenAPI specification."""
        import json
        
        spec = {
            "openapi": "3.0.0",
            "info": {"title": schema.title, "version": schema.version},
            "paths": {},
        }
        
        for endpoint in schema.endpoints:
            method = endpoint.method.lower()
            if method not in spec["paths"]:
                spec["paths"][endpoint.path] = {}
            
            spec["paths"][endpoint.path][method] = {
                "deprecated": endpoint.deprecated,
                "tags": endpoint.tags,
                "parameters": endpoint.parameters,
            }
        
        return json.dumps(spec, indent=2)


# Export public API
__all__ = [
    "EndpointType",
    "DiscoveredEndpoint",
    "APISchema",
    "DiscoveryConfig",
    "SchemaInferrer",
    "APIEndpointsScanner",
    "APIDiscoveryAction",
]
