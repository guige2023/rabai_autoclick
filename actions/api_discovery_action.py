"""API Discovery Action Module.

Provides API endpoint discovery, documentation generation,
and schema inference capabilities.
"""

import json
import time
import re
import hashlib
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AuthenticationType(Enum):
    """API authentication type."""
    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    CUSTOM = "custom"


@dataclass
class DiscoveredEndpoint:
    """A discovered API endpoint."""
    path: str
    method: str
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    response_schema: Optional[Dict[str, Any]] = None
    authentication: AuthenticationType = AuthenticationType.NONE
    tags: List[str] = field(default_factory=list)
    summary: Optional[str] = None
    discovered_at: float = field(default_factory=time.time)


@dataclass
class APIProfile:
    """Profile of a discovered API."""
    base_url: str
    name: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    endpoints: List[DiscoveredEndpoint] = field(default_factory=list)
    authentication: AuthenticationType = AuthenticationType.NONE
    tags: Set[str] = field(default_factory=set)


class APIDiscoveryEngine:
    """Discovers and analyzes API endpoints."""

    def __init__(self):
        self._profiles: Dict[str, APIProfile] = {}
        self._discovered_paths: Set[str] = set()
        self._parameter_extractors: Dict[str, List[str]] = {}

    def discover_from_openapi(
        self,
        spec: Dict[str, Any],
        base_url: str
    ) -> APIProfile:
        """Discover endpoints from OpenAPI specification."""
        info = spec.get("info", {})
        servers = spec.get("servers", [])

        profile = APIProfile(
            base_url=servers[0].get("url", base_url) if servers else base_url,
            name=info.get("title"),
            version=info.get("version"),
            description=info.get("description")
        )

        paths = spec.get("paths", {})
        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method not in ("get", "post", "put", "delete", "patch", "options", "head"):
                    continue

                endpoint = DiscoveredEndpoint(
                    path=path,
                    method=method.upper(),
                    parameters=operation.get("parameters", []),
                    request_body=operation.get("requestBody"),
                    response_schema=self._extract_response_schema(operation),
                    tags=operation.get("tags", []),
                    summary=operation.get("summary")
                )

                profile.endpoints.append(endpoint)

        profile.tags = {tag for ep in profile.endpoints for tag in ep.tags}
        self._profiles[profile.base_url] = profile

        return profile

    def discover_from_requests(
        self,
        base_url: str,
        requests: List[Dict[str, Any]]
    ) -> APIProfile:
        """Discover endpoints from observed requests."""
        profile = APIProfile(base_url=base_url)

        for req in requests:
            endpoint = DiscoveredEndpoint(
                path=req.get("path", "/"),
                method=req.get("method", "GET").upper(),
                parameters=req.get("params", []),
                authentication=self._detect_authentication(req.get("headers", {}))
            )

            if not self._endpoint_exists(profile, endpoint):
                profile.endpoints.append(endpoint)

        profile.tags = self._infer_tags(profile)
        self._profiles[base_url] = profile

        return profile

    def discover_from_response(
        self,
        base_url: str,
        response: Dict[str, Any]
    ) -> APIProfile:
        """Infer API structure from response."""
        profile = APIProfile(base_url=base_url)

        if "paths" in response:
            for path in response["paths"]:
                endpoint = DiscoveredEndpoint(
                    path=path,
                    method="GET",
                    summary=f"Discovered endpoint: {path}"
                )
                profile.endpoints.append(endpoint)

        self._profiles[base_url] = profile
        return profile

    def _endpoint_exists(
        self,
        profile: APIProfile,
        endpoint: DiscoveredEndpoint
    ) -> bool:
        """Check if endpoint already exists in profile."""
        return any(
            e.path == endpoint.path and e.method == endpoint.method
            for e in profile.endpoints
        )

    def _detect_authentication(
        self,
        headers: Dict[str, str]
    ) -> AuthenticationType:
        """Detect authentication type from headers."""
        auth_header = headers.get("Authorization", "")

        if not auth_header:
            return AuthenticationType.NONE
        elif auth_header.startswith("Bearer "):
            return AuthenticationType.BEARER
        elif auth_header.startswith("Basic "):
            return AuthenticationType.BASIC
        elif auth_header.lower().startswith("bearer"):
            return AuthenticationType.BEARER
        elif "X-API-Key" in headers or "api_key" in headers:
            return AuthenticationType.API_KEY
        else:
            return AuthenticationType.CUSTOM

    def _extract_response_schema(
        self,
        operation: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Extract response schema from operation."""
        responses = operation.get("responses", {})
        for status_code, response in responses.items():
            if status_code.startswith("2"):
                content = response.get("content", {})
                json_content = content.get("application/json", {})
                return json_content.get("schema")
        return None

    def _infer_tags(self, profile: APIProfile) -> Set[str]:
        """Infer tags from endpoint paths."""
        tags = set()

        for endpoint in profile.endpoints:
            path_parts = endpoint.path.strip("/").split("/")
            if len(path_parts) > 1:
                tags.add(path_parts[0])

        return tags

    def infer_schema_from_response(
        self,
        data: Any,
        path: str = "root"
    ) -> Dict[str, Any]:
        """Infer JSON schema from response data."""
        if data is None:
            return {"type": "null"}
        elif isinstance(data, bool):
            return {"type": "boolean"}
        elif isinstance(data, int):
            return {"type": "integer"}
        elif isinstance(data, float):
            return {"type": "number"}
        elif isinstance(data, str):
            return {"type": "string"}
        elif isinstance(data, list):
            if not data:
                return {"type": "array", "items": {}}
            return {
                "type": "array",
                "items": self.infer_schema_from_response(data[0], f"{path}[]")
            }
        elif isinstance(data, dict):
            properties = {}
            required = []

            for key, value in data.items():
                properties[key] = self.infer_schema_from_response(value, f"{path}.{key}")
                required.append(key)

            return {
                "type": "object",
                "properties": properties,
                "required": required
            }

        return {"type": "unknown"}

    def extract_parameters_from_path(self, path: str) -> List[Dict[str, Any]]:
        """Extract parameter definitions from path."""
        parameters = []
        path_params = re.findall(r'\{([^}]+)\}', path)

        for param in path_params:
            param_type = "string"
            param_format = None

            if "id" in param.lower():
                param_type = "string"
            elif "page" in param.lower() or "limit" in param.lower():
                param_type = "integer"
            elif "date" in param.lower() or "time" in param.lower():
                param_type = "string"
                param_format = "date-time"

            parameters.append({
                "name": param,
                "in": "path",
                "required": True,
                "schema": {
                    "type": param_type,
                    "format": param_format
                }
            })

        return parameters

    def generate_openapi_spec(self, profile: APIProfile) -> Dict[str, Any]:
        """Generate OpenAPI specification from profile."""
        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": profile.name or "Discovered API",
                "version": profile.version or "1.0.0",
                "description": profile.description
            },
            "servers": [{"url": profile.base_url}],
            "paths": {}
        }

        for endpoint in profile.endpoints:
            if endpoint.path not in spec["paths"]:
                spec["paths"][endpoint.path] = {}

            path_item = spec["paths"][endpoint.path]
            path_item[endpoint.method.lower()] = {
                "summary": endpoint.summary,
                "tags": endpoint.tags,
                "parameters": endpoint.parameters,
                "responses": {
                    "200": {
                        "description": "Successful response",
                        "content": {
                            "application/json": {
                                "schema": endpoint.response_schema or {}
                            }
                        }
                    }
                }
            }

            if endpoint.request_body:
                path_item[endpoint.method.lower()]["requestBody"] = endpoint.request_body

        return spec

    def get_profile(self, base_url: str) -> Optional[APIProfile]:
        """Get discovered API profile."""
        return self._profiles.get(base_url)

    def get_all_profiles(self) -> Dict[str, APIProfile]:
        """Get all discovered profiles."""
        return self._profiles.copy()


class APIDiscoveryAction(BaseAction):
    """Action for API discovery operations."""

    def __init__(self):
        super().__init__("api_discovery")
        self._engine = APIDiscoveryEngine()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute API discovery action."""
        try:
            operation = params.get("operation", "discover")

            if operation == "discover":
                return self._discover(params)
            elif operation == "discover_openapi":
                return self._discover_openapi(params)
            elif operation == "discover_from_requests":
                return self._discover_from_requests(params)
            elif operation == "infer_schema":
                return self._infer_schema(params)
            elif operation == "extract_params":
                return self._extract_params(params)
            elif operation == "generate_spec":
                return self._generate_spec(params)
            elif operation == "get_profile":
                return self._get_profile(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _discover(self, params: Dict[str, Any]) -> ActionResult:
        """Discover API from response."""
        base_url = params.get("base_url", "http://localhost")
        response = params.get("response", {})

        profile = self._engine.discover_from_response(base_url, response)

        return ActionResult(
            success=True,
            data={
                "base_url": profile.base_url,
                "endpoint_count": len(profile.endpoints),
                "endpoints": [
                    {"path": e.path, "method": e.method}
                    for e in profile.endpoints
                ]
            }
        )

    def _discover_openapi(self, params: Dict[str, Any]) -> ActionResult:
        """Discover from OpenAPI spec."""
        base_url = params.get("base_url", "http://localhost")
        spec = params.get("spec", {})

        profile = self._engine.discover_from_openapi(spec, base_url)

        return ActionResult(
            success=True,
            data={
                "name": profile.name,
                "version": profile.version,
                "base_url": profile.base_url,
                "endpoint_count": len(profile.endpoints),
                "tags": list(profile.tags)
            }
        )

    def _discover_from_requests(self, params: Dict[str, Any]) -> ActionResult:
        """Discover from request history."""
        base_url = params.get("base_url", "http://localhost")
        requests = params.get("requests", [])

        profile = self._engine.discover_from_requests(base_url, requests)

        return ActionResult(
            success=True,
            data={
                "base_url": profile.base_url,
                "endpoint_count": len(profile.endpoints),
                "endpoints": [
                    {
                        "path": e.path,
                        "method": e.method,
                        "authentication": e.authentication.value
                    }
                    for e in profile.endpoints
                ]
            }
        )

    def _infer_schema(self, params: Dict[str, Any]) -> ActionResult:
        """Infer JSON schema from data."""
        data = params.get("data", {})

        schema = self._engine.infer_schema_from_response(data)

        return ActionResult(success=True, data={"schema": schema})

    def _extract_params(self, params: Dict[str, Any]) -> ActionResult:
        """Extract parameters from path."""
        path = params.get("path", "/")

        parameters = self._engine.extract_parameters_from_path(path)

        return ActionResult(
            success=True,
            data={"parameters": parameters}
        )

    def _generate_spec(self, params: Dict[str, Any]) -> ActionResult:
        """Generate OpenAPI spec from profile."""
        base_url = params.get("base_url")

        if not base_url:
            return ActionResult(success=False, message="base_url required")

        profile = self._engine.get_profile(base_url)
        if not profile:
            return ActionResult(success=False, message=f"No profile for: {base_url}")

        spec = self._engine.generate_openapi_spec(profile)

        return ActionResult(success=True, data={"spec": spec})

    def _get_profile(self, params: Dict[str, Any]) -> ActionResult:
        """Get API profile."""
        base_url = params.get("base_url")

        if not base_url:
            profiles = self._engine.get_all_profiles()
            return ActionResult(
                success=True,
                data={
                    "profiles": [
                        {"base_url": p.base_url, "name": p.name}
                        for p in profiles.values()
                    ]
                }
            )

        profile = self._engine.get_profile(base_url)
        if not profile:
            return ActionResult(success=False, message=f"No profile for: {base_url}")

        return ActionResult(
            success=True,
            data={
                "base_url": profile.base_url,
                "name": profile.name,
                "version": profile.version,
                "endpoint_count": len(profile.endpoints),
                "tags": list(profile.tags)
            }
        )
