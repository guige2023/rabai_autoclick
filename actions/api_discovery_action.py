"""
API Discovery and Introspection Module.

Provides automatic API endpoint discovery, schema inference,
and service capability detection for dynamic API integration.
"""

import re
import json
from typing import (
    Dict, List, Optional, Any, Set, Tuple, Callable,
    Pattern, Match
)
from dataclasses import dataclass, field
from enum import Enum, auto
from urllib.parse import urlparse, urljoin
import logging

logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    """HTTP methods for API endpoints."""
    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


@dataclass
class EndpointInfo:
    """Information about a discovered API endpoint."""
    path: str
    method: HttpMethod
    summary: Optional[str] = None
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: List[str] = field(default_factory=list)


@dataclass
class ApiCapability:
    """Represents a discovered API capability."""
    name: str
    category: str
    endpoints: List[EndpointInfo] = field(default_factory=list)
    version: Optional[str] = None
    base_url: Optional[str] = None


class OpenApiParser:
    """Parser for OpenAPI/Swagger specifications."""
    
    PATH_PATTERN: Pattern = re.compile(r'^(/[^/?#]*)')
    
    def __init__(self) -> None:
        self.endpoints: List[EndpointInfo] = []
    
    def parse_spec(self, spec: Dict[str, Any]) -> List[EndpointInfo]:
        """
        Parse OpenAPI specification and extract endpoint information.
        
        Args:
            spec: OpenAPI specification dictionary
            
        Returns:
            List of discovered endpoints
        """
        self.endpoints = []
        base_path = spec.get("servers", [{}])[0].get("url", "")
        paths = spec.get("paths", {})
        
        for path, path_item in paths.items():
            for method_name, operation in path_item.items():
                if method_name.upper() in ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]:
                    endpoint = self._parse_operation(path, method_name.upper(), operation)
                    if endpoint:
                        self.endpoints.append(endpoint)
        
        return self.endpoints
    
    def _parse_operation(
        self, path: str, method: str, operation: Dict[str, Any]
    ) -> Optional[EndpointInfo]:
        """Parse a single API operation."""
        try:
            return EndpointInfo(
                path=path,
                method=HttpMethod[method],
                summary=operation.get("summary"),
                description=operation.get("description"),
                parameters=operation.get("parameters", []),
                request_body=operation.get("requestBody"),
                responses=operation.get("responses", {}),
                tags=operation.get("tags", []),
                deprecated=operation.get("deprecated", False),
                security=operation.get("security", [])
            )
        except (KeyError, ValueError):
            return None


class ApiScanner:
    """Scans API endpoints through various discovery methods."""
    
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.discovered_endpoints: List[EndpointInfo] = []
        self._visited_urls: Set[str] = set()
    
    def discover_from_openapi(self, spec_url: str) -> List[EndpointInfo]:
        """
        Discover endpoints from OpenAPI specification URL.
        
        Args:
            spec_url: URL to OpenAPI JSON/YAML spec
            
        Returns:
            List of discovered endpoints
        """
        # Placeholder for actual HTTP fetch
        logger.info(f"Discovering from OpenAPI spec: {spec_url}")
        return []
    
    def discover_from_swagger(self, swagger_url: str) -> List[EndpointInfo]:
        """
        Discover endpoints from Swagger endpoint.
        
        Args:
            swagger_url: URL to Swagger JSON
            
        Returns:
            List of discovered endpoints
        """
        logger.info(f"Discovering from Swagger: {swagger_url}")
        return []
    
    def probe_endpoints(
        self, paths: List[str], methods: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Probe API endpoints to discover available functionality.
        
        Args:
            paths: List of paths to probe
            methods: Optional list of HTTP methods to test
            
        Returns:
            Dictionary of discovered capabilities
        """
        if methods is None:
            methods = ["GET", "POST", "PUT", "PATCH", "DELETE"]
        
        results = {}
        for path in paths:
            full_url = urljoin(self.base_url + "/", path.lstrip("/"))
            if full_url in self._visited_urls:
                continue
            self._visited_urls.add(full_url)
            
            for method in methods:
                results[f"{method}:{path}"] = {
                    "available": False,
                    "requires_auth": False,
                    "response_codes": []
                }
        
        return results


class CapabilityDetector:
    """Detects API capabilities based on response patterns."""
    
    def __init__(self) -> None:
        self.capabilities: Dict[str, ApiCapability] = {}
    
    def detect_pagination(self, response: Dict[str, Any]) -> bool:
        """Detect if response supports pagination."""
        pagination_indicators = [
            "total", "page", "per_page", "limit", "offset",
            "next", "previous", "has_more", "cursor"
        ]
        return any(
            indicator in str(response).lower() 
            for indicator in pagination_indicators
        )
    
    def detect_filtering(self, params: List[Dict[str, Any]]) -> bool:
        """Detect filtering capabilities from parameter definitions."""
        filter_params = {"filter", "q", "query", "search", "where"}
        return any(
            p.get("name", "").lower() in filter_params 
            for p in params
        )
    
    def detect_sorting(self, params: List[Dict[str, Any]]) -> bool:
        """Detect sorting capabilities."""
        sort_params = {"sort", "order", "order_by", "sort_by"}
        return any(
            p.get("name", "").lower() in sort_params 
            for p in params
        )
    
    def detect_auth_methods(
        self, security: List[str]
    ) -> Dict[str, bool]:
        """Detect authentication methods."""
        return {
            "bearer": any("bearer" in s.lower() for s in security),
            "basic": any("basic" in s.lower() for s in security),
            "api_key": any("apiKey" in str(s) for s in security),
            "oauth2": any("oauth" in s.lower() for s in security)
        }


class SchemaInferrer:
    """Infers schema from API response samples."""
    
    def infer_schema(self, data: Any, depth: int = 0) -> Dict[str, Any]:
        """
        Infer JSON schema from response data.
        
        Args:
            data: Response data sample
            depth: Current recursion depth
            
        Returns:
            Inferred JSON schema
        """
        if depth > 10:
            return {"type": "object", "max_depth_exceeded": True}
        
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
            if data:
                return {
                    "type": "array",
                    "items": self.infer_schema(data[0], depth + 1)
                }
            return {"type": "array", "items": {}}
        elif isinstance(data, dict):
            properties = {}
            for key, value in data.items():
                properties[key] = self.infer_schema(value, depth + 1)
            return {
                "type": "object",
                "properties": properties
            }
        
        return {"type": "unknown"}


class ApiDiscovery:
    """
    Main API discovery orchestrator.
    
    Provides unified interface for discovering API capabilities
    through multiple discovery mechanisms.
    """
    
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.scanner = ApiScanner(base_url)
        self.capability_detector = CapabilityDetector()
        self.schema_inferrer = SchemaInferrer()
        self.api_capabilities: Dict[str, ApiCapability] = {}
    
    def discover(
        self,
        use_openapi: bool = True,
        use_swagger: bool = True,
        use_probing: bool = False
    ) -> ApiCapability:
        """
        Run complete API discovery process.
        
        Args:
            use_openapi: Whether to use OpenAPI discovery
            use_swagger: Whether to use Swagger discovery
            use_probing: Whether to probe endpoints
            
        Returns:
            Discovered API capability information
        """
        capability = ApiCapability(
            name="Discovered API",
            category="api",
            base_url=self.base_url
        )
        
        # Run discovery methods
        if use_openapi:
            self._discover_openapi()
        
        if use_swagger:
            self._discover_swagger()
        
        if use_probing:
            self._probe_common_paths()
        
        return capability
    
    def _discover_openapi(self) -> None:
        """Discover using OpenAPI specifications."""
        openapi_urls = [
            f"{self.base_url}/openapi.json",
            f"{self.base_url}/api/openapi.json",
            f"{self.base_url}/swagger.json"
        ]
        
        for url in openapi_urls:
            endpoints = self.scanner.discover_from_openapi(url)
            if endpoints:
                self.scanner.discovered_endpoints.extend(endpoints)
                break
    
    def _discover_swagger(self) -> None:
        """Discover using Swagger."""
        swagger_url = f"{self.base_url}/api-docs"
        self.scanner.discover_from_swagger(swagger_url)
    
    def _probe_common_paths(self) -> None:
        """Probe common API paths."""
        common_paths = [
            "/api/v1/users", "/api/v1/products", "/api/v1/orders",
            "/api/health", "/api/status", "/api/info"
        ]
        self.scanner.probe_endpoints(common_paths)
    
    def generate_client_code(
        self, language: str = "python"
    ) -> str:
        """
        Generate client code for discovered API.
        
        Args:
            language: Target programming language
            
        Returns:
            Generated client code
        """
        if language == "python":
            return self._generate_python_client()
        return f"# Client generation for {language} not implemented"
    
    def _generate_python_client(self) -> str:
        """Generate Python client code."""
        lines = [
            "import requests",
            "",
            "",
            "class ApiClient:",
            "    def __init__(self, base_url: str, api_key: str = None):",
            "        self.base_url = base_url.rstrip('/')",
            "        self.session = requests.Session()",
            "        if api_key:",
            "            self.session.headers['Authorization'] = f'Bearer {api_key}'",
            "",
            "    def request(self, method: str, path: str, **kwargs):",
            "        url = f'{self.base_url}{path}'",
            "        return self.session.request(method, url, **kwargs)",
            ""
        ]
        
        for endpoint in self.scanner.discovered_endpoints:
            method_name = endpoint.method.name.lower()
            path = endpoint.path.replace("/", "_").lstrip("_")
            lines.append(
                f"    def {path}(self, **kwargs):"
            )
            lines.append(
                f"        return self.request('{method_name}', '{endpoint.path}')"
            )
            lines.append("")
        
        return "\n".join(lines)


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    discovery = ApiDiscovery("https://api.example.com")
    capability = discovery.discover(use_probing=False)
    
    print(f"Discovered {len(discovery.scanner.discovered_endpoints)} endpoints")
