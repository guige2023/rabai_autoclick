"""
API Discovery and Introspection Module.

Provides runtime API endpoint discovery, schema introspection,
and capability detection for dynamic API clients.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    """HTTP methods supported by API endpoints."""
    GET = auto()
    POST = auto()
    PUT = auto()
    PATCH = auto()
    DELETE = auto()
    HEAD = auto()
    OPTIONS = auto()


@dataclass(frozen=True)
class EndpointInfo:
    """Immutable description of a single API endpoint."""
    path: str
    method: HttpMethod
    summary: str = ""
    description: str = ""
    parameters: Tuple[Tuple[str, str, bool], ...] = field(default_factory=tuple)
    request_body: Optional[str] = None
    responses: FrozenSet[str] = field(default_factory=frozenset)
    deprecated: bool = False
    tags: FrozenSet[str] = field(default_factory=frozenset)
    security: FrozenSet[str] = field(default_factory=frozenset)

    def param_names(self) -> List[str]:
        return [p[0] for p in self.parameters]


@dataclass
class ApiCapability:
    """Represents a discovered API capability."""
    name: str
    version: str
    endpoints: List[EndpointInfo] = field(default_factory=list)
    discovered_at: datetime = field(default_factory=datetime.utcnow)
    last_verified: Optional[datetime] = None
    is_available: bool = True
    latency_ms: Optional[float] = None


class OpenApiSchemaParser:
    """Parses OpenAPI 3.x schemas to extract endpoint information."""

    METHOD_MAP = {
        "get": HttpMethod.GET,
        "post": HttpMethod.POST,
        "put": HttpMethod.PUT,
        "patch": HttpMethod.PATCH,
        "delete": HttpMethod.DELETE,
        "head": HttpMethod.HEAD,
        "options": HttpMethod.OPTIONS,
    }

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self._errors: List[str] = []

    def parse(self) -> List[EndpointInfo]:
        endpoints: List[EndpointInfo] = []
        paths = self.schema.get("paths", {})

        for path, path_item in paths.items():
            for method_str, operation in path_item.items():
                if method_str not in self.METHOD_MAP:
                    continue

                try:
                    endpoint = self._parse_operation(path, method_str, operation)
                    if endpoint:
                        endpoints.append(endpoint)
                except Exception as exc:
                    self._errors.append(f"Error parsing {method_str.upper()} {path}: {exc}")
                    logger.debug("Parse error: %s", exc)

        return endpoints

    def _parse_operation(
        self, path: str, method: str, operation: Dict[str, Any]
    ) -> Optional[EndpointInfo]:
        method_enum = self.METHOD_MAP.get(method)
        if not method_enum:
            return None

        summary = operation.get("summary", "")
        description = operation.get("description", "")
        deprecated = operation.get("deprecated", False)

        tags = frozenset(operation.get("tags", []))
        security = frozenset(operation.get("security", []))

        responses = frozenset(
            str(code) for code in operation.get("responses", {}).keys()
        )

        params: List[Tuple[str, str, bool]] = []
        for param in operation.get("parameters", []):
            name = param.get("name", "")
            ptype = param.get("type", "string")
            required = param.get("required", False)
            params.append((name, ptype, required))

        request_body = None
        rb = operation.get("requestBody", {})
        if rb:
            content = rb.get("content", {})
            if "application/json" in content:
                schema = content["application/json"].get("schema", {})
                request_body = json.dumps(schema, separators=(",", ":"))

        return EndpointInfo(
            path=path,
            method=method_enum,
            summary=summary,
            description=description,
            parameters=tuple(params),
            request_body=request_body,
            responses=responses,
            deprecated=deprecated,
            tags=tags,
            security=security,
        )

    def errors(self) -> List[str]:
        return list(self._errors)


class ApiIntrospector:
    """
    Runtime API introspection client.

    Discovers API capabilities by querying introspection endpoints
    and parsing response headers/metadata.
    """

    def __init__(self, base_url: str, auth_token: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self._capabilities: Dict[str, ApiCapability] = {}
        self._endpoint_cache: Dict[str, List[EndpointInfo]] = {}

    async def discover_openapi(self, spec_url: str) -> Optional[ApiCapability]:
        """
        Fetch and parse an OpenAPI specification document.

        Args:
            spec_url: URL to the OpenAPI JSON/YAML spec

        Returns:
            ApiCapability with discovered endpoints, or None on failure
        """
        import aiohttp

        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(spec_url, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning("OpenAPI spec fetch failed: %s", resp.status)
                        return None

                    spec = await resp.json()
                    version = spec.get("info", {}).get("version", "unknown")
                    title = spec.get("info", {}).get("title", "API")

                    parser = OpenApiSchemaParser(spec)
                    endpoints = parser.parse()

                    capability = ApiCapability(
                        name=title,
                        version=version,
                        endpoints=endpoints,
                    )
                    self._capabilities[title] = capability
                    self._endpoint_cache[spec_url] = endpoints

                    logger.info(
                        "Discovered %d endpoints from %s v%s",
                        len(endpoints), title, version,
                    )
                    return capability

        except Exception as exc:
            logger.error("Failed to discover OpenAPI: %s", exc)
            return None

    async def probe_endpoints(
        self, endpoints: List[EndpointInfo], timeout: float = 5.0
    ) -> Dict[str, bool]:
        """
        Probe a list of endpoints to check availability.

        Returns dict mapping endpoint path+method to availability bool.
        """
        import aiohttp

        results: Dict[str, bool] = {}
        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        sem = asyncio.Semaphore(5)

        async def probe(ep: EndpointInfo) -> Tuple[str, bool]:
            async with sem:
                url = f"{self.base_url}{ep.path}"
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.request(
                            ep.method.name,
                            url,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=timeout),
                            allow_redirects=False,
                        ) as resp:
                            return (f"{ep.method.name} {ep.path}", resp.status < 500)
                except Exception:
                    return (f"{ep.method.name} {ep.path}", False)

        probes = await asyncio.gather(*[probe(ep) for ep in endpoints])
        results = dict(probes)
        return results

    def find_endpoint(
        self, capability_name: str, path: str, method: Optional[str] = None
    ) -> Optional[EndpointInfo]:
        """Find an endpoint by path, optionally filtering by method."""
        endpoints = self._endpoint_cache.get(capability_name, [])
        for ep in endpoints:
            if ep.path == path:
                if method is None or ep.method.name == method.upper():
                    return ep
        return None

    def match_by_tag(self, capability_name: str, tag: str) -> List[EndpointInfo]:
        """Return all endpoints with a given tag."""
        endpoints = self._endpoint_cache.get(capability_name, [])
        return [ep for ep in endpoints if tag in ep.tags]

    def generate_client_code(
        self, capability_name: str, language: str = "python"
    ) -> str:
        """Generate simple client code stub for the discovered API."""
        endpoints = self._endpoint_cache.get(capability_name, [])

        if language == "python":
            return self._generate_python_client(capability_name, endpoints)
        elif language == "typescript":
            return self._generate_typescript_client(capability_name, endpoints)
        else:
            raise ValueError(f"Unsupported language: {language}")

    def _generate_python_client(
        self, name: str, endpoints: List[EndpointInfo]
    ) -> str:
        lines = [
            f'"""Auto-generated client for {name}."""',
            "import requests",
            "",
            f'BASE_URL = "{self.base_url}"',
            "",
            "class APIClient:",
            "    def __init__(self, token: str):",
            "        self.token = token",
            "        self.headers = {\"Authorization\": f\"Bearer {token}\"}",
            "",
        ]

        for ep in endpoints:
            safe_name = re.sub(r"[^a-zA-Z0-9]", "_", ep.summary or ep.path)
            lines.append(
                f'    def {safe_name}(self, **kwargs):'
            )
            params_str = ", ".join([f'{p[0]}: {p[1]}' for p in ep.parameters])
            if params_str:
                lines.append(f"        \"\"\"{ep.summary} ({params_str})\"\"\"")
            else:
                lines.append(f'        """{ep.summary}"""')
            lines.append(
                f'        url = f"{{self.BASE_URL}}{ep.path}"'
            )
            lines.append(
                f'        return requests.{ep.method.name.lower()}(url, headers=self.headers, **kwargs)'
            )
            lines.append("")

        return "\n".join(lines)

    def _generate_typescript_client(
        self, name: str, endpoints: List[EndpointInfo]
    ) -> str:
        lines = [
            f"// Auto-generated client for {name}",
            f"const BASE_URL = '{self.base_url}';",
            "",
            f"class {name.replace(' ', '')}Client {{",
            "  constructor(private token: string) {{}}",
            "",
            "  private headers() {{",
            "    return {{ 'Authorization': `Bearer ${{this.token}}` }};",
            "  }}",
            "",
        ]

        for ep in endpoints:
            safe_name = re.sub(r"[^a-zA-Z0-9]", "_", ep.summary or ep.path)
            lines.append(
                f'  async {safe_name}(params: Record<string, any> = {{}}): Promise<any> {{'
            )
            lines.append(f'    const url = `${{BASE_URL}}{ep.path}`;')
            method_lower = ep.method.name.lower()
            lines.append(
                f'    const resp = await fetch(url, {{ method: "{method_lower.upper()}", headers: this.headers() }});'
            )
            lines.append("    return resp.json();")
            lines.append("  }")
            lines.append("")

        lines.append("}")
        return "\n".join(lines)


class ApiSchemaCache:
    """Caches API schemas with content-addressed storage."""

    def __init__(self, cache_dir: str = "/tmp/api_cache"):
        import os
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _content_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def put(self, key: str, schema: Dict[str, Any], ttl_seconds: int = 3600) -> str:
        content = json.dumps(schema, separators=(",", ":"))
        content_hash = self._content_hash(content)
        cache_file = f"{self.cache_dir}/{key}_{content_hash}.json"
        with open(cache_file, "w") as f:
            f.write(content)
        expiry_file = f"{cache_dir}/{key}_expiry.txt"
        with open(expiry_file, "a") as f:
            f.write(f"{content_hash},{datetime.utcnow().isoformat()}\n")
        return content_hash

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        import os, time
        expiry_file = f"{self.cache_dir}/{key}_expiry.txt"
        if not os.path.exists(expiry_file):
            return None
        with open(expiry_file) as f:
            for line in f:
                content_hash, iso_time = line.strip().split(",")
                cached_at = datetime.fromisoformat(iso_time)
                if datetime.utcnow() - cached_at < timedelta(seconds=3600):
                    cache_file = f"{self.cache_dir}/{key}_{content_hash}.json"
                    if os.path.exists(cache_file):
                        with open(cache_file) as cf:
                            return json.load(cf)
        return None


def create_discovery_client(
    base_url: str,
    auth_token: Optional[str] = None,
    schema_cache_dir: Optional[str] = None,
) -> ApiIntrospector:
    """Factory to create a configured API discovery client."""
    client = ApiIntrospector(base_url, auth_token)
    if schema_cache_dir:
        client._cache = ApiSchemaCache(schema_cache_dir)
    return client
