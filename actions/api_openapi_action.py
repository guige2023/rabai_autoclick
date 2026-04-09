"""OpenAPI specification action module.

Handles OpenAPI spec parsing, validation, client generation,
and API documentation operations.
"""

from __future__ import annotations

import json
import re
import urllib.request
import urllib.error
from typing import Any, Optional, Dict, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SpecFormat(Enum):
    """OpenAPI specification format."""
    JSON = "json"
    YAML = "yaml"


@dataclass
class EndpointSpec:
    """Specification for an API endpoint."""
    path: str
    method: str
    operation_id: Optional[str]
    summary: Optional[str]
    description: Optional[str]
    parameters: List[Dict[str, Any]]
    request_body: Optional[Dict[str, Any]]
    responses: Dict[str, Any]
    security: List[Dict[str, List[str]]]


@dataclass
class OpenAPISpec:
    """Parsed OpenAPI specification."""
    version: str
    title: str
    description: Optional[str]
    version_number: str
    servers: List[Dict[str, Any]]
    endpoints: List[EndpointSpec]
    schemas: Dict[str, Any]
    security_schemes: Dict[str, Any]
    raw: Dict[str, Any]


class OpenAPIParser:
    """Parse and validate OpenAPI specifications."""

    def __init__(self, spec: Optional[Dict[str, Any]] = None):
        self._spec = spec or {}

    @classmethod
    def from_url(cls, url: str, timeout: int = 30) -> "OpenAPIParser":
        """Load OpenAPI spec from a URL."""
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                content = response.read()
                spec = json.loads(content.decode("utf-8"))
            logger.info(f"Loaded OpenAPI spec from {url}")
            return cls(spec)
        except (urllib.error.URLError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load spec from {url}: {e}")
            raise

    @classmethod
    def from_file(cls, path: str) -> "OpenAPIParser":
        """Load OpenAPI spec from a file."""
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        try:
            spec = json.loads(content)
        except json.JSONDecodeError:
            import yaml
            spec = yaml.safe_load(content)
        return cls(spec)

    def parse(self) -> OpenAPISpec:
        """Parse the specification into a structured object."""
        info = self._spec.get("info", {})
        servers = self._spec.get("servers", [{"url": "/"}])
        paths = self._spec.get("paths", {})
        components = self._spec.get("components", {})

        endpoints = []
        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method in ("get", "post", "put", "delete", "patch", "options", "head"):
                    endpoint = EndpointSpec(
                        path=path,
                        method=method.upper(),
                        operation_id=operation.get("operationId"),
                        summary=operation.get("summary"),
                        description=operation.get("description"),
                        parameters=operation.get("parameters", []),
                        request_body=operation.get("requestBody"),
                        responses=operation.get("responses", {}),
                        security=operation.get("security", []),
                    )
                    endpoints.append(endpoint)

        schemas = components.get("schemas", {})
        security_schemes = components.get("securitySchemes", {})

        return OpenAPISpec(
            version=self._spec.get("openapi", "3.0.0"),
            title=info.get("title", "API"),
            description=info.get("description"),
            version_number=info.get("version", "1.0.0"),
            servers=servers,
            endpoints=endpoints,
            schemas=schemas,
            security_schemes=security_schemes,
            raw=self._spec,
        )

    def validate(self) -> List[str]:
        """Validate the OpenAPI spec and return errors."""
        errors = []
        if "openapi" not in self._spec:
            errors.append("Missing 'openapi' version field")
        if "paths" not in self._spec:
            errors.append("Missing 'paths' field")
        if "info" not in self._spec:
            errors.append("Missing 'info' field")
        return errors


class OpenAPIClientGenerator:
    """Generate client code from OpenAPI spec."""

    def __init__(self, spec: OpenAPISpec):
        self.spec = spec

    def generate_python(self) -> str:
        """Generate Python client code."""
        lines = [
            "import requests",
            "",
            "",
            f"class {self.spec.title.replace(' ', '')}Client:",
            f"    def __init__(self, base_url: str, api_key: str = None):",
            f"        self.base_url = base_url.rstrip('/')",
            f"        self.api_key = api_key",
            "",
        ]
        for endpoint in self.spec.endpoints:
            method = endpoint.method.lower()
            op_id = endpoint.operation_id or f"{method}_{endpoint.path.replace('/', '_')}"
            lines.append(f"    def {op_id}(self, params=None):")
            lines.append(f'        """{endpoint.summary or ""}"""')
            lines.append(f"        url = f\"{self.base_url}{endpoint.path}\"")
            lines.append(f"        return requests.{method}(url, json=params)")
            lines.append("")
        return "\n".join(lines)


class OpenAPIDocGenerator:
    """Generate documentation from OpenAPI spec."""

    def __init__(self, spec: OpenAPISpec):
        self.spec = spec

    def generate_markdown(self) -> str:
        """Generate Markdown API documentation."""
        lines = [f"# {self.spec.title}", ""]
        if self.spec.description:
            lines.append(f"{self.spec.description}")
            lines.append("")
        lines.append(f"**Version:** {self.spec.version_number}")
        lines.append("")

        if self.spec.servers:
            lines.append("## Servers")
            for server in self.spec.servers:
                lines.append(f"- `{server.get('url', '/')}`")
            lines.append("")

        lines.append("## Endpoints")
        for endpoint in self.spec.endpoints:
            lines.append(f"### `{endpoint.method} {endpoint.path}`")
            if endpoint.summary:
                lines.append(f"**{endpoint.summary}**")
            if endpoint.description:
                lines.append(endpoint.description)
            if endpoint.parameters:
                lines.append("**Parameters:**")
                for param in endpoint.parameters:
                    lines.append(f"- `{param.get('name')}` ({param.get('in')}): {param.get('description', '')}")
            lines.append("")
        return "\n".join(lines)


def parse_openapi_spec(spec_dict: Dict[str, Any]) -> OpenAPISpec:
    """Parse an OpenAPI spec dictionary."""
    parser = OpenAPIParser(spec_dict)
    return parser.parse()


def validate_openapi_spec(spec_dict: Dict[str, Any]) -> List[str]:
    """Validate an OpenAPI spec and return errors."""
    parser = OpenAPIParser(spec_dict)
    return parser.validate()
