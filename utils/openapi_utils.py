"""
OpenAPI/Swagger utilities for API documentation and client generation.

Provides OpenAPI spec parsing, endpoint extraction, request/response
modeling, client generation, and validation utilities.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class HttpMethod(Enum):
    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    PATCH = "patch"
    HEAD = "head"
    OPTIONS = "options"


@dataclass
class Parameter:
    """OpenAPI parameter definition."""
    name: str
    location: str  # query, path, header, cookie
    schema: dict[str, Any]
    required: bool = False
    description: str = ""


@dataclass
class RequestBody:
    """OpenAPI request body definition."""
    content_type: str
    schema: dict[str, Any]
    required: bool = False
    description: str = ""


@dataclass
class Response:
    """OpenAPI response definition."""
    status_code: int
    description: str
    schema: Optional[dict[str, Any]] = None
    content_type: Optional[str] = None


@dataclass
class Endpoint:
    """Represents a single API endpoint."""
    path: str
    method: HttpMethod
    operation_id: Optional[str] = None
    summary: str = ""
    description: str = ""
    parameters: list[Parameter] = field(default_factory=list)
    request_body: Optional[RequestBody] = None
    responses: list[Response] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False


@dataclass
class OpenAPISpec:
    """Parsed OpenAPI specification."""
    title: str
    version: str
    description: str = ""
    endpoints: list[Endpoint] = field(default_factory=list)
    schemas: dict[str, dict[str, Any]] = field(default_factory=dict)
    servers: list[str] = field(default_factory=list)
    security_schemes: dict[str, dict[str, Any]] = field(default_factory=dict)


class OpenAPIParser:
    """Parses OpenAPI 3.x specifications."""

    @staticmethod
    def parse(spec: dict[str, Any] | str) -> OpenAPISpec:
        """Parse an OpenAPI spec from dict or JSON string."""
        if isinstance(spec, str):
            spec = json.loads(spec)

        info = spec.get("info", {})
        title = info.get("title", "API")
        version = info.get("version", "1.0.0")
        description = info.get("description", "")

        servers = [s.get("url", "") for s in spec.get("servers", [])]
        schemas = spec.get("components", {}).get("schemas", {})
        security_schemes = spec.get("components", {}).get("securitySchemes", {})

        endpoints = []
        for path, path_item in spec.get("paths", {}).items():
            for method_name, operation in path_item.items():
                if method_name.upper() not in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]:
                    continue

                try:
                    method = HttpMethod(method_name)
                except ValueError:
                    continue

                params = []
                for p in operation.get("parameters", []):
                    params.append(Parameter(
                        name=p.get("name", ""),
                        location=p.get("in", "query"),
                        schema=p.get("schema", {}),
                        required=p.get("required", False),
                        description=p.get("description", ""),
                    ))

                request_body = None
                rb = operation.get("requestBody", {})
                if rb:
                    content = rb.get("content", {})
                    ct = list(content.keys())[0] if content else "application/json"
                    schema_data = content.get(ct, {}).get("schema", {}) if ct in content else {}
                    request_body = RequestBody(
                        content_type=ct,
                        schema=schema_data,
                        required=rb.get("required", False),
                        description=rb.get("description", ""),
                    )

                responses = []
                for status_str, resp in operation.get("responses", {}).items():
                    status_code = int(status_str.replace("x", "")) if status_str != "default" else 500
                    content = resp.get("content", {})
                    ct = list(content.keys())[0] if content else None
                    schema_data = content.get(ct, {}).get("schema", {}) if ct in content else None
                    responses.append(Response(
                        status_code=status_code,
                        description=resp.get("description", ""),
                        schema=schema_data,
                        content_type=ct,
                    ))

                endpoints.append(Endpoint(
                    path=path,
                    method=method,
                    operation_id=operation.get("operationId"),
                    summary=operation.get("summary", ""),
                    description=operation.get("description", ""),
                    parameters=params,
                    request_body=request_body,
                    responses=responses,
                    tags=operation.get("tags", []),
                    deprecated=operation.get("deprecated", False),
                ))

        return OpenAPISpec(
            title=title,
            version=version,
            description=description,
            endpoints=endpoints,
            schemas=schemas,
            servers=servers,
            security_schemes=security_schemes,
        )

    @staticmethod
    def from_url(url: str) -> OpenAPISpec:
        """Fetch and parse OpenAPI spec from a URL."""
        import httpx
        response = httpx.get(url, timeout=30)
        response.raise_for_status()
        return OpenAPIParser.parse(response.json())

    @staticmethod
    def from_file(path: str) -> OpenAPISpec:
        """Load and parse OpenAPI spec from a file."""
        with open(path) as f:
            return OpenAPIParser.parse(json.load(f))


class OpenAPIClientGenerator:
    """Generates Python client code from an OpenAPI spec."""

    def __init__(self, spec: OpenAPISpec) -> None:
        self.spec = spec
        self._imports = ["import httpx", "from typing import Any, Optional", "from dataclasses import dataclass"]

    def generate(self) -> str:
        """Generate complete Python client code."""
        lines = []
        lines.extend(self._imports)
        lines.append("")
        lines.append(f"class {self._class_name(self.spec.title)}Client:")
        lines.append(f'    """Auto-generated client for {self.spec.title}."""')
        lines.append("")
        lines.append("    def __init__(self, base_url: str, api_key: Optional[str] = None) -> None:")
        lines.append("        self.base_url = base_url.rstrip('/')")
        lines.append("        self.api_key = api_key")
        lines.append("        self._client = httpx.Client(timeout=30.0)")
        lines.append("")

        for endpoint in self.spec.endpoints:
            lines.extend(self._generate_method(endpoint))

        lines.append("")
        lines.append("    def close(self) -> None:")
        lines.append("        self._client.close()")

        return "\n".join(lines)

    def _generate_method(self, endpoint: Endpoint) -> list[str]:
        """Generate a single method for an endpoint."""
        method_name = endpoint.operation_id or self._method_name(endpoint.path, endpoint.method)
        method_name = self._safe_name(method_name)

        lines = []
        params_str = self._format_params(endpoint.parameters)
        lines.append(f"    def {method_name}(self{params_str}) -> dict[str, Any]:")
        lines.append(f'        """{endpoint.summary or endpoint.description}"""')
        lines.append(f'        url = f"{{self.base_url}}{endpoint.path}"')

        if endpoint.request_body:
            lines.append("        payload = {")
            lines.append(f'            "content": content,')
            lines.append("        }")

        headers = self._generate_headers(endpoint)
        if headers:
            lines.append(f"        headers = {{{headers}}}")

        lines.append(f"        response = self._client.{endpoint.method.value}(url")
        if endpoint.request_body:
            lines.append("            json=payload,")
        lines.append("        )")
        lines.append("        response.raise_for_status()")
        lines.append("        return response.json()")
        lines.append("")
        return lines

    def _format_params(self, params: list[Parameter]) -> str:
        if not params:
            return ""
        parts = []
        for p in params:
            ptype = self._schema_to_type(p.schema)
            default = "" if p.required else " = None"
            parts.append(f"{p.name}: {ptype}{default}")
        return ", " + ", ".join(parts)

    def _generate_headers(self, endpoint: Endpoint) -> str:
        parts = []
        if endpoint.request_body:
            ct = endpoint.request_body.content_type
            parts.append(f'"Content-Type": "{ct}"')
        return ", ".join(parts)

    def _schema_to_type(self, schema: dict[str, Any]) -> str:
        t = schema.get("type", "string")
        if t == "integer":
            return "int"
        elif t == "number":
            return "float"
        elif t == "boolean":
            return "bool"
        elif t == "array":
            items = schema.get("items", {})
            inner = self._schema_to_type(items)
            return f"list[{inner}]"
        elif t == "object":
            return "dict[str, Any]"
        return "str"

    def _class_name(self, title: str) -> str:
        return "".join(w.capitalize() for w in title.replace("-", " ").split())

    def _method_name(self, path: str, method: HttpMethod) -> str:
        parts = path.strip("/").replace("/", "_").replace("-", "_").split("_")
        return f"{method.value}_{parts[0]}"

    def _safe_name(self, name: str) -> str:
        import re
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        if name[0].isdigit():
            name = "_" + name
        return name


class SchemaValidator:
    """Validates data against OpenAPI schemas."""

    def validate(self, data: Any, schema: dict[str, Any]) -> list[str]:
        """Validate data against a schema, returning list of errors."""
        errors = []
        self._validate_recursive(data, schema, "", errors)
        return errors

    def _validate_recursive(self, data: Any, schema: dict[str, Any], path: str, errors: list[str]) -> None:
        if data is None:
            if schema.get("required", False):
                errors.append(f"{path}: required field is missing")
            return

        schema_type = schema.get("type")
        if schema_type == "string" and not isinstance(data, str):
            errors.append(f"{path}: expected string, got {type(data).__name__}")
        elif schema_type == "integer" and not isinstance(data, int):
            errors.append(f"{path}: expected integer, got {type(data).__name__}")
        elif schema_type == "number" and not isinstance(data, (int, float)):
            errors.append(f"{path}: expected number, got {type(data).__name__}")
        elif schema_type == "boolean" and not isinstance(data, bool):
            errors.append(f"{path}: expected boolean, got {type(data).__name__}")
        elif schema_type == "array":
            if not isinstance(data, list):
                errors.append(f"{path}: expected array, got {type(data).__name__}")
            else:
                items_schema = schema.get("items", {})
                for i, item in enumerate(data):
                    self._validate_recursive(item, items_schema, f"{path}[{i}]", errors)
        elif schema_type == "object":
            if not isinstance(data, dict):
                errors.append(f"{path}: expected object, got {type(data).__name__}")
            else:
                required = schema.get("required", [])
                for req_field in required:
                    if req_field not in data:
                        errors.append(f"{path}.{req_field}: required field missing")
                props = schema.get("properties", {})
                for key, val in data.items():
                    if key in props:
                        self._validate_recursive(val, props[key], f"{path}.{key}", errors)
