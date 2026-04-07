"""
API Documentation Generator Utilities.

Provides utilities for auto-generating API documentation from code,
OpenAPI/Swagger specs, and maintaining documentation versions.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class HttpMethod(Enum):
    """HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ParamType(Enum):
    """Parameter types."""
    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    COOKIE = "cookie"
    BODY = "body"
    FORM = "form"


class ParamStyle(Enum):
    """Parameter serialization styles."""
    SIMPLE = "simple"
    FORM = "form"
    DEEP_OBJECT = "deepObject"
    PIPED_DELIMITED = "pipeDelimited"
    SPACE_DELIMITED = "spaceDelimited"


@dataclass
class APIParameter:
    """API endpoint parameter definition."""
    name: str
    param_type: ParamType
    data_type: str
    required: bool = False
    description: str = ""
    default: Optional[Any] = None
    enum_values: Optional[list[Any]] = None
    schema: Optional[dict[str, Any]] = None
    style: Optional[ParamStyle] = None
    explode: bool = False


@dataclass
class APIRequest:
    """API request body definition."""
    content_type: str
    schema: dict[str, Any]
    example: Optional[Any] = None
    description: str = ""


@dataclass
class APIResponse:
    """API response definition."""
    status_code: int
    description: str
    content_type: str = "application/json"
    schema: Optional[dict[str, Any]] = None
    example: Optional[Any] = None
    headers: Optional[dict[str, str]] = None


@dataclass
class APIEndpoint:
    """Single API endpoint definition."""
    path: str
    method: HttpMethod
    operation_id: str
    summary: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    parameters: list[APIParameter] = field(default_factory=list)
    request_body: Optional[APIRequest] = None
    responses: list[APIResponse] = field(default_factory=list)
    deprecated: bool = False
    security: Optional[list[str]] = None
    servers: Optional[list[str]] = None


@dataclass
class APISpec:
    """Complete API specification."""
    title: str
    version: str
    description: str = ""
    endpoints: list[APIEndpoint] = field(default_factory=list)
    servers: list[str] = field(default_factory=list)
    security_schemes: dict[str, dict[str, Any]] = field(default_factory=dict)
    tags: list[dict[str, str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)


class APIDocumentationGenerator:
    """Generator for API documentation from various sources."""

    def __init__(self) -> None:
        self.specs: dict[str, APISpec] = {}

    def generate_from_openapi(
        self,
        spec_data: dict[str, Any],
    ) -> APISpec:
        """Generate APISpec from OpenAPI 3.0 specification."""
        paths = spec_data.get("paths", {})
        endpoints: list[APIEndpoint] = []

        for path, path_item in paths.items():
            for method_str, operation in path_item.items():
                if method_str.upper() in [m.value for m in HttpMethod]:
                    method = HttpMethod(method_str.upper())

                    endpoint = self._parse_operation(path, method, operation, spec_data)
                    endpoints.append(endpoint)

        servers = []
        if "servers" in spec_data:
            servers = [s.get("url", "") for s in spec_data["servers"]]

        tags = []
        if "tags" in spec_data:
            tags = spec_data["tags"]

        security_schemes = {}
        if "components" in spec_data and "securitySchemes" in spec_data["components"]:
            security_schemes = spec_data["components"]["securitySchemes"]

        spec = APISpec(
            title=spec_data.get("info", {}).get("title", "API"),
            version=spec_data.get("info", {}).get("version", "1.0.0"),
            description=spec_data.get("info", {}).get("description", ""),
            endpoints=endpoints,
            servers=servers,
            security_schemes=security_schemes,
            tags=tags,
        )

        self.specs[spec.title] = spec
        return spec

    def _parse_operation(
        self,
        path: str,
        method: HttpMethod,
        operation: dict[str, Any],
        spec_data: dict[str, Any],
    ) -> APIEndpoint:
        """Parse a single operation from OpenAPI spec."""
        parameters: list[APIParameter] = []

        if "parameters" in operation:
            for param in operation["parameters"]:
                api_param = APIParameter(
                    name=param.get("name", ""),
                    param_type=ParamType(param.get("in", "query")),
                    data_type=param.get("schema", {}).get("type", "string"),
                    required=param.get("required", False),
                    description=param.get("description", ""),
                    enum_values=param.get("schema", {}).get("enum"),
                )
                parameters.append(api_param)

        request_body = None
        if "requestBody" in operation:
            rb = operation["requestBody"]
            content = rb.get("content", {})
            if "application/json" in content:
                json_content = content["application/json"]
                request_body = APIRequest(
                    content_type="application/json",
                    schema=json_content.get("schema", {}),
                    example=json_content.get("example"),
                    description=rb.get("description", ""),
                )

        responses: list[APIResponse] = []
        if "responses" in operation:
            for status_code, response in operation["responses"].items():
                content = response.get("content", {})
                response_schema = None
                response_example = None
                response_content_type = "application/json"

                if "application/json" in content:
                    json_content = content["application/json"]
                    response_schema = json_content.get("schema")
                    response_example = json_content.get("example")

                api_response = APIResponse(
                    status_code=int(status_code.replace("x", "")) if status_code[0].isdigit() else 500,
                    description=response.get("description", ""),
                    content_type=response_content_type,
                    schema=response_schema,
                    example=response_example,
                )
                responses.append(api_response)

        tags = operation.get("tags", [])

        return APIEndpoint(
            path=path,
            method=method,
            operation_id=operation.get("operationId", f"{method.value}_{path}"),
            summary=operation.get("summary", ""),
            description=operation.get("description", ""),
            tags=tags,
            parameters=parameters,
            request_body=request_body,
            responses=responses,
            deprecated=operation.get("deprecated", False),
            security=operation.get("security"),
        )

    def generate_markdown(
        self,
        spec: APISpec,
        include_examples: bool = True,
    ) -> str:
        """Generate Markdown documentation from APISpec."""
        lines: list[str] = []

        lines.append(f"# {spec.title}")
        lines.append(f"\n**Version:** {spec.version}\n")
        lines.append(f"{spec.description}\n")

        if spec.servers:
            lines.append("## Servers\n")
            for server in spec.servers:
                lines.append(f"- {server}")
            lines.append("")

        if spec.security_schemes:
            lines.append("## Authentication\n")
            for name, scheme in spec.security_schemes.items():
                lines.append(f"### {name}")
                lines.append(f"- **Type:** {scheme.get('type', 'unknown')}")
                if "scheme" in scheme:
                    lines.append(f"- **Scheme:** {scheme['scheme']}")
                lines.append("")
            lines.append("")

        tags_dict: dict[str, list[APIEndpoint]] = {}
        for endpoint in spec.endpoints:
            for tag in endpoint.tags:
                if tag not in tags_dict:
                    tags_dict[tag] = []
                tags_dict[tag].append(endpoint)

        for tag, endpoints in tags_dict.items():
            lines.append(f"## {tag}\n")

            for endpoint in endpoints:
                lines.append(self._format_endpoint_markdown(endpoint, include_examples))
                lines.append("")

        return "\n".join(lines)

    def _format_endpoint_markdown(
        self,
        endpoint: APIEndpoint,
        include_examples: bool,
    ) -> str:
        """Format a single endpoint as Markdown."""
        lines: list[str] = []

        method_badge = f"`{endpoint.method.value}`"
        lines.append(f"### {method_badge} {endpoint.path}\n")
        lines.append(f"**Operation ID:** `{endpoint.operation_id}`\n")

        if endpoint.summary:
            lines.append(f"**Summary:** {endpoint.summary}\n")

        if endpoint.deprecated:
            lines.append("> **⚠️ DEPRECATED**\n")

        if endpoint.description:
            lines.append(f"{endpoint.description}\n")

        if endpoint.parameters:
            lines.append("**Parameters:**\n")
            lines.append("| Name | Location | Type | Required | Description |")
            lines.append("|------|----------|------|----------|-------------|")

            for param in endpoint.parameters:
                req = "Yes" if param.required else "No"
                enum_str = f" ({', '.join(str(v) for v in param.enum_values)})" if param.enum_values else ""
                lines.append(f"| {param.name} | {param.param_type.value} | {param.data_type}{enum_str} | {req} | {param.description} |")

            lines.append("")

        if endpoint.request_body:
            rb = endpoint.request_body
            lines.append(f"**Request Body ({rb.content_type}):**\n")
            lines.append(f"{rb.description}\n")

            if include_examples and rb.example:
                lines.append("```json")
                lines.append(json.dumps(rb.example, indent=2))
                lines.append("```\n")

        if endpoint.responses:
            lines.append("**Responses:**\n")

            for response in endpoint.responses:
                lines.append(f"- `{response.status_code}`: {response.description}")

                if include_examples and response.example:
                    lines.append("  ```json")
                    lines.append(json.dumps(response.example, indent=2))
                    lines.append("  ```")

            lines.append("")

        return "\n".join(lines)

    def generate_postman_collection(
        self,
        spec: APISpec,
        variable_defaults: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        """Generate Postman Collection format from APISpec."""
        variable_defaults = variable_defaults or {}

        collection: dict[str, Any] = {
            "info": {
                "name": spec.title,
                "description": spec.description,
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
            },
            "variable": [
                {"key": k, "value": v} for k, v in variable_defaults.items()
            ],
            "item": [],
        }

        endpoints_by_tag: dict[str, list[dict[str, Any]]] = {}

        for endpoint in spec.endpoints:
            item = self._endpoint_to_postman_item(endpoint, spec.servers)

            tag = endpoint.tags[0] if endpoint.tags else "Default"
            if tag not in endpoints_by_tag:
                endpoints_by_tag[tag] = []
            endpoints_by_tag[tag].append(item)

        for tag, items in endpoints_by_tag.items():
            collection["item"].append({
                "name": tag,
                "item": items,
            })

        return collection

    def _endpoint_to_postman_item(
        self,
        endpoint: APIEndpoint,
        servers: list[str],
    ) -> dict[str, Any]:
        """Convert an endpoint to Postman item format."""
        base_url = servers[0] if servers else "{{base_url}}"

        item: dict[str, Any] = {
            "name": f"{endpoint.method.value} {endpoint.path}",
            "request": {
                "method": endpoint.method.value,
                "url": {
                    "raw": f"{base_url}{endpoint.path}",
                    "host": [base_url.replace("https://", "").replace("http://", "")],
                    "path": endpoint.path.strip("/").split("/"),
                },
                "description": endpoint.description,
            },
            "response": [],
        }

        url = item["request"]["url"]

        path_params = [p for p in endpoint.parameters if p.param_type == ParamType.PATH]
        for param in path_params:
            url["path"] = [p.replace(f"{{{param.name}}}", f":{param.name}") if p == f"{{{param.name}}}" else p for p in url["path"]]

        query_params = [p for p in endpoint.parameters if p.param_type == ParamType.QUERY]
        if query_params:
            url["query"] = [
                {
                    "key": p.name,
                    "value": str(p.default) if p.default is not None else "",
                    "description": p.description,
                    "disabled": not p.required,
                }
                for p in query_params
            ]

        header_params = [p for p in endpoint.parameters if p.param_type == ParamType.HEADER]
        if header_params:
            item["request"]["header"] = [
                {
                    "key": p.name,
                    "value": str(p.default) if p.default is not None else "",
                    "description": p.description,
                }
                for p in header_params
            ]

        if endpoint.request_body:
            item["request"]["body"] = {
                "mode": "raw",
                "raw": json.dumps(endpoint.request_body.schema, indent=2) if endpoint.request_body.schema else "",
            }

        return item

    def validate_spec(
        self,
        spec: APISpec,
    ) -> list[str]:
        """Validate an API spec and return list of issues."""
        issues: list[str] = []

        if not spec.title:
            issues.append("Missing API title")

        if not spec.version:
            issues.append("Missing API version")

        endpoint_ids = set()
        for endpoint in spec.endpoints:
            if endpoint.operation_id in endpoint_ids:
                issues.append(f"Duplicate operationId: {endpoint.operation_id}")
            endpoint_ids.add(endpoint.operation_id)

            if not endpoint.path.startswith("/"):
                issues.append(f"Invalid path format: {endpoint.path}")

            if not endpoint.responses:
                issues.append(f"Endpoint {endpoint.path} has no responses defined")

            for param in endpoint.parameters:
                if param.param_type == ParamType.PATH and not param.required:
                    issues.append(f"Path parameter {param.name} should be required")

        return issues

    def generate_sdk_stub(
        self,
        spec: APISpec,
        language: str = "python",
    ) -> str:
        """Generate SDK stub code for the API."""
        if language == "python":
            return self._generate_python_stub(spec)
        elif language == "typescript":
            return self._generate_typescript_stub(spec)
        else:
            return f"# SDK generation for {language} not implemented"

    def _generate_python_stub(self, spec: APISpec) -> str:
        """Generate Python SDK stub."""
        lines = [
            '"""',
            f"{spec.title} SDK",
            f"Auto-generated from API spec v{spec.version}",
            '"""',
            "",
            "import requests",
            "from typing import Any, Optional, Dict, List",
            "",
            "",
            f"class {spec.title.replace(' ', '')}Client:",
            f'    """{spec.description}"""',
            "",
            f"    def __init__(self, base_url: str, api_key: Optional[str] = None):",
            '        """Initialize the client."""',
            "        self.base_url = base_url.rstrip('/')",
            "        self.api_key = api_key",
            "        self.session = requests.Session()",
            "        if api_key:",
            '            self.session.headers.update({"Authorization": f"Bearer {api_key}"})',
            "",
        ]

        for endpoint in spec.endpoints:
            func_name = re.sub(r'[^\w]', '_', endpoint.operation_id).lower()

            params = [p for p in endpoint.parameters if p.param_type != ParamType.HEADER]
            header_params = [p for p in endpoint.parameters if p.param_type == ParamType.HEADER]

            param_str = ", ".join([f"{p.name}: {self._python_type(p.data_type)}" for p in params])
            if header_params:
                if param_str:
                    param_str += ", "
                param_str += f"headers: Optional[Dict[str, str]] = None"

            lines.append(f"    def {func_name}(")
            lines.append(f"        self, {param_str}")
            lines.append(f"    ) -> Dict[str, Any]:")
            lines.append(f'        """{endpoint.summary or endpoint.description}"""')

            method_lower = endpoint.method.value.lower()
            url = f"f'{self.base_url}{endpoint.path}'"

            if params:
                path_params = [p for p in params if p.param_type == ParamType.PATH]
                query_params = [p for p in params if p.param_type == ParamType.QUERY]
                body_params = [p for p in params if p.param_type == ParamType.BODY]

                if path_params:
                    url = f"f'{self.base_url}{endpoint.path}'"
                    for p in path_params:
                        url = url.replace(f"{{{p.name}}}", f"{{{p.name}}}")

                if query_params:
                    lines.append(f"        params = {{{', '.join(f\"'{p.name}': {p.name}\" for p in query_params)}}}")
                    lines.append(f"        response = self.session.{method_lower}({url}, params=params)")
                else:
                    lines.append(f"        response = self.session.{method_lower}({url})")
            else:
                lines.append(f"        response = self.session.{method_lower}({url})")

            lines.append(f"        response.raise_for_status()")
            lines.append(f"        return response.json()")
            lines.append("")

        return "\n".join(lines)

    def _generate_typescript_stub(self, spec: APISpec) -> str:
        """Generate TypeScript SDK stub."""
        lines = [
            f"// {spec.title} SDK",
            f"// Auto-generated from API spec v{spec.version}",
            "",
            f"export class {spec.title.replace(' ', '')}Client {{",
            "  private baseUrl: string;",
            "  private apiKey?: string;",
            "",
            "  constructor(baseUrl: string, apiKey?: string) {",
            "    this.baseUrl = baseUrl.replace(/\\/$/, '');",
            "    this.apiKey = apiKey;",
            "  }",
            "",
        ]

        for endpoint in spec.endpoints:
            func_name = re.sub(r'[^\w]', '_', endpoint.operation_id).replace('__', '_').lower()

            params = [p for p in endpoint.parameters if p.param_type != ParamType.HEADER]
            header_params = [p for p in endpoint.parameters if p.param_type == ParamType.HEADER]

            param_parts = [f"{p.name}: {self._typescript_type(p.data_type)}" for p in params]
            if header_params:
                param_parts.append("headers?: Record<string, string>")

            param_str = ", ".join(param_parts)

            lines.append(f"  async {func_name}({param_str}): Promise<any> {{")
            lines.append(f'    // {endpoint.summary or endpoint.description}')
            lines.append(f"    const url = `${this.baseUrl}{endpoint.path}`;")

            method_lower = endpoint.method.value.lower()
            lines.append(f"    const response = await fetch(url, {{")
            lines.append(f"      method: '{endpoint.method.value}',")
            lines.append(f"    }});")
            lines.append(f"    return response.json();")
            lines.append(f"  }}")
            lines.append("")

        lines.append("}")
        return "\n".join(lines)

    def _python_type(self, data_type: str) -> str:
        """Map OpenAPI type to Python type."""
        type_map = {
            "string": "str",
            "integer": "int",
            "number": "float",
            "boolean": "bool",
            "array": "List[Any]",
            "object": "Dict[str, Any]",
        }
        return type_map.get(data_type, "Any")

    def _typescript_type(self, data_type: str) -> str:
        """Map OpenAPI type to TypeScript type."""
        type_map = {
            "string": "string",
            "integer": "number",
            "number": "number",
            "boolean": "boolean",
            "array": "any[]",
            "object": "Record<string, any>",
        }
        return type_map.get(data_type, "any")
