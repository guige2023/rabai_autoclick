"""API Documentation Action Module.

Provides API documentation generation, OpenAPI specification
building, and interactive documentation serving.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import re
from datetime import datetime


class ParameterType(Enum):
    """API parameter types."""
    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    COOKIE = "cookie"
    BODY = "body"


class ParameterStyle(Enum):
    """Parameter serialization styles."""
    DEEP_OBJECT = "deepObject"
    EXPLODE = "explode"
    FORM = "form"
    PIPE_DELIMITED = "pipeDelimited"
    SPACE_DELIMITED = "spaceDelimited"
    COMMA_DELIMITED = "commaDelimited"


@dataclass
class APIParameter:
    """API parameter definition."""
    name: str
    param_type: ParameterType
    schema: Dict[str, Any]
    description: str = ""
    required: bool = False
    deprecated: bool = False
    style: Optional[ParameterStyle] = None
    explode: bool = False
    example: Any = None

    def to_openapi(self) -> Dict[str, Any]:
        """Convert to OpenAPI parameter format."""
        result = {
            "name": self.name,
            "in": self.param_type.value,
            "description": self.description,
            "required": self.required,
            "deprecated": self.deprecated,
            "schema": self.schema,
        }
        if self.example is not None:
            result["example"] = self.example
        if self.style:
            result["style"] = self.style.value
            result["explode"] = self.explode
        return result


@dataclass
class APIRequestBody:
    """API request body definition."""
    content_type: str
    schema: Dict[str, Any]
    description: str = ""
    required: bool = True
    example: Any = None

    def to_openapi(self) -> Dict[str, Any]:
        """Convert to OpenAPI format."""
        content = {
            self.content_type: {
                "schema": self.schema,
            }
        }
        if self.example is not None:
            content[self.content_type]["example"] = self.example

        return {
            "description": self.description,
            "required": self.required,
            "content": content,
        }


@dataclass
class APIResponse:
    """API response definition."""
    status_code: int
    description: str
    schema: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None
    example: Any = None

    def to_openapi(self) -> Dict[str, Any]:
        """Convert to OpenAPI format."""
        content = {}
        if self.schema:
            content["application/json"] = {"schema": self.schema}
            if self.example is not None:
                content["application/json"]["example"] = self.example

        return {
            "description": self.description,
            "content": content if content else None,
        }


@dataclass
class APIEndpoint:
    """API endpoint definition."""
    path: str
    method: str
    operation_id: str
    summary: str = ""
    description: str = ""
    parameters: List[APIParameter] = field(default_factory=list)
    request_body: Optional[APIRequestBody] = None
    responses: Dict[str, APIResponse] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: Optional[List[Dict[str, List[str]]]] = None
    summary_i18n: Dict[str, str] = field(default_factory=dict)
    description_i18n: Dict[str, str] = field(default_factory=dict)

    def to_openapi(self) -> Dict[str, Any]:
        """Convert to OpenAPI Operation format."""
        result = {
            "operationId": self.operation_id,
            "summary": self.summary,
            "description": self.description,
            "tags": self.tags,
            "deprecated": self.deprecated,
        }

        if self.parameters:
            result["parameters"] = [
                p.to_openapi() for p in self.parameters
            ]

        if self.request_body:
            result["requestBody"] = self.request_body.to_openapi()

        if self.responses:
            result["responses"] = {
                str(code): resp.to_openapi()
                for code, resp in self.responses.items()
            }

        if self.security is not None:
            result["security"] = self.security

        return result


@dataclass
class APIServer:
    """API server configuration."""
    url: str
    description: str = ""
    variables: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class APISchema:
    """API schema component."""
    name: str
    schema_type: str
    properties: Dict[str, Any] = field(default_factory=dict)
    required: List[str] = field(default_factory=list)
    description: str = ""
    example: Any = None
    all_of: Optional[List[Dict[str, Any]]] = None
    any_of: Optional[List[Dict[str, Any]]] = None
    one_of: Optional[List[Dict[str, Any]]] = None

    def to_openapi(self) -> Dict[str, Any]:
        """Convert to OpenAPI schema format."""
        result: Dict[str, Any] = {
            "type": self.schema_type,
            "description": self.description,
        }

        if self.properties:
            result["properties"] = self.properties
        if self.required:
            result["required"] = self.required
        if self.example is not None:
            result["example"] = self.example
        if self.all_of:
            result["allOf"] = self.all_of
        if self.any_of:
            result["anyOf"] = self.any_of
        if self.one_of:
            result["oneOf"] = self.one_of

        return result


class OpenAPIBuilder:
    """Builds OpenAPI specifications."""

    def __init__(
        self,
        title: str,
        version: str,
        description: str = "",
    ):
        self.spec = {
            "openapi": "3.0.3",
            "info": {
                "title": title,
                "version": version,
                "description": description,
            },
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": {},
            },
        }
        self._servers: List[APIServer] = []

    def add_server(self, server: APIServer):
        """Add a server configuration."""
        self._servers.append(server)

    def add_endpoint(self, endpoint: APIEndpoint):
        """Add an API endpoint."""
        if endpoint.path not in self.spec["paths"]:
            self.spec["paths"][endpoint.path] = {}

        self.spec["paths"][endpoint.path][endpoint.method.lower()] = (
            endpoint.to_openapi()
        )

    def add_schema(self, schema: APISchema):
        """Add a schema component."""
        self.spec["components"]["schemas"][schema.name] = schema.to_openapi()

    def add_security_scheme(
        self,
        name: str,
        scheme_type: str,
        scheme: str = "",
        bearer_format: str = "",
        api_key_name: str = "",
        in_location: str = "",
    ):
        """Add a security scheme."""
        if scheme_type == "http":
            self.spec["components"]["securitySchemes"][name] = {
                "type": "http",
                "scheme": scheme,
                "bearerFormat": bearer_format,
            }
        elif scheme_type == "apiKey":
            self.spec["components"]["securitySchemes"][name] = {
                "type": "apiKey",
                "name": api_key_name,
                "in": in_location,
            }
        elif scheme_type == "oauth2":
            self.spec["components"]["securitySchemes"][name] = {
                "type": "oauth2",
            }

    def build(self) -> Dict[str, Any]:
        """Build the complete OpenAPI spec."""
        if self._servers:
            self.spec["servers"] = [
                {
                    "url": s.url,
                    "description": s.description,
                    "variables": s.variables,
                }
                for s in self._servers
            ]
        return self.spec

    def to_json(self, indent: int = 2) -> str:
        """Export as JSON string."""
        return json.dumps(self.build(), indent=indent, ensure_ascii=False)


class MarkdownDocGenerator:
    """Generates Markdown documentation from API specs."""

    def __init__(self, openapi_spec: Dict[str, Any]):
        self.spec = openapi_spec

    def generate(self) -> str:
        """Generate complete Markdown documentation."""
        sections = []

        sections.append(f"# {self.spec['info']['title']}\n")
        sections.append(f"**Version:** {self.spec['info']['version']}\n")

        if self.spec['info'].get('description'):
            sections.append(f"\n{self.spec['info']['description']}\n")

        servers = self.spec.get('servers', [])
        if servers:
            sections.append("\n## Servers\n\n")
            for server in servers:
                sections.append(f"- **{server['url']}**")
                if server.get('description'):
                    sections.append(f" - {server['description']}")
                sections.append("\n")

        if self.spec.get('components', {}).get('schemas'):
            sections.append("\n## Schemas\n\n")
            sections.append(self._generate_schemas())

        if self.spec.get('paths'):
            sections.append("\n## Endpoints\n\n")
            sections.append(self._generate_endpoints())

        return "".join(sections)

    def _generate_schemas(self) -> str:
        """Generate schemas documentation."""
        lines = []
        schemas = self.spec['components']['schemas']

        for name, schema in schemas.items():
            lines.append(f"### {name}\n")
            if schema.get('description'):
                lines.append(f"{schema['description']}\n")
            if 'properties' in schema:
                lines.append("\n| Field | Type | Description |\n")
                lines.append("|-------|------|-------------|\n")
                for field_name, field_schema in schema['properties'].items():
                    field_type = field_schema.get('type', 'any')
                    description = field_schema.get('description', '')
                    required = field_schema.get('required', False)
                    req_mark = " *(required)*" if required else ""
                    lines.append(f"| {field_name} | {field_type} | {description}{req_mark} |\n")
            lines.append("\n")
        return "".join(lines)

    def _generate_endpoints(self) -> str:
        """Generate endpoints documentation."""
        lines = []
        paths = self.spec['paths']

        for path, methods in paths.items():
            for method, operation in methods.items():
                lines.append(f"### {method.upper()} {path}\n")
                lines.append(f"**{operation.get('summary', '')}**\n\n")

                if operation.get('description'):
                    lines.append(f"{operation['description']}\n\n")

                params = operation.get('parameters', [])
                if params:
                    lines.append("**Parameters:**\n\n")
                    lines.append("| Name | In | Type | Required | Description |\n")
                    lines.append("|------|----|------|----------|-------------|\n")
                    for param in params:
                        lines.append(
                            f"| {param['name']} | {param['in']} | "
                            f"{param.get('schema', {}).get('type', 'any')} | "
                            f"{param.get('required', False)} | "
                            f"{param.get('description', '')} |\n"
                        )
                    lines.append("\n")

                if operation.get('requestBody'):
                    lines.append("**Request Body:**\n\n")
                    rb = operation['requestBody']
                    lines.append(f"{rb.get('description', '')}\n\n")

                responses = operation.get('responses', {})
                if responses:
                    lines.append("**Responses:**\n\n")
                    for code, resp in responses.items():
                        lines.append(f"- **{code}**: {resp.get('description', '')}\n")
                    lines.append("\n")

        return "".join(lines)


class APIDocumentationAction:
    """High-level API documentation action."""

    def __init__(self):
        self._endpoints: Dict[str, APIEndpoint] = {}
        self._schemas: Dict[str, APISchema] = {}
        self._servers: List[APIServer] = []

    def add_endpoint(
        self,
        path: str,
        method: str,
        operation_id: str,
        summary: str = "",
        description: str = "",
    ) -> APIEndpoint:
        """Add an endpoint definition."""
        endpoint = APIEndpoint(
            path=path,
            method=method,
            operation_id=operation_id,
            summary=summary,
            description=description,
        )
        key = f"{method.upper()}:{path}"
        self._endpoints[key] = endpoint
        return endpoint

    def add_schema(
        self,
        name: str,
        schema_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> APISchema:
        """Add a schema definition."""
        schema = APISchema(
            name=name,
            schema_type=schema_type,
            properties=properties or {},
        )
        self._schemas[name] = schema
        return schema

    def add_server(self, url: str, description: str = "") -> APIServer:
        """Add a server configuration."""
        server = APIServer(url=url, description=description)
        self._servers.append(server)
        return server

    def build_openapi(
        self,
        title: str,
        version: str,
        description: str = "",
    ) -> Dict[str, Any]:
        """Build OpenAPI specification."""
        builder = OpenAPIBuilder(title, version, description)

        for server in self._servers:
            builder.add_server(server)

        for schema in self._schemas.values():
            builder.add_schema(schema)

        for endpoint in self._endpoints.values():
            builder.add_endpoint(endpoint)

        return builder.build()

    def generate_markdown(self, title: str, version: str) -> str:
        """Generate Markdown documentation."""
        spec = self.build_openapi(title, version)
        generator = MarkdownDocGenerator(spec)
        return generator.generate()

    def export_json(self, title: str, version: str, indent: int = 2) -> str:
        """Export specification as JSON."""
        spec = self.build_openapi(title, version)
        return json.dumps(spec, indent=indent, ensure_ascii=False)


# Module exports
__all__ = [
    "APIDocumentationAction",
    "OpenAPIBuilder",
    "MarkdownDocGenerator",
    "APIEndpoint",
    "APIParameter",
    "APIRequestBody",
    "APIResponse",
    "APISchema",
    "APIServer",
    "ParameterType",
    "ParameterStyle",
]
