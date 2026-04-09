"""
API Documentation Action Module.

Provides API documentation generation from code annotations,
OpenAPI/Swagger spec generation, and interactive documentation.

Author: RabAi Team
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type


class ParamType(Enum):
    """Parameter types."""
    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    BODY = "body"
    FORM = "form"


class HTTPMethod(Enum):
    """HTTP methods."""
    GET = "get"
    POST = "post"
    PUT = "put"
    PATCH = "patch"
    DELETE = "delete"
    OPTIONS = "options"
    HEAD = "head"


@dataclass
class APIEndpoint:
    """API endpoint definition."""
    path: str
    method: HTTPMethod
    summary: str
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: Optional[List[str]] = None


@dataclass
class Schema:
    """OpenAPI schema definition."""
    type: str = "object"
    properties: Dict[str, Any] = field(default_factory=dict)
    required: List[str] = field(default_factory=list)
    items: Optional[Dict[str, Any]] = None
    description: Optional[str] = None
    example: Optional[Any] = None


class DocGenerator:
    """OpenAPI documentation generator."""

    def __init__(self, title: str = "API", version: str = "1.0.0") -> None:
        self.title = title
        self.version = version
        self.endpoints: List[APIEndpoint] = []
        self.schemas: Dict[str, Schema] = {}
        self.tags: Dict[str, Dict[str, str]] = {}

    def add_tag(self, name: str, description: str) -> None:
        """Add tag for grouping endpoints."""
        self.tags[name] = {"name": name, "description": description}

    def add_endpoint(self, endpoint: APIEndpoint) -> None:
        """Add endpoint to documentation."""
        self.endpoints.append(endpoint)

    def register_schema(self, name: str, schema: Schema) -> None:
        """Register a schema."""
        self.schemas[name] = schema

    def document_function(
        self,
        func: Callable,
        path: str,
        method: HTTPMethod,
        **kwargs,
    ) -> APIEndpoint:
        """Document a function as API endpoint."""
        endpoint = APIEndpoint(
            path=path,
            method=method,
            summary=kwargs.get("summary", func.__name__),
            description=kwargs.get("description", func.__doc__),
            tags=kwargs.get("tags", []),
            deprecated=kwargs.get("deprecated", False),
            security=kwargs.get("security"),
        )

        # Extract parameters
        sig = inspect.signature(func)
        for param_name, param in sig.parameters.items():
            param_info = {
                "name": param_name,
                "required": param.default is inspect.Parameter.empty,
                "type": "string",  # Default type
            }

            if param.annotation != inspect.Parameter.empty:
                param_info["type"] = self._get_type_string(param.annotation)

            if param_name in kwargs.get("path_params", []):
                param_info["in"] = "path"
            elif param_name in kwargs.get("query_params", []):
                param_info["in"] = "query"
            elif param_name in kwargs.get("header_params", []):
                param_info["in"] = "header"

            endpoint.parameters.append(param_info)

        # Request body
        if kwargs.get("request_model"):
            endpoint.request_body = {
                "description": kwargs.get("request_description", ""),
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{kwargs['request_model']}"}
                    }
                },
            }

        # Responses
        endpoint.responses = kwargs.get("responses", {
            "200": {"description": "Successful response"},
            "400": {"description": "Bad request"},
            "401": {"description": "Unauthorized"},
            "500": {"description": "Internal server error"},
        })

        self.add_endpoint(endpoint)
        return endpoint

    def _get_type_string(self, annotation: Any) -> str:
        """Convert type annotation to string."""
        type_map = {
            str: "string",
            int: "integer",
            float: "number",
            bool: "boolean",
            list: "array",
            dict: "object",
        }

        if annotation in type_map:
            return type_map[annotation]

        # Handle Optional
        if hasattr(annotation, "__origin__"):
            if annotation.__origin__ is list:
                return "array"
            if annotation.__origin__ is dict:
                return "object"

        # Handle custom classes
        return annotation.__name__ if hasattr(annotation, "__name__") else "string"

    def generate_openapi(self) -> Dict[str, Any]:
        """Generate OpenAPI specification."""
        paths: Dict[str, Dict[str, Any]] = {}

        for endpoint in self.endpoints:
            path = endpoint.path
            if path not in paths:
                paths[path] = {}

            path_item = paths[path]
            method_str = endpoint.method.value

            path_item[method_str] = {
                "summary": endpoint.summary,
                "description": endpoint.description or "",
                "tags": endpoint.tags,
                "deprecated": endpoint.deprecated,
                "parameters": endpoint.parameters,
            }

            if endpoint.request_body:
                path_item[method_str]["requestBody"] = endpoint.request_body

            if endpoint.responses:
                path_item[method_str]["responses"] = endpoint.responses

            if endpoint.security:
                path_item[method_str]["security"] = [{"BearerAuth": []}]

        # Build schemas
        components = {}
        if self.schemas:
            schemas_dict = {}
            for name, schema in self.schemas.items():
                schemas_dict[name] = {
                    "type": schema.type,
                    "properties": schema.properties,
                    "required": schema.required,
                }
                if schema.description:
                    schemas_dict[name]["description"] = schema.description
                if schema.items:
                    schemas_dict[name]["items"] = schema.items
            components["schemas"] = schemas_dict

        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": self.title,
                "version": self.version,
            },
            "paths": paths,
        }

        if self.tags:
            spec["tags"] = list(self.tags.values())

        if components:
            spec["components"] = components

        return spec

    def to_json(self, indent: int = 2) -> str:
        """Export OpenAPI spec as JSON."""
        import json
        return json.dumps(self.generate_openapi(), indent=indent)


class EndpointBuilder:
    """Builder for constructing API endpoints."""

    def __init__(
        self,
        path: str,
        method: HTTPMethod,
        summary: str,
    ) -> None:
        self.endpoint = APIEndpoint(path=path, method=method, summary=summary)

    def with_description(self, description: str) -> "EndpointBuilder":
        self.endpoint.description = description
        return self

    def with_tags(self, tags: List[str]) -> "EndpointBuilder":
        self.endpoint.tags = tags
        return self

    def add_parameter(
        self,
        name: str,
        param_type: ParamType,
        required: bool = False,
        description: str = "",
        schema: Optional[Dict] = None,
    ) -> "EndpointBuilder":
        param = {
            "name": name,
            "in": param_type.value,
            "required": required,
            "description": description,
        }
        if schema:
            param["schema"] = schema
        self.endpoint.parameters.append(param)
        return self

    def add_response(
        self,
        status_code: str,
        description: str,
        schema: Optional[Dict] = None,
    ) -> "EndpointBuilder":
        response = {"description": description}
        if schema:
            response["content"] = {
                "application/json": {"schema": schema}
            }
        self.endpoint.responses[status_code] = response
        return self

    def with_request_body(
        self,
        description: str,
        schema: Dict[str, Any],
        required: bool = True,
    ) -> "EndpointBuilder":
        self.endpoint.request_body = {
            "description": description,
            "required": required,
            "content": {
                "application/json": {
                    "schema": schema
                }
            },
        }
        return self

    def build(self) -> APIEndpoint:
        return self.endpoint


def document_endpoint(
    path: str,
    method: HTTPMethod,
    summary: str,
) -> Callable:
    """Decorator for documenting endpoints."""
    def decorator(func: Callable) -> Callable:
        func._endpoint_doc = {
            "path": path,
            "method": method,
            "summary": summary,
        }
        return func
    return decorator
