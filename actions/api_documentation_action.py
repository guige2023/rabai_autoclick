"""
API documentation generator module for auto-generating OpenAPI/Swagger specs.

Supports endpoint documentation, schema generation, and interactive API explorers.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ParamType(Enum):
    """Parameter types."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class ParamLocation(Enum):
    """Parameter locations."""
    QUERY = "query"
    PATH = "path"
    HEADER = "header"
    COOKIE = "cookie"


@dataclass
class Schema:
    """OpenAPI schema definition."""
    type: str
    format: Optional[str] = None
    description: Optional[str] = None
    enum: Optional[list] = None
    default: Optional[Any] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    items: Optional[Schema] = None
    properties: Optional[dict[str, Schema]] = None
    required: Optional[list[str]] = None


@dataclass
class Parameter:
    """API parameter definition."""
    name: str
    param_type: ParamType
    location: ParamLocation
    required: bool = False
    description: str = ""
    schema: Optional[Schema] = None
    example: Optional[Any] = None


@dataclass
class Endpoint:
    """API endpoint definition."""
    path: str
    method: str
    summary: str
    description: str = ""
    parameters: list[Parameter] = field(default_factory=list)
    request_body: Optional[Schema] = None
    responses: dict = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)
    deprecated: bool = False
    security: list[str] = field(default_factory=list)


@dataclass
class APISpec:
    """Complete API specification."""
    title: str
    version: str
    description: str = ""
    endpoints: list[Endpoint] = field(default_factory=list)
    schemas: dict[str, Schema] = field(default_factory=dict)
    servers: list[str] = field(default_factory=list)
    contact: dict = field(default_factory=dict)
    license: dict = field(default_factory=dict)


class APIDocumentationGenerator:
    """
    API documentation generator for auto-generating OpenAPI specs.

    Supports endpoint documentation, schema generation,
    and interactive API explorers.
    """

    def __init__(self, title: str = "API", version: str = "1.0.0"):
        self._spec = APISpec(title=title, version=version)
        self._endpoints: dict[str, Endpoint] = {}

    def set_info(
        self,
        description: str = "",
        servers: Optional[list[str]] = None,
        contact: Optional[dict] = None,
        license: Optional[dict] = None,
    ) -> None:
        """Set API metadata."""
        self._spec.description = description
        if servers:
            self._spec.servers = servers
        if contact:
            self._spec.contact = contact
        if license:
            self._spec.license = license

    def add_endpoint(
        self,
        path: str,
        method: str,
        summary: str,
        description: str = "",
        parameters: Optional[list[Parameter]] = None,
        request_body: Optional[Schema] = None,
        responses: Optional[dict] = None,
        tags: Optional[list[str]] = None,
        deprecated: bool = False,
        security: Optional[list[str]] = None,
    ) -> Endpoint:
        """Add an API endpoint."""
        endpoint = Endpoint(
            path=path,
            method=method.upper(),
            summary=summary,
            description=description,
            parameters=parameters or [],
            request_body=request_body,
            responses=responses or {"200": {"description": "Success"}},
            tags=tags or [],
            deprecated=deprecated,
            security=security or [],
        )

        key = f"{method.upper()}:{path}"
        self._endpoints[key] = endpoint
        self._spec.endpoints.append(endpoint)

        return endpoint

    def add_parameter(
        self,
        path: str,
        method: str,
        name: str,
        param_type: ParamType,
        location: ParamLocation,
        required: bool = False,
        description: str = "",
        schema: Optional[Schema] = None,
        example: Optional[Any] = None,
    ) -> None:
        """Add a parameter to an endpoint."""
        key = f"{method.upper()}:{path}"
        endpoint = self._endpoints.get(key)

        if not endpoint:
            raise ValueError(f"Endpoint not found: {key}")

        param = Parameter(
            name=name,
            param_type=param_type,
            location=location,
            required=required,
            description=description,
            schema=schema,
            example=example,
        )

        endpoint.parameters.append(param)

    def add_schema(
        self,
        name: str,
        schema: Schema,
    ) -> None:
        """Add a reusable schema."""
        self._spec.schemas[name] = schema

    def add_response(
        self,
        path: str,
        method: str,
        status_code: str,
        description: str,
        schema: Optional[Schema] = None,
    ) -> None:
        """Add a response to an endpoint."""
        key = f"{method.upper()}:{path}"
        endpoint = self._endpoints.get(key)

        if not endpoint:
            raise ValueError(f"Endpoint not found: {key}")

        response = {"description": description}
        if schema:
            response["content"] = {
                "application/json": {
                    "schema": self._schema_to_dict(schema)
                }
            }

        endpoint.responses[status_code] = response

    def _schema_to_dict(self, schema: Schema) -> dict:
        """Convert a Schema to a dictionary."""
        result = {"type": schema.type}

        if schema.format:
            result["format"] = schema.format
        if schema.description:
            result["description"] = schema.description
        if schema.enum:
            result["enum"] = schema.enum
        if schema.default is not None:
            result["default"] = schema.default
        if schema.minimum is not None:
            result["minimum"] = schema.minimum
        if schema.maximum is not None:
            result["maximum"] = schema.maximum
        if schema.min_length is not None:
            result["minLength"] = schema.min_length
        if schema.max_length is not None:
            result["maxLength"] = schema.max_length
        if schema.pattern:
            result["pattern"] = schema.pattern
        if schema.items:
            result["items"] = self._schema_to_dict(schema.items)
        if schema.properties:
            result["properties"] = {
                k: self._schema_to_dict(v)
                for k, v in schema.properties.items()
            }
        if schema.required:
            result["required"] = schema.required

        return result

    def _endpoint_to_dict(self, endpoint: Endpoint) -> dict:
        """Convert an Endpoint to a dictionary."""
        result = {
            "summary": endpoint.summary,
            "description": endpoint.description,
            "responses": endpoint.responses,
            "deprecated": endpoint.deprecated,
        }

        if endpoint.parameters:
            result["parameters"] = [
                {
                    "name": p.name,
                    "in": p.location.value,
                    "required": p.required,
                    "description": p.description,
                    "schema": self._schema_to_dict(p.schema) if p.schema else {"type": p.param_type.value},
                }
                for p in endpoint.parameters
            ]

        if endpoint.request_body:
            result["requestBody"] = {
                "content": {
                    "application/json": {
                        "schema": self._schema_to_dict(endpoint.request_body)
                    }
                }
            }

        if endpoint.tags:
            result["tags"] = endpoint.tags

        if endpoint.security:
            result["security"] = endpoint.security

        return result

    def generate_openapi(self) -> dict:
        """Generate an OpenAPI 3.0 specification."""
        paths = {}

        for endpoint in self._spec.endpoints:
            if endpoint.path not in paths:
                paths[endpoint.path] = {}

            paths[endpoint.path][endpoint.method.lower()] = self._endpoint_to_dict(endpoint)

        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": self._spec.title,
                "version": self._spec.version,
                "description": self._spec.description,
            },
            "paths": paths,
        }

        if self._spec.servers:
            spec["servers"] = [{"url": s} for s in self._spec.servers]

        if self._spec.schemas:
            spec["components"] = {
                "schemas": {
                    k: self._schema_to_dict(v)
                    for k, v in self._spec.schemas.items()
                }
            }

        if self._spec.contact:
            spec["info"]["contact"] = self._spec.contact

        if self._spec.license:
            spec["info"]["license"] = self._spec.license

        return spec

    def generate_swagger(self) -> dict:
        """Generate a Swagger 2.0 specification."""
        paths = {}

        for endpoint in self._spec.endpoints:
            if endpoint.path not in paths:
                paths[endpoint.path] = {}

            paths[endpoint.path][endpoint.method.lower()] = self._endpoint_to_dict(endpoint)

        spec = {
            "swagger": "2.0",
            "info": {
                "title": self._spec.title,
                "version": self._spec.version,
                "description": self._spec.description,
            },
            "paths": paths,
            "schemes": ["https"] if self._spec.servers else ["https"],
        }

        if self._spec.servers:
            spec["host"] = self._spec.servers[0].replace("https://", "").replace("http://", "").split("/")[0]

        if self._spec.schemas:
            spec["definitions"] = {
                k: self._schema_to_dict(v)
                for k, v in self._spec.schemas.items()
            }

        return spec

    def save_to_file(self, filename: str, format: str = "openapi") -> None:
        """Save the specification to a file."""
        if format == "openapi":
            spec = self.generate_openapi()
        else:
            spec = self.generate_swagger()

        with open(filename, "w") as f:
            json.dump(spec, f, indent=2)

    def get_spec(self) -> APISpec:
        """Get the API specification object."""
        return self._spec
