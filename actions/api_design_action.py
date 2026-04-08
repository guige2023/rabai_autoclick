"""API Design Action Module.

Provides intelligent API design assistance including endpoint
generation, schema design, and best practices enforcement.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HttpMethod(Enum):
    """Standard HTTP methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


class ParamLocation(Enum):
    """Parameter location in request."""
    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    COOKIE = "cookie"
    BODY = "body"


class ParamType(Enum):
    """Parameter data types."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    FILE = "file"


@dataclass
class EndpointParameter:
    """API endpoint parameter definition."""
    name: str
    location: ParamLocation
    param_type: ParamType
    required: bool = False
    description: str = ""
    default: Any = None
    enum_values: List[Any] = field(default_factory=list)
    schema: Optional[Dict[str, Any]] = None


@dataclass
class Endpoint:
    """API endpoint definition."""
    path: str
    method: HttpMethod
    operation_id: str
    summary: str = ""
    description: str = ""
    parameters: List[EndpointParameter] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: List[Dict[str, List[str]]] = field(default_factory=list)


@dataclass
class APISchema:
    """API schema definition."""
    name: str
    schema_type: str
    properties: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    required_fields: List[str] = field(default_factory=list)
    description: str = ""
    example: Optional[Dict[str, Any]] = None


class APIDesignAction(BaseAction):
    """
    Intelligent API design assistance.

    Helps design REST APIs with proper endpoints, parameters,
    schemas, and follows best practices.

    Example:
        designer = APIDesignAction()
        result = designer.execute(ctx, {
            "action": "generate_crud",
            "resource": "users"
        })
    """
    action_type = "api_design"
    display_name = "API设计助手"
    description = "智能API设计辅助，包括端点生成、模式设计和最佳实践"

    CRUD_MAPPING = {
        "create": HttpMethod.POST,
        "read": HttpMethod.GET,
        "update": HttpMethod.PUT,
        "patch": HttpMethod.PATCH,
        "delete": HttpMethod.DELETE,
        "list": HttpMethod.GET,
    }

    def __init__(self) -> None:
        super().__init__()
        self._generated_endpoints: List[Endpoint] = []
        self._generated_schemas: Dict[str, APISchema] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an API design action.

        Args:
            context: Execution context.
            params: Dict with keys: action, resource, fields, etc.

        Returns:
            ActionResult with design output.
        """
        action = params.get("action", "")

        try:
            if action == "generate_crud":
                return self._generate_crud_endpoints(params)
            elif action == "generate_endpoint":
                return self._generate_single_endpoint(params)
            elif action == "generate_schema":
                return self._generate_schema(params)
            elif action == "add_parameter":
                return self._add_parameter_to_endpoint(params)
            elif action == "add_response":
                return self._add_response(params)
            elif action == "validate_design":
                return self._validate_design(params)
            elif action == "get_design":
                return self._get_generated_design(params)
            elif action == "suggest_improvements":
                return self._suggest_improvements(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Design error: {str(e)}")

    def _generate_crud_endpoints(self, params: Dict[str, Any]) -> ActionResult:
        """Generate CRUD endpoints for a resource."""
        resource = params.get("resource", "")
        fields = params.get("fields", {})
        base_path = params.get("base_path", "")
        options = params.get("options", {})

        if not resource:
            return ActionResult(success=False, message="resource is required")

        resource_lower = resource.lower().replace(" ", "_")
        resource_singular = options.get("singular", resource_lower.rstrip("s"))
        base_path = base_path or f"/{resource_lower}"

        endpoints: List[Dict[str, Any]] = []
        schema = self._generate_schema_from_fields(resource, fields)

        endpoint_defs = [
            {
                "operation": "list",
                "method": HttpMethod.GET,
                "path": base_path,
                "summary": f"List {resource}",
                "description": f"Retrieve a paginated list of {resource}",
                "has_pagination": True,
                "has_filtering": True,
            },
            {
                "operation": "create",
                "method": HttpMethod.POST,
                "path": base_path,
                "summary": f"Create {resource_singular}",
                "description": f"Create a new {resource_singular}",
                "has_request_body": True,
            },
            {
                "operation": "read",
                "method": HttpMethod.GET,
                "path": f"{base_path}/{{id}}",
                "summary": f"Get {resource_singular}",
                "description": f"Retrieve a specific {resource_singular} by ID",
                "has_path_id": True,
            },
            {
                "operation": "update",
                "method": HttpMethod.PUT,
                "path": f"{base_path}/{{id}}",
                "summary": f"Update {resource_singular}",
                "description": f"Update an existing {resource_singular}",
                "has_path_id": True,
                "has_request_body": True,
            },
            {
                "operation": "patch",
                "method": HttpMethod.PATCH,
                "path": f"{base_path}/{{id}}",
                "summary": f"Partially update {resource_singular}",
                "description": f"Partially update a {resource_singular}",
                "has_path_id": True,
                "has_request_body": True,
            },
            {
                "operation": "delete",
                "method": HttpMethod.DELETE,
                "path": f"{base_path}/{{id}}",
                "summary": f"Delete {resource_singular}",
                "description": f"Delete a {resource_singular} by ID",
                "has_path_id": True,
            },
        ]

        for ep_def in endpoint_defs:
            endpoint = self._build_endpoint(ep_def, schema, resource)
            self._generated_endpoints.append(endpoint)

            endpoints.append({
                "path": endpoint.path,
                "method": endpoint.method.value,
                "operation_id": endpoint.operation_id,
                "summary": endpoint.summary,
            })

        return ActionResult(
            success=True,
            message=f"Generated {len(endpoints)} CRUD endpoints for {resource}",
            data={
                "endpoints": endpoints,
                "schema_name": schema.name,
                "schema_properties": list(schema.properties.keys()),
            }
        )

    def _generate_single_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Generate a single custom endpoint."""
        path = params.get("path", "")
        method_str = params.get("method", "GET").upper()
        operation_id = params.get("operation_id", "")
        summary = params.get("summary", "")
        description = params.get("description", "")
        parameters = params.get("parameters", [])
        responses = params.get("responses", {})

        if not path or not method_str:
            return ActionResult(success=False, message="path and method are required")

        try:
            method = HttpMethod(method_str)
        except ValueError:
            return ActionResult(success=False, message=f"Invalid HTTP method: {method_str}")

        endpoint = Endpoint(
            path=path,
            method=method,
            operation_id=operation_id or self._generate_operation_id(path, method),
            summary=summary,
            description=description,
            responses=responses,
        )

        for param_data in parameters:
            param = EndpointParameter(
                name=param_data.get("name", ""),
                location=ParamLocation(param_data.get("location", "query")),
                param_type=ParamType(param_data.get("type", "string")),
                required=param_data.get("required", False),
                description=param_data.get("description", ""),
            )
            endpoint.parameters.append(param)

        self._generated_endpoints.append(endpoint)

        return ActionResult(
            success=True,
            message=f"Endpoint generated: {method.value} {path}",
            data={
                "operation_id": endpoint.operation_id,
                "path": endpoint.path,
                "method": endpoint.method.value,
            }
        )

    def _generate_schema(self, params: Dict[str, Any]) -> ActionResult:
        """Generate an API schema."""
        name = params.get("name", "")
        fields = params.get("fields", {})
        schema_type = params.get("type", "object")

        if not name or not fields:
            return ActionResult(success=False, message="name and fields are required")

        schema = self._generate_schema_from_fields(name, fields)
        schema.schema_type = schema_type

        self._generated_schemas[name] = schema

        return ActionResult(
            success=True,
            message=f"Schema generated: {name}",
            data={
                "name": schema.name,
                "type": schema.schema_type,
                "properties": list(schema.properties.keys()),
                "required": schema.required_fields,
            }
        )

    def _generate_schema_from_fields(
        self,
        name: str,
        fields: Dict[str, Any],
    ) -> APISchema:
        """Generate a schema from field definitions."""
        properties: Dict[str, Dict[str, Any]] = {}
        required_fields: List[str] = []

        type_mapping = {
            "string": "string",
            "str": "string",
            "int": "integer",
            "integer": "integer",
            "number": "number",
            "float": "number",
            "double": "number",
            "bool": "boolean",
            "boolean": "boolean",
            "array": "array",
            "list": "array",
            "object": "object",
            "dict": "object",
            "file": "file",
        }

        for field_name, field_def in fields.items():
            if isinstance(field_def, dict):
                field_type = field_def.get("type", "string")
                required_fields.append(field_name) if field_def.get("required") else None
            else:
                field_type = type_mapping.get(str(field_def).lower(), "string")

            prop: Dict[str, Any] = {"type": type_mapping.get(field_type, "string")}

            if isinstance(field_def, dict):
                if "description" in field_def:
                    prop["description"] = field_def["description"]
                if "format" in field_def:
                    prop["format"] = field_def["format"]
                if "enum" in field_def:
                    prop["enum"] = field_def["enum"]
                if "default" in field_def:
                    prop["default"] = field_def["default"]
                if "example" in field_def:
                    prop["example"] = field_def["example"]
                if field_type in ("array", "list") and "items" in field_def:
                    prop["items"] = field_def["items"]

            properties[field_name] = prop

        schema = APISchema(
            name=name,
            schema_type="object",
            properties=properties,
            required_fields=required_fields,
        )

        self._generated_schemas[name] = schema
        return schema

    def _add_parameter_to_endpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Add a parameter to an existing endpoint."""
        operation_id = params.get("operation_id", "")
        param_data = params.get("parameter", {})

        endpoint = self._find_endpoint(operation_id)
        if not endpoint:
            return ActionResult(success=False, message=f"Endpoint not found: {operation_id}")

        param = EndpointParameter(
            name=param_data.get("name", ""),
            location=ParamLocation(param_data.get("in", "query")),
            param_type=ParamType(param_data.get("type", "string")),
            required=param_data.get("required", False),
            description=param_data.get("description", ""),
            default=param_data.get("default"),
            enum_values=param_data.get("enum", []),
        )

        endpoint.parameters.append(param)

        return ActionResult(
            success=True,
            message=f"Parameter added to {operation_id}",
            data={"parameter_name": param.name, "total_parameters": len(endpoint.parameters)}
        )

    def _add_response(self, params: Dict[str, Any]) -> ActionResult:
        """Add a response definition to an endpoint."""
        operation_id = params.get("operation_id", "")
        status_code = str(params.get("status_code", "200"))
        response_data = params.get("response", {})

        endpoint = self._find_endpoint(operation_id)
        if not endpoint:
            return ActionResult(success=False, message=f"Endpoint not found: {operation_id}")

        endpoint.responses[status_code] = {
            "description": response_data.get("description", ""),
            "content": response_data.get("content", {}),
            "headers": response_data.get("headers", {}),
        }

        return ActionResult(
            success=True,
            message=f"Response {status_code} added to {operation_id}",
            data={"status_code": status_code, "total_responses": len(endpoint.responses)}
        )

    def _validate_design(self, params: Dict[str, Any]) -> ActionResult:
        """Validate the current API design."""
        errors: List[str] = []
        warnings: List[str] = []
        suggestions: List[str] = []

        for endpoint in self._generated_endpoints:
            if not endpoint.path.startswith("/"):
                errors.append(f"Endpoint {endpoint.operation_id}: Path must start with /")

            if not endpoint.summary:
                warnings.append(f"Endpoint {endpoint.operation_id}: Missing summary")

            if not endpoint.responses:
                warnings.append(f"Endpoint {endpoint.operation_id}: No response defined")

            if endpoint.method in (HttpMethod.POST, HttpMethod.PUT, HttpMethod.PATCH):
                if not endpoint.request_body and not any(p.location == ParamLocation.BODY for p in endpoint.parameters):
                    warnings.append(f"Endpoint {endpoint.operation_id}: POST/PUT/PATCH should define request body")

            path_params = [p for p in endpoint.parameters if p.location == ParamLocation.PATH]
            import re
            path_param_names = set(re.findall(r"\{(\w+)\}", endpoint.path))
            declared_params = set(p.name for p in path_params)

            missing = path_param_names - declared_params
            if missing:
                errors.append(f"Endpoint {endpoint.operation_id}: Path params declared but not defined: {missing}")

        self._check_naming_conventions(warnings, suggestions)

        return ActionResult(
            success=len(errors) == 0,
            message="Validation complete",
            data={
                "errors": errors,
                "warnings": warnings,
                "suggestions": suggestions,
                "total_endpoints": len(self._generated_endpoints),
            }
        )

    def _check_naming_conventions(
        self,
        warnings: List[str],
        suggestions: List[str],
    ) -> None:
        """Check naming conventions across the design."""
        seen_paths: Set[str] = set()

        for endpoint in self._generated_endpoints:
            path_lower = endpoint.path.lower()

            if path_lower in seen_paths:
                suggestions.append(f"Duplicate path pattern: {endpoint.path}")
            seen_paths.add(path_lower)

            if endpoint.method == HttpMethod.GET and "{id}" in endpoint.path:
                if "filter" not in endpoint.path and "search" not in endpoint.path:
                    suggestions.append(f"GET {endpoint.path}: Consider adding query params for filtering instead of path /{{id}}")

    def _get_generated_design(self, params: Dict[str, Any]) -> ActionResult:
        """Get the current generated design."""
        format_type = params.get("format", "summary")

        if format_type == "openapi":
            spec = self._export_as_openapi()
            return ActionResult(success=True, data={"spec": spec})
        else:
            endpoints_data = [
                {
                    "path": e.path,
                    "method": e.method.value,
                    "operation_id": e.operation_id,
                    "summary": e.summary,
                    "parameters": [
                        {"name": p.name, "in": p.location.value, "type": p.param_type.value}
                        for p in e.parameters
                    ],
                    "responses": list(e.responses.keys()),
                }
                for e in self._generated_endpoints
            ]

            schemas_data = {
                name: {
                    "name": s.name,
                    "type": s.schema_type,
                    "properties": list(s.properties.keys()),
                    "required": s.required_fields,
                }
                for name, s in self._generated_schemas.items()
            }

            return ActionResult(
                success=True,
                data={
                    "endpoints": endpoints_data,
                    "schemas": schemas_data,
                    "totals": {
                        "endpoints": len(endpoints_data),
                        "schemas": len(schemas_data),
                    },
                }
            )

    def _suggest_improvements(self, params: Dict[str, Any]) -> ActionResult:
        """Suggest improvements for the API design."""
        suggestions: List[Dict[str, str]] = []

        resource_endpoints: Dict[str, List[Endpoint]] = {}
        for endpoint in self._generated_endpoints:
            import re
            match = re.match(r"^/(.+?)(?:/\{.*)?$", endpoint.path)
            if match:
                resource = match.group(1)
                if resource not in resource_endpoints:
                    resource_endpoints[resource] = []
                resource_endpoints[resource].append(endpoint)

        for resource, endpoints in resource_endpoints.items():
            methods = set(e.method for e in endpoints)

            if HttpMethod.GET not in methods:
                suggestions.append({
                    "type": "missing_capability",
                    "resource": resource,
                    "suggestion": f"Add GET /{resource} for listing {resource}",
                    "priority": "high",
                })

            if HttpMethod.POST not in methods:
                suggestions.append({
                    "type": "missing_capability",
                    "resource": resource,
                    "suggestion": f"Add POST /{resource} for creating {resource}",
                    "priority": "high",
                })

        return ActionResult(
            success=True,
            data={"suggestions": suggestions, "count": len(suggestions)}
        )

    def _build_endpoint(
        self,
        ep_def: Dict[str, Any],
        schema: APISchema,
        resource: str,
    ) -> Endpoint:
        """Build an Endpoint object from a definition."""
        operation = ep_def["operation"]

        endpoint = Endpoint(
            path=ep_def["path"],
            method=ep_def["method"],
            operation_id=self._generate_operation_id(ep_def["path"], ep_def["method"]),
            summary=ep_def["summary"],
            description=ep_def["description"],
            tags=[resource],
        )

        if ep_def.get("has_pagination"):
            endpoint.parameters.append(
                EndpointParameter(
                    name="page",
                    location=ParamLocation.QUERY,
                    param_type=ParamType.INTEGER,
                    required=False,
                    description="Page number",
                    default=1,
                )
            )
            endpoint.parameters.append(
                EndpointParameter(
                    name="limit",
                    location=ParamLocation.QUERY,
                    param_type=ParamType.INTEGER,
                    required=False,
                    description="Items per page",
                    default=20,
                )
            )

        if ep_def.get("has_filtering"):
            endpoint.parameters.append(
                EndpointParameter(
                    name="sort",
                    location=ParamLocation.QUERY,
                    param_type=ParamType.STRING,
                    required=False,
                    description="Sort field (prefix with - for descending)",
                )
            )

        if ep_def.get("has_path_id"):
            endpoint.parameters.append(
                EndpointParameter(
                    name="id",
                    location=ParamLocation.PATH,
                    param_type=ParamType.STRING,
                    required=True,
                    description="Resource ID",
                )
            )

        if ep_def.get("has_request_body"):
            endpoint.request_body = {
                "content": {
                    "application/json": {
                        "schema": {
                            "$ref": f"#/components/schemas/{schema.name}"
                        }
                    }
                }
            }

        endpoint.responses = {
            "200": {"description": "Successful response"},
            "400": {"description": "Bad request"},
            "401": {"description": "Unauthorized"},
            "404": {"description": "Not found"},
            "500": {"description": "Internal server error"},
        }

        if operation == "create":
            endpoint.responses["201"] = {"description": "Created"}
            endpoint.responses["204"] = {"description": "No content (delete)"}

        if operation == "list":
            endpoint.responses["200"]["description"] = "Paginated list response"

        return endpoint

    def _find_endpoint(self, operation_id: str) -> Optional[Endpoint]:
        """Find an endpoint by operation ID."""
        for endpoint in self._generated_endpoints:
            if endpoint.operation_id == operation_id:
                return endpoint
        return None

    def _generate_operation_id(self, path: str, method: HttpMethod) -> str:
        """Generate a unique operation ID."""
        clean_path = path.replace("/", "_").replace("{", "").replace("}", "")
        return f"{method.value.lower()}_{clean_path}".strip("_")

    def _export_as_openapi(self) -> Dict[str, Any]:
        """Export the design as OpenAPI 3.0 specification."""
        spec: Dict[str, Any] = {
            "openapi": "3.0.0",
            "info": {
                "title": "Generated API",
                "version": "1.0.0",
            },
            "paths": {},
            "components": {
                "schemas": {}
            },
        }

        for endpoint in self._generated_endpoints:
            path_item = spec["paths"].setdefault(endpoint.path, {})

            path_item[endpoint.method.value.lower()] = {
                "operationId": endpoint.operation_id,
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "parameters": [
                    {
                        "name": p.name,
                        "in": p.location.value,
                        "required": p.required,
                        "description": p.description,
                        "schema": {"type": p.param_type.value},
                    }
                    for p in endpoint.parameters
                ],
                "responses": endpoint.responses,
            }

            if endpoint.request_body:
                path_item[endpoint.method.value.lower()]["requestBody"] = endpoint.request_body

        for schema_name, schema in self._generated_schemas.items():
            spec["components"]["schemas"][schema_name] = {
                "type": schema.schema_type,
                "properties": schema.properties,
                "required": schema.required_fields if schema.required_fields else None,
            }

        return spec

    def clear_design(self) -> None:
        """Clear all generated endpoints and schemas."""
        self._generated_endpoints.clear()
        self._generated_schemas.clear()
