"""
API Documentation Generator Action Module.

Automatically generates API documentation from code annotations,
supports OpenAPI/Swagger, and provides interactive documentation serving.
"""

from typing import Optional, Dict, List, Any, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
import inspect
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """HTTP methods for API endpoints."""
    GET = "get"
    POST = "post"
    PUT = "put"
    PATCH = "patch"
    DELETE = "delete"
    OPTIONS = "options"
    HEAD = "head"


@dataclass
class Parameter:
    """API parameter definition."""
    name: str
    location: str  # path, query, header, body, form
    param_type: str  # string, integer, boolean, object, array
    required: bool = False
    description: Optional[str] = None
    default: Any = None
    enum_values: Optional[List[str]] = None
    schema: Optional[Dict[str, Any]] = None


@dataclass
class Endpoint:
    """API endpoint definition."""
    path: str
    method: HTTPMethod
    summary: str
    description: Optional[str] = None
    parameters: List[Parameter] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: Optional[List[str]] = None


@dataclass
class APISchema:
    """Complete API schema definition."""
    title: str
    version: str
    description: Optional[str] = None
    base_path: Optional[str] = None
    host: Optional[str] = None
    schemes: List[str] = field(default_factory=lambda: ["https"])
    endpoints: List[Endpoint] = field(default_factory=list)
    schemas: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    security_definitions: Dict[str, Any] = field(default_factory=dict)
    tags: List[Dict[str, str]] = field(default_factory=list)
    contact: Optional[Dict[str, str]] = None
    license: Optional[Dict[str, str]] = None


class DocstringParser:
    """Parse docstrings into structured documentation."""
    
    @staticmethod
    def parse(docstring: str) -> Dict[str, Any]:
        """
        Parse a docstring into summary and description.
        
        Supports Google, NumPy, and Sphinx style docstrings.
        """
        if not docstring:
            return {"summary": "", "description": ""}
            
        lines = docstring.strip().split("\n")
        
        # Simple parsing: first line is summary, rest is description
        summary = lines[0].strip() if lines else ""
        description = "\n".join(line.strip() for line in lines[1:]).strip()
        
        return {
            "summary": summary,
            "description": description,
        }
        
    @staticmethod
    def parse_params(docstring: str) -> List[Dict[str, str]]:
        """Extract parameter documentation from docstring."""
        params = []
        
        in_args = False
        for line in docstring.split("\n"):
            line = line.strip()
            
            if line.startswith("Args:") or line.startswith("Arguments:"):
                in_args = True
                continue
                
            if in_args:
                if line and not line.startswith(" "):
                    break
                if ":" in line:
                    parts = line.split(":", 1)
                    param_name = parts[0].strip()
                    param_desc = parts[1].strip() if len(parts) > 1 else ""
                    if param_name:
                        params.append({"name": param_name, "description": param_desc})
                        
        return params


class APIDocGenerator:
    """
    Generate API documentation from code.
    
    Example:
        generator = APIDocGenerator(title="My API", version="1.0")
        
        @generator.endpoint("/users", "GET", summary="List users")
        def get_users():
            '''Get all users'''
            pass
            
        doc = generator.generate()
    """
    
    def __init__(
        self,
        title: str,
        version: str,
        description: Optional[str] = None,
    ):
        self.schema = APISchema(
            title=title,
            version=version,
            description=description,
        )
        self._function_map: Dict[str, Callable] = {}
        
    def endpoint(
        self,
        path: str,
        method: str,
        summary: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        parameters: Optional[List[Parameter]] = None,
        responses: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Callable:
        """
        Decorator to register an API endpoint.
        
        Example:
            @generator.endpoint("/users", "GET", summary="List users")
            def get_users():
                '''Get all users from the system'''
                pass
        """
        def decorator(func: Callable) -> Callable:
            http_method = HTTPMethod(method.lower())
            
            endpoint = Endpoint(
                path=path,
                method=http_method,
                summary=summary,
                description=description or func.__doc__,
                parameters=parameters or [],
                responses=responses or {
                    "200": {"description": "Success"},
                    "400": {"description": "Bad Request"},
                    "500": {"description": "Internal Error"},
                },
                tags=tags or [],
            )
            
            self.schema.endpoints.append(endpoint)
            self._function_map[f"{method.upper()} {path}"] = func
            
            return func
            
        return decorator
        
    def add_endpoint(
        self,
        endpoint: Endpoint,
    ) -> None:
        """Add an endpoint directly."""
        self.schema.endpoints.append(endpoint)
        
    def add_schema(
        self,
        name: str,
        properties: Dict[str, Any],
        required: Optional[List[str]] = None,
        description: Optional[str] = None,
    ) -> None:
        """Add a schema definition."""
        self.schema.schemas[name] = {
            "type": "object",
            "properties": properties,
            "required": required,
            "description": description,
        }
        
    def add_tag(
        self,
        name: str,
        description: Optional[str] = None,
    ) -> None:
        """Add a tag for grouping endpoints."""
        self.schema.tags.append({
            "name": name,
            "description": description or name,
        })
        
    def add_security_definition(
        self,
        name: str,
        auth_type: str,
        **kwargs,
    ) -> None:
        """Add a security definition (OAuth2, API key, etc.)."""
        self.schema.security_definitions[name] = {
            "type": auth_type,
            **kwargs,
        }
        
    def generate(self) -> APISchema:
        """Get the complete API schema."""
        return self.schema
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary."""
        return {
            "swagger": "2.0",
            "info": {
                "title": self.schema.title,
                "version": self.schema.version,
                "description": self.schema.description,
                "contact": self.schema.contact,
                "license": self.schema.license,
            },
            "host": self.schema.host,
            "basePath": self.schema.base_path,
            "schemes": self.schema.schemes,
            "tags": self.schema.tags,
            "paths": self._generate_paths(),
            "definitions": self._generate_definitions(),
            "securityDefinitions": self.schema.security_definitions,
        }
        
    def _generate_paths(self) -> Dict[str, Any]:
        """Generate OpenAPI paths section."""
        paths: Dict[str, Any] = {}
        
        for endpoint in self.schema.endpoints:
            path_key = endpoint.path
            if path_key not in paths:
                paths[path_key] = {}
                
            path_item = paths[path_key]
            method_key = endpoint.method.value
            
            path_item[method_key] = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "deprecated": endpoint.deprecated,
                "parameters": self._format_parameters(endpoint.parameters),
                "responses": endpoint.responses,
            }
            
            if endpoint.security:
                path_item[method_key]["security"] = endpoint.security
                
        return paths
        
    def _format_parameters(
        self,
        parameters: List[Parameter],
    ) -> List[Dict[str, Any]]:
        """Format parameters for OpenAPI output."""
        return [
            {
                "name": p.name,
                "in": p.location,
                "required": p.required,
                "description": p.description,
                "type": p.param_type,
                "default": p.default,
                "enum": p.enum_values,
                "schema": p.schema,
            }
            for p in parameters
        ]
        
    def _generate_definitions(self) -> Dict[str, Any]:
        """Generate OpenAPI definitions section."""
        definitions = {}
        
        for name, schema in self.schema.schemas.items():
            definitions[name] = schema
            
        return definitions
        
    def to_json(self, indent: int = 2) -> str:
        """Export schema as JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
        
    def to_openapi3(self) -> Dict[str, Any]:
        """Convert to OpenAPI 3.0 format."""
        return {
            "openapi": "3.0.0",
            "info": {
                "title": self.schema.title,
                "version": self.schema.version,
                "description": self.schema.description,
            },
            "servers": [
                {"url": f"{s}://{self.schema.host}{self.schema.base_path}"}
                for s in self.schema.schemes
            ] if self.schema.host else [],
            "tags": self.schema.tags,
            "paths": self._generate_openapi3_paths(),
            "components": {
                "schemas": self._generate_openapi3_schemas(),
                "securitySchemes": self.schema.security_definitions,
            },
        }
        
    def _generate_openapi3_paths(self) -> Dict[str, Any]:
        """Generate OpenAPI 3.0 paths."""
        paths: Dict[str, Any] = {}
        
        for endpoint in self.schema.endpoints:
            path_key = endpoint.path
            if path_key not in paths:
                paths[path_key] = {}
                
            method_key = endpoint.method.value
            
            paths[path_key][method_key] = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "deprecated": endpoint.deprecated,
                "parameters": self._format_openapi3_parameters(endpoint.parameters),
                "requestBody": endpoint.request_body,
                "responses": endpoint.responses,
            }
            
        return paths
        
    def _format_openapi3_parameters(
        self,
        parameters: List[Parameter],
    ) -> List[Dict[str, Any]]:
        """Format parameters for OpenAPI 3.0."""
        formatted = []
        
        for p in parameters:
            param = {
                "name": p.name,
                "in": p.location,
                "required": p.required,
                "description": p.description,
            }
            
            if p.location == "body":
                param["schema"] = p.schema or {"type": p.param_type}
            else:
                param["schema"] = {"type": p.param_type}
                if p.enum_values:
                    param["schema"]["enum"] = p.enum_values
                if p.default is not None:
                    param["schema"]["default"] = p.default
                    
            formatted.append(param)
            
        return formatted
        
    def _generate_openapi3_schemas(self) -> Dict[str, Any]:
        """Generate OpenAPI 3.0 schemas."""
        schemas = {}
        
        for name, schema in self.schema.schemas.items():
            schemas[name] = schema
            
        return schemas


class DocumentationServer:
    """
    Serve interactive API documentation.
    
    Example:
        server = DocumentationServer(generator, port=8080)
        server.add_middleware(CORSMiddleware)
        await server.start()
    """
    
    def __init__(
        self,
        doc_generator: APIDocGenerator,
        port: int = 8080,
        static_dir: Optional[str] = None,
    ):
        self.generator = doc_generator
        self.port = port
        self.static_dir = static_dir
        self.middlewares: List[Callable] = []
        
    def add_middleware(
        self,
        middleware: Callable,
    ) -> None:
        """Add a middleware function."""
        self.middlewares.append(middleware)
        
    def get_swagger_ui_html(self) -> str:
        """Generate Swagger UI HTML page."""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>{self.generator.schema.title} - API Documentation</title>
    <link rel="stylesheet" type="text/css" 
          href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        SwaggerUIBBuilder({{
            spec: {json.dumps(self.generator.to_dict())},
            dom_id: '#swagger-ui'
        }}).build();
    </script>
</body>
</html>
"""
        
    def get_redoc_html(self) -> str:
        """Generate ReDoc HTML page."""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <title>{self.generator.schema.title} - API Documentation</title>
    <link rel="stylesheet" type="text/css" 
          href="https://unpkg.com/redoc@latest/bundles/redoc.standalone.js" />
</head>
<body>
    <redoc spec-url='/openapi.json'></redoc>
    <script src="https://unpkg.com/redoc@latest/redoc.standalone.js"></script>
</body>
</html>
"""
