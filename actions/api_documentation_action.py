"""API documentation action module for RabAI AutoClick.

Provides API documentation:
- APIDocGenerator: Generate API documentation
- OpenAPIGenerator: Generate OpenAPI specs
- MarkdownDocGenerator: Generate Markdown docs
- EndpointAnalyzer: Analyze endpoints
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Endpoint:
    """API endpoint definition."""
    path: str
    method: str
    summary: str
    description: Optional[str] = None
    parameters: List[Dict] = field(default_factory=list)
    request_body: Optional[Dict] = None
    responses: Dict[str, Dict] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class APISpec:
    """API specification."""
    title: str
    version: str
    description: Optional[str] = None
    endpoints: List[Endpoint] = field(default_factory=list)
    schemas: Dict[str, Dict] = field(default_factory=dict)


class OpenAPIGenerator:
    """Generate OpenAPI specifications."""

    def __init__(self, spec: Optional[APISpec] = None):
        self.spec = spec

    def generate(self) -> Dict:
        """Generate OpenAPI spec."""
        if not self.spec:
            return {}

        paths = {}
        for endpoint in self.spec.endpoints:
            path_item = paths.get(endpoint.path, {})
            path_item[endpoint.method.lower()] = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "parameters": endpoint.parameters,
                "requestBody": endpoint.request_body,
                "responses": endpoint.responses,
                "tags": endpoint.tags,
            }
            paths[endpoint.path] = path_item

        return {
            "openapi": "3.0.0",
            "info": {
                "title": self.spec.title,
                "version": self.spec.version,
                "description": self.spec.description,
            },
            "paths": paths,
            "components": {
                "schemas": self.spec.schemas,
            },
        }


class MarkdownDocGenerator:
    """Generate Markdown documentation."""

    def __init__(self, spec: Optional[APISpec] = None):
        self.spec = spec

    def generate(self) -> str:
        """Generate Markdown docs."""
        if not self.spec:
            return ""

        lines = []
        lines.append(f"# {self.spec.title}")
        lines.append(f"\n**Version:** {self.spec.version}\n")

        if self.spec.description:
            lines.append(f"\n{self.spec.description}\n")

        lines.append("\n## Endpoints\n")

        by_tag: Dict[str, List[Endpoint]] = {}
        for endpoint in self.spec.endpoints:
            tag = endpoint.tags[0] if endpoint.tags else "General"
            if tag not in by_tag:
                by_tag[tag] = []
            by_tag[tag].append(endpoint)

        for tag, endpoints in by_tag.items():
            lines.append(f"\n### {tag}\n")

            for endpoint in endpoints:
                lines.append(f"\n#### {endpoint.method.upper()} {endpoint.path}")
                lines.append(f"\n{endpoint.summary}")

                if endpoint.description:
                    lines.append(f"\n{endpoint.description}")

                if endpoint.parameters:
                    lines.append("\n**Parameters:**\n")
                    lines.append("| Name | Type | Required | Description |")
                    lines.append("|------|------|----------|-------------|")
                    for p in endpoint.parameters:
                        lines.append(
                            f"| {p.get('name')} | {p.get('type')} | {p.get('required', False)} | {p.get('description', '')} |"
                        )

                if endpoint.request_body:
                    lines.append("\n**Request Body:**\n")
                    lines.append("```json")
                    import json
                    lines.append(json.dumps(endpoint.request_body, indent=2))
                    lines.append("```")

                if endpoint.responses:
                    lines.append("\n**Responses:**\n")
                    for code, response in endpoint.responses.items():
                        lines.append(f"- `{code}`: {response.get('description', '')}")

        return "\n".join(lines)


class APIDocGenerator:
    """Generate API documentation."""

    def __init__(self):
        self.spec = None

    def create_spec(
        self,
        title: str,
        version: str,
        description: Optional[str] = None,
    ) -> APISpec:
        """Create API spec."""
        self.spec = APISpec(
            title=title,
            version=version,
            description=description,
        )
        return self.spec

    def add_endpoint(
        self,
        path: str,
        method: str,
        summary: str,
        description: Optional[str] = None,
        parameters: Optional[List[Dict]] = None,
        request_body: Optional[Dict] = None,
        responses: Optional[Dict[str, Dict]] = None,
        tags: Optional[List[str]] = None,
    ) -> "APIDocGenerator":
        """Add endpoint to spec."""
        if not self.spec:
            return self

        endpoint = Endpoint(
            path=path,
            method=method.upper(),
            summary=summary,
            description=description,
            parameters=parameters or [],
            request_body=request_body,
            responses=responses or {},
            tags=tags or [],
        )
        self.spec.endpoints.append(endpoint)
        return self

    def add_schema(self, name: str, schema: Dict) -> "APIDocGenerator":
        """Add schema to spec."""
        if self.spec:
            self.spec.schemas[name] = schema
        return self

    def to_openapi(self) -> Dict:
        """Generate OpenAPI spec."""
        generator = OpenAPIGenerator(self.spec)
        return generator.generate()

    def to_markdown(self) -> str:
        """Generate Markdown docs."""
        generator = MarkdownDocGenerator(self.spec)
        return generator.generate()


class APIDocumentationAction(BaseAction):
    """API documentation action."""
    action_type = "api_documentation"
    display_name = "API文档"
    description = "API文档生成"

    def __init__(self):
        super().__init__()
        self._generator = APIDocGenerator()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "generate")

            if operation == "create_spec":
                return self._create_spec(params)
            elif operation == "add_endpoint":
                return self._add_endpoint(params)
            elif operation == "add_schema":
                return self._add_schema(params)
            elif operation == "generate":
                return self._generate(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Documentation error: {str(e)}")

    def _create_spec(self, params: Dict) -> ActionResult:
        """Create API spec."""
        title = params.get("title", "API")
        version = params.get("version", "1.0.0")
        description = params.get("description")

        spec = self._generator.create_spec(title, version, description)

        return ActionResult(
            success=True,
            message=f"API spec '{title}' created",
            data={"title": spec.title, "version": spec.version},
        )

    def _add_endpoint(self, params: Dict) -> ActionResult:
        """Add endpoint."""
        path = params.get("path")
        method = params.get("method", "GET")
        summary = params.get("summary", "")

        if not path:
            return ActionResult(success=False, message="path is required")

        self._generator.add_endpoint(
            path=path,
            method=method,
            summary=summary,
            description=params.get("description"),
            parameters=params.get("parameters"),
            request_body=params.get("request_body"),
            responses=params.get("responses"),
            tags=params.get("tags"),
        )

        return ActionResult(success=True, message=f"Endpoint {method.upper()} {path} added")

    def _add_schema(self, params: Dict) -> ActionResult:
        """Add schema."""
        name = params.get("name")
        schema = params.get("schema", {})

        if not name:
            return ActionResult(success=False, message="name is required")

        self._generator.add_schema(name, schema)

        return ActionResult(success=True, message=f"Schema '{name}' added")

    def _generate(self, params: Dict) -> ActionResult:
        """Generate documentation."""
        format_type = params.get("format", "openapi").lower()

        if format_type == "openapi":
            result = self._generator.to_openapi()
            return ActionResult(
                success=True,
                message="OpenAPI spec generated",
                data={"spec": result},
            )
        elif format_type == "markdown":
            result = self._generator.to_markdown()
            return ActionResult(
                success=True,
                message="Markdown docs generated",
                data={"markdown": result},
            )
        else:
            return ActionResult(success=False, message=f"Unknown format: {format_type}")
