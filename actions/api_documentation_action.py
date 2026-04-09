"""
API Documentation Action Module.

Auto-generate API documentation from schemas, endpoints,
and code annotations with multiple format support.
"""

import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DocFormat(Enum):
    """Documentation output formats."""
    MARKDOWN = "markdown"
    OPENAPI = "openapi"
    SWAGGER = "swagger"
    HTML = "html"
    POSTMAN = "postman"


@dataclass
class EndpointDoc:
    """Endpoint documentation."""
    path: str
    method: str
    summary: str
    description: str = ""
    parameters: list = field(default_factory=list)
    request_body: Optional[dict] = None
    responses: dict = field(default_factory=dict)
    tags: list = field(default_factory=list)
    deprecated: bool = False


@dataclass
class SchemaDoc:
    """Schema documentation."""
    name: str
    description: str = ""
    fields: list = field(default_factory=list)
    example: Optional[dict] = None


@dataclass
class APIDoc:
    """Complete API documentation."""
    title: str
    version: str
    description: str
    endpoints: list = field(default_factory=list)
    schemas: list = field(default_factory=list)
    generated_at: float = field(default_factory=time.time, init=False)


class APIDocumentationAction:
    """
    Auto-generate API documentation.

    Example:
        doc_generator = APIDocumentationAction()
        doc_generator.add_endpoint(endpoint)
        markdown = doc_generator.generate(DocFormat.MARKDOWN)
        openapi = doc_generator.generate(DocFormat.OPENAPI)
    """

    def __init__(self, title: str = "API", version: str = "1.0.0"):
        """
        Initialize API documentation action.

        Args:
            title: API title.
            version: API version.
        """
        self.api_doc = APIDoc(
            title=title,
            version=version,
            description=""
        )

    def set_description(self, description: str) -> None:
        """Set API description."""
        self.api_doc.description = description

    def add_endpoint(self, endpoint: EndpointDoc) -> None:
        """Add endpoint to documentation."""
        self.api_doc.endpoints.append(endpoint)

    def add_schema(self, schema: SchemaDoc) -> None:
        """Add schema to documentation."""
        self.api_doc.schemas.append(schema)

    def add_endpoint_from_function(
        self,
        func: Callable,
        path: str,
        method: str = "GET",
        **kwargs
    ) -> EndpointDoc:
        """
        Add endpoint documentation from function signature.

        Args:
            func: Function to document.
            path: Endpoint path.
            method: HTTP method.
            **kwargs: Additional endpoint properties.

        Returns:
            Created EndpointDoc.
        """
        sig = inspect.signature(func)
        parameters = []

        for name, param in sig.parameters.items():
            param_doc = {
                "name": name,
                "type": param.annotation.__name__ if param.annotation != inspect.Parameter.empty else "any",
                "required": param.default == inspect.Parameter.empty,
                "default": param.default if param.default != inspect.Parameter.empty else None
            }
            parameters.append(param_doc)

        endpoint = EndpointDoc(
            path=path,
            method=method,
            summary=kwargs.get("summary", func.__name__),
            description=kwargs.get("description", func.__doc__ or ""),
            parameters=parameters,
            tags=kwargs.get("tags", [])
        )

        self.api_doc.endpoints.append(endpoint)
        return endpoint

    def generate(self, format: DocFormat = DocFormat.MARKDOWN) -> str:
        """
        Generate documentation in specified format.

        Args:
            format: Output format.

        Returns:
            Generated documentation string.
        """
        if format == DocFormat.MARKDOWN:
            return self._generate_markdown()
        elif format in (DocFormat.OPENAPI, DocFormat.SWAGGER):
            return self._generate_openapi()
        elif format == DocFormat.POSTMAN:
            return self._generate_postman()
        elif format == DocFormat.HTML:
            return self._generate_html()

        return ""

    def _generate_markdown(self) -> str:
        """Generate Markdown documentation."""
        lines = [
            f"# {self.api_doc.title}",
            f"**Version:** {self.api_doc.version}",
            "",
            self.api_doc.description,
            ""
        ]

        if self.api_doc.schemas:
            lines.extend(["## Schemas", ""])
            for schema in self.api_doc.schemas:
                lines.extend([
                    f"### {schema.name}",
                    schema.description,
                    ""
                ])

                if schema.fields:
                    lines.append("| Field | Type | Description |")
                    lines.append("|-------|------|-------------|")
                    for field in schema.fields:
                        lines.append(f"| {field['name']} | {field['type']} | {field.get('description', '')} |")
                    lines.append("")

        if self.api_doc.endpoints:
            lines.extend(["## Endpoints", ""])

            for endpoint in self.api_doc.endpoints:
                lines.append(f"### `{endpoint.method} {endpoint.path}`")
                lines.append(f"**Summary:** {endpoint.summary}")

                if endpoint.description:
                    lines.append(f"**Description:** {endpoint.description}")

                if endpoint.tags:
                    lines.append(f"**Tags:** {', '.join(endpoint.tags)}")

                if endpoint.parameters:
                    lines.append("**Parameters:**")
                    lines.append("| Name | Type | Required | Default |")
                    lines.append("|------|------|----------|---------|")
                    for param in endpoint.parameters:
                        lines.append(
                            f"| {param['name']} | {param['type']} | "
                            f"{'Yes' if param.get('required') else 'No'} | "
                            f"{param.get('default', '-')} |"
                        )

                if endpoint.responses:
                    lines.append("**Responses:**")
                    for status, response in endpoint.responses.items():
                        lines.append(f"- `{status}`: {response}")

                lines.append("")

        return "\n".join(lines)

    def _generate_openapi(self) -> str:
        """Generate OpenAPI 3.0 specification."""
        import json

        openapi = {
            "openapi": "3.0.0",
            "info": {
                "title": self.api_doc.title,
                "version": self.api_doc.version,
                "description": self.api_doc.description
            },
            "paths": {}
        }

        for endpoint in self.api_doc.endpoints:
            path_key = endpoint.path
            if path_key not in openapi["paths"]:
                openapi["paths"][path_key] = {}

            method_lower = endpoint.method.lower()
            endpoint_doc = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "responses": {}
            }

            for status, description in endpoint.responses.items():
                endpoint_doc["responses"][status] = {"description": description}

            if endpoint.parameters:
                endpoint_doc["parameters"] = []
                for param in endpoint.parameters:
                    endpoint_doc["parameters"].append({
                        "name": param["name"],
                        "in": "query",
                        "schema": {"type": param["type"]},
                        "required": param.get("required", False)
                    })

            openapi["paths"][path_key][method_lower] = endpoint_doc

        return json.dumps(openapi, indent=2)

    def _generate_postman(self) -> str:
        """Generate Postman collection."""
        import json

        collection = {
            "info": {
                "name": self.api_doc.title,
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": []
        }

        for endpoint in self.api_doc.endpoints:
            item = {
                "name": endpoint.summary,
                "request": {
                    "method": endpoint.method,
                    "header": [],
                    "url": {
                        "raw": endpoint.path,
                        "path": endpoint.path.strip("/").split("/")
                    }
                }
            }

            if endpoint.request_body:
                item["request"]["body"] = {
                    "mode": "raw",
                    "raw": json.dumps(endpoint.request_body)
                }

            collection["item"].append(item)

        return json.dumps(collection, indent=2)

    def _generate_html(self) -> str:
        """Generate HTML documentation."""
        markdown = self._generate_markdown()

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{self.api_doc.title}</title>
    <meta charset="utf-8">
    <style>
        body {{ font-family: -apple-system, sans-serif; line-height: 1.6; max-width: 900px; margin: 0 auto; padding: 20px; }}
        h1, h2, h3 {{ color: #333; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 3px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background: #f4f4f4; }}
        .endpoint {{ background: #f9f9f9; padding: 15px; margin: 10px 0; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>{self.api_doc.title}</h1>
    <p><strong>Version:</strong> {self.api_doc.version}</p>
    <p>{self.api_doc.description}</p>
    <div id="content"></div>
</body>
</html>"""

        return html

    def save(self, path: str, format: DocFormat = DocFormat.MARKDOWN) -> None:
        """
        Save documentation to file.

        Args:
            path: Output file path.
            format: Documentation format.
        """
        content = self.generate(format)

        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"Documentation saved to: {path}")

    def get_endpoint_count(self) -> int:
        """Get number of documented endpoints."""
        return len(self.api_doc.endpoints)

    def get_schema_count(self) -> int:
        """Get number of documented schemas."""
        return len(self.api_doc.schemas)
