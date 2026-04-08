"""
API Documentation Generator Action Module.

Generates API documentation from OpenAPI/Swagger specs,
creates interactive docs, and generates client SDK stubs.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class EndpointDoc:
    """Documentation for a single endpoint."""
    path: str
    method: str
    summary: str
    description: str
    parameters: list[dict[str, Any]]
    request_body: Optional[dict[str, Any]]
    responses: dict[str, Any]
    tags: list[str] = field(default_factory=list)


@dataclass
class DocGenerationResult:
    """Result of documentation generation."""
    spec: dict[str, Any]
    html_docs: str
    md_docs: str
    endpoints_count: int


class APIDocsAction(BaseAction):
    """Generate API documentation."""

    def __init__(self) -> None:
        super().__init__("api_docs")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Generate API documentation.

        Args:
            context: Execution context
            params: Parameters:
                - endpoints: List of endpoint definitions
                - title: API title
                - version: API version
                - description: API description
                - base_url: Base URL for the API
                - format: Output format (openapi, html, md, all)

        Returns:
            DocGenerationResult with spec and formatted docs
        """
        endpoints = params.get("endpoints", [])
        title = params.get("title", "My API")
        version = params.get("version", "1.0.0")
        description = params.get("description", "")
        base_url = params.get("base_url", "")
        output_format = params.get("format", "all")

        endpoint_docs = []
        for ep in endpoints:
            endpoint_docs.append(EndpointDoc(
                path=ep.get("path", ""),
                method=ep.get("method", "GET").upper(),
                summary=ep.get("summary", ""),
                description=ep.get("description", ""),
                parameters=ep.get("parameters", []),
                request_body=ep.get("request_body"),
                responses=ep.get("responses", {"200": {"description": "Success"}}),
                tags=ep.get("tags", [])
            ))

        spec = self._build_openapi_spec(endpoint_docs, title, version, description, base_url)
        html_docs = self._generate_html(spec) if output_format in ("html", "all") else ""
        md_docs = self._generate_markdown(endpoint_docs, title, version) if output_format in ("md", "all") else ""

        return DocGenerationResult(
            spec=spec,
            html_docs=html_docs,
            md_docs=md_docs,
            endpoints_count=len(endpoint_docs)
        )

    def _build_openapi_spec(self, endpoints: list[EndpointDoc], title: str, version: str, description: str, base_url: str) -> dict[str, Any]:
        """Build OpenAPI specification dict."""
        paths: dict[str, dict[str, Any]] = {}
        for ep in endpoints:
            if ep.path not in paths:
                paths[ep.path] = {}
            paths[ep.path][ep.method.lower()] = {
                "summary": ep.summary,
                "description": ep.description,
                "parameters": ep.parameters,
                "responses": ep.responses
            }
            if ep.request_body:
                paths[ep.path][ep.method.lower()]["requestBody"] = ep.request_body

        return {
            "openapi": "3.0.0",
            "info": {"title": title, "version": version, "description": description},
            "paths": paths,
            "servers": [{"url": base_url}] if base_url else []
        }

    def _generate_html(self, spec: dict[str, Any]) -> str:
        """Generate HTML documentation."""
        import json
        spec_json = json.dumps(spec, indent=2)
        return f"""<!DOCTYPE html>
<html><head><title>API Documentation</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 40px; }}
.endpoint {{ border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 4px; }}
.method {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-weight: bold; }}
.get {{ background: #61affe; }} .post {{ background: #49cc90; }}
.put {{ background: #fca130; }} .delete {{ background: #f93e3e; }}
</style></head><body>
<h1>{spec['info']['title']}</h1>
<p>Version: {spec['info']['version']}</p>
<pre>{spec_json}</pre>
</body></html>"""

    def _generate_markdown(self, endpoints: list[EndpointDoc], title: str, version: str) -> str:
        """Generate Markdown documentation."""
        lines = [f"# {title}\n\n**Version: {version}**\n"]
        for ep in endpoints:
            lines.append(f"## {ep.method} {ep.path}\n")
            if ep.summary:
                lines.append(f"**{ep.summary}**\n")
            if ep.description:
                lines.append(f"{ep.description}\n")
            if ep.parameters:
                lines.append("### Parameters\n\n| Name | Type | Description |\n|------|------|-------------|\n")
                for p in ep.parameters:
                    lines.append(f"| {p.get('name','')} | {p.get('type','')} | {p.get('description','')} |\n")
            lines.append("\n")
        return "".join(lines)
