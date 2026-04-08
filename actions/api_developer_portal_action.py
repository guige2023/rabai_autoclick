"""
API Developer Portal Action Module.

Generates developer documentation, SDK stubs,
and interactive API explorers from API metadata.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class DocEndpoint:
    """Documented API endpoint."""
    method: str
    path: str
    summary: str
    description: str
    parameters: list[dict[str, Any]] = field(default_factory=list)
    request_body: Optional[dict[str, Any]] = None
    responses: dict[str, str] = field(default_factory=dict)
    code_samples: list[dict[str, str]] = field(default_factory=list)


@dataclass
class SDKSnippet:
    """Generated SDK code snippet."""
    language: str
    code: str
    description: str


class APIDeveloperPortalAction:
    """
    Generates developer documentation and SDK stubs.

    Creates Markdown docs, code samples in multiple languages,
    and interactive examples from API metadata.

    Example:
        portal = APIDeveloperPortalAction()
        docs = portal.generate_markdown(api_catalog)
        snippets = portal.generate_sdk_stubs(endpoints)
    """

    def __init__(
        self,
        title: str = "API Reference",
        version: str = "1.0.0",
    ) -> None:
        self.title = title
        self.version = version
        self._custom_code_samples: dict[str, list[SDKSnippet]] = {}

    def generate_markdown(
        self,
        endpoints: list[DocEndpoint],
    ) -> str:
        """Generate Markdown API documentation."""
        lines = [
            f"# {self.title}",
            f"**Version:** {self.version}",
            "",
            "## Table of Contents",
            "",
        ]

        for idx, endpoint in enumerate(endpoints, 1):
            safe_id = self._slugify(endpoint.path)
            lines.append(f"{idx}. [{endpoint.method} {endpoint.path}](#{safe_id})")

        lines.extend(["", "---", ""])

        for endpoint in endpoints:
            lines.extend(self._format_endpoint(endpoint))

        return "\n".join(lines)

    def _format_endpoint(self, endpoint: DocEndpoint) -> list[str]:
        """Format single endpoint as Markdown."""
        safe_id = self._slugify(endpoint.path)

        lines = [
            f"### `{endpoint.method} {endpoint.path}`",
            f"**{endpoint.summary}**",
            "",
            endpoint.description,
            "",
        ]

        if endpoint.parameters:
            lines.extend([
                "**Parameters:**",
                "",
                "| Name | Location | Type | Required | Description |",
                "|------|----------|------|----------|-------------|",
            ])

            for param in endpoint.parameters:
                lines.append(
                    f"| {param.get('name', '')} | "
                    f"{param.get('in', '')} | "
                    f"{param.get('type', 'string')} | "
                    f"{'Yes' if param.get('required') else 'No'} | "
                    f"{param.get('description', '')} |"
                )
            lines.append("")

        if endpoint.request_body:
            lines.extend([
                "**Request Body:**",
                "",
                "```json",
                self._format_json(endpoint.request_body),
                "```",
                "",
            ])

        if endpoint.responses:
            lines.extend([
                "**Responses:**",
                "",
            ])
            for status, description in endpoint.responses.items():
                lines.append(f"- `{status}`: {description}")
            lines.append("")

        if endpoint.code_samples:
            lines.extend(["**Code Examples:**", ""])

            for sample in endpoint.code_samples:
                lang = sample.get("language", "python")
                code = sample.get("code", "")
                lines.extend([
                    f"```{lang}",
                    code,
                    "```",
                    "",
                ])

        lines.extend(["---", ""])
        return lines

    def generate_sdk_stubs(
        self,
        endpoints: list[DocEndpoint],
        language: str = "python",
    ) -> dict[str, str]:
        """Generate SDK stubs for all endpoints."""
        if language == "python":
            return self._generate_python_stubs(endpoints)
        elif language == "typescript":
            return self._generate_typescript_stubs(endpoints)
        elif language == "go":
            return self._generate_go_stubs(endpoints)
        else:
            return {"error": f"Unsupported language: {language}"}

    def _generate_python_stubs(
        self,
        endpoints: list[DocEndpoint],
    ) -> dict[str, str]:
        """Generate Python SDK stubs."""
        lines = [
            "\"\"\"Auto-generated SDK stubs.\"\"\"",
            "",
            "import requests",
            "",
            "",
            "class APIClient:",
            "    def __init__(self, base_url: str, api_key: str = None):",
            "        self.base_url = base_url.rstrip('/')",
            "        self.api_key = api_key",
            "        self.session = requests.Session()",
            "        if api_key:",
            "            self.session.headers['Authorization'] = f'Bearer {api_key}'",
            "",
        ]

        for endpoint in endpoints:
            method = endpoint.method.lower()
            func_name = self._to_snake_case(endpoint.path.split("/")[-1])
            path_params = [p["name"] for p in endpoint.parameters if p.get("in") == "path"]
            query_params = [p["name"] for p in endpoint.parameters if p.get("in") == "query"]

            params_doc = []
            if path_params:
                params_doc.append(f"        {', '.join(path_params)}: str")
            if query_params:
                params_doc.append(f"        params: dict = {{}}")

            lines.append(f"    def {func_name}(self, {', '.join(path_params)}{', params: dict = {}' if query_params else ''}) -> dict:")
            lines.append(f'        """{endpoint.summary}"""')

            url = endpoint.path
            for p in path_params:
                url = url.replace(f"{{{p}}}", f"{{{p}}}")

            lines.append(f"        url = f'{{self.base_url}}{url}'")

            if query_params:
                lines.append(f"        if params:")
                lines.append(f"            url = f'{{url}}?{self._build_query_string(query_params)}'")

            lines.append(f"        response = self.session.{method}(url)")
            lines.append(f"        response.raise_for_status()")
            lines.append(f"        return response.json()")
            lines.append("")

        return {"sdk.py": "\n".join(lines)}

    def _generate_typescript_stubs(
        self,
        endpoints: list[DocEndpoint],
    ) -> dict[str, str]:
        """Generate TypeScript SDK stubs."""
        lines = [
            "// Auto-generated SDK stubs",
            "",
            "export class APIClient {",
            "  constructor(private baseUrl: string, private apiKey?: string) {}",
            "",
        ]

        for endpoint in endpoints:
            method = endpoint.method.lower()
            func_name = self._to_camel_case(endpoint.path.split("/")[-1])
            path_params = [p["name"] for p in endpoint.parameters if p.get("in") == "path"]

            lines.append(f"  async {func_name}({', '.join(path_params)}: string): Promise<any> {{")
            lines.append(f'    const url = `${{this.baseUrl}}{endpoint.path}`;')
            lines.append(f"    const response = await fetch(url, {{ method: '{method}' }});")
            lines.append(f"    return response.json();")
            lines.append(f"  }}")
            lines.append("")

        lines.append("}")
        return {"sdk.ts": "\n".join(lines)}

    def _generate_go_stubs(
        self,
        endpoints: list[DocEndpoint],
    ) -> dict[str, str]:
        """Generate Go SDK stubs."""
        lines = [
            "// Auto-generated SDK stubs",
            "",
            "package api",
            "",
            "type Client struct {",
            "    BaseURL string",
            "    APIKey  string",
            "}",
            "",
        ]

        for endpoint in endpoints:
            method = endpoint.method.upper()
            func_name = self._to_pascal_case(endpoint.path.split("/")[-1])
            path_params = [p["name"] for p in endpoint.parameters if p.get("in") == "path"]

            lines.append(f"func (c *Client) {func_name}({', '.join(path_params)} string) error {{")
            lines.append(f'    // {endpoint.summary}')
            lines.append(f'    return nil')
            lines.append(f"}}")
            lines.append("")

        return {"sdk.go": "\n".join(lines)}

    @staticmethod
    def _slugify(text: str) -> str:
        """Convert text to URL-safe slug."""
        return re.sub(r'[^\w\s-]', '', text).strip().lower().replace('/', '-')

    @staticmethod
    def _to_snake_case(text: str) -> str:
        """Convert text to snake_case."""
        text = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
        return re.sub('_', '_', text).lower()

    @staticmethod
    def _to_camel_case(text: str) -> str:
        """Convert text to camelCase."""
        components = text.replace('-', '_').split('_')
        return components[0] + ''.join(x.title() for x in components[1:])

    @staticmethod
    def _to_pascal_case(text: str) -> str:
        """Convert text to PascalCase."""
        components = text.replace('-', '_').split('_')
        return ''.join(x.title() for x in components)

    @staticmethod
    def _format_json(data: Any) -> str:
        """Format dict as JSON string."""
        import json
        return json.dumps(data, indent=2, default=str)

    @staticmethod
    def _build_query_string(params: list[str]) -> str:
        """Build query string from parameter names."""
        return "&".join(f"{p}={{{p}}}" for p in params)
