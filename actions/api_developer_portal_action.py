"""
API Developer Portal Action Module.

Generates developer portal content: API documentation, SDK code,
interactive examples, and usage guides.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class PortalContent:
    """Generated portal content."""
    docs: str
    sdk_code: str
    examples: list[dict[str, Any]]
    guides: list[str]


class APIDeveloperPortalAction(BaseAction):
    """Generate developer portal content."""

    def __init__(self) -> None:
        super().__init__("api_developer_portal")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Generate developer portal content.

        Args:
            context: Execution context
            params: Parameters:
                - api_name: Name of the API
                - endpoints: List of endpoint definitions
                - language: SDK language (python, javascript, go, etc.)
                - include_auth: Include authentication examples
                - include_errors: Include error handling examples

        Returns:
            PortalContent with generated documentation and SDK code
        """
        api_name = params.get("api_name", "MyAPI")
        endpoints = params.get("endpoints", [])
        language = params.get("language", "python")
        include_auth = params.get("include_auth", True)
        include_errors = params.get("include_errors", True)

        docs = self._generate_markdown_docs(api_name, endpoints)
        sdk_code = self._generate_sdk_code(api_name, endpoints, language)
        examples = self._generate_examples(api_name, endpoints, language, include_auth, include_errors)
        guides = self._generate_guides(api_name, endpoints)

        return PortalContent(
            docs=docs,
            sdk_code=sdk_code,
            examples=examples,
            guides=guides
        ).__dict__

    def _generate_markdown_docs(self, api_name: str, endpoints: list[dict]) -> str:
        """Generate Markdown API documentation."""
        lines = [f"# {api_name} API Documentation\n\n"]
        for ep in endpoints:
            method = ep.get("method", "GET").upper()
            path = ep.get("path", "")
            summary = ep.get("summary", "")
            lines.append(f"## {method} {path}\n\n{summary}\n\n")
            if ep.get("parameters"):
                lines.append("### Parameters\n\n| Name | Type | Description |\n|------|------|-------------|\n")
                for p in ep.get("parameters", []):
                    lines.append(f"| {p.get('name','')} | {p.get('type','')} | {p.get('description','')} |\n")
            lines.append("\n")
        return "".join(lines)

    def _generate_sdk_code(self, api_name: str, endpoints: list[dict], language: str) -> str:
        """Generate SDK code."""
        if language == "python":
            lines = [f"class {api_name}Client:\n", "    def __init__(self, base_url: str, api_key: str):\n", "        self.base_url = base_url\n", "        self.api_key = api_key\n\n"]
            for ep in endpoints:
                method = ep.get("method", "get").lower()
                path = ep.get("path", "").replace("/", "_").strip("_")
                summary = ep.get("summary", "")
                lines.append(f"    def {path}(self, **kwargs):\n        \"{summary}\"\n        return self._request('{ep.get('method','GET')}', '{ep.get('path','')}', **kwargs)\n\n")
            return "".join(lines)
        return f"// {language} SDK for {api_name}"

    def _generate_examples(self, api_name: str, endpoints: list[dict], language: str, include_auth: bool, include_errors: bool) -> list[dict[str, Any]]:
        """Generate code examples."""
        examples = []
        for ep in endpoints:
            examples.append({
                "title": f"{ep.get('method')} {ep.get('path')}",
                "description": ep.get("summary", ""),
                "code": f"# Example: {ep.get('method')} {ep.get('path')}\n"
            })
        return examples

    def _generate_guides(self, api_name: str, endpoints: list[dict]) -> list[str]:
        """Generate usage guides."""
        return [
            f"Getting Started with {api_name}",
            f"Authentication Guide",
            f"Rate Limiting and Best Practices",
            f"Error Handling in {api_name}",
            f"Webhooks Integration"
        ]
