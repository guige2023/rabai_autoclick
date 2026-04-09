"""OpenAPI specification generation action module for RabAI AutoClick.

Provides OpenAPI operations:
- OpenAPIGeneratorAction: Generate OpenAPI 3.0 specs from API metadata
- OpenAPIValidatorAction: Validate OpenAPI specifications
- OpenAPIClientGeneratorAction: Generate client code from OpenAPI spec
- OpenAPIMergerAction: Merge multiple OpenAPI specs
- OpenAPIFilterAction: Filter paths/endpoints from OpenAPI spec
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OpenAPIGeneratorAction(BaseAction):
    """Generate OpenAPI 3.0 specification from API metadata."""
    action_type = "openapi_generator"
    display_name = "OpenAPI生成"
    description = "从API元数据生成OpenAPI 3.0规范"

    DEFAULT_CONTENT_TYPE = "application/json"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_info = params.get("api_info", {})
            paths = params.get("paths", [])
            components = params.get("components", {})
            servers = params.get("servers", [])
            if not api_info.get("title"):
                return ActionResult(success=False, message="api_info.title is required")

            spec: Dict[str, Any] = {
                "openapi": "3.0.3",
                "info": {
                    "title": api_info["title"],
                    "version": api_info.get("version", "1.0.0"),
                    "description": api_info.get("description", ""),
                    "contact": api_info.get("contact", {}),
                    "license": api_info.get("license", {}),
                },
                "paths": self._build_paths(paths),
                "components": self._build_components(components),
                "servers": [{"url": s} for s in servers] if servers else [{"url": "/"}],
                "tags": api_info.get("tags", []),
            }

            tags_used: Set[str] = set()
            for path_item in spec["paths"].values():
                for method_data in path_item.values():
                    if isinstance(method_data, dict) and "tags" in method_data:
                        tags_used.update(method_data["tags"])

            if tags_used and not spec.get("tags"):
                spec["tags"] = [{"name": t} for t in sorted(tags_used)]

            return ActionResult(
                success=True,
                message="OpenAPI spec generated",
                data={"spec": spec},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI generation failed: {e}")

    def _build_paths(self, paths: List[Dict[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for path_item in paths:
            path = path_item.get("path", "")
            if not path:
                continue
            if path not in result:
                result[path] = {}
            operations = ["get", "post", "put", "patch", "delete", "options", "head"]
            for op in operations:
                op_data = path_item.get(op)
                if op_data:
                    result[path][op] = self._build_operation(op_data)
        return result

    def _build_operation(self, op_data: Dict[str, Any]) -> Dict[str, Any]:
        operation: Dict[str, Any] = {
            "summary": op_data.get("summary", ""),
            "description": op_data.get("description", ""),
            "operationId": op_data.get("operation_id", str(uuid.uuid4())),
            "tags": op_data.get("tags", []),
            "parameters": self._build_parameters(op_data.get("parameters", [])),
            "requestBody": self._build_request_body(op_data.get("request_body")),
            "responses": self._build_responses(op_data.get("responses", {})),
            "deprecated": op_data.get("deprecated", False),
            "security": op_data.get("security", []),
        }
        return {k: v for k, v in operation.items() if v or k in ("deprecated",)}

    def _build_parameters(self, params: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "name": p["name"],
                "in": p.get("in", "query"),
                "description": p.get("description", ""),
                "required": p.get("required", False),
                "schema": p.get("schema", {"type": "string"}),
                "deprecated": p.get("deprecated", False),
            }
            for p in params
        ]

    def _build_request_body(self, rb: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rb:
            return None
        return {
            "description": rb.get("description", ""),
            "required": rb.get("required", False),
            "content": {
                self.DEFAULT_CONTENT_TYPE: {
                    "schema": rb.get("schema", {"type": "object"}),
                }
            },
        }

    def _build_responses(self, responses: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for code, resp in responses.items():
            result[str(code)] = {
                "description": resp.get("description", ""),
                "content": resp.get("content", {
                    self.DEFAULT_CONTENT_TYPE: {
                        "schema": resp.get("schema", {"type": "string"})
                    }
                }),
            }
        return result

    def _build_components(self, components: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "schemas": components.get("schemas", {}),
            "securitySchemes": components.get("security_schemes", {}),
            "parameters": components.get("parameters", {}),
            "responses": components.get("responses", {}),
        }


class OpenAPIValidatorAction(BaseAction):
    """Validate OpenAPI specifications."""
    action_type = "openapi_validator"
    display_name = "OpenAPI验证"
    description = "验证OpenAPI规范的正确性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            spec = params.get("spec", {})
            strict = params.get("strict", False)
            errors: List[str] = []
            warnings: List[str] = []

            if not spec:
                return ActionResult(success=False, message="spec is required")

            if "openapi" not in spec:
                errors.append("Missing 'openapi' field")
            elif not re.match(r"3\.\d+\.\d+", str(spec["openapi"])):
                errors.append(f"Invalid OpenAPI version: {spec['openapi']}")

            if "info" not in spec:
                errors.append("Missing 'info' field")
            else:
                info = spec["info"]
                if "title" not in info:
                    errors.append("Missing info.title")
                if "version" not in info:
                    errors.append("Missing info.version")

            if "paths" not in spec:
                errors.append("Missing 'paths' field")
            elif not spec["paths"]:
                warnings.append("No paths defined")

            paths = spec.get("paths", {})
            for path, path_item in paths.items():
                if not path.startswith("/"):
                    errors.append(f"Path must start with '/': {path}")
                for method, operation in path_item.items():
                    if method not in ("get", "post", "put", "patch", "delete", "options", "head"):
                        continue
                    if not isinstance(operation, dict):
                        continue
                    if "responses" not in operation:
                        errors.append(f"Missing responses in {method.upper()} {path}")
                    if "operationId" not in operation and strict:
                        warnings.append(f"Missing operationId in {method.upper()} {path}")

            return ActionResult(
                success=len(errors) == 0,
                message=f"Validation {'passed' if not errors else 'failed'}",
                data={"valid": len(errors) == 0, "errors": errors, "warnings": warnings},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI validation failed: {e}")


class OpenAPIClientGeneratorAction(BaseAction):
    """Generate client code from OpenAPI specification."""
    action_type = "openapi_client_generator"
    display_name = "OpenAPI客户端生成"
    description = "从OpenAPI规范生成客户端代码"

    LANGUAGE_TEMPLATES = ["python", "typescript", "go", "java"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            spec = params.get("spec", {})
            language = params.get("language", "python")
            if not spec:
                return ActionResult(success=False, message="spec is required")
            if language not in self.LANGUAGE_TEMPLATES:
                return ActionResult(success=False, message=f"Unsupported language: {language}")

            code = self._generate_client(spec, language)
            return ActionResult(
                success=True,
                message=f"OpenAPI client generated in {language}",
                data={"language": language, "code": code, "lines": len(code.splitlines())},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Client generation failed: {e}")

    def _generate_client(self, spec: Dict[str, Any], language: str) -> str:
        info = spec.get("info", {})
        title = info.get("title", "APIClient")
        version = info.get("version", "1.0.0")
        base_url = spec.get("servers", [{}])[0].get("url", "/")
        paths = spec.get("paths", {})

        if language == "python":
            return self._generate_python(title, version, base_url, paths)
        elif language == "typescript":
            return self._generate_typescript(title, version, base_url, paths)
        return f"// Generated {language} client for {title} v{version}"

    def _generate_python(self, title: str, version: str, base_url: str, paths: Dict[str, Any]) -> str:
        lines = [
            f'"""{title} API Client - Generated from OpenAPI spec."""',
            f"",
            f"import requests",
            f"",
            f"",
            f"class {title.replace(' ', '')}Client:",
            f'    """Auto-generated API client."""',
            f"",
            f"    def __init__(self, base_url: str = '{base_url}', api_key: str = ''):",
            f'        self.base_url = base_url.rstrip("/")',
            f"        self.session = requests.Session()",
            f'        if api_key:',
            f'            self.session.headers.update({{"X-API-Key": api_key}})',
            f"",
        ]
        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method in ("get", "post", "put", "patch", "delete"):
                    op_id = operation.get("operationId", method + path.replace("/", "_"))
                    summary = operation.get("summary", "")
                    lines.append(f"    def {op_id}(self, **kwargs):")
                    lines.append(f'        """{summary}"""')
                    lines.append(f'        url = f"{{self.base_url}}{path}"')
                    lines.append(f"        return self.session.{method}(url, **kwargs)")
                    lines.append(f"")
        return "\n".join(lines)

    def _generate_typescript(self, title: str, version: str, base_url: str, paths: Dict[str, Any]) -> str:
        lines = [
            f"// {title} API Client - Generated from OpenAPI spec",
            f"",
            f"export class {title.replace(' ', '')}Client {{",
            f"  constructor(private baseUrl: string = '{base_url}') {{}}",
            f"",
        ]
        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method in ("get", "post", "put", "patch", "delete"):
                    op_id = operation.get("operationId", method + path.replace("/", "_"))
                    summary = operation.get("summary", "")
                    lines.append(f"  async {op_id}(params?: any): Promise<any> {{")
                    lines.append(f'    const url = `${{this.baseUrl}}{path}`;')
                    lines.append(f'    return fetch(url, {{ method: "{method.upper()}" }}).then(r => r.json());')
                    lines.append(f"  }}")
                    lines.append(f"")
        lines.append(f"}}")
        return "\n".join(lines)


class OpenAPIMergerAction(BaseAction):
    """Merge multiple OpenAPI specifications."""
    action_type = "openapi_merger"
    display_name = "OpenAPI合并"
    description = "合并多个OpenAPI规范为一个"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            specs = params.get("specs", [])
            if len(specs) < 2:
                return ActionResult(success=False, message="At least 2 specs required")

            merged: Dict[str, Any] = {
                "openapi": "3.0.3",
                "info": {"title": "Merged API", "version": "1.0.0"},
                "paths": {},
                "components": {"schemas": {}, "securitySchemes": {}},
            }
            for spec in specs:
                merged["paths"].update(spec.get("paths", {}))
                for comp_type in ("schemas", "securitySchemes"):
                    if comp_type in spec.get("components", {}):
                        merged["components"][comp_type].update(spec["components"][comp_type])
                if "info" in spec and "title" in spec["info"]:
                    merged["info"]["title"] = spec["info"]["title"]

            return ActionResult(
                success=True,
                message=f"Merged {len(specs)} specs into one",
                data={"merged": merged, "path_count": len(merged["paths"])},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI merge failed: {e}")


class OpenAPIFilterAction(BaseAction):
    """Filter paths/endpoints from OpenAPI spec."""
    action_type = "openapi_filter"
    display_name = "OpenAPI过滤"
    description = "从OpenAPI规范中过滤特定路径/端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            spec = params.get("spec", {})
            include_paths = params.get("include_paths", [])
            exclude_paths = params.get("exclude_paths", [])
            include_tags = params.get("include_tags", [])

            if not spec:
                return ActionResult(success=False, message="spec is required")

            filtered_spec = json.loads(json.dumps(spec))
            filtered_paths: Dict[str, Any] = {}

            for path, path_item in spec.get("paths", {}).items():
                should_include = True
                if include_paths and not any(self._match_pattern(path, p) for p in include_paths):
                    should_include = False
                if exclude_paths and any(self._match_pattern(path, p) for p in exclude_paths):
                    should_include = False
                if include_tags:
                    tagged_methods = [op for op in path_item.values() if isinstance(op, dict) and "tags" in op]
                    if tagged_methods and not any(tag in include_tags for tag in tagged_methods[0].get("tags", [])):
                        should_include = False
                if should_include:
                    filtered_paths[path] = path_item

            filtered_spec["paths"] = filtered_paths
            return ActionResult(
                success=True,
                message=f"Filtered spec: {len(filtered_paths)} paths retained",
                data={"spec": filtered_spec, "path_count": len(filtered_paths)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPI filter failed: {e}")

    def _match_pattern(self, path: str, pattern: str) -> bool:
        if pattern.endswith("*"):
            return path.startswith(pattern[:-1])
        return path == pattern
