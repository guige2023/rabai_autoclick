"""API discovery and documentation action module for RabAI AutoClick.

Provides API discovery operations:
- ApiDiscoveryAction: Discover API endpoints
- OpenAPIParserAction: Parse OpenAPI specifications
- ApiDocumentationAction: Generate API documentation
- EndpointTesterAction: Test API endpoints
"""

import json
import re
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiDiscoveryAction(BaseAction):
    """Discover API endpoints."""
    action_type = "api_discovery"
    display_name = "API发现"
    description = "发现API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            include_paths = params.get("include_paths", ["/api", "/v1", "/v2"])
            exclude_paths = params.get("exclude_paths", ["/health", "/metrics"])
            follow_redirects = params.get("follow_redirects", True)

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            discovered: List[Dict] = []
            parsed = urlparse(base_url)
            base_host = f"{parsed.scheme}://{parsed.netloc}"

            common_paths = [
                "/api",
                "/api/v1",
                "/api/v2",
                "/api/users",
                "/api/products",
                "/api/health",
                "/api/docs",
                "/swagger",
                "/openapi.json",
            ]

            for path in common_paths:
                should_include = any(path.startswith(ip) for ip in include_paths)
                should_exclude = any(path.startswith(ep) for ep in exclude_paths)

                if should_include and not should_exclude:
                    discovered.append({
                        "path": path,
                        "url": f"{base_host}{path}",
                        "method": "GET",
                        "discovered_by": "path_scan",
                    })

            return ActionResult(
                success=True,
                message=f"Discovered {len(discovered)} endpoints",
                data={
                    "endpoints": discovered,
                    "endpoint_count": len(discovered),
                    "base_url": base_url,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ApiDiscovery error: {e}")


class OpenAPIParserAction(BaseAction):
    """Parse OpenAPI specifications."""
    action_type = "openapi_parser"
    display_name = "OpenAPI解析"
    description = "解析OpenAPI规范"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            spec_source = params.get("spec_source", "")
            extract_paths = params.get("extract_paths", True)
            extract_schemas = params.get("extract_schemas", True)
            extract_operations = params.get("extract_operations", True)

            if not spec_source:
                return ActionResult(success=False, message="spec_source is required")

            if spec_source.startswith("http"):
                import urllib.request
                with urllib.request.urlopen(spec_source, timeout=30) as resp:
                    spec = json.loads(resp.read().decode("utf-8"))
            else:
                with open(spec_source, "r") as f:
                    spec = json.load(f)

            result = {
                "info": spec.get("info", {}),
                "servers": spec.get("servers", []),
            }

            paths = spec.get("paths", {})
            if extract_paths:
                path_summary = []
                for path, methods in paths.items():
                    path_info = {"path": path, "methods": list(methods.keys())}
                    if extract_operations:
                        for method, details in methods.items():
                            if method.upper() in ("GET", "POST", "PUT", "DELETE", "PATCH"):
                                path_info.setdefault("operations", []).append({
                                    "method": method.upper(),
                                    "operationId": details.get("operationId"),
                                    "summary": details.get("summary"),
                                    "parameters": len(details.get("parameters", [])),
                                    "responses": list(details.get("responses", {}).keys()),
                                })
                    path_summary.append(path_info)
                result["paths"] = path_summary
                result["path_count"] = len(path_summary)

            schemas = spec.get("components", {}).get("schemas", {})
            if extract_schemas and schemas:
                schema_summary = []
                for name, schema in schemas.items():
                    schema_summary.append({
                        "name": name,
                        "type": schema.get("type", "object"),
                        "properties": list(schema.get("properties", {}).keys()) if "properties" in schema else None,
                        "required": schema.get("required", []),
                    })
                result["schemas"] = schema_summary
                result["schema_count"] = len(schema_summary)

            return ActionResult(
                success=True,
                message=f"OpenAPI parsed: {result.get('path_count', 0)} paths, {result.get('schema_count', 0)} schemas",
                data=result,
            )
        except Exception as e:
            return ActionResult(success=False, message=f"OpenAPIParser error: {e}")


class ApiDocumentationAction(BaseAction):
    """Generate API documentation."""
    action_type = "api_documentation"
    display_name = "API文档生成"
    description = "生成API文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            format = params.get("format", "markdown")
            include_examples = params.get("include_examples", True)

            if not endpoints:
                return ActionResult(success=False, message="endpoints is required")

            if format == "markdown":
                doc = self._generate_markdown(endpoints, include_examples)
            elif format == "html":
                doc = self._generate_html(endpoints, include_examples)
            elif format == "openapi":
                doc = self._generate_openapi(endpoints)
            else:
                doc = self._generate_text(endpoints)

            return ActionResult(
                success=True,
                message=f"API documentation generated ({format}) for {len(endpoints)} endpoints",
                data={"documentation": doc, "endpoint_count": len(endpoints), "format": format},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ApiDocumentation error: {e}")

    def _generate_markdown(self, endpoints: List[Dict], include_examples: bool) -> str:
        lines = ["# API Documentation\n"]
        for ep in endpoints:
            path = ep.get("path", "/")
            method = ep.get("method", "GET")
            summary = ep.get("summary", "No description")
            params = ep.get("parameters", [])
            responses = ep.get("responses", [])

            lines.append(f"## {method.upper()} {path}\n")
            lines.append(f"**Summary:** {summary}\n")
            if params:
                lines.append("**Parameters:**\n")
                lines.append("| Name | Type | Required | Description |\n")
                lines.append("|------|------|----------|-------------|\n")
                for p in params:
                    lines.append(f"| {p.get('name', '')} | {p.get('type', 'string')} | {p.get('required', False)} | {p.get('description', '')} |\n")
            if responses:
                lines.append(f"**Responses:** {', '.join(responses)}\n")
            lines.append("\n")
        return "\n".join(lines)

    def _generate_html(self, endpoints: List[Dict], include_examples: bool) -> str:
        lines = ["<!DOCTYPE html><html><head><title>API Documentation</title></head><body>", "<h1>API Documentation</h1>"]
        for ep in endpoints:
            path = ep.get("path", "/")
            method = ep.get("method", "GET").upper()
            summary = ep.get("summary", "No description")
            lines.append(f"<div><h2>{method} {path}</h2><p>{summary}</p></div>")
        lines.append("</body></html>")
        return "\n".join(lines)

    def _generate_openapi(self, endpoints: List[Dict]) -> Dict:
        return {
            "openapi": "3.0.0",
            "info": {"title": "Generated API", "version": "1.0.0"},
            "paths": {ep.get("path", "/"): {ep.get("method", "get").lower(): {}} for ep in endpoints},
        }

    def _generate_text(self, endpoints: List[Dict]) -> str:
        lines = ["API DOCUMENTATION", "=" * 50, ""]
        for ep in endpoints:
            lines.append(f"{ep.get('method', 'GET')} {ep.get('path', '/')}: {ep.get('summary', '')}")
        return "\n".join(lines)


class EndpointTesterAction(BaseAction):
    """Test API endpoints."""
    action_type = "endpoint_tester"
    display_name = "端点测试"
    description = "测试API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoints = params.get("endpoints", [])
            timeout = params.get("timeout", 10)
            expected_status = params.get("expected_status", 200)

            if not endpoints:
                return ActionResult(success=False, message="endpoints is required")

            results = []
            for ep in endpoints:
                url = ep.get("url") or ep.get("path")
                method = ep.get("method", "GET").upper()
                headers = ep.get("headers", {})
                body = ep.get("body")

                result = {
                    "url": url,
                    "method": method,
                    "status": None,
                    "success": False,
                    "latency_ms": None,
                    "error": None,
                }

                try:
                    import urllib.request
                    import time

                    req = urllib.request.Request(url, method=method)
                    for k, v in headers.items():
                        req.add_header(k, v)

                    start = time.time()
                    with urllib.request.urlopen(req, timeout=timeout) as resp:
                        result["status"] = resp.status
                        result["latency_ms"] = int((time.time() - start) * 1000)
                        result["success"] = resp.status == expected_status
                except Exception as e:
                    result["error"] = str(e)

                results.append(result)

            passed = sum(1 for r in results if r["success"])
            failed = len(results) - passed

            return ActionResult(
                success=failed == 0,
                message=f"Endpoint tests: {passed}/{len(results)} passed",
                data={
                    "results": results,
                    "passed": passed,
                    "failed": failed,
                    "total": len(results),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EndpointTester error: {e}")
