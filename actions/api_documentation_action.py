"""API documentation action module for RabAI AutoClick.

Provides API documentation operations:
- DocGeneratorAction: Generate API documentation
- DocPublisherAction: Publish documentation
- DocSearcherAction: Search documentation
- OpenAPIExporterAction: Export OpenAPI specifications
"""

import sys
import os
import logging
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class EndpointDoc:
    """Documentation for an API endpoint."""
    path: str
    method: str
    summary: str = ""
    description: str = ""
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False


@dataclass
class APIDoc:
    """Complete API documentation."""
    title: str
    version: str
    description: str = ""
    endpoints: List[EndpointDoc] = field(default_factory=list)
    schemas: Dict[str, Any] = field(default_factory=dict)
    generated_at: datetime = field(default_factory=datetime.now)


class DocGenerator:
    """Generates API documentation."""

    def __init__(self) -> None:
        self._docs: Dict[str, APIDoc] = {}

    def create_doc(self, title: str, version: str, description: str = "") -> APIDoc:
        doc = APIDoc(title=title, version=version, description=description)
        self._docs[title] = doc
        return doc

    def add_endpoint(self, doc_title: str, endpoint: EndpointDoc) -> bool:
        doc = self._docs.get(doc_title)
        if not doc:
            return False
        doc.endpoints.append(endpoint)
        return True

    def add_schema(self, doc_title: str, name: str, schema: Dict[str, Any]) -> bool:
        doc = self._docs.get(doc_title)
        if not doc:
            return False
        doc.schemas[name] = schema
        return True

    def get_doc(self, title: str) -> Optional[APIDoc]:
        return self._docs.get(title)

    def to_markdown(self, doc: APIDoc) -> str:
        lines = [f"# {doc.title}", f"**Version:** {doc.version}", "", doc.description, ""]

        for tag in sorted(set(e.tags[0] if e.tags else "General" for e in doc.endpoints)):
            lines.append(f"## {tag}")
            lines.append("")
            for endpoint in doc.endpoints:
                if (tag == "General" and not endpoint.tags) or (endpoint.tags and endpoint.tags[0] == tag):
                    lines.append(f"### `{endpoint.method.upper()} {endpoint.path}`")
                    lines.append(f"**{endpoint.summary}**" if endpoint.summary else "")
                    if endpoint.description:
                        lines.append(endpoint.description)
                    lines.append("")

                    if endpoint.parameters:
                        lines.append("**Parameters:**")
                        lines.append("| Name | Type | Required | Description |")
                        lines.append("|------|------|----------|-------------|")
                        for p in endpoint.parameters:
                            lines.append(f"| {p.get('name', '')} | {p.get('type', '')} | {p.get('required', False)} | {p.get('description', '')} |")
                        lines.append("")

                    if endpoint.responses:
                        lines.append("**Responses:**")
                        for code, resp in endpoint.responses.items():
                            lines.append(f"- `{code}`: {resp.get('description', '')}")
                        lines.append("")

        return "\n".join(lines)


_generator = DocGenerator()
_docs_storage: Dict[str, str] = {}


class DocGeneratorAction(BaseAction):
    """Generate API documentation."""
    action_type = "api_doc_generator"
    display_name = "API文档生成器"
    description = "生成API接口文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "create")
        doc_title = params.get("title", "API Documentation")
        version = params.get("version", "1.0.0")
        description = params.get("description", "")

        if operation == "create":
            doc = _generator.create_doc(doc_title, version, description)
            _docs_storage[doc_title] = json.dumps({
                "title": doc.title,
                "version": doc.version,
                "description": doc.description,
                "endpoints": [],
                "schemas": {}
            })
            return ActionResult(
                success=True,
                message=f"文档 {doc_title} 已创建",
                data={"title": doc_title, "version": version}
            )

        if operation == "add_endpoint":
            endpoint = EndpointDoc(
                path=params.get("path", "/"),
                method=params.get("method", "GET"),
                summary=params.get("summary", ""),
                description=params.get("description", ""),
                parameters=params.get("parameters", []),
                responses=params.get("responses", {}),
                tags=params.get("tags", [])
            )
            if _generator.add_endpoint(doc_title, endpoint):
                return ActionResult(success=True, message=f"端点已添加: {endpoint.method} {endpoint.path}")
            return ActionResult(success=False, message=f"文档 {doc_title} 不存在")

        if operation == "get":
            doc = _generator.get_doc(doc_title)
            if not doc:
                return ActionResult(success=False, message=f"文档 {doc_title} 不存在")
            return ActionResult(
                success=True,
                message=f"文档: {doc.title} v{doc.version}",
                data={
                    "title": doc.title,
                    "version": doc.version,
                    "description": doc.description,
                    "endpoint_count": len(doc.endpoints)
                }
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class DocPublisherAction(BaseAction):
    """Publish documentation."""
    action_type = "api_doc_publisher"
    display_name = "API文档发布器"
    description = "发布API文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "publish")
        doc_title = params.get("title", "")
        format_type = params.get("format", "markdown")

        if not doc_title:
            return ActionResult(success=False, message="title是必需的")

        doc = _generator.get_doc(doc_title)
        if not doc:
            return ActionResult(success=False, message=f"文档 {doc_title} 不存在")

        if operation == "publish":
            markdown = _generator.to_markdown(doc)
            return ActionResult(
                success=True,
                message=f"文档已发布 ({len(markdown)} 字符)",
                data={"markdown": markdown, "format": "markdown"}
            )

        if operation == "export_html":
            markdown = _generator.to_markdown(doc)
            html = f"<html><head><title>{doc.title}</title></head><body><pre>{markdown}</pre></body></html>"
            return ActionResult(
                success=True,
                message="HTML已导出",
                data={"html": html}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class DocSearcherAction(BaseAction):
    """Search documentation."""
    action_type = "api_doc_searcher"
    display_name = "API文档搜索器"
    description = "搜索API文档"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "")
        doc_title = params.get("title", "")

        if not query:
            return ActionResult(success=False, message="query是必需的")

        results = []
        if doc_title:
            doc = _generator.get_doc(doc_title)
            if doc:
                for endpoint in doc.endpoints:
                    if query.lower() in endpoint.path.lower() or query.lower() in endpoint.summary.lower():
                        results.append({
                            "path": endpoint.path,
                            "method": endpoint.method,
                            "summary": endpoint.summary
                        })
        else:
            for doc in _generator._docs.values():
                for endpoint in doc.endpoints:
                    if query.lower() in endpoint.path.lower() or query.lower() in endpoint.summary.lower():
                        results.append({
                            "doc": doc.title,
                            "path": endpoint.path,
                            "method": endpoint.method,
                            "summary": endpoint.summary
                        })

        return ActionResult(
            success=True,
            message=f"找到 {len(results)} 个匹配",
            data={"results": results, "count": len(results)}
        )


class OpenAPIExporterAction(BaseAction):
    """Export OpenAPI specifications."""
    action_type = "api_openapi_exporter"
    display_name = "OpenAPI导出器"
    description = "导出OpenAPI规范"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        doc_title = params.get("title", "")

        if not doc_title:
            return ActionResult(success=False, message="title是必需的")

        doc = _generator.get_doc(doc_title)
        if not doc:
            return ActionResult(success=False, message=f"文档 {doc_title} 不存在")

        spec: Dict[str, Any] = {
            "openapi": "3.0.0",
            "info": {
                "title": doc.title,
                "version": doc.version,
                "description": doc.description
            },
            "paths": {}
        }

        for endpoint in doc.endpoints:
            path_item = spec["paths"].get(endpoint.path, {})
            method_lower = endpoint.method.lower()
            path_item[method_lower] = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "parameters": endpoint.parameters,
                "responses": endpoint.responses,
                "tags": endpoint.tags
            }
            spec["paths"][endpoint.path] = path_item

        if doc.schemas:
            spec["components"] = {"schemas": doc.schemas}

        return ActionResult(
            success=True,
            message=f"OpenAPI规范已导出 ({len(json.dumps(spec))} 字节)",
            data={"openapi": spec}
        )
