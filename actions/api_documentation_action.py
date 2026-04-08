"""API Documentation Action Module.

Manages API documentation generation, versioning,
publishing, and interactive API explorer functionality.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DocFormat(Enum):
    """Documentation format types."""
    OPENAPI = "openapi"
    MARKDOWN = "markdown"
    HTML = "html"
    REDOC = "redoc"
    SWAGGER_UI = "swagger_ui"


@dataclass
class DocSection:
    """A documentation section."""
    section_id: str
    title: str
    content: str
    order: int = 0
    children: List["DocSection"] = field(default_factory=list)


class APIDocumentationAction(BaseAction):
    """
    API documentation generation and management.

    Generates documentation from API specs, manages versions,
    and provides interactive documentation features.

    Example:
        doc = APIDocumentationAction()
        result = doc.execute(ctx, {"action": "generate", "api_id": "my-api"})
    """
    action_type = "api_documentation"
    display_name = "API文档管理"
    description = "API文档生成、版本管理和发布"

    def __init__(self) -> None:
        super().__init__()
        self._docs: Dict[str, Dict[str, Any]] = {}
        self._specs: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "generate":
                return self._generate_docs(params)
            elif action == "publish":
                return self._publish_docs(params)
            elif action == "get_section":
                return self._get_section(params)
            elif action == "list_docs":
                return self._list_docs(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Doc error: {str(e)}")

    def _generate_docs(self, params: Dict[str, Any]) -> ActionResult:
        api_id = params.get("api_id", "")
        spec = params.get("spec", {})
        format_str = params.get("format", "markdown")

        if not api_id:
            return ActionResult(success=False, message="api_id is required")

        doc_id = f"{api_id}_{int(time.time())}"
        content = self._generate_content(spec, format_str)

        self._docs[doc_id] = {
            "doc_id": doc_id,
            "api_id": api_id,
            "format": format_str,
            "content": content,
            "created_at": time.time(),
        }

        return ActionResult(success=True, message=f"Docs generated: {doc_id}", data={"doc_id": doc_id})

    def _publish_docs(self, params: Dict[str, Any]) -> ActionResult:
        doc_id = params.get("doc_id", "")
        if doc_id not in self._docs:
            return ActionResult(success=False, message=f"Doc not found: {doc_id}")
        return ActionResult(success=True, message=f"Docs published: {doc_id}")

    def _get_section(self, params: Dict[str, Any]) -> ActionResult:
        doc_id = params.get("doc_id", "")
        section_id = params.get("section_id", "")
        return ActionResult(success=True, data={"doc_id": doc_id, "section_id": section_id})

    def _list_docs(self, params: Dict[str, Any]) -> ActionResult:
        return ActionResult(success=True, data={"docs": list(self._docs.keys())})

    def _generate_content(self, spec: Dict[str, Any], format_str: str) -> str:
        if format_str == "markdown":
            lines = [f"# API Documentation\n", f"## Endpoints\n"]
            for path in spec.get("paths", {}):
                for method in spec["paths"][path]:
                    lines.append(f"- **{method.upper()}** `{path}`")
            return "\n".join(lines)
        return json.dumps(spec, indent=2)
