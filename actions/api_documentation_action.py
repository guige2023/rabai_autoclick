"""API Documentation Action Module. Generates OpenAPI specs and docs."""
import sys, os, json
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class EndpointSpec:
    path: str; method: str; summary: str; description: str
    parameters: list = field(default_factory=list)
    request_body: Optional[dict] = None; responses: dict = field(default_factory=dict)
    tags: list = field(default_factory=list); deprecated: bool = False

class APIDocumentationAction(BaseAction):
    action_type = "api_documentation"; display_name = "API文档生成"
    description = "生成OpenAPI规范"
    def __init__(self) -> None:
        super().__init__(); self._endpoints = []; self._version = "1.0.0"; self._title = "My API"
    def _to_openapi(self) -> dict:
        paths = {}
        for ep in self._endpoints:
            if ep.path not in paths: paths[ep.path] = {}
            paths[ep.path][ep.method.lower()] = {"summary": ep.summary, "description": ep.description,
                "parameters": ep.parameters, "responses": ep.responses, "tags": ep.tags, "deprecated": ep.deprecated}
            if ep.request_body: paths[ep.path][ep.method.lower()]["requestBody"] = ep.request_body
        return {"openapi": "3.0.0", "info": {"title": self._title, "version": self._version}, "paths": paths}
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "generate")
        if mode == "set_info":
            self._title = params.get("title", self._title)
            self._version = params.get("version", self._version)
            return ActionResult(success=True, message=f"API info: {self._title} v{self._version}")
        if mode == "register":
            endpoint = EndpointSpec(path=params.get("path","/"), method=params.get("method","GET").upper(),
                                   summary=params.get("summary",""), description=params.get("description",""),
                                   parameters=params.get("parameters",[]), request_body=params.get("request_body"),
                                   responses=params.get("responses",{}), tags=params.get("tags",[]),
                                   deprecated=params.get("deprecated", False))
            self._endpoints = [e for e in self._endpoints if not (e.path==endpoint.path and e.method==endpoint.method)]
            self._endpoints.append(endpoint)
            return ActionResult(success=True, message=f"Registered {endpoint.method} {endpoint.path}")
        spec = self._to_openapi()
        output_file = params.get("output_file")
        if output_file:
            try:
                with open(output_file, "w") as f: json.dump(spec, f, indent=2)
                return ActionResult(success=True, message=f"Written to {output_file}", data={"spec": spec})
            except Exception as e: return ActionResult(success=False, message=f"Write failed: {e}")
        return ActionResult(success=True, message=f"Spec with {len(self._endpoints)} endpoints", data={"spec": spec})
