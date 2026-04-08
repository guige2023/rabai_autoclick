"""API Developer Portal Action Module. Generates developer portal content."""
import sys, os, json
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class EndpointDoc:
    path: str; method: str; summary: str; description: str
    parameters: list = field(default_factory=list)
    samples: list = field(default_factory=list)

class APIDeveloperPortalAction(BaseAction):
    action_type = "api_developer_portal"; display_name = "开发者门户"
    description = "生成开发者门户"
    def __init__(self) -> None: super().__init__(); self._endpoints = []
    def _generate_samples(self, endpoint: EndpointDoc) -> list:
        codes = {"python": f'import requests\\nresp = requests.{endpoint.method.lower()}("{endpoint.path}")\\nprint(resp.json())',
                 "curl": f'curl -X {endpoint.method} "{endpoint.path}"',
                 "javascript": f'const resp = await fetch("{endpoint.path}");\\nconst data = await resp.json();'}
        return [{"language": lang, "code": code, "description": f"{endpoint.method} in {lang}"}
                for lang, code in codes.items()]
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "generate")
        if mode == "register":
            ep = EndpointDoc(path=params.get("path","/"), method=params.get("method","GET").upper(),
                            summary=params.get("summary",""), description=params.get("description",""),
                            parameters=params.get("parameters",[]))
            ep.samples = self._generate_samples(ep)
            self._endpoints.append(ep)
            return ActionResult(success=True, message=f"Registered {ep.method} {ep.path}")
        sections = ["# API Developer Portal", "", f"This API provides {len(self._endpoints)} endpoints.", ""]
        sections.append("## Endpoints")
        for ep in self._endpoints:
            sections.append(f"### {ep.method} {ep.path}")
            sections.append(f"**{ep.summary}**")
            if ep.description: sections.append(ep.description)
            if ep.parameters:
                sections.append("\n**Parameters:**")
                for p in ep.parameters: sections.append(f"- `{p.get('name','param')}`: {p.get('description','')}")
            sections.append("")
            if ep.samples:
                sections.append("**Code Samples:**")
                for s in ep.samples:
                    sections.append(f"\n**{s['language'].title()}:**")
                    sections.append(f"```\\n{s['code']}\\n```")
            sections.append("")
        return ActionResult(success=True, message=f"Portal: {len(self._endpoints)} endpoints",
                          data={"content": "\n".join(sections), "endpoints": len(self._endpoints)})
