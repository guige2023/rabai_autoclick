"""API Finder Action Module. Discovers APIs from registries."""
import sys, os
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class APIDiscovery:
    name: str; description: str; base_url: str; category: str
    tags: list = field(default_factory=list); docs_url: Optional[str] = None
    swagger_url: Optional[str] = None; auth_type: str = "none"

class APIFinderAction(BaseAction):
    action_type = "api_finder"; display_name = "API发现"
    description = "发现和搜索API"
    def __init__(self) -> None: super().__init__(); self._registry = []
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "search"); limit = params.get("limit", 20)
        if mode == "register":
            api = APIDiscovery(name=params.get("name","Unknown"), description=params.get("description",""),
                              base_url=params.get("base_url",""), category=params.get("category","general"),
                              tags=params.get("tags",[]), docs_url=params.get("docs_url"),
                              swagger_url=params.get("swagger_url"), auth_type=params.get("auth_type","none"))
            self._registry.append(api)
            return ActionResult(success=True, message=f"Registered API: {api.name}")
        if mode == "list":
            return ActionResult(success=True, message=f"{len(self._registry)} APIs", data={"apis": [vars(a) for a in self._registry]})
        query = params.get("query","").lower(); category = params.get("category"); tags_f = set(params.get("tags",[]))
        auth_type = params.get("auth_type")
        results = []
        for api in self._registry:
            if query and query not in api.name.lower() and query not in api.description.lower():
                if not any(query in t.lower() for t in api.tags): continue
            if category and api.category != category: continue
            if tags_f and not tags_f.issubset(set(api.tags)): continue
            if auth_type and api.auth_type != auth_type: continue
            results.append(vars(api))
            if len(results) >= limit: break
        return ActionResult(success=True, message=f"Found {len(results)} APIs", data={"results": results, "count": len(results)})
