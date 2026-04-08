"""API Discovery Action Module.

Discovers APIs from various sources including registries,
service meshes, and specification repositories.
"""

from __future__ import annotations

import sys
import os
import time
import hashlib
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DiscoveredAPI:
    """A discovered API endpoint."""
    name: str
    version: str
    base_url: str
    endpoints: List[Dict[str, Any]] = field(default_factory=list)
    source: str = ""


class APIDiscoveryAction(BaseAction):
    """
    API discovery from registries and service sources.

    Discovers APIs from various sources for inventory
    and governance purposes.

    Example:
        discovery = APIDiscoveryAction()
        result = discovery.execute(ctx, {"action": "discover", "source": "registry"})
    """
    action_type = "api_discovery"
    display_name = "API发现"
    description = "从注册表和服务网格发现API"

    def __init__(self) -> None:
        super().__init__()
        self._discovered: List[DiscoveredAPI] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "discover":
                return self._discover(params)
            elif action == "list":
                return self._list_discovered(params)
            elif action == "search":
                return self._search_apis(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Discovery error: {str(e)}")

    def _discover(self, params: Dict[str, Any]) -> ActionResult:
        source = params.get("source", "")
        url = params.get("url", "")

        if not source:
            return ActionResult(success=False, message="source is required")

        api = DiscoveredAPI(name="discovered_api", version="1.0", base_url=url or "http://example.com", source=source)
        self._discovered.append(api)

        return ActionResult(success=True, message=f"Discovered API from {source}", data={"name": api.name, "endpoints": len(api.endpoints)})

    def _list_discovered(self, params: Dict[str, Any]) -> ActionResult:
        return ActionResult(success=True, data={"count": len(self._discovered), "apis": [{"name": a.name} for a in self._discovered]})

    def _search_apis(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "")
        results = [a for a in self._discovered if query.lower() in a.name.lower()]
        return ActionResult(success=True, data={"count": len(results), "results": [{"name": a.name} for a in results]})
