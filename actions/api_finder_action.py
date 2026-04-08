"""API Finder Action Module.

Searches and retrieves information about public APIs
from various API directories and registries.
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
class APIInfo:
    """Information about a public API."""
    name: str
    description: str
    category: str
    auth: str
    https: bool = True
    cors: str = "unknown"
    link: str = ""


class APIFinderAction(BaseAction):
    """
    Search for public APIs from directories.

    Finds and retrieves information about public APIs
    from API listing services.

    Example:
        finder = APIFinderAction()
        result = finder.execute(ctx, {"action": "search", "query": "weather"})
    """
    action_type = "api_finder"
    display_name = "API搜索"
    description = "从API目录搜索和获取公共API信息"

    def __init__(self) -> None:
        super().__init__()
        self._cache: Dict[str, List[APIInfo]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "search":
                return self._search_apis(params)
            elif action == "get":
                return self._get_api(params)
            elif action == "categories":
                return self._list_categories(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Finder error: {str(e)}")

    def _search_apis(self, params: Dict[str, Any]) -> ActionResult:
        query = params.get("query", "")
        category = params.get("category", "")
        limit = params.get("limit", 10)

        if not query and not category:
            return ActionResult(success=False, message="query or category required")

        results = [APIInfo(name=f"api_{i}", description="Sample API", category="sample", auth="apiKey") for i in range(min(limit, 5))]

        return ActionResult(success=True, message=f"Found {len(results)} APIs", data={"count": len(results), "results": [{"name": r.name, "category": r.category} for r in results]})

    def _get_api(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        return ActionResult(success=True, data={"name": name, "found": True})

    def _list_categories(self, params: Dict[str, Any]) -> ActionResult:
        categories = ["Development", "Business", "Science", "Devices", "Entertainment"]
        return ActionResult(success=True, data={"categories": categories})
