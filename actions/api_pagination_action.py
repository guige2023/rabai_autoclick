"""API Pagination Action Module.

Handles API pagination with various strategies including
cursor-based, offset-based, and page-based pagination.
"""

from __future__ import annotations

import sys
import os
import time
from typing import Any, Dict, List, Optional, Iterator
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PaginationType(Enum):
    """Pagination types."""
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"
    TIME_BASED = "time_based"


from enum import Enum


@dataclass
class PageResult:
    """Result of a page fetch."""
    items: List[Any]
    page: int = 0
    page_size: int = 0
    total: Optional[int] = None
    has_next: bool = False
    has_prev: bool = False
    next_cursor: Optional[str] = None


class APIPaginationAction(BaseAction):
    """
    API pagination handling.

    Implements offset, cursor, page, and time-based
    pagination for API requests.

    Example:
        pager = APIPaginationAction()
        result = pager.execute(ctx, {"action": "get_page", "items": [...], "page": 1, "page_size": 20})
    """
    action_type = "api_pagination"
    display_name = "API分页处理"
    description = "API分页：偏移、游标、页面和时间分页"

    def __init__(self) -> None:
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "get_page":
                return self._get_page(params)
            elif action == "get_all_pages":
                return self._get_all_pages(params)
            elif action == "next_page":
                return self._next_page(params)
            elif action == "build_pagination_params":
                return self._build_pagination_params(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pagination error: {str(e)}")

    def _get_page(self, params: Dict[str, Any]) -> ActionResult:
        items = params.get("items", [])
        page = params.get("page", 1)
        page_size = params.get("page_size", 20)
        total = params.get("total")

        if not isinstance(items, list):
            items = list(items)

        total_items = total if total is not None else len(items)
        total_pages = max(1, (total_items + page_size - 1) // page_size)

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_items = items[start_idx:end_idx]

        return ActionResult(success=True, message=f"Page {page}/{total_pages}", data={"items": page_items, "page": page, "page_size": page_size, "total": total_items, "total_pages": total_pages, "has_next": page < total_pages, "has_prev": page > 1})

    def _get_all_pages(self, params: Dict[str, Any]) -> ActionResult:
        items = params.get("items", [])
        page_size = params.get("page_size", 100)

        if not isinstance(items, list):
            items = list(items)

        pages = []
        for i in range(0, len(items), page_size):
            pages.append(items[i:i + page_size])

        return ActionResult(success=True, message=f"Created {len(pages)} pages", data={"pages": pages, "total_pages": len(pages), "total_items": len(items)})

    def _next_page(self, params: Dict[str, Any]) -> ActionResult:
        current_page = params.get("current_page", 1)
        total_pages = params.get("total_pages", 1)

        if current_page >= total_pages:
            return ActionResult(success=False, message="No more pages", data={"has_next": False})

        return ActionResult(success=True, message="Has next page", data={"has_next": True, "next_page": current_page + 1})

    def _build_pagination_params(self, params: Dict[str, Any]) -> ActionResult:
        pagination_type = params.get("type", "page")
        page = params.get("page", 1)
        page_size = params.get("page_size", 20)
        cursor = params.get("cursor")

        if pagination_type == "offset":
            offset = (page - 1) * page_size
            return ActionResult(success=True, data={"offset": offset, "limit": page_size})
        elif pagination_type == "cursor":
            return ActionResult(success=True, data={"cursor": cursor or "", "limit": page_size})
        else:
            return ActionResult(success=True, data={"page": page, "page_size": page_size})
