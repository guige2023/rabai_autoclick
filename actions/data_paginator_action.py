"""Data paginator action module for RabAI AutoClick.

Provides pagination:
- DataPaginator: Paginate data collections
- CursorPaginator: Cursor-based pagination
- OffsetPaginator: Offset-based pagination
- PagePaginator: Page-based pagination
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class PaginationResult:
    """Pagination result."""
    data: List[Any]
    page: int
    page_size: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool


class DataPaginator:
    """General data paginator."""

    def paginate(
        self,
        data: List[Any],
        page: int = 1,
        page_size: int = 10,
    ) -> PaginationResult:
        """Paginate data."""
        total = len(data)
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        paginated_data = data[start_idx:end_idx]

        return PaginationResult(
            data=paginated_data,
            page=page,
            page_size=page_size,
            total=total,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )


class CursorPaginator:
    """Cursor-based pagination."""

    def __init__(self, cursor_field: str = "id"):
        self.cursor_field = cursor_field
        self._cursor_cache: Dict[int, Any] = {}

    def paginate(
        self,
        data: List[Dict],
        cursor: Optional[Any] = None,
        page_size: int = 10,
    ) -> Tuple[List[Dict], Optional[Any]]:
        """Paginate with cursor."""
        if not data:
            return [], None

        if cursor is not None:
            data = [item for item in data if item.get(self.cursor_field) > cursor]

        page_data = data[:page_size]
        next_cursor = page_data[-1].get(self.cursor_field) if page_data else None

        return page_data, next_cursor


class OffsetPaginator:
    """Offset-based pagination."""

    def paginate(
        self,
        data: List[Any],
        offset: int = 0,
        limit: int = 10,
    ) -> Tuple[List[Any], int]:
        """Paginate with offset."""
        total = len(data)
        paginated = data[offset:offset + limit]
        return paginated, total


class DataPaginatorAction(BaseAction):
    """Data paginator action."""
    action_type = "data_paginator"
    display_name = "数据分页器"
    description = "数据分页处理"

    def __init__(self):
        super().__init__()
        self._paginator = DataPaginator()
        self._cursor_paginator = CursorPaginator()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "paginate")
            data = params.get("data", [])

            if operation == "paginate":
                return self._paginate(data, params)
            elif operation == "cursor":
                return self._cursor_paginate(data, params)
            elif operation == "offset":
                return self._offset_paginate(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Pagination error: {str(e)}")

    def _paginate(self, data: List[Any], params: Dict) -> ActionResult:
        """Page-based pagination."""
        page = params.get("page", 1)
        page_size = params.get("page_size", 10)

        result = self._paginator.paginate(data, page, page_size)

        return ActionResult(
            success=True,
            message=f"Page {result.page} of {result.total_pages}",
            data={
                "data": result.data,
                "page": result.page,
                "page_size": result.page_size,
                "total": result.total,
                "total_pages": result.total_pages,
                "has_next": result.has_next,
                "has_prev": result.has_prev,
            },
        )

    def _cursor_paginate(self, data: List[Dict], params: Dict) -> ActionResult:
        """Cursor-based pagination."""
        cursor = params.get("cursor")
        page_size = params.get("page_size", 10)
        cursor_field = params.get("cursor_field", "id")

        if cursor_field != self._cursor_paginator.cursor_field:
            self._cursor_paginator = CursorPaginator(cursor_field)

        paginated, next_cursor = self._cursor_paginator.paginate(data, cursor, page_size)

        return ActionResult(
            success=True,
            message=f"Returned {len(paginated)} items",
            data={
                "data": paginated,
                "next_cursor": next_cursor,
                "count": len(paginated),
            },
        )

    def _offset_paginate(self, data: List[Any], params: Dict) -> ActionResult:
        """Offset-based pagination."""
        offset = params.get("offset", 0)
        limit = params.get("limit", 10)

        paginator = OffsetPaginator()
        paginated, total = paginator.paginate(data, offset, limit)

        return ActionResult(
            success=True,
            message=f"Offset {offset}, limit {limit}",
            data={
                "data": paginated,
                "offset": offset,
                "limit": limit,
                "total": total,
            },
        )
