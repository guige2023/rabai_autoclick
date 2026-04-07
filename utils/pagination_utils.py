"""
Pagination utilities for API pagination and cursor management.

Provides offset, cursor-based, and keyset pagination strategies
with automatic detection and unified response formatting.
"""

from __future__ import annotations

import base64
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar, Optional

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PaginationType(Enum):
    OFFSET = auto()
    CURSOR = auto()
    KEYSET = auto()
    PAGE = auto()


@dataclass
class PageInfo:
    """Pagination metadata."""
    page: int = 1
    per_page: int = 20
    total: int = 0
    total_pages: int = 0
    has_next: bool = False
    has_prev: bool = False
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None
    first_item: Optional[int] = None
    last_item: Optional[int] = None


@dataclass
class PaginatedResponse(Generic[T]):
    """Generic paginated response."""
    items: list[T]
    pagination: PageInfo
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CursorInfo:
    """Cursor for pagination."""
    sort_field: str
    sort_value: Any
    id: Optional[int] = None

    def encode(self) -> str:
        """Encode cursor to string."""
        import json
        data = {
            "s": self.sort_field,
            "v": self.sort_value,
            "i": self.id,
        }
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()

    @classmethod
    def decode(cls, cursor: str) -> "CursorInfo":
        """Decode cursor from string."""
        import json
        try:
            data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
            return CursorInfo(
                sort_field=data["s"],
                sort_value=data["v"],
                id=data.get("i"),
            )
        except Exception:
            return CursorInfo(sort_field="id", sort_value=0)


class OffsetPaginator:
    """Offset-based pagination."""

    def __init__(self, page: int = 1, per_page: int = 20) -> None:
        self.page = max(1, page)
        self.per_page = max(1, min(per_page, 1000))
        self._offset = (self.page - 1) * self.per_page

    @property
    def limit(self) -> int:
        return self.per_page

    @property
    def offset(self) -> int:
        return self._offset

    def paginate(self, items: list[T], total: int) -> PaginatedResponse[T]:
        """Create paginated response from items."""
        total_pages = (total + self.per_page - 1) // self.per_page if self.per_page > 0 else 0
        page_info = PageInfo(
            page=self.page,
            per_page=self.per_page,
            total=total,
            total_pages=total_pages,
            has_next=self.page < total_pages,
            has_prev=self.page > 1,
            first_item=self._offset + 1 if total > 0 else None,
            last_item=min(self._offset + self.per_page, total) if total > 0 else None,
        )
        return PaginatedResponse(items=items, pagination=page_info)

    def next_page(self) -> Optional[int]:
        """Get next page number."""
        total = getattr(self, "_total", 0)
        total_pages = (total + self.per_page - 1) // self.per_page if self.per_page > 0 else 0
        if self.page < total_pages:
            return self.page + 1
        return None


class CursorPaginator:
    """Cursor-based pagination (infinite scroll)."""

    def __init__(self, cursor: Optional[str] = None, limit: int = 20) -> None:
        self.limit = max(1, min(limit, 1000))
        self.cursor_info = CursorInfo.decode(cursor) if cursor else None

    @property
    def sort_field(self) -> str:
        return self.cursor_info.sort_field if self.cursor_info else "created_at"

    @property
    def sort_value(self) -> Any:
        return self.cursor_info.sort_value if self.cursor_info else None

    def paginate(self, items: list[T], has_more: bool = True) -> PaginatedResponse[T]:
        """Create paginated response with cursor."""
        next_cursor = None
        prev_cursor = None

        if items and has_more:
            last_item = items[-1]
            next_cursor = CursorInfo(
                sort_field=self.sort_field,
                sort_value=getattr(last_item, self.sort_field, None),
                id=getattr(last_item, "id", None),
            ).encode()

        if self.cursor_info:
            prev_cursor = self.cursor_info.encode()

        page_info = PageInfo(
            page=1,
            per_page=self.limit,
            total=len(items),
            total_pages=0,
            has_next=has_more,
            has_prev=bool(self.cursor_info),
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )
        return PaginatedResponse(items=items, pagination=page_info)

    def build_filter(self, sort_field: str, sort_direction: str = "ASC") -> dict[str, Any]:
        """Build filter conditions for cursor pagination."""
        if not self.cursor_info:
            return {}

        if sort_direction.upper() == "DESC":
            return {f"{sort_field}__lt": self.sort_value}
        return {f"{sort_field}__gt": self.sort_value}


class KeysetPaginator:
    """Keyset pagination using composite sort keys."""

    def __init__(self, last_keys: Optional[dict[str, Any]] = None, limit: int = 20) -> None:
        self.limit = max(1, min(limit, 1000))
        self.last_keys = last_keys or {}

    def paginate(self, items: list[T], total: int) -> PaginatedResponse[T]:
        """Create paginated response with keyset."""
        page_info = PageInfo(
            page=1,
            per_page=self.limit,
            total=total,
            has_next=len(items) == self.limit,
        )
        return PaginatedResponse(items=items, pagination=page_info)

    def build_where_clause(self, sort_columns: list[str], direction: str = "ASC") -> str:
        """Build SQL WHERE clause for keyset pagination."""
        if not self.last_keys:
            return ""

        conditions = []
        for col in sort_columns:
            value = self.last_keys.get(col)
            if value is not None:
                op = "<" if direction.upper() == "DESC" else ">"
                conditions.append(f"{col} {op} :{col}")

        return " AND ".join(conditions)


class PaginationResolver:
    """Resolves pagination type and returns appropriate paginator."""

    @staticmethod
    def resolve(
        page: Optional[int] = None,
        per_page: Optional[int] = None,
        cursor: Optional[str] = None,
        keyset: Optional[dict[str, Any]] = None,
    ) -> tuple[Any, PaginationType]:
        """Resolve pagination type from request parameters."""
        if cursor:
            return CursorPaginator(cursor=cursor, limit=per_page or 20), PaginationType.CURSOR
        if keyset:
            return KeysetPaginator(last_keys=keyset, limit=per_page or 20), PaginationType.KEYSET
        if page is not None:
            return OffsetPaginator(page=page, per_page=per_page or 20), PaginationType.OFFSET
        return OffsetPaginator(page=1, per_page=per_page or 20), PaginationType.PAGE
