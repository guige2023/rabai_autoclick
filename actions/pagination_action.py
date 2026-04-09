"""Pagination Action Module.

Cursor-based and offset pagination utilities.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class PaginationType(Enum):
    """Pagination types."""
    OFFSET = "offset"
    CURSOR = "cursor"
    KEYSET = "keyset"


@dataclass
class Page(Generic[T]):
    """Single page of results."""
    items: list[T]
    page_size: int
    has_next: bool
    has_previous: bool
    total_count: int | None = None
    next_cursor: str | None = None
    previous_cursor: str | None = None


@dataclass
class PaginationParams:
    """Pagination parameters."""
    page_size: int = 20
    max_page_size: int = 100
    offset: int | None = None
    cursor: str | None = None


class CursorPagination(Generic[T]):
    """Cursor-based pagination."""

    def __init__(self, encode_fn: callable = None, decode_fn: callable = None) -> None:
        self._encode = encode_fn or self._default_encode
        self._decode = decode_fn or self._default_decode

    def encode_cursor(self, data: dict) -> str:
        """Encode cursor from data."""
        json_str = json.dumps(data, sort_keys=True)
        return base64.urlsafe_b64encode(json_str.encode()).decode()

    def decode_cursor(self, cursor: str) -> dict:
        """Decode cursor to data."""
        json_str = base64.urlsafe_b64decode(cursor.encode()).decode()
        return json.loads(json_str)

    def _default_encode(self, data: dict) -> str:
        """Default encode function."""
        return self.encode_cursor(data)

    def _default_decode(self, cursor: str) -> dict:
        """Default decode function."""
        return self.decode_cursor(cursor)

    def paginate(
        self,
        items: list[T],
        has_more: bool,
        cursor_data: dict | None = None
    ) -> Page[T]:
        """Create page with cursor."""
        next_cursor = None
        if has_more and cursor_data:
            next_cursor = self._encode(cursor_data)
        return Page(
            items=items,
            page_size=len(items),
            has_next=has_more,
            has_previous=cursor_data is not None,
            next_cursor=next_cursor
        )


class OffsetPagination(Generic[T]):
    """Offset-based pagination."""

    def paginate(
        self,
        items: list[T],
        total_count: int | None,
        params: PaginationParams
    ) -> Page[T]:
        """Create page with offset."""
        has_next = params.offset + len(items) < total_count if total_count else len(items) == params.page_size
        has_previous = params.offset > 0
        return Page(
            items=items,
            page_size=len(items),
            has_next=has_next,
            has_previous=has_previous,
            total_count=total_count
        )


class KeysetPagination(Generic[T]):
    """Keyset pagination for efficient large dataset traversal."""

    def __init__(self, sort_field: str = "id", sort_direction: str = "asc") -> None:
        self.sort_field = sort_field
        self.sort_direction = sort_direction

    def build_keyset(self, last_item: T) -> dict:
        """Build keyset from last item."""
        return {self.sort_field: getattr(last_item, self.sort_field, None)}

    def paginate(
        self,
        items: list[T],
        has_more: bool,
        keyset: dict | None = None
    ) -> Page[T]:
        """Create page with keyset."""
        next_keyset = None
        if has_more and items:
            next_keyset = self.build_keyset(items[-1])
        return Page(
            items=items,
            page_size=len(items),
            has_next=has_more,
            has_previous=keyset is not None,
            next_cursor=json.dumps(next_keyset) if next_keyset else None
        )
