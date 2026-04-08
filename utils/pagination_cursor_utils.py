"""
Pagination utilities with cursor-based pagination.

Provides cursor generation, parsing, and page iteration
for efficient paginated data access.
"""

from __future__ import annotations

import base64
import json
import math
from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterator, TypeVar


T = TypeVar("T")


@dataclass
class Page:
    """Represents a single page of results."""
    items: list[T]
    page_number: int
    page_size: int
    total_items: int | None = None
    has_next: bool = False
    has_previous: bool = False

    @property
    def total_pages(self) -> int | None:
        if self.total_items is None:
            return None
        return math.ceil(self.total_items / self.page_size) if self.page_size > 0 else None

    @property
    def first_item(self) -> int | None:
        if not self.items:
            return None
        return (self.page_number - 1) * self.page_size + 1

    @property
    def last_item(self) -> int | None:
        if not self.items:
            return None
        return min(self.page_number * self.page_size, self.total_items or 0)


@dataclass
class Cursor:
    """Cursor for cursor-based pagination."""
    offset: int
    limit: int
    sort_field: str | None = None
    sort_direction: str = "asc"
    filter_hash: str | None = None
    metadata: dict[str, Any] | None = None

    def encode(self) -> str:
        """Encode cursor to base64 string."""
        data = {
            "offset": self.offset,
            "limit": self.limit,
            "sort_field": self.sort_field,
            "sort_dir": self.sort_direction,
            "filter": self.filter_hash,
            "meta": self.metadata,
        }
        json_str = json.dumps(data, sort_keys=True)
        return base64.urlsafe_b64encode(json_str.encode()).decode()

    @classmethod
    def decode(cls, cursor_str: str) -> "Cursor":
        """Decode cursor from base64 string."""
        try:
            json_str = base64.urlsafe_b64decode(cursor_str.encode()).decode()
            data = json.loads(json_str)
            return cls(
                offset=data["offset"],
                limit=data["limit"],
                sort_field=data.get("sort_field"),
                sort_direction=data.get("sort_dir", "asc"),
                filter_hash=data.get("filter"),
                metadata=data.get("meta"),
            )
        except (json.JSONDecodeError, KeyError):
            return cls(offset=0, limit=10)


class Paginator(Generic[T]):
    """
    Generic paginator for any data source.

    Supports offset-based, cursor-based, and page-based pagination.
    """

    def __init__(
        self,
        fetch_fn: Callable[[int, int], tuple[list[T], int | None]],
        page_size: int = 20,
    ):
        self.fetch_fn = fetch_fn
        self.page_size = page_size

    def get_page(self, page_number: int) -> Page[T]:
        """Get specific page by number (1-indexed)."""
        offset = (page_number - 1) * self.page_size
        items, total = self.fetch_fn(offset, self.page_size)
        return Page(
            items=items,
            page_number=page_number,
            page_size=self.page_size,
            total_items=total,
            has_next=(total is not None and offset + len(items) < total),
            has_previous=page_number > 1,
        )

    def get_cursor_page(self, cursor: Cursor) -> Page[T]:
        """Get page by cursor."""
        items, total = self.fetch_fn(cursor.offset, cursor.limit)
        has_more = total is not None and cursor.offset + len(items) < total
        page_num = cursor.offset // cursor.limit + 1
        return Page(
            items=items,
            page_number=page_num,
            page_size=cursor.limit,
            total_items=total,
            has_next=has_more,
            has_previous=cursor.offset > 0,
        )

    def iterate_pages(
        self,
        max_pages: int | None = None,
    ) -> Iterator[Page[T]]:
        """Iterate over all pages."""
        page = 1
        while True:
            current_page = self.get_page(page)
            if not current_page.items:
                break
            yield current_page
            if not current_page.has_next:
                break
            page += 1
            if max_pages and page > max_pages:
                break

    def iterate_all(self) -> Iterator[T]:
        """Iterate over all items lazily."""
        for page in self.iterate_pages():
            yield from page.items


class CursorPaginator(Generic[T]):
    """Cursor-based paginator with automatic cursor generation."""

    def __init__(
        self,
        fetch_fn: Callable[[Cursor], tuple[list[T], Cursor | None]],
        initial_cursor: Cursor | None = None,
    ):
        self.fetch_fn = fetch_fn
        self._current_cursor = initial_cursor or Cursor(offset=0, limit=20)
        self._exhausted = False

    def next_page(self) -> Page[T] | None:
        """Fetch next page."""
        if self._exhausted:
            return None

        items, next_cursor = self.fetch_fn(self._current_cursor)
        if next_cursor is None or not items:
            self._exhausted = True
            return None

        self._current_cursor = next_cursor
        page_num = self._current_cursor.offset // self._current_cursor.limit + 1
        return Page(
            items=items,
            page_number=page_num,
            page_size=self._current_cursor.limit,
            has_next=True,
        )

    def reset(self) -> None:
        """Reset pagination to beginning."""
        self._current_cursor = Cursor(offset=0, limit=self._current_cursor.limit)
        self._exhausted = False


def make_cursor(
    offset: int,
    limit: int,
    sort_field: str | None = None,
    **kwargs: Any,
) -> str:
    """Factory to create encoded cursor string."""
    cursor = Cursor(
        offset=offset,
        limit=limit,
        sort_field=sort_field,
        **kwargs,
    )
    return cursor.encode()
