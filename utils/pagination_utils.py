"""Pagination utilities for API results, database cursors, and infinite scrolling."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "Page",
    "Cursor",
    "OffsetPagination",
    "CursorPagination",
    "paginate",
]

T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """A single page of results."""
    items: list[T]
    page: int
    page_size: int
    total_items: int | None = None
    has_next: bool = False
    has_prev: bool = False

    @property
    def total_pages(self) -> int | None:
        if self.total_items is None:
            return None
        return (self.total_items + self.page_size - 1) // self.page_size

    @property
    def start_index(self) -> int:
        return (self.page - 1) * self.page_size + 1

    @property
    def end_index(self) -> int:
        return min(self.page * self.page_size, self.total_items or len(self.items))


@dataclass
class Cursor:
    """Opaque cursor for cursor-based pagination."""
    value: str
    position: str = "after"
    metadata: dict[str, Any] = field(default_factory=dict)

    def encode(self) -> str:
        import base64
        import json
        data = json.dumps({"v": self.value, "p": self.position, "m": self.metadata})
        return base64.urlsafe_b64encode(data.encode()).decode()

    @classmethod
    def decode(cls, encoded: str) -> "Cursor":
        import base64
        import json
        data = json.loads(base64.urlsafe_b64decode(encoded.encode()))
        return cls(value=data["v"], position=data.get("p", "after"), metadata=data.get("m", {}))


class OffsetPagination:
    """Offset-based pagination helper."""

    def __init__(self, items: list[T], page: int = 1, page_size: int = 20, total: int | None = None) -> None:
        self.items = items
        self.page = page
        self.page_size = page_size
        self.total = total or len(items)
        self._has_next = (page * page_size) < self.total
        self._has_prev = page > 1

    def as_page(self) -> Page[T]:
        return Page(
            items=self.items,
            page=self.page,
            page_size=self.page_size,
            total_items=self.total,
            has_next=self._has_next,
            has_prev=self._has_prev,
        )

    @classmethod
    def from_getter(
        cls,
        getter: Callable[[int, int], tuple[list[T], int]],
        page: int = 1,
        page_size: int = 20,
    ) -> Page[T]:
        items, total = getter(page, page_size)
        p = cls(items, page, page_size, total)
        return p.as_page()


class CursorPagination:
    """Cursor-based pagination helper for API-style pagination."""

    def __init__(
        self,
        items: list[T],
        next_cursor: str | None = None,
        prev_cursor: str | None = None,
        has_more: bool = False,
    ) -> None:
        self.items = items
        self.next_cursor = next_cursor
        self.prev_cursor = prev_cursor
        self.has_more = has_more

    def next_page(self) -> "CursorPagination[T] | None":
        if not self.next_cursor:
            return None
        return CursorPagination(items=[], next_cursor=self.next_cursor, has_more=self.has_more)


def paginate(
    items: list[T],
    page: int | None = None,
    page_size: int = 20,
    cursor: str | None = None,
    total: int | None = None,
) -> Page[T] | CursorPagination[T]:
    """Generic paginate function."""
    if cursor:
        return CursorPagination(items=items, has_more=len(items) >= page_size)
    if page is not None:
        p = OffsetPagination(items, page, page_size, total or len(items))
        return p.as_page()
    return OffsetPagination(items, 1, page_size, total or len(items)).as_page()
