"""
Pagination utilities for various data types.

Provides:
- List/sequence pagination
- Cursor-based pagination
- Offset pagination
- Infinite scroll helpers
- Page metadata generation
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Iterator, Sequence, TypeVar

T = TypeVar("T")


@dataclass
class Page(Generic[T]):
    """Represents a single page of results."""

    items: list[T]
    page_number: int
    page_size: int
    total_items: int
    total_pages: int

    @property
    def has_next(self) -> bool:
        """Check if there's a next page."""
        return self.page_number < self.total_pages

    @property
    def has_previous(self) -> bool:
        """Check if there's a previous page."""
        return self.page_number > 1

    @property
    def is_first(self) -> bool:
        """Check if this is the first page."""
        return self.page_number == 1

    @property
    def is_last(self) -> bool:
        """Check if this is the last page."""
        return self.page_number == self.total_pages

    @property
    def start_index(self) -> int:
        """Get the starting index (1-based) of items in this page."""
        if self.total_items == 0:
            return 0
        return (self.page_number - 1) * self.page_size + 1

    @property
    def end_index(self) -> int:
        """Get the ending index (1-based) of items in this page."""
        if self.total_items == 0:
            return 0
        return min(self.page_number * self.page_size, self.total_items)

    @property
    def items_on_page(self) -> int:
        """Number of items on this page."""
        return len(self.items)

    @property
    def total_items_on_page(self) -> int:
        """Alias for items_on_page for compatibility."""
        return len(self.items)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "items": self.items,
            "page_number": self.page_number,
            "page_size": self.page_size,
            "total_items": self.total_items,
            "total_pages": self.total_pages,
            "has_next": self.has_next,
            "has_previous": self.has_previous,
            "is_first": self.is_first,
            "is_last": self.is_last,
            "start_index": self.start_index,
            "end_index": self.end_index,
        }


@dataclass
class Cursor:
    """Cursor for cursor-based pagination."""

    value: Any
    direction: str = "after"  # 'after' or 'before'
    size: int = 10

    def encode(self) -> str:
        """Encode cursor to string for URL use."""
        import base64
        import json

        data = {"value": self.value, "direction": self.direction, "size": self.size}
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()

    @classmethod
    def decode(cls, cursor_str: str) -> Cursor:
        """Decode cursor from string."""
        import base64
        import json

        data = json.loads(base64.urlsafe_b64decode(cursor_str.encode()).decode())
        return cls(value=data["value"], direction=data.get("direction", "after"), size=data.get("size", 10))


@dataclass
class PaginationOptions:
    """Configuration for pagination."""

    page_size: int = 10
    max_page_size: int = 100
    allow_server_count: bool = True
    page_param: str = "page"
    page_size_param: str = "page_size"


def paginate_list(
    items: Sequence[T],
    page: int = 1,
    page_size: int = 10,
) -> Page[T]:
    """
    Paginate a list or sequence.

    Args:
        items: Sequence to paginate
        page: Page number (1-based)
        page_size: Items per page

    Returns:
        Page object with items and metadata

    Example:
        >>> items = list(range(100))
        >>> page = paginate_list(items, page=3, page_size=10)
        >>> page.items
        [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
    """
    if page < 1:
        page = 1

    total_items = len(items)
    total_pages = math.ceil(total_items / page_size) if total_items > 0 else 0

    if total_pages > 0 and page > total_pages:
        page = total_pages

    start = (page - 1) * page_size
    end = start + page_size

    return Page(
        items=list(items[start:end]),
        page_number=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=total_pages,
    )


def paginate_offset(
    items: Sequence[T],
    offset: int = 0,
    limit: int = 10,
) -> tuple[list[T], int]:
    """
    Paginate using offset and limit.

    Args:
        items: Sequence to paginate
        offset: Number of items to skip
        limit: Maximum items to return

    Returns:
        Tuple of (items, total_count)
    """
    total = len(items)
    offset = max(0, min(offset, total))
    limit = max(0, min(limit, total - offset))

    return list(items[offset : offset + limit]), total


def cursor_paginate(
    items: Sequence[T],
    cursor: Cursor | None,
    key: Callable[[T], Any] | None = None,
) -> tuple[list[T], Cursor | None, bool]:
    """
    Cursor-based pagination.

    Args:
        items: Sequence to paginate
        cursor: Current cursor (None for first page)
        key: Function to extract sort key from item

    Returns:
        Tuple of (items, next_cursor, has_more)
    """
    if key is None:
        key = lambda x: x  # type: ignore

    page_size = cursor.size if cursor else 10
    direction = cursor.direction if cursor else "after"

    sorted_items = sorted(items, key=key)  # type: ignore

    if cursor is None:
        result = sorted_items[:page_size]
        has_more = len(sorted_items) > page_size
        next_cursor = Cursor(value=key(result[-1]), direction="after", size=page_size) if result else None
        return result, next_cursor, has_more

    cursor_index = -1
    for i, item in enumerate(sorted_items):
        if key(item) == cursor.value:  # type: ignore
            cursor_index = i
            break

    if direction == "after":
        start_idx = cursor_index + 1 if cursor_index >= 0 else 0
        result = sorted_items[start_idx : start_idx + page_size]
        has_more = start_idx + page_size < len(sorted_items)
        if result:
            next_cursor = Cursor(value=key(result[-1]), direction="after", size=page_size)  # type: ignore
        else:
            next_cursor = None
    else:
        end_idx = cursor_index if cursor_index >= 0 else page_size
        start_idx = max(0, end_idx - page_size)
        result = sorted_items[start_idx:end_idx]
        has_more = start_idx > 0
        if result:
            next_cursor = Cursor(value=key(result[0]), direction="before", size=page_size)  # type: ignore
        else:
            next_cursor = None

    return result, next_cursor, has_more


def infinite_scroll_paginate(
    items: Sequence[T],
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[T], bool]:
    """
    Paginate for infinite scroll pattern.

    Args:
        items: Sequence to paginate
        page: Current page number
        page_size: Items per page

    Returns:
        Tuple of (items, has_more)
    """
    start = (page - 1) * page_size
    end = start + page_size

    result = list(items[start:end])
    has_more = end < len(items)

    return result, has_more


def generate_page_numbers(
    current_page: int,
    total_pages: int,
    max_visible: int = 7,
) -> list[int | str]:
    """
    Generate page numbers for pagination UI.

    Args:
        current_page: Current page number
        total_pages: Total number of pages
        max_visible: Maximum page numbers to show

    Returns:
        List of page numbers (using '...' for gaps)

    Example:
        >>> generate_page_numbers(5, 20)
        [1, '...', 4, 5, 6, '...', 20]
    """
    if total_pages <= max_visible:
        return list(range(1, total_pages + 1))

    half = max_visible // 2
    start = max(1, current_page - half)
    end = min(total_pages, current_page + half)

    if start == 1:
        end = min(total_pages, max_visible)
    if end == total_pages:
        start = max(1, total_pages - max_visible + 1)

    pages: list[int | str] = []

    if start > 1:
        pages.append(1)
        if start > 2:
            pages.append("...")

    pages.extend(range(start, end + 1))

    if end < total_pages:
        if end < total_pages - 1:
            pages.append("...")
        pages.append(total_pages)

    return pages


class Paginator(Generic[T]):
    """
    Iterator-based paginator for any iterable.

    Example:
        >>> paginator = Paginator(range(100), page_size=10)
        >>> for page in paginator:
        ...     print(page.items)
    """

    def __init__(
        self,
        items: Sequence[T] | Iterator[T],
        page_size: int = 10,
        keep_alive: bool = False,
    ):
        """
        Initialize paginator.

        Args:
            items: Items to paginate (must support len if keep_alive=False)
            page_size: Items per page
            keep_alive: If True, don't calculate total upfront (lazy)
        """
        self._items = items if isinstance(items, list) else list(items)
        self._page_size = page_size
        self._current_page = 0
        self._total = len(self._items) if keep_alive else len(self._items)
        self._total_pages = math.ceil(self._total / page_size) if self._total > 0 else 0

    def __iter__(self) -> Iterator[Page[T]]:
        """Iterate over pages."""
        while self._current_page < self._total_pages:
            yield self.page(self._current_page + 1)
            self._current_page += 1

    def __len__(self) -> int:
        """Return total number of pages."""
        return self._total_pages

    def page(self, page_number: int) -> Page[T]:
        """Get a specific page."""
        return paginate_list(self._items, page_number, self._page_size)

    @property
    def total_items(self) -> int:
        """Total number of items."""
        return self._total

    @property
    def total_pages(self) -> int:
        """Total number of pages."""
        return self._total_pages


@dataclass
class SlicePagination:
    """Slice-based pagination helper."""

    total: int
    offset: int = 0
    limit: int = 10

    @property
    def page(self) -> int:
        """Current page number (1-based)."""
        if self.limit == 0:
            return 1
        return self.offset // self.limit + 1

    @property
    def page_count(self) -> int:
        """Total number of pages."""
        if self.limit == 0:
            return 1
        return math.ceil(self.total / self.limit)

    @property
    def has_next(self) -> bool:
        """Check if there's a next page."""
        return self.page < self.page_count

    @property
    def has_previous(self) -> bool:
        """Check if there's a previous page."""
        return self.page > 1

    def slice(self, items: Sequence[T]) -> list[T]:
        """Apply pagination to a sequence."""
        return list(items[self.offset : self.offset + self.limit])


def batch_items(items: Sequence[T], batch_size: int) -> Iterator[list[T]]:
    """
    Split items into batches.

    Args:
        items: Items to batch
        batch_size: Size of each batch

    Yields:
        Batches of items

    Example:
        >>> list(batch_items([1,2,3,4,5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(items), batch_size):
        yield list(items[i : i + batch_size])


def window_items(items: Sequence[T], window_size: int, step: int = 1) -> Iterator[list[T]]:
    """
    Create sliding windows over items.

    Args:
        items: Items to window
        window_size: Size of each window
        step: Step size between windows

    Yields:
        Windows of items

    Example:
        >>> list(window_items([1,2,3,4,5], 3))
        [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    """
    for i in range(0, len(items) - window_size + 1, step):
        yield list(items[i : i + window_size])
