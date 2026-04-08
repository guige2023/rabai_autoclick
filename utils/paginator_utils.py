"""
Paginator Utilities

Provides utilities for paginating through
large datasets in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class Paginator(Generic[T]):
    """
    Paginates through collections of items.
    
    Provides methods for navigating pages
    and fetching page contents.
    """

    def __init__(
        self,
        items: list[T] | Callable[[int, int], list[T]],
        page_size: int = 20,
    ) -> None:
        self._items = items if isinstance(items, list) else None
        self._fetcher = items if callable(items) and not isinstance(items, list) else None
        self._page_size = page_size
        self._total: int | None = None

    def get_page(self, page: int) -> list[T]:
        """
        Get items for a specific page.
        
        Args:
            page: Page number (0-indexed).
            
        Returns:
            List of items on that page.
        """
        if self._items is not None:
            start = page * self._page_size
            end = start + self._page_size
            return self._items[start:end]

        if self._fetcher is not None:
            start = page * self._page_size
            return self._fetcher(start, self._page_size)

        return []

    def total_pages(self) -> int:
        """Get total number of pages."""
        if self._total is not None:
            return self._total
        if self._items is not None:
            self._total = (len(self._items) + self._page_size - 1) // self._page_size
            return self._total
        return 0

    def total_items(self) -> int | None:
        """Get total number of items if known."""
        if self._items is not None:
            return len(self._items)
        return self._total

    def has_next(self, page: int) -> bool:
        """Check if there's a next page."""
        return page < self.total_pages() - 1

    def has_prev(self, page: int) -> bool:
        """Check if there's a previous page."""
        return page > 0

    def page_range(self) -> range:
        """Get range of page numbers."""
        return range(self.total_pages())
