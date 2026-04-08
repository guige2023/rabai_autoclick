"""
Cursor and pagination utilities for paginated data access.

Provides cursor-based and offset-based pagination strategies
with async support and automatic iteration.

Example:
    >>> from utils.cursor_utils import CursorPaginator, OffsetPaginator
    >>> paginator = CursorPaginator(fetch_func, page_size=50)
    >>> async for page in paginator:
    ...     process(page)
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Callable, Iterator, List, Optional, TypeVar, Union

T = TypeVar("T")


class Paginator(ABC, Generic[T]):
    """
    Abstract base class for paginators.

    Subclasses must implement _fetch_page and provide
    async and sync iteration interfaces.
    """

    def __init__(self, page_size: int = 20) -> None:
        """
        Initialize the paginator.

        Args:
            page_size: Number of items per page.
        """
        self.page_size = page_size

    @abstractmethod
    def _fetch_page(self, cursor: Optional[str], offset: int) -> tuple[List[T], Any]:
        """
        Fetch a page of items.

        Args:
            cursor: Cursor for cursor-based pagination.
            offset: Offset for offset-based pagination.

        Returns:
            Tuple of (items, next_cursor_or_offset).
        """
        pass

    def __iter__(self) -> Iterator[List[T]]:
        """Iterate over pages synchronously."""
        cursor: Optional[str] = None
        offset = 0

        while True:
            items, next_pos = self._fetch_page(cursor, offset)
            if not items:
                break
            yield items

            if next_pos is None:
                break

            if isinstance(next_pos, str):
                cursor = next_pos
            else:
                offset = next_pos

    def __aiter__(self) -> AsyncIterator[List[T]]:
        """Iterate over pages asynchronously."""
        return self._async_iter()

    async def _async_iter(self) -> AsyncIterator[List[T]]:
        """Async iteration implementation."""
        cursor: Optional[str] = None
        offset = 0

        while True:
            items, next_pos = self._fetch_page(cursor, offset)
            if not items:
                break
            yield items

            if next_pos is None:
                break

            if isinstance(next_pos, str):
                cursor = next_pos
            else:
                offset = next_pos

    def iterate_items(self) -> Iterator[T]:
        """
        Iterate over individual items across all pages.

        Yields:
            Individual items.
        """
        for page in self:
            yield from page

    async def iterate_items_async(self) -> AsyncIterator[T]:
        """
        Iterate over individual items asynchronously.

        Yields:
            Individual items.
        """
        async for page in self:
            for item in page:
                yield item


class CursorPaginator(Paginator[T]):
    """
    Cursor-based pagination.

    More efficient than offset-based for large datasets
    as it avoids skipping items.
    """

    def __init__(
        self,
        fetch_func: Callable[[Optional[str], int], tuple[List[T], Optional[str]]],
        page_size: int = 20,
    ) -> None:
        """
        Initialize the cursor paginator.

        Args:
            fetch_func: Function(cursor, limit) -> (items, next_cursor).
            page_size: Number of items per page.
        """
        super().__init__(page_size)
        self._fetch_func = fetch_func

    def _fetch_page(self, cursor: Optional[str], offset: int) -> tuple[List[T], Any]:
        """Fetch a page using cursor."""
        items, next_cursor = self._fetch_func(cursor, self.page_size)
        return items, next_cursor


class OffsetPaginator(Paginator[T]):
    """
    Offset-based pagination.

    Simple but less efficient for large datasets.
    """

    def __init__(
        self,
        fetch_func: Callable[[int, int], tuple[List[T], int]],
        total_count: Optional[int] = None,
        page_size: int = 20,
    ) -> None:
        """
        Initialize the offset paginator.

        Args:
            fetch_func: Function(offset, limit) -> (items, total).
            total_count: Total number of items (fetched once if None).
            page_size: Number of items per page.
        """
        super().__init__(page_size)
        self._fetch_func = fetch_func
        self._total_count = total_count
        self._fetched_total = False

    def _fetch_page(self, cursor: Optional[str], offset: int) -> tuple[List[T], Any]:
        """Fetch a page using offset."""
        if not self._fetched_total and self._total_count is None:
            _, total = self._fetch_func(0, 0)
            self._total_count = total
            self._fetched_total = True

        items, _ = self._fetch_func(offset, self.page_size)

        next_offset = offset + self.page_size
        if self._total_count is not None and next_offset >= self._total_count:
            return items, None

        return items, next_offset


class SeekPaginator(Paginator[T]):
    """
    Seek-based pagination using ID or timestamp.

    Efficient for append-only datasets where items
    are ordered by a sortable key.
    """

    def __init__(
        self,
        fetch_func: Callable[[Optional[Any], int], tuple[List[T], Optional[Any]]],
        page_size: int = 20,
        sort_key: str = "id",
    ) -> None:
        """
        Initialize the seek paginator.

        Args:
            fetch_func: Function(seek_value, limit) -> (items, next_seek).
            page_size: Number of items per page.
            sort_key: Key to sort by ('id' or 'created_at').
        """
        super().__init__(page_size)
        self._fetch_func = fetch_func
        self._sort_key = sort_key

    def _fetch_page(self, cursor: Optional[str], offset: int) -> tuple[List[T], Any]:
        """Fetch a page using seek value."""
        items, next_seek = self._fetch_func(cursor, self.page_size)

        if not items or next_seek is None:
            return items, None

        return items, next_seek


class AsyncCursorPaginator(Paginator[T]):
    """
    Async cursor-based paginator with concurrent page fetching.
    """

    def __init__(
        self,
        fetch_func: Callable[[Optional[str], int], Any],
        page_size: int = 20,
    ) -> None:
        """
        Initialize the async cursor paginator.

        Args:
            fetch_func: Async function(cursor, limit) -> (items, next_cursor).
            page_size: Number of items per page.
        """
        super().__init__(page_size)
        self._fetch_func = fetch_func

    async def _fetch_page(self, cursor: Optional[str], offset: int) -> tuple[List[T], Any]:
        """Fetch a page asynchronously."""
        return await self._fetch_func(cursor, self.page_size)


class Page:
    """
    Container for a page of results with metadata.
    """

    def __init__(
        self,
        items: List[T],
        page_number: int,
        page_size: int,
        total_count: Optional[int] = None,
        has_next: bool = False,
        has_previous: bool = False,
    ) -> None:
        """
        Initialize a page.

        Args:
            items: Items in this page.
            page_number: Current page number (1-indexed).
            page_size: Items per page.
            total_count: Total number of items across all pages.
            has_next: Whether there is a next page.
            has_previous: Whether there is a previous page.
        """
        self.items = items
        self.page_number = page_number
        self.page_size = page_size
        self.total_count = total_count
        self.has_next = has_next
        self.has_previous = has_previous

    @property
    def total_pages(self) -> Optional[int]:
        """Calculate total number of pages."""
        if self.total_count is None:
            return None
        return (self.total_count + self.page_size - 1) // self.page_size

    @property
    def is_empty(self) -> bool:
        """Check if page has no items."""
        return len(self.items) == 0

    def __repr__(self) -> str:
        return (
            f"Page(items={len(self.items)}, page={self.page_number}, "
            f"total={self.total_count})"
        )


def paginate(
    items: List[T],
    page: int = 1,
    page_size: int = 20,
) -> Page[T]:
    """
    Paginate a list of items.

    Args:
        items: Full list of items.
        page: Page number (1-indexed).
        page_size: Items per page.

    Returns:
        Page object with items and metadata.
    """
    total_count = len(items)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = items[start:end]

    total_pages = (total_count + page_size - 1) // page_size

    return Page(
        items=page_items,
        page_number=page,
        page_size=page_size,
        total_count=total_count,
        has_next=page < total_pages,
        has_previous=page > 1,
    )


def create_paginator(
    paginator_type: str,
    fetch_func: Callable,
    page_size: int = 20,
    **kwargs
) -> Paginator:
    """
    Factory function to create a paginator.

    Args:
        paginator_type: One of 'cursor', 'offset', 'seek'.
        fetch_func: Function to fetch pages.
        page_size: Items per page.
        **kwargs: Additional arguments.

    Returns:
        Paginator instance.

    Raises:
        ValueError: If paginator_type is unknown.
    """
    types = {
        "cursor": CursorPaginator,
        "offset": OffsetPaginator,
        "seek": SeekPaginator,
    }

    if paginator_type not in types:
        raise ValueError(
            f"Unknown paginator type: {paginator_type}. "
            f"Available: {list(types.keys())}"
        )

    return types[paginator_type](fetch_func=fetch_func, page_size=page_size, **kwargs)
