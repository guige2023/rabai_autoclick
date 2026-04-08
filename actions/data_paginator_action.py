"""Data Paginator Action Module.

Provides data pagination with offset, cursor, and page-based
strategies for large datasets.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PaginationStrategy(Enum):
    """Pagination strategy."""
    OFFSET = "offset"
    CURSOR = "cursor"
    PAGE = "page"


from enum import Enum


@dataclass
class Page:
    """Pagination page result."""
    items: List[Any]
    page_number: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_prev: bool


@dataclass
class CursorPage:
    """Cursor pagination result."""
    items: List[Any]
    cursor: Optional[str]
    has_more: bool


class DataPaginatorAction:
    """Data paginator with multiple strategies.

    Example:
        paginator = DataPaginatorAction()

        page = paginator.paginate(
            data=list(range(1000)),
            strategy=PaginationStrategy.OFFSET,
            page=1,
            page_size=20
        )

        print(page.items)      # [0, 1, ..., 19]
        print(page.total_pages)  # 50
    """

    def __init__(self) -> None:
        pass

    def paginate(
        self,
        data: List[T],
        strategy: PaginationStrategy,
        page: Optional[int] = None,
        page_size: int = 20,
        cursor: Optional[str] = None,
        sort_field: Optional[str] = None,
        sort_order: str = "asc",
    ) -> Union[Page, CursorPage]:
        """Paginate data.

        Args:
            data: Data to paginate
            strategy: Pagination strategy
            page: Page number (for OFFSET/PAGE)
            page_size: Items per page
            cursor: Cursor value (for CURSOR)
            sort_field: Field to sort by
            sort_order: Sort order (asc/desc)

        Returns:
            Page or CursorPage result
        """
        if sort_field:
            data = self._sort_data(data, sort_field, sort_order)

        if strategy == PaginationStrategy.OFFSET or strategy == PaginationStrategy.PAGE:
            return self._paginate_offset(data, page or 1, page_size)
        elif strategy == PaginationStrategy.CURSOR:
            return self._paginate_cursor(data, cursor, page_size)

        return self._paginate_offset(data, 1, page_size)

    def _paginate_offset(
        self,
        data: List[T],
        page: int,
        page_size: int,
    ) -> Page:
        """Offset-based pagination."""
        total_items = len(data)
        total_pages = (total_items + page_size - 1) // page_size if page_size > 0 else 0

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size

        items = data[start_idx:end_idx]

        return Page(
            items=items,
            page_number=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )

    def _paginate_cursor(
        self,
        data: List[T],
        cursor: Optional[str],
        page_size: int,
    ) -> CursorPage:
        """Cursor-based pagination."""
        start_idx = 0

        if cursor:
            try:
                start_idx = int(cursor)
            except ValueError:
                logger.warning(f"Invalid cursor: {cursor}")

        items = data[start_idx:start_idx + page_size + 1]
        has_more = len(items) > page_size
        items = items[:page_size]

        next_cursor = str(start_idx + page_size) if has_more else None

        return CursorPage(
            items=items,
            cursor=next_cursor,
            has_more=has_more,
        )

    def _sort_data(
        self,
        data: List[Dict],
        sort_field: str,
        sort_order: str,
    ) -> List[Dict]:
        """Sort data by field."""
        reverse = sort_order.lower() == "desc"
        return sorted(
            data,
            key=lambda x: x.get(sort_field, ""),
            reverse=reverse,
        )

    def paginate_lazy(
        self,
        fetch_fn: Callable[[int, int], Tuple[List, int]],
        page: int,
        page_size: int,
    ) -> Page:
        """Lazy pagination with data fetching callback.

        Args:
            fetch_fn: Function(offset, limit) -> (items, total_count)
            page: Page number
            page_size: Page size

        Returns:
            Page result
        """
        offset = (page - 1) * page_size
        items, total_items = fetch_fn(offset, page_size)
        total_pages = (total_items + page_size - 1) // page_size if page_size > 0 else 0

        return Page(
            items=items,
            page_number=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1,
        )

    def get_page_numbers(
        self,
        current: int,
        total: int,
        delta: int = 2,
    ) -> List[int]:
        """Get page numbers for navigation display.

        Args:
            current: Current page number
            total: Total number of pages
            delta: Pages to show on each side of current

        Returns:
            List of page numbers to display
        """
        if total <= 7:
            return list(range(1, total + 1))

        pages: List[int] = []

        left = max(1, current - delta)
        right = min(total, current + delta)

        if left > 1:
            pages.append(1)
            if left > 2:
                pages.append(-1)

        pages.extend(range(left, right + 1))

        if right < total:
            if right < total - 1:
                pages.append(-1)
            pages.append(total)

        return pages
