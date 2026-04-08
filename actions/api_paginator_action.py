"""
API Paginator Action Module.

Handles pagination for API requests with support for
 cursor-based, offset-based, and page-based pagination.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class PaginationType(Enum):
    """Type of pagination."""
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"
    LINK = "link"


@dataclass
class Page:
    """A single page of results."""
    items: list[Any]
    page_number: int = 0
    page_size: int = 0
    total_items: int = 0
    total_pages: int = 0
    has_next: bool = False
    has_previous: bool = False
    next_cursor: Optional[str] = None
    previous_cursor: Optional[str] = None


class APIPaginatorAction:
    """
    API pagination handler.

    Fetches paginated data from APIs using various pagination
    strategies with automatic traversal.

    Example:
        paginator = APIPaginatorAction(pagination_type=PaginationType.OFFSET)
        paginator.set_page_size(20)
        all_items = await paginator.fetch_all(fetch_page_func)
    """

    def __init__(
        self,
        pagination_type: PaginationType = PaginationType.OFFSET,
        page_size: int = 20,
    ) -> None:
        self.pagination_type = pagination_type
        self.page_size = page_size
        self._total_items: Optional[int] = None
        self._total_pages: Optional[int] = None

    def set_page_size(self, size: int) -> "APIPaginatorAction":
        """Set the page size."""
        self.page_size = size
        return self

    async def fetch_page(
        self,
        fetch_func: Callable,
        page: int = 0,
        cursor: Optional[str] = None,
        **kwargs: Any,
    ) -> Page:
        """Fetch a single page of results."""
        params: dict[str, Any] = {**kwargs}

        if self.pagination_type == PaginationType.OFFSET:
            params["offset"] = page * self.page_size
            params["limit"] = self.page_size

        elif self.pagination_type == PaginationType.PAGE:
            params["page"] = page + 1
            params["page_size"] = self.page_size

        elif self.pagination_type == PaginationType.CURSOR:
            if cursor:
                params["cursor"] = cursor

        response = await fetch_func(**params)

        return self._parse_response(response, page)

    async def fetch_all(
        self,
        fetch_func: Callable,
        max_items: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> list[Any]:
        """Fetch all items across all pages."""
        all_items: list[Any] = []
        page_num = 0
        cursor = None

        while True:
            page = await self.fetch_page(fetch_func, page=page_num, cursor=cursor)
            all_items.extend(page.items)

            if max_items and len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break

            if max_pages and page_num >= max_pages - 1:
                break

            if not page.has_next:
                break

            cursor = page.next_cursor
            page_num += 1

        return all_items

    def _parse_response(
        self,
        response: Any,
        page_number: int,
    ) -> Page:
        """Parse API response into Page object."""
        items: list[Any] = []
        total_items = 0
        next_cursor = None
        previous_cursor = None

        if isinstance(response, dict):
            items = response.get("data") or response.get("items") or []
            total_items = response.get("total", len(items))
            next_cursor = response.get("next_cursor") or response.get("cursor")

            if self.pagination_type == PaginationType.LINK:
                links = response.get("links", {})
                next_cursor = links.get("next")

        elif isinstance(response, list):
            items = response
            total_items = len(response)

        self._total_items = total_items
        self._total_pages = (total_items + self.page_size - 1) // self.page_size if total_items > 0 else 1

        return Page(
            items=items,
            page_number=page_number,
            page_size=self.page_size,
            total_items=total_items,
            total_pages=self._total_pages or 1,
            has_next=page_number < (self._total_pages or 1) - 1,
            has_previous=page_number > 0,
            next_cursor=next_cursor,
            previous_cursor=previous_cursor,
        )

    def get_total_pages(self) -> Optional[int]:
        """Get total number of pages."""
        return self._total_pages
