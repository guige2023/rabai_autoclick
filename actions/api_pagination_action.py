"""
API Pagination Action - Handles paginated API responses.

This module provides pagination handling capabilities including
cursor-based and offset-based pagination.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class PaginationConfig:
    """Configuration for pagination."""
    page_size: int = 100
    max_pages: int | None = None
    cursor_field: str = "cursor"
    offset_field: str = "offset"
    total_field: str | None = None


@dataclass
class PageResult:
    """Result of fetching a page."""
    data: list[Any]
    page: int
    has_next: bool
    next_cursor: str | None = None


class Paginator:
    """Handles paginated API requests."""
    
    def __init__(self, config: PaginationConfig | None = None) -> None:
        self.config = config or PaginationConfig()
    
    async def fetch_all(
        self,
        fetch_page: Callable[[str | None, int], Any],
    ) -> list[Any]:
        """Fetch all pages."""
        all_data = []
        cursor = None
        page = 0
        
        while True:
            if self.config.max_pages and page >= self.config.max_pages:
                break
            
            result = await fetch_page(cursor, self.config.page_size)
            
            if not result.get("data"):
                break
            
            all_data.extend(result["data"])
            cursor = result.get(self.config.cursor_field)
            
            if not cursor:
                break
            
            page += 1
        
        return all_data


class APIPaginationAction:
    """API pagination action for automation workflows."""
    
    def __init__(self, page_size: int = 100) -> None:
        self.config = PaginationConfig(page_size=page_size)
        self.paginator = Paginator(self.config)
    
    async def fetch_all(self, fetch_page: Callable) -> list[Any]:
        """Fetch all pages from a paginated API."""
        return await self.paginator.fetch_all(fetch_page)


__all__ = ["PaginationConfig", "PageResult", "Paginator", "APIPaginationAction"]
