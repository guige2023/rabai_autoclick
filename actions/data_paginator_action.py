# Copyright (c) 2024. coded by claude
"""Data Paginator Action Module.

Provides pagination utilities for API responses with support for
offset, cursor-based, and keyset pagination strategies.
"""
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class PaginationStrategy(Enum):
    OFFSET = "offset"
    CURSOR = "cursor"
    KEYSET = "keyset"


@dataclass
class Page:
    items: List[Dict[str, Any]]
    page_number: int
    page_size: int
    total_items: int
    total_pages: int
    has_next: bool
    has_previous: bool
    next_cursor: Optional[str] = None
    previous_cursor: Optional[str] = None


@dataclass
class PaginationConfig:
    strategy: PaginationStrategy = PaginationStrategy.OFFSET
    default_page_size: int = 20
    max_page_size: int = 100
    cursor_field: Optional[str] = None


class DataPaginator:
    def __init__(self, config: Optional[PaginationConfig] = None):
        self.config = config or PaginationConfig()

    def paginate_offset(self, items: List[Dict[str, Any]], page: int = 1, page_size: Optional[int] = None) -> Page:
        page_size = self._normalize_page_size(page_size)
        total_items = len(items)
        total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]
        return Page(
            items=page_items,
            page_number=page,
            page_size=page_size,
            total_items=total_items,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1,
        )

    def paginate_cursor(self, items: List[Dict[str, Any]], cursor: Optional[str] = None, page_size: Optional[int] = None) -> Page:
        page_size = self._normalize_page_size(page_size)
        start_index = 0
        if cursor and self.config.cursor_field:
            for i, item in enumerate(items):
                if str(item.get(self.config.cursor_field)) == cursor:
                    start_index = i + 1
                    break
        end_index = min(start_index + page_size, len(items))
        page_items = items[start_index:end_index]
        next_cursor = None
        if end_index < len(items) and page_items:
            last_item = page_items[-1]
            next_cursor = str(last_item.get(self.config.cursor_field, ""))
        previous_cursor = None
        if start_index > 0 and page_items:
            first_item = page_items[0]
            previous_cursor = str(first_item.get(self.config.cursor_field, ""))
        total_pages = (len(items) + page_size - 1) // page_size if len(items) > 0 else 1
        current_page = start_index // page_size + 1
        return Page(
            items=page_items,
            page_number=current_page,
            page_size=page_size,
            total_items=len(items),
            total_pages=total_pages,
            has_next=end_index < len(items),
            has_previous=start_index > 0,
            next_cursor=next_cursor,
            previous_cursor=previous_cursor,
        )

    def paginate_keyset(self, items: List[Dict[str, Any]], last_key: Optional[Any] = None, page_size: Optional[int] = None, sort_field: str = "id") -> Page:
        page_size = self._normalize_page_size(page_size)
        filtered_items = items
        if last_key is not None:
            filtered_items = [item for item in items if item.get(sort_field, "") > last_key]
        page_items = filtered_items[:page_size]
        next_key = None
        if len(page_items) == page_size and len(filtered_items) > page_size:
            next_key = page_items[-1].get(sort_field)
        total_pages = (len(items) + page_size - 1) // page_size if len(items) > 0 else 1
        return Page(
            items=page_items,
            page_number=1,
            page_size=page_size,
            total_items=len(items),
            total_pages=total_pages,
            has_next=next_key is not None,
            has_previous=last_key is not None,
        )

    def paginate(self, items: List[Dict[str, Any]], page: Optional[int] = None, cursor: Optional[str] = None, page_size: Optional[int] = None) -> Page:
        if self.config.strategy == PaginationStrategy.OFFSET:
            return self.paginate_offset(items, page or 1, page_size)
        elif self.config.strategy == PaginationStrategy.CURSOR:
            return self.paginate_cursor(items, cursor, page_size)
        else:
            return self.paginate_offset(items, page or 1, page_size)

    def _normalize_page_size(self, page_size: Optional[int]) -> int:
        if page_size is None:
            return self.config.default_page_size
        return max(1, min(page_size, self.config.max_page_size))
