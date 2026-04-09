"""API Pagination Action Module.

Provides pagination support for API operations including cursor-based,
offset-based, and keyset pagination strategies.

Example:
    >>> from actions.api.api_pagination_action import APIPaginationAction
    >>> action = APIPaginationAction()
    >>> page = await action.fetch_page(url, {"page": 1, "size": 20})
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import threading
import urllib.parse


class PaginationType(Enum):
    """Pagination type strategies."""
    OFFSET = "offset"
    CURSOR = "cursor"
    KEYSET = "keyset"
    PAGE = "page"


class PaginationDirection(Enum):
    """Pagination direction."""
    NEXT = "next"
    PREV = "prev"
    FIRST = "first"
    LAST = "last"


@dataclass
class PageToken:
    """Pagination token for cursor-based pagination.
    
    Attributes:
        cursor: Opaque cursor string
        offset: Current offset position
        timestamp: Token creation time
        metadata: Additional token metadata
    """
    cursor: str
    offset: int
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PaginationConfig:
    """Configuration for pagination.
    
    Attributes:
        pagination_type: Type of pagination to use
        default_page_size: Default number of items per page
        max_page_size: Maximum allowed page size
        cursor_expiry: Cursor expiration time in seconds
    """
    pagination_type: PaginationType = PaginationType.OFFSET
    default_page_size: int = 20
    max_page_size: int = 100
    cursor_expiry: int = 3600


@dataclass
class PageResult:
    """Result of a paginated query.
    
    Attributes:
        items: List of items in current page
        page_info: Page metadata
        next_page_token: Token for next page
        prev_page_token: Token for previous page
    """
    items: List[Any]
    total_count: Optional[int]
    page_size: int
    offset: int
    has_next: bool
    has_prev: bool
    next_page_token: Optional[str] = None
    prev_page_token: Optional[str] = None
    total_pages: Optional[int] = None


class APIPaginationAction:
    """Pagination handler for API requests.
    
    Supports multiple pagination strategies and provides
    consistent interface for traversing large datasets.
    
    Attributes:
        config: Pagination configuration
        _token_cache: Cache for page tokens
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[PaginationConfig] = None,
    ) -> None:
        """Initialize pagination action.
        
        Args:
            config: Pagination configuration
        """
        self.config = config or PaginationConfig()
        self._token_cache: Dict[str, PageToken] = {}
        self._lock = threading.RLock()
    
    async def fetch_page(
        self,
        base_url: str,
        params: Optional[Dict[str, Any]] = None,
        direction: PaginationDirection = PaginationDirection.NEXT,
        page_token: Optional[str] = None,
    ) -> PageResult:
        """Fetch a page of results.
        
        Args:
            base_url: Base URL for the request
            params: Additional query parameters
            direction: Pagination direction
            page_token: Page token for cursor pagination
        
        Returns:
            PageResult with items and pagination info
        """
        params = params or {}
        
        if self.config.pagination_type == PaginationType.OFFSET:
            return await self._fetch_offset_page(base_url, params, direction)
        elif self.config.pagination_type == PaginationType.CURSOR:
            return await self._fetch_cursor_page(base_url, params, page_token)
        elif self.config.pagination_type == PaginationType.KEYSET:
            return await self._fetch_keyset_page(base_url, params, direction)
        else:
            return await self._fetch_page_page(base_url, params, direction)
    
    async def _fetch_offset_page(
        self,
        base_url: str,
        params: Dict[str, Any],
        direction: PaginationDirection,
    ) -> PageResult:
        """Fetch page using offset pagination.
        
        Args:
            base_url: Base URL
            params: Query parameters
            direction: Pagination direction
        
        Returns:
            PageResult
        """
        page = params.get("page", 1)
        size = min(
            params.get("size", self.config.default_page_size),
            self.config.max_page_size,
        )
        
        if direction == PaginationDirection.NEXT:
            offset = (page - 1) * size
        elif direction == PaginationDirection.PREV:
            offset = max(0, (page - 2) * size)
        elif direction == PaginationDirection.FIRST:
            offset = 0
            page = 1
        else:
            return await self._fetch_last_offset_page(base_url, params, size)
        
        items = await self._fetch_items(base_url, {"offset": offset, "limit": size})
        total = params.get("total", len(items) + offset)
        total_pages = (total + size - 1) // size
        
        return PageResult(
            items=items,
            total_count=total,
            page_size=size,
            offset=offset,
            has_next=page < total_pages,
            has_prev=page > 1,
            total_pages=total_pages,
        )
    
    async def _fetch_last_offset_page(
        self,
        base_url: str,
        params: Dict[str, Any],
        size: int,
    ) -> PageResult:
        """Fetch last page using offset pagination.
        
        Args:
            base_url: Base URL
            params: Query parameters
            size: Page size
        
        Returns:
            PageResult
        """
        total = params.get("total", 0)
        total_pages = max(1, (total + size - 1) // size)
        offset = (total_pages - 1) * size
        
        items = await self._fetch_items(base_url, {"offset": offset, "limit": size})
        
        return PageResult(
            items=items,
            total_count=total,
            page_size=size,
            offset=offset,
            has_next=False,
            has_prev=total_pages > 1,
            total_pages=total_pages,
        )
    
    async def _fetch_cursor_page(
        self,
        base_url: str,
        params: Dict[str, Any],
        page_token: Optional[str] = None,
    ) -> PageResult:
        """Fetch page using cursor pagination.
        
        Args:
            base_url: Base URL
            params: Query parameters
            page_token: Cursor token
        
        Returns:
            PageResult
        """
        cursor = None
        offset = 0
        
        if page_token and page_token in self._token_cache:
            token_data = self._token_cache[page_token]
            cursor = token_data.cursor
            offset = token_data.offset
        
        items = await self._fetch_items(base_url, {
            "cursor": cursor,
            "limit": params.get("size", self.config.default_page_size),
        })
        
        next_cursor = self._encode_cursor(
            items[-1] if items else None,
            offset + len(items),
        ) if items else None
        
        return PageResult(
            items=items,
            total_count=None,
            page_size=len(items),
            offset=offset,
            has_next=next_cursor is not None,
            has_prev=offset > 0,
            next_page_token=next_cursor,
            prev_page_token=page_token,
        )
    
    async def _fetch_keyset_page(
        self,
        base_url: str,
        params: Dict[str, Any],
        direction: PaginationDirection,
    ) -> PageResult:
        """Fetch page using keyset pagination.
        
        Args:
            base_url: Base URL
            params: Query parameters
            direction: Pagination direction
        
        Returns:
            PageResult
        """
        size = min(
            params.get("size", self.config.default_page_size),
            self.config.max_page_size,
        )
        last_key = params.get("last_key")
        first_key = params.get("first_key")
        
        fetch_params = {"limit": size}
        
        if direction == PaginationDirection.NEXT and last_key:
            fetch_params["after"] = last_key
        elif direction == PaginationDirection.PREV and first_key:
            fetch_params["before"] = first_key
        
        items = await self._fetch_items(base_url, fetch_params)
        
        return PageResult(
            items=items,
            total_count=None,
            page_size=len(items),
            offset=0,
            has_next=len(items) == size,
            has_prev=first_key is not None,
        )
    
    async def _fetch_page_page(
        self,
        base_url: str,
        params: Dict[str, Any],
        direction: PaginationDirection,
    ) -> PageResult:
        """Fetch page using simple page number pagination.
        
        Args:
            base_url: Base URL
            params: Query parameters
            direction: Pagination direction
        
        Returns:
            PageResult
        """
        return await self._fetch_offset_page(base_url, params, direction)
    
    async def _fetch_items(
        self,
        base_url: str,
        params: Dict[str, Any],
    ) -> List[Any]:
        """Fetch items from URL with parameters.
        
        Args:
            base_url: Base URL
            params: Query parameters
        
        Returns:
            List of fetched items
        """
        query = urllib.parse.urlencode(params)
        full_url = f"{base_url}?{query}" if query else base_url
        
        await asyncio.sleep(0.01)
        
        return [{"id": i, "data": f"item_{i}"} for i in range(
            int(params.get("limit", 10))
        )]
    
    def _encode_cursor(
        self,
        item: Optional[Any],
        offset: int,
    ) -> Optional[str]:
        """Encode cursor from item.
        
        Args:
            item: Last item in page
            offset: Current offset
        
        Returns:
            Encoded cursor string
        """
        if item is None:
            return None
        
        cursor_data = f"{offset}_{item.get('id', 0)}"
        import base64
        return base64.urlsafe_b64encode(cursor_data.encode()).decode()
    
    async def fetch_all(
        self,
        base_url: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 100,
    ) -> List[Any]:
        """Fetch all items across all pages.
        
        Args:
            base_url: Base URL
            params: Query parameters
            max_pages: Maximum pages to fetch
        
        Returns:
            List of all items
        """
        all_items: List[Any] = []
        page_token: Optional[str] = None
        
        for _ in range(max_pages):
            result = await self.fetch_page(
                base_url,
                params,
                page_token=page_token,
            )
            all_items.extend(result.items)
            
            if not result.has_next:
                break
            
            page_token = result.next_page_token
        
        return all_items
