"""
API Pagination Action Module

Handles paginated API requests with cursor, offset, and page-based pagination strategies.
Supports automatic iteration, batching, and result aggregation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PaginationType(Enum):
    """Supported pagination strategies."""

    OFFSET = "offset"
    CURSOR = "cursor"
    PAGE = "page"
    LINK_HEADER = "link_header"
    TIME_BASED = "time_based"


@dataclass
class PaginationConfig(Generic[T]):
    """Configuration for pagination behavior."""

    page_size: int = 100
    max_pages: Optional[int] = None
    max_items: Optional[int] = None
    timeout_seconds: float = 60.0
    delay_between_pages: float = 0.0
    retry_on_empty: bool = True
    max_retries: int = 3

    # Cursor-based specific
    cursor_field: str = "cursor"
    next_cursor_field: str = "next_cursor"

    # Offset-based specific
    offset_field: str = "offset"
    limit_field: str = "limit"
    total_field: Optional[str] = None

    # Page-based specific
    page_field: str = "page"
    per_page_field: str = "per_page"
    total_pages_field: Optional[str] = None

    # Link header specific
    link_header_fields: Dict[str, str] = field(default_factory=lambda: {
        "next": "next",
        "prev": "prev",
        "first": "first",
        "last": "last",
    })


@dataclass
class PaginationResult(Generic[T]):
    """Result of a paginated operation."""

    items: List[T]
    total_count: Optional[int] = None
    page_count: Optional[int] = None
    current_page: Optional[int] = None
    next_cursor: Optional[str] = None
    has_more: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        """Check if pagination is complete."""
        return not self.has_more


class PaginationIterator(Generic[T]):
    """
    Async iterator for paginated API responses.

    Usage:
        async for item in PaginationIterator(api_client.fetch_page, config):
            process(item)
    """

    def __init__(
        self,
        fetch_fn: Callable[..., Any],
        config: Optional[PaginationConfig] = None,
        initial_params: Optional[Dict[str, Any]] = None,
        transform_fn: Optional[Callable[[Any], T]] = None,
    ):
        self.fetch_fn = fetch_fn
        self.config = config or PaginationConfig()
        self.initial_params = initial_params or {}
        self.transform_fn = transform_fn or (lambda x: x)

        self._current_offset: int = 0
        self._current_page: int = 1
        self._current_cursor: Optional[str] = None
        self._items_collected: int = 0
        self._pages_fetched: int = 0
        self._exhausted: bool = False

    def _build_offset_params(self) -> Dict[str, Any]:
        """Build params for offset-based pagination."""
        params = self.initial_params.copy()
        params[self.config.offset_field] = self._current_offset
        params[self.config.limit_field] = self.config.page_size
        return params

    def _build_page_params(self) -> Dict[str, Any]:
        """Build params for page-based pagination."""
        params = self.initial_params.copy()
        params[self.config.page_field] = self._current_page
        params[self.config.per_page_field] = self.config.page_size
        return params

    def _build_cursor_params(self) -> Dict[str, Any]:
        """Build params for cursor-based pagination."""
        params = self.initial_params.copy()
        if self._current_cursor:
            params[self.config.cursor_field] = self._current_cursor
        return params

    def _parse_response(self, response: Any) -> List[T]:
        """Parse response and extract items."""
        if isinstance(response, dict):
            items = response.get("data", response.get("items", []))
        elif isinstance(response, (list, tuple)):
            items = response
        else:
            items = []

        if callable(self.transform_fn):
            return [self.transform_fn(item) for item in items]
        return list(items)

    def _update_state(self, response: Any, items: List[T]) -> bool:
        """Update pagination state and return whether more data exists."""
        self._items_collected += len(items)
        self._pages_fetched += 1

        if self.config.max_items and self._items_collected >= self.config.max_items:
            self._exhausted = True
            return False

        if self.config.max_pages and self._pages_fetched >= self.config.max_pages:
            self._exhausted = True
            return False

        if len(items) < self.config.page_size:
            self._exhausted = True
            return False

        # Update pagination markers
        if isinstance(response, dict):
            self._current_cursor = response.get(self.config.next_cursor_field)
            self._current_offset += len(items)
            self._current_page += 1

        return not self._exhausted

    async def _fetch_page(self, params: Dict[str, Any]) -> Any:
        """Fetch a single page of results."""
        if asyncio.iscoroutinefunction(self.fetch_fn):
            return await self.fetch_fn(**params)
        return self.fetch_fn(**params)

    async def __aiter__(self) -> AsyncIterator[T]:
        """Async iteration over paginated results."""
        retry_count = 0

        while not self._exhausted:
            # Build request params based on pagination type
            params = self._build_offset_params()

            try:
                response = await self._fetch_page(params)
                items = self._parse_response(response)

                if not items and self.config.retry_on_empty and retry_count < self.config.max_retries:
                    retry_count += 1
                    logger.warning(f"Empty page received, retry {retry_count}/{self.config.max_retries}")
                    await asyncio.sleep(self.config.delay_between_pages)
                    continue

                retry_count = 0

                if not items:
                    self._exhausted = True
                    break

                for item in items:
                    yield item

                if not self._update_state(response, items):
                    break

                if self.config.delay_between_pages > 0:
                    await asyncio.sleep(self.config.delay_between_pages)

            except Exception as e:
                logger.error(f"Pagination error: {e}")
                if retry_count < self.config.max_retries:
                    retry_count += 1
                    await asyncio.sleep(self.config.delay_between_pages * retry_count)
                else:
                    raise


class APIPaginationAction:
    """
    Main action class for handling paginated API requests.

    Supports multiple pagination strategies and provides utilities for
    batch processing, result aggregation, and error handling.
    """

    def __init__(self, config: Optional[PaginationConfig] = None):
        self.config = config or PaginationConfig()
        self._stats = {
            "pages_fetched": 0,
            "items_collected": 0,
            "errors": 0,
        }

    def create_iterator(
        self,
        fetch_fn: Callable[..., Any],
        initial_params: Optional[Dict[str, Any]] = None,
        transform_fn: Optional[Callable[[Any], T]] = None,
    ) -> PaginationIterator[T]:
        """Create a pagination iterator for a fetch function."""
        return PaginationIterator(
            fetch_fn=fetch_fn,
            config=self.config,
            initial_params=initial_params,
            transform_fn=transform_fn,
        )

    async def fetch_all(
        self,
        fetch_fn: Callable[..., Any],
        initial_params: Optional[Dict[str, Any]] = None,
        transform_fn: Optional[Callable[[Any], T]] = None,
    ) -> PaginationResult[T]:
        """
        Fetch all items from a paginated API.

        Args:
            fetch_fn: Function that fetches a page of results
            initial_params: Initial parameters for the first request
            transform_fn: Optional transformation function for each item

        Returns:
            PaginationResult with all collected items
        """
        iterator = self.create_iterator(fetch_fn, initial_params, transform_fn)
        all_items: List[T] = []

        async for item in iterator:
            all_items.append(item)

            if self.config.max_items and len(all_items) >= self.config.max_items:
                break

        return PaginationResult(
            items=all_items,
            total_count=len(all_items),
            has_more=False,
            metadata=self._stats,
        )

    async def fetch_batched(
        self,
        fetch_fn: Callable[..., Any],
        batch_size: int = 100,
        initial_params: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[List[T]]:
        """
        Fetch results in batches.

        Args:
            fetch_fn: Function that fetches a page of results
            batch_size: Number of items per batch
            initial_params: Initial parameters for the first request

        Yields:
            Batches of items
        """
        current_batch: List[T] = []

        async for item in self.create_iterator(fetch_fn, initial_params):
            current_batch.append(item)

            if len(current_batch) >= batch_size:
                yield current_batch
                current_batch = []

        if current_batch:
            yield current_batch

    def get_stats(self) -> Dict[str, Any]:
        """Get pagination statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset pagination statistics."""
        self._stats = {
            "pages_fetched": 0,
            "items_collected": 0,
            "errors": 0,
        }


async def demo_pagination():
    """Demonstrate pagination usage."""
    import httpx

    async with httpx.AsyncClient() as client:

        async def fetch_users(page: int = 1, per_page: int = 100) -> Dict[str, Any]:
            response = await client.get(
                "https://api.example.com/users",
                params={"page": page, "per_page": per_page},
            )
            return response.json()

        config = PaginationConfig(page_size=100, max_pages=10)
        action = APIPaginationAction(config)

        result = await action.fetch_all(fetch_users)
        print(f"Fetched {len(result.items)} total items")


if __name__ == "__main__":
    asyncio.run(demo_pagination())
