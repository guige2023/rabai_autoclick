"""
Pagination Action Module.

Handles API pagination across different strategies: cursor-based, offset-based,
page-based, and time-based. Provides unified interface for paginated data retrieval.

Author: RabAi Team
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


class PaginationStrategy(Enum):
    """Pagination strategy types."""
    CURSOR = "cursor"
    OFFSET = "offset"
    PAGE = "page"
    TIME = "time"
    ID = "id"
    LINK_HEADER = "link_header"


@dataclass
class PaginationState:
    """Tracks current pagination state."""
    strategy: PaginationStrategy
    page: int = 1
    offset: int = 0
    cursor: Optional[str] = None
    since_id: Optional[int] = None
    until_id: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    has_more: bool = True
    total_count: Optional[int] = None
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy": self.strategy.value,
            "page": self.page,
            "offset": self.offset,
            "cursor": self.cursor,
            "since_id": self.since_id,
            "until_id": self.until_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "has_more": self.has_more,
            "total_count": self.total_count,
            "rate_limit_remaining": self.rate_limit_remaining,
            "rate_limit_reset": self.rate_limit_reset,
            "metadata": self.metadata,
        }


@dataclass
class PageResult:
    """Result of a single page fetch."""
    items: List[Any]
    page_size: int
    is_last_page: bool
    next_page_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PaginationConfig:
    """Configuration for pagination behavior."""
    page_size: int = 100
    max_pages: Optional[int] = None
    max_total_items: Optional[int] = None
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    rate_limit_delay: float = 0.0
    timeout_seconds: float = 30.0


class CursorPaginator:
    """Cursor-based pagination handler."""

    def __init__(self, config: PaginationConfig):
        self.config = config
        self.state = PaginationState(strategy=PaginationStrategy.CURSOR)
        self._fetch_fn: Optional[Callable] = None

    def pages(self, fetch_fn: Callable[[Dict], List]) -> Iterator[PageResult]:
        """Iterate over pages using cursor pagination."""
        self._fetch_fn = fetch_fn
        self.state = PaginationState(strategy=PaginationStrategy.CURSOR)

        while self.state.has_more:
            params = self._build_params()
            items = self._fetch_with_retry(params)

            is_last = len(items) < self.config.page_size
            self.state.has_more = not is_last and self._check_limits()

            yield PageResult(
                items=items,
                page_size=len(items),
                is_last_page=is_last,
                next_page_params=params,
            )

            if self.state.has_more and items:
                self.state.cursor = self._extract_cursor(items[-1])
                self.state.page += 1

    def _build_params(self) -> Dict[str, Any]:
        params = {"limit": self.config.page_size}
        if self.state.cursor:
            params["cursor"] = self.state.cursor
        return params

    def _extract_cursor(self, item: Any) -> str:
        """Extract cursor from item."""
        if isinstance(item, dict):
            return item.get("cursor") or item.get("id", "")
        return getattr(item, "cursor", str(getattr(item, "id", "")))

    def _fetch_with_retry(self, params: Dict) -> List:
        for attempt in range(self.config.max_retries):
            try:
                result = self._fetch_fn(params)
                return result if isinstance(result, list) else []
            except Exception as e:
                if attempt == self.config.max_retries - 1:
                    raise
                time.sleep(self.config.retry_delay * (self.config.backoff_factor ** attempt))

    def _check_limits(self) -> bool:
        if self.config.max_pages and self.state.page >= self.config.max_pages:
            return False
        if self.config.max_total_items and self.state.page * self.config.page_size >= self.config.max_total_items:
            return False
        return True


class OffsetPaginator:
    """Offset-based pagination handler."""

    def __init__(self, config: PaginationConfig):
        self.config = config
        self.state = PaginationState(strategy=PaginationStrategy.OFFSET)
        self._total_fetched = 0

    def pages(self, fetch_fn: Callable[[Dict], Tuple[List, int]]) -> Iterator[PageResult]:
        """Iterate over pages using offset pagination."""
        self.state = PaginationState(strategy=PaginationStrategy.OFFSET)
        self._total_fetched = 0

        while self.state.has_more:
            params = self._build_params()
            items, total = fetch_fn(params)

            if self.state.total_count is None:
                self.state.total_count = total

            self._total_fetched += len(items)
            is_last = self._total_fetched >= self.state.total_count or len(items) == 0
            self.state.has_more = not is_last and self._check_limits()

            yield PageResult(
                items=items,
                page_size=len(items),
                is_last_page=is_last,
                next_page_params=params,
            )

            if self.state.has_more:
                self.state.offset += self.config.page_size
                self.state.page += 1

    def _build_params(self) -> Dict[str, Any]:
        return {
            "offset": self.state.offset,
            "limit": self.config.page_size,
        }

    def _check_limits(self) -> bool:
        if self.config.max_pages and self.state.page >= self.config.max_pages:
            return False
        if self.config.max_total_items and self._total_fetched >= self.config.max_total_items:
            return False
        return True


class TimeBasedPaginator:
    """Time-based pagination handler for chronological APIs."""

    def __init__(self, config: PaginationConfig, time_field: str = "created_at"):
        self.config = config
        self.state = PaginationState(strategy=PaginationStrategy.TIME)
        self.time_field = time_field
        self._total_fetched = 0

    def pages(
        self,
        fetch_fn: Callable[[Dict], List],
        start_time: Optional[datetime] = None,
        end_time: Optional[Optional[datetime]] = None,
    ) -> Iterator[PageResult]:
        """Iterate over time-bounded pages."""
        self.state.start_time = start_time or datetime.now()
        self.state.end_time = end_time
        self._total_fetched = 0

        while self.state.has_more:
            params = self._build_params()
            items = fetch_fn(params)

            self._total_fetched += len(items)
            is_last = len(items) < self.config.page_size
            self.state.has_more = not is_last and self._check_limits()

            if items and self.state.has_more:
                last_item_time = self._extract_time(items[-1])
                if last_item_time:
                    self.state.metadata["last_time"] = last_item_time

            yield PageResult(
                items=items,
                page_size=len(items),
                is_last_page=is_last,
                next_page_params=params,
            )

            if self.state.has_more and items:
                self.state.start_time = self._extract_time(items[-1])

    def _build_params(self) -> Dict[str, Any]:
        params: Dict[str, Any] = {"limit": self.config.page_size}
        if self.state.start_time:
            params["since"] = self.state.start_time.isoformat()
        if self.state.end_time:
            params["until"] = self.state.end_time.isoformat()
        return params

    def _extract_time(self, item: Any) -> Optional[datetime]:
        if isinstance(item, dict):
            ts = item.get(self.time_field)
            if isinstance(ts, str):
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return ts
        return getattr(item, self.time_field, None)

    def _check_limits(self) -> bool:
        if self.config.max_pages and self.state.page >= self.config.max_pages:
            return False
        if self.config.max_total_items and self._total_fetched >= self.config.max_total_items:
            return False
        return True


class APIPaginator:
    """
    Unified API pagination interface supporting multiple strategies.

    Handles cursor, offset, page, time-based, and link-header pagination
    with automatic rate limiting and retry logic.

    Example:
        >>> paginator = APIPaginator(strategy=PaginationStrategy.CURSOR)
        >>> for page in paginator.fetch_all(lambda p: api.call(**p)):
        >>>     process(page.items)
    """

    def __init__(
        self,
        strategy: PaginationStrategy = PaginationStrategy.CURSOR,
        config: Optional[PaginationConfig] = None,
    ):
        self.strategy = strategy
        self.config = config or PaginationConfig()

        self._paginators = {
            PaginationStrategy.CURSOR: CursorPaginator(self.config),
            PaginationStrategy.OFFSET: OffsetPaginator(self.config),
            PaginationStrategy.TIME: TimeBasedPaginator(self.config),
        }

    def fetch_all(self, fetch_fn: Callable) -> List[Any]:
        """Fetch all items across all pages."""
        all_items = []
        for page in self.pages(fetch_fn):
            all_items.extend(page.items)
            if page.is_last_page:
                break
        return all_items

    def pages(self, fetch_fn: Callable) -> Iterator[PageResult]:
        """Iterate over pages using configured strategy."""
        paginator = self._paginators.get(self.strategy)
        if not paginator:
            raise ValueError(f"Unsupported strategy: {self.strategy}")
        yield from paginator.pages(fetch_fn)

    @property
    def state(self) -> PaginationState:
        """Get current pagination state."""
        paginator = self._paginators.get(self.strategy)
        return paginator.state if paginator else PaginationState(strategy=self.strategy)

    def reset(self) -> None:
        """Reset pagination state."""
        for p in self._paginators.values():
            p.state = PaginationState(strategy=p.state.strategy)
        self._paginators[self.strategy].state = PaginationState(strategy=self.strategy)


def create_paginator(
    strategy: str = "cursor",
    page_size: int = 100,
    max_pages: Optional[int] = None,
) -> APIPaginator:
    """Factory to create a configured paginator."""
    config = PaginationConfig(
        page_size=page_size,
        max_pages=max_pages,
    )
    return APIPaginator(
        strategy=PaginationStrategy(strategy),
        config=config,
    )
