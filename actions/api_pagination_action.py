"""API pagination utilities for handling paginated API responses.

Supports cursor-based, offset-based, and link-header pagination.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class Page:
    """A single page of results."""

    items: list[Any]
    page_number: int | None = None
    total_pages: int | None = None
    total_count: int | None = None
    has_next: bool = False
    has_prev: bool = False
    next_cursor: str | None = None
    prev_cursor: str | None = None
    next_url: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class PaginationConfig:
    """Configuration for pagination behavior."""

    page_size: int = 100
    max_pages: int | None = None
    max_total: int | None = None
    timeout_seconds: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


class CursorPaginator(Generic[T]):
    """Cursor-based pagination handler.

    Args:
        fetch_fn: Async function that fetches a page given cursor and size.
        config: Pagination configuration.
    """

    def __init__(
        self,
        fetch_fn: Callable[[str | None, int], Any],
        config: PaginationConfig | None = None,
    ) -> None:
        self.fetch_fn = fetch_fn
        self.config = config or PaginationConfig()
        self._total_fetched = 0

    async def fetch_all(self) -> list[T]:
        """Fetch all pages and return combined results."""
        results: list[T] = []
        cursor: str | None = None

        while True:
            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break

            page_size = self.config.page_size
            if self.config.max_total:
                page_size = min(page_size, self.config.max_total - self._total_fetched)

            try:
                page = await self.fetch_fn(cursor, page_size)
            except Exception as e:
                logger.error("Page fetch failed: %s", e)
                break

            items = page.get("items", page.get("data", []))
            results.extend(items)
            self._total_fetched += len(items)

            cursor = page.get("next_cursor", page.get("cursor", page.get("next_page_token")))
            if not cursor:
                break

            if self.config.max_pages and len(results) // self.config.page_size >= self.config.max_pages:
                break

        return results

    async def fetch_pages(self):
        """Async generator yielding pages."""
        cursor: str | None = None
        page_num = 0

        while True:
            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break

            page_size = self.config.page_size
            if self.config.max_total:
                page_size = min(page_size, self.config.max_total - self._total_fetched)

            try:
                page = await self.fetch_fn(cursor, page_size)
            except Exception as e:
                logger.error("Page fetch failed at page %d: %s", page_num, e)
                break

            items = page.get("items", page.get("data", []))
            self._total_fetched += len(items)

            page_obj = Page(
                items=items,
                page_number=page_num,
                next_cursor=page.get("next_cursor"),
                prev_cursor=page.get("prev_cursor"),
                raw=page,
            )

            yield page_obj

            cursor = page_obj.next_cursor
            if not cursor:
                break

            page_num += 1
            if self.config.max_pages and page_num >= self.config.max_pages:
                break


class OffsetPaginator(Generic[T]):
    """Offset-based (page number) pagination handler."""

    def __init__(
        self,
        fetch_fn: Callable[[int, int], Any],
        config: PaginationConfig | None = None,
    ) -> None:
        self.fetch_fn = fetch_fn
        self.config = config or PaginationConfig()
        self._total_fetched = 0

    async def fetch_all(self) -> list[T]:
        """Fetch all pages and return combined results."""
        results: list[T] = []
        offset = 0
        page_num = 0

        while True:
            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break

            page_size = self.config.page_size
            if self.config.max_total:
                page_size = min(page_size, self.config.max_total - self._total_fetched)

            try:
                page = await self.fetch_fn(offset, page_size)
            except Exception as e:
                logger.error("Page fetch failed at offset %d: %s", offset, e)
                break

            items = page.get("items", page.get("data", page.get("results", [])))
            results.extend(items)
            self._total_fetched += len(items)

            total = page.get("total", page.get("total_count"))
            if total is not None:
                if offset + page_size >= total:
                    break
            elif not items:
                break

            offset += page_size
            page_num += 1

            if self.config.max_pages and page_num >= self.config.max_pages:
                break

        return results

    async def fetch_pages(self):
        """Async generator yielding pages."""
        offset = 0
        page_num = 0

        while True:
            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break

            page_size = self.config.page_size
            if self.config.max_total:
                page_size = min(page_size, self.config.max_total - self._total_fetched)

            try:
                page = await self.fetch_fn(offset, page_size)
            except Exception as e:
                logger.error("Page fetch failed at offset %d: %s", offset, e)
                break

            items = page.get("items", page.get("data", page.get("results", [])))
            total = page.get("total", page.get("total_count"))
            total_pages = page.get("total_pages")

            self._total_fetched += len(items)

            page_obj = Page(
                items=items,
                page_number=page_num,
                total_pages=total_pages,
                total_count=total,
                has_next=(offset + page_size) < (total or float("inf")),
                has_prev=offset > 0,
                raw=page,
            )

            yield page_obj

            if not page_obj.has_next:
                break

            offset += page_size
            page_num += 1

            if self.config.max_pages and page_num >= self.config.max_pages:
                break


class LinkHeaderPaginator(Generic[T]):
    """Link-header based pagination (GitHub-style)."""

    def __init__(
        self,
        fetch_fn: Callable[[str | None], Any],
        config: PaginationConfig | None = None,
    ) -> None:
        self.fetch_fn = fetch_fn
        self.config = config or PaginationConfig()
        self._total_fetched = 0

    def _parse_link_header(self, header: str | None) -> dict[str, str]:
        """Parse Link header into dict of rel -> URL."""
        if not header:
            return {}
        links = {}
        for part in header.split(","):
            part = part.strip()
            if not part:
                continue
            url, rel = part.split(";")
            url = url.strip()[1:-1]
            rel = rel.strip().replace('rel="', "").replace('"', "")
            links[rel] = url
        return links

    def _extract_cursor_from_url(self, url: str) -> str | None:
        """Extract cursor/page token from URL."""
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        for key in ("cursor", "page_token", "after", "page"):
            if key in query:
                return query[key][0]
        return None

    async def fetch_all(self) -> list[T]:
        """Fetch all pages and return combined results."""
        results: list[T] = []
        url: str | None = None
        page_num = 0

        while True:
            try:
                response = await self.fetch_fn(url)
            except Exception as e:
                logger.error("Page fetch failed: %s", e)
                break

            items = response.get("items", response.get("data", response.get("results", [])))
            results.extend(items)
            self._total_fetched += len(items)

            link_header = response.get("link_header", response.get("links", ""))
            links = self._parse_link_header(link_header)

            if self.config.max_pages and page_num >= self.config.max_pages:
                break

            if "next" not in links:
                break

            url = links["next"]
            page_num += 1

            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break

        return results

    async def fetch_pages(self):
        """Async generator yielding pages."""
        url: str | None = None
        page_num = 0

        while True:
            try:
                response = await self.fetch_fn(url)
            except Exception as e:
                logger.error("Page fetch failed at page %d: %s", page_num, e)
                break

            items = response.get("items", response.get("data", response.get("results", [])))
            self._total_fetched += len(items)

            link_header = response.get("link_header", response.get("links", ""))
            links = self._parse_link_header(link_header)

            next_url = links.get("next")
            prev_url = links.get("prev")

            page_obj = Page(
                items=items,
                page_number=page_num,
                next_url=next_url,
                prev_url=prev_url,
                next_cursor=self._extract_cursor_from_url(next_url) if next_url else None,
                prev_cursor=self._extract_cursor_from_url(prev_url) if prev_url else None,
                has_next="next" in links,
                has_prev="prev" in links,
                raw=response,
            )

            yield page_obj

            if not next_url:
                break

            url = next_url
            page_num += 1

            if self.config.max_pages and page_num >= self.config.max_pages:
                break

            if self.config.max_total and self._total_fetched >= self.config.max_total:
                break


def auto_detect_paginator(
    fetch_fn: Callable[..., Any],
    config: PaginationConfig | None = None,
) -> CursorPaginator | OffsetPaginator | LinkHeaderPaginator:
    """Auto-detect pagination type based on fetch function signature.

    Args:
        fetch_fn: Page fetch function.
        config: Pagination configuration.

    Returns:
        Appropriate paginator instance.
    """
    import inspect

    sig = inspect.signature(fetch_fn)
    params = list(sig.parameters.keys())

    if "url" in params or "link" in params:
        return LinkHeaderPaginator(fetch_fn, config)
    elif "cursor" in params or "page_token" in params:
        return CursorPaginator(fetch_fn, config)
    elif "offset" in params or "page" in params:
        return OffsetPaginator(fetch_fn, config)
    else:
        return OffsetPaginator(fetch_fn, config)
