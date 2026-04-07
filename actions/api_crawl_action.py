"""
API Crawler Action Module.

Crawls REST APIs, handles pagination, extracts nested data,
and manages concurrent API requests with exponential backoff.

Example:
    >>> from api_crawl_action import APICrawlerAction, APIEndpoint
    >>> crawler = APICrawlerAction()
    >>> endpoint = APIEndpoint(url="https://api.example.com/users", method="GET")
    >>> results = await crawler.crawl_paginated(endpoint, max_pages=10)
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class APIEndpoint:
    """API endpoint definition."""
    url: str
    method: str = "GET"
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    body: Optional[str] = None
    timeout: float = 30.0


@dataclass
class APIResponse:
    """API response wrapper."""
    status: int
    data: Any
    headers: dict[str, str]
    elapsed_ms: float
    error: Optional[str] = None


@dataclass
class PaginationRule:
    """Rule for extracting next page cursor/link."""
    type: str = "offset"
    offset_param: str = "offset"
    limit_param: str = "limit"
    page_param: str = "page"
    next_link_path: Optional[str] = None
    next_header: Optional[str] = None
    total_path: Optional[str] = None
    has_more_path: Optional[str] = None


@dataclass
class CrawlResult:
    """Result of a crawl operation."""
    total_items: int
    pages_crawled: int
    responses: list[APIResponse]
    errors: list[str]


class APICrawlerAction:
    """Async API crawler with pagination and rate limiting."""

    def __init__(
        self,
        requests_per_second: float = 5.0,
        max_concurrent: int = 10,
        retry_count: int = 3,
    ):
        self.rate_limiter = _AsyncRateLimiter(requests_per_second)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.retry_count = retry_count

    async def crawl_paginated(
        self,
        endpoint: APIEndpoint,
        pagination: Optional[PaginationRule] = None,
        max_pages: int = 100,
        max_items: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        Crawl paginated API endpoint.

        Args:
            endpoint: Initial API endpoint
            pagination: Pagination rule (offset or cursor-based)
            max_pages: Maximum number of pages to crawl
            max_items: Stop after this many total items

        Returns:
            Flattened list of all items found across pages
        """
        pagination = pagination or PaginationRule()
        all_items: list[dict[str, Any]] = []
        page = 1
        offset = 0
        has_more = True

        current_endpoint = endpoint

        while has_more and page <= max_pages:
            await self.rate_limiter.acquire()

            async with self.semaphore:
                response = await self._request(current_endpoint)

            if response.error:
                break

            items = self._extract_items(response.data, pagination)

            if not items:
                has_more = False
                break

            all_items.extend(items)

            if max_items and len(all_items) >= max_items:
                all_items = all_items[:max_items]
                break

            if pagination.next_link_path:
                next_url = self._extract_next_link(response.data, pagination.next_link_path)
                if next_url:
                    current_endpoint = APIEndpoint(url=next_url, headers=endpoint.headers)
                else:
                    has_more = False
            else:
                offset += len(items)
                params = dict(endpoint.params)
                if pagination.type == "offset":
                    params[pagination.offset_param] = offset
                    params[pagination.limit_param] = params.get(pagination.limit_param, 20)
                elif pagination.type == "page":
                    params[pagination.page_param] = page + 1
                current_endpoint = APIEndpoint(
                    url=endpoint.url,
                    method=endpoint.method,
                    params=params,
                    headers=endpoint.headers,
                    body=endpoint.body,
                )

            if pagination.has_more_path:
                has_more = self._extract_bool(response.data, pagination.has_more_path)
            elif pagination.total_path:
                total = self._extract_int(response.data, pagination.total_path)
                has_more = len(all_items) < total

            page += 1

        return all_items

    async def crawl_all(
        self,
        endpoints: list[APIEndpoint],
        on_response: Optional[Callable[[APIResponse], None]] = None,
    ) -> list[APIResponse]:
        """Crawl multiple endpoints concurrently."""
        tasks = [self._request_with_retry(ep) for ep in endpoints]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        results: list[APIResponse] = []
        for r in responses:
            if isinstance(r, APIResponse):
                if on_response:
                    on_response(r)
                results.append(r)
            elif isinstance(r, Exception):
                results.append(APIResponse(0, None, {}, 0, error=str(r)))
        return results

    async def _request(self, endpoint: APIEndpoint) -> APIResponse:
        start = time.monotonic()

        def _sync():
            url = endpoint.url
            if endpoint.params and endpoint.method == "GET":
                query = urllib.parse.urlencode(endpoint.params)
                url = f"{endpoint.url}?{query}" if "?" not in endpoint.url else f"{endpoint.url}&{query}"

            headers = dict(endpoint.headers)
            headers.setdefault("Accept", "application/json")

            req = urllib.request.Request(url, method=endpoint.method, headers=headers)
            if endpoint.body:
                req.data = endpoint.body.encode("utf-8")
                headers.setdefault("Content-Type", "application/json")

            try:
                with urllib.request.urlopen(req, timeout=endpoint.timeout) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    data = json.loads(body) if body else None
                    return APIResponse(
                        status=resp.status,
                        data=data,
                        headers=dict(resp.headers),
                        elapsed_ms=(time.monotonic() - start) * 1000,
                    )
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    data = body
                return APIResponse(
                    status=e.code,
                    data=data,
                    headers=dict(e.headers),
                    elapsed_ms=(time.monotonic() - start) * 1000,
                    error=f"HTTP {e.code}",
                )
            except Exception as ex:
                return APIResponse(0, None, {}, (time.monotonic() - start) * 1000, error=str(ex))

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _sync)

    async def _request_with_retry(self, endpoint: APIEndpoint) -> APIResponse:
        last_error: Optional[str] = None
        for attempt in range(self.retry_count):
            response = await self._request(endpoint)
            if not response.error and response.status < 500:
                return response
            last_error = response.error
            if attempt < self.retry_count - 1:
                await asyncio.sleep(2 ** attempt)
        return APIResponse(0, None, {}, 0, error=last_error)

    def _extract_items(self, data: Any, pagination: PaginationRule) -> list:
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("results", "data", "items", "records"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            if "items" in str(data):
                return data.get("items", [])
        return []

    def _extract_next_link(self, data: Any, path: str) -> Optional[str]:
        if not path or not isinstance(data, dict):
            return None
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current if isinstance(current, str) else None

    def _extract_bool(self, data: Any, path: str) -> bool:
        val = self._extract_value(data, path)
        return bool(val)

    def _extract_int(self, data: Any, path: str) -> int:
        val = self._extract_value(data, path)
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0

    def _extract_value(self, data: Any, path: str) -> Any:
        if not path or not isinstance(data, dict):
            return None
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current


class _AsyncRateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


if __name__ == "__main__":
    crawler = APICrawlerAction(requests_per_second=5.0)

    endpoint = APIEndpoint(
        url="https://jsonplaceholder.typicode.com/posts",
        params={"_limit": 10},
    )
    result = asyncio.run(crawler.crawl_paginated(endpoint, max_pages=3))
    print(f"Crawled {len(result)} items")
