"""
Batch Crawler Action Module.

Crawls multiple URLs concurrently with queue management,
priority scheduling, and crawl result aggregation.

Example:
    >>> from batch_crawler_action import BatchCrawler
    >>> crawler = BatchCrawler(max_concurrent=10)
    >>> crawler.add_urls(["https://a.com", "https://b.com"])
    >>> results = await crawler.crawl_all()
"""
from __future__ import annotations

import asyncio
import time
import urllib.parse
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Optional


class Priority(IntEnum):
    """Crawl priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class CrawlTask:
    """A crawl task with metadata."""
    url: str
    priority: Priority = Priority.NORMAL
    depth: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    added_at: float = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class CrawlResult:
    """Result of crawling a single URL."""
    url: str
    status: int
    content: bytes
    headers: dict[str, str]
    elapsed_ms: float
    error: Optional[str] = None
    depth: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class BatchCrawler:
    """Concurrent batch crawler with priority queue."""

    def __init__(
        self,
        max_concurrent: int = 5,
        max_retries: int = 3,
        timeout: float = 30.0,
        respect_robots: bool = False,
    ):
        self._max_concurrent = max_concurrent
        self._max_retries = max_retries
        self._timeout = timeout
        self._respect_robots = respect_robots
        self._queue: list[CrawlTask] = []
        self._results: list[CrawlResult] = []
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._in_progress: int = 0
        self._rate_limiter = _RateLimiter(requests_per_second=10.0)

    def add_url(
        self,
        url: str,
        priority: Priority = Priority.NORMAL,
        depth: int = 0,
        **metadata,
    ) -> "BatchCrawler":
        """Add a URL to the crawl queue."""
        task = CrawlTask(
            url=url,
            priority=priority,
            depth=depth,
            metadata=metadata,
            added_at=time.time(),
        )
        self._queue.append(task)
        self._queue.sort(key=lambda t: t.priority, reverse=True)
        return self

    def add_urls(
        self,
        urls: list[str],
        priority: Priority = Priority.NORMAL,
        depth: int = 0,
    ) -> "BatchCrawler":
        """Add multiple URLs to the crawl queue."""
        for url in urls:
            self.add_url(url, priority, depth)
        return self

    async def crawl_all(
        self,
        on_result: Optional[Callable[[CrawlResult], None]] = None,
    ) -> list[CrawlResult]:
        """
        Crawl all queued URLs concurrently.

        Args:
            on_result: Optional callback for each completed crawl

        Returns:
            List of CrawlResult
        """
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        tasks = []

        for task in self._queue:
            task = asyncio.create_task(self._crawl(task, on_result))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, CrawlResult):
                self._results.append(result)
            elif isinstance(result, Exception):
                pass

        return self._results

    async def _crawl(
        self,
        task: CrawlTask,
        on_result: Optional[Callable[[CrawlResult], None]],
    ) -> CrawlResult:
        async with self._semaphore:
            self._in_progress += 1
            task.started_at = time.time()

            await self._rate_limiter.acquire()

            result = await self._do_crawl(task)

            task.completed_at = time.time()
            self._in_progress -= 1

            if on_result:
                on_result(result)

            return result

    async def _do_crawl(self, task: CrawlTask) -> CrawlResult:
        start = time.monotonic()

        for attempt in range(self._max_retries):
            try:
                import urllib.request
                req = urllib.request.Request(
                    task.url,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; BatchCrawler/1.0)"},
                )
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    content = resp.read()
                    elapsed = (time.monotonic() - start) * 1000
                    return CrawlResult(
                        url=task.url,
                        status=resp.status,
                        content=content,
                        headers=dict(resp.headers),
                        elapsed_ms=elapsed,
                        depth=task.depth,
                        metadata=task.metadata,
                    )
            except urllib.error.HTTPError as e:
                if e.code >= 500 and attempt < self._max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                elapsed = (time.monotonic() - start) * 1000
                return CrawlResult(
                    url=task.url,
                    status=e.code,
                    content=b"",
                    headers=dict(e.headers) if e.headers else {},
                    elapsed_ms=elapsed,
                    error=f"HTTP {e.code}",
                    depth=task.depth,
                    metadata=task.metadata,
                )
            except Exception as e:
                elapsed = (time.monotonic() - start) * 1000
                return CrawlResult(
                    url=task.url,
                    status=0,
                    content=b"",
                    headers={},
                    elapsed_ms=elapsed,
                    error=str(e),
                    depth=task.depth,
                    metadata=task.metadata,
                )

        elapsed = (time.monotonic() - start) * 1000
        return CrawlResult(
            url=task.url,
            status=0,
            content=b"",
            headers={},
            elapsed_ms=elapsed,
            error="Max retries exceeded",
            depth=task.depth,
            metadata=task.metadata,
        )

    def get_results(self) -> list[CrawlResult]:
        """Get all crawl results."""
        return list(self._results)

    def get_stats(self) -> dict[str, Any]:
        """Get crawl statistics."""
        if not self._results:
            return {
                "total": 0,
                "successful": 0,
                "failed": 0,
                "avg_latency_ms": 0,
            }
        successful = [r for r in self._results if r.status == 200]
        failed = [r for r in self._results if r.status != 200]
        latencies = [r.elapsed_ms for r in self._results if r.elapsed_ms > 0]
        return {
            "total": len(self._results),
            "successful": len(successful),
            "failed": len(failed),
            "avg_latency_ms": sum(latencies) / len(latencies) if latencies else 0,
            "pending": len(self._queue),
            "in_progress": self._in_progress,
        }

    def clear(self) -> None:
        """Clear queue and results."""
        self._queue.clear()
        self._results.clear()


class _RateLimiter:
    def __init__(self, requests_per_second: float):
        self.rate = requests_per_second
        self.tokens = requests_per_second
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
    async def test():
        crawler = BatchCrawler(max_concurrent=3)
        crawler.add_urls([
            "https://example.com",
            "https://httpbin.org/get",
            "https://httpbin.org/status/200",
        ])
        results = await crawler.crawl_all()
        print(f"Crawled {len(results)} URLs")
        for r in results:
            print(f"  {r.url}: {r.status} ({r.elapsed_ms:.0f}ms)")

    asyncio.run(test())
