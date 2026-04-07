"""Webpage crawler action for multi-page content extraction.

This module provides web crawling with link following,
depth control, rate limiting, and duplicate detection.

Example:
    >>> action = WebpageCrawlerAction()
    >>> result = action.execute(start_url="https://example.com", max_pages=10)
"""

from __future__ import annotations

import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Set


@dataclass
class CrawlResult:
    """Result from crawling a page."""
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    links: list[str] = field(default_factory=list)
    depth: int = 0
    status: str = "success"


@dataclass
class CrawlConfig:
    """Configuration for crawler."""
    max_pages: int = 100
    max_depth: int = 3
    delay: float = 1.0
    same_domain_only: bool = True
    follow_external: bool = False
    respect_robots_txt: bool = True
    user_agent: str = "Mozilla/5.0 (compatible; rabai_crawler/1.0)"


class WebpageCrawlerAction:
    """Webpage crawling action with link following.

    Provides multi-page crawling with depth control,
    rate limiting, and content extraction.

    Example:
        >>> action = WebpageCrawlerAction()
        >>> result = action.execute(
        ...     start_url="https://example.com",
        ...     max_pages=20,
        ...     extract_content=True
        ... )
    """

    def __init__(self, config: Optional[CrawlConfig] = None) -> None:
        """Initialize webpage crawler.

        Args:
            config: Optional crawler configuration.
        """
        self.config = config or CrawlConfig()
        self._visited: Set[str] = set()
        self._results: list[CrawlResult] = []

    def execute(
        self,
        start_url: str,
        max_pages: Optional[int] = None,
        max_depth: Optional[int] = None,
        extract_content: bool = True,
        extract_links: bool = True,
        callback: Optional[Callable[[CrawlResult], None]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute web crawling operation.

        Args:
            start_url: Starting URL for crawl.
            max_pages: Maximum pages to crawl.
            max_depth: Maximum crawl depth.
            extract_content: Whether to extract page content.
            extract_links: Whether to extract links.
            callback: Optional callback for each crawled page.
            **kwargs: Additional parameters.

        Returns:
            Crawling result dictionary.

        Raises:
            ValueError: If start_url is invalid.
        """
        import requests
        from urllib.parse import urljoin, urlparse

        if not start_url:
            raise ValueError("start_url is required")

        max_pages = max_pages or self.config.max_pages
        max_depth = max_depth or self.config.max_depth

        result: dict[str, Any] = {
            "success": True,
            "start_url": start_url,
            "crawled": 0,
            "results": [],
        }

        # Parse start URL for domain checking
        start_parsed = urlparse(start_url)
        base_domain = start_parsed.netloc

        # Queue: (url, depth)
        queue: list[tuple[str, int]] = [(start_url, 0)]
        self._visited.clear()
        self._results.clear()

        while queue and len(self._visited) < max_pages:
            url, depth = queue.pop(0)

            # Skip if already visited
            if url in self._visited:
                continue

            # Check depth
            if depth > max_depth:
                continue

            # Check domain
            url_parsed = urlparse(url)
            if self.config.same_domain_only and url_parsed.netloc != base_domain:
                if not self.config.follow_external:
                    continue

            try:
                # Fetch page
                response = requests.get(
                    url,
                    headers={"User-Agent": self.config.user_agent},
                    timeout=30,
                )
                response.raise_for_status()

                self._visited.add(url)

                # Parse page
                crawl_result = CrawlResult(
                    url=url,
                    depth=depth,
                    status="success" if response.ok else f"error_{response.status_code}",
                )

                # Extract title
                import re
                title_match = re.search(r"<title[^>]*>([^<]+)</title>", response.text, re.IGNORECASE)
                if title_match:
                    crawl_result.title = title_match.group(1).strip()

                # Extract content
                if extract_content:
                    # Remove scripts and styles
                    text = re.sub(r"<script[^>]*>.*?</script>", "", response.text, flags=re.DOTALL | re.IGNORECASE)
                    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
                    text = re.sub(r"<[^>]+>", " ", text)
                    crawl_result.content = re.sub(r"\s+", " ", text).strip()

                # Extract links
                if extract_links:
                    link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.IGNORECASE)
                    for match in link_pattern.finditer(response.text):
                        href = match.group(1)
                        if href.startswith("/"):
                            href = urljoin(url, href)
                        if href.startswith("http"):
                            crawl_result.links.append(href)
                            # Add to queue
                            if href not in self._visited:
                                queue.append((href, depth + 1))

                self._results.append(crawl_result)

                # Call callback if provided
                if callback:
                    callback(crawl_result)

                # Rate limiting
                time.sleep(self.config.delay)

            except requests.RequestException as e:
                crawl_result = CrawlResult(
                    url=url,
                    depth=depth,
                    status=f"error: {str(e)}",
                )
                self._results.append(crawl_result)
                self._visited.add(url)

            result["crawled"] = len(self._visited)
            if result["crawled"] >= max_pages:
                break

        result["results"] = [
            {
                "url": r.url,
                "title": r.title,
                "depth": r.depth,
                "status": r.status,
                "link_count": len(r.links),
            }
            for r in self._results
        ]

        return result

    def get_results(self) -> list[CrawlResult]:
        """Get all crawl results.

        Returns:
            List of CrawlResult objects.
        """
        return self._results

    def get_page_content(self, url: str) -> Optional[str]:
        """Get cached content for URL.

        Args:
            url: URL to look up.

        Returns:
            Page content or None.
        """
        for result in self._results:
            if result.url == url:
                return result.content
        return None

    def get_links(self, url: str) -> list[str]:
        """Get links found on URL.

        Args:
            url: URL to look up.

        Returns:
            List of links.
        """
        for result in self._results:
            if result.url == url:
                return result.links
        return []

    def clear_cache(self) -> None:
        """Clear all cached results."""
        self._visited.clear()
        self._results.clear()
