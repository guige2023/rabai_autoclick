"""
Web Scraping Action Module.

Provides multi-strategy web scraping with rate limiting, retries,
proxy rotation, session management, and robots.txt compliance.

Example:
    >>> from web_scraper_action import WebScraperAction, ScrapeConfig
    >>> scraper = WebScraperAction()
    >>> config = ScrapeConfig(url="https://example.com", selectors={".title": "text", "img": "src"})
    >>> result = await scraper.scrape(config)
"""
from __future__ import annotations

import asyncio
import hashlib
import time
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass, field
from typing import Any, Optional
from html.parser import HTMLParser


@dataclass
class ScrapeConfig:
    """Configuration for a scrape operation."""
    url: str
    selectors: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0
    follow_redirects: bool = True
    user_agent: str = "Mozilla/5.0 (compatible; RabAiBot/1.0)"
    cookies: dict[str, str] = field(default_factory=dict)
    proxy: Optional[str] = None


@dataclass
class ScrapeResult:
    """Result of a scrape operation."""
    url: str
    status_code: int
    content: str
    headers: dict[str, str]
    elapsed_ms: float
    error: Optional[str] = None


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, requests_per_second: float = 1.0):
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


class RobotsTxtChecker:
    """Check robots.txt compliance for URLs."""

    def __init__(self, cache_ttl: float = 3600.0):
        self._cache: dict[str, tuple[str, float]] = {}
        self.cache_ttl = cache_ttl

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        parsed = urllib.parse.urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

        now = time.monotonic()
        if base_url in self._cache:
            content, cached_at = self._cache[base_url]
            if now - cached_at < self.cache_ttl:
                return self._check_rule(content, parsed.path, user_agent)

        try:
            req = urllib.request.Request(base_url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=10) as resp:
                content = resp.read().decode("utf-8", errors="ignore")
                self._cache[base_url] = (content, now)
                return self._check_rule(content, parsed.path, user_agent)
        except urllib.error.URLError:
            return True

    def _check_rule(self, robots_txt: str, path: str, user_agent: str) -> bool:
        current_agent: Optional[str] = None
        for line in robots_txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("user-agent:"):
                current_agent = line.split(":", 1)[1].strip()
            elif line.lower().startswith("disallow:") and current_agent in (user_agent, "*"):
                disallowed = line.split(":", 1)[1].strip()
                if disallowed and path.startswith(disallowed):
                    return False
            elif line.lower().startswith("allow:") and current_agent in (user_agent, "*"):
                allowed = line.split(":", 1)[1].strip()
                if allowed and path.startswith(allowed):
                    return True
        return True


@dataclass
class ProxyPool:
    """Rotating proxy pool."""
    proxies: list[str] = field(default_factory=list)
    _index: int = 0

    def get(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self._index % len(self.proxies)]
        self._index += 1
        return proxy

    def mark_failed(self, proxy: str) -> None:
        if proxy in self.proxies:
            self.proxies.remove(proxy)


class WebScraperAction:
    """Async web scraper with retry, rate limiting, and proxy support."""

    def __init__(
        self,
        requests_per_second: float = 1.0,
        proxy_list: Optional[list[str]] = None,
        cache_dir: Optional[str] = None,
    ):
        self.rate_limiter = RateLimiter(requests_per_second)
        self.robots_checker = RobotsTxtChecker()
        self.proxy_pool = ProxyPool(proxies=proxy_list or [])
        self.cache_dir = cache_dir

    async def scrape(self, config: ScrapeConfig) -> ScrapeResult:
        """
        Scrape a URL with the given configuration.

        Args:
            config: ScrapeConfig with URL and extraction rules

        Returns:
            ScrapeResult with content and metadata
        """
        start = time.monotonic()
        last_error: Optional[str] = None

        if not self.robots_checker.can_fetch(config.url, config.user_agent):
            return ScrapeResult(
                url=config.url,
                status_code=0,
                content="",
                headers={},
                elapsed_ms=(time.monotonic() - start) * 1000,
                error="Blocked by robots.txt",
            )

        for attempt in range(config.retry_count):
            await self.rate_limiter.acquire()
            try:
                result = await self._do_request(config)
                result.elapsed_ms = (time.monotonic() - start) * 1000
                return result
            except Exception as e:
                last_error = str(e)
                if attempt < config.retry_count - 1:
                    await asyncio.sleep(config.retry_delay * (attempt + 1))

        return ScrapeResult(
            url=config.url,
            status_code=0,
            content="",
            headers={},
            elapsed_ms=(time.monotonic() - start) * 1000,
            error=last_error,
        )

    async def scrape_multiple(self, configs: list[ScrapeConfig]) -> list[ScrapeResult]:
        """Scrape multiple URLs concurrently."""
        tasks = [self.scrape(c) for c in configs]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _do_request(self, config: ScrapeConfig) -> ScrapeResult:
        loop = asyncio.get_event_loop()
        headers = dict(config.headers)
        headers["User-Agent"] = config.user_agent
        if config.cookies:
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in config.cookies.items())

        proxy = self.proxy_pool.get()

        def _sync_request() -> tuple[int, str, dict[str, str]]:
            req = urllib.request.Request(config.url, headers=headers)
            if proxy:
                req.set_proxy(proxy, "http")
            try:
                with urllib.request.urlopen(req, timeout=config.timeout) as resp:
                    content = resp.read().decode("utf-8", errors="replace")
                    headers_out = dict(resp.headers)
                    return resp.status, content, headers_out
            except urllib.error.HTTPError as e:
                return e.code, "", dict(e.headers)
            except Exception as e:
                raise

        status, content, headers_out = await loop.run_in_executor(None, _sync_request)
        return ScrapeResult(
            url=config.url,
            status_code=status,
            content=content,
            headers=headers_out,
            elapsed_ms=0.0,
        )

    def extract_text(self, html: str, selector: str) -> list[str]:
        """Extract text from HTML using CSS selector."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector(selector)
        elems = sel.match_all(doc.root)
        return [parser.get_text(e) for e in elems]

    def extract_links(self, html: str, base_url: str = "") -> list[str]:
        """Extract all hyperlinks from HTML."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector("a")
        links: list[str] = []
        for elem in sel.match_all(doc.root):
            href = elem.get_attribute("href")
            if href:
                if href.startswith("/") and base_url:
                    parsed = urllib.parse.urlparse(base_url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"
                elif not href.startswith(("http://", "https://", "mailto:", "tel:")):
                    if base_url:
                        href = urllib.parse.urljoin(base_url, href)
                links.append(href)
        return links

    def cache_key(self, url: str) -> str:
        """Generate cache key for URL."""
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def scrape_with_cache(self, config: ScrapeConfig) -> ScrapeResult:
        """Scrape with in-memory caching based on URL hash."""
        key = self.cache_key(config.url)
        if hasattr(self, "_cache") and key in self._cache:
            return self._cache[key]
        result = asyncio.run(self.scrape(config))
        if not result.error:
            if not hasattr(self, "_cache"):
                self._cache = {}
            self._cache[key] = result
        return result


if __name__ == "__main__":
    import json

    scraper = WebScraperAction(requests_per_second=2.0)

    config = ScrapeConfig(
        url="https://example.com",
        selectors={"h1": "text", "a": "href"},
    )
    result = asyncio.run(scraper.scrape(config))
    print(f"Status: {result.status_code}")
    print(f"Content length: {len(result.content)}")
    if result.error:
        print(f"Error: {result.error}")
