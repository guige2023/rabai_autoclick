"""
Navigation and Flow Walker Action Module.

Manages multi-step navigation flows, URL sequences,
breadcrumbs, and site traversal patterns.

Example:
    >>> from navigation_walker_action import NavigationWalker
    >>> walker = NavigationWalker()
    >>> await walker.follow_nav(page, steps=[("/home", "/products", "/checkout")])
"""
from __future__ import annotations

import asyncio
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class NavStep:
    """A single navigation step."""
    url: str
    label: str = ""
    wait_for: Optional[str] = None
    wait_timeout: float = 10000
    expected_title: Optional[str] = None


@dataclass
class NavigationFlow:
    """A sequence of navigation steps."""
    name: str
    steps: list[NavStep]
    on_complete: Optional[Callable] = None
    on_error: Optional[Callable[[int, str], None]] = None


@dataclass
class NavResult:
    """Result of a navigation operation."""
    success: bool
    current_url: str
    current_title: str
    steps_completed: int
    error: Optional[str] = None


class NavigationWalker:
    """Walk through site navigation flows."""

    def __init__(self):
        self._flows: dict[str, NavigationFlow] = {}
        self._history: list[str] = []

    def add_flow(self, name: str, flow: NavigationFlow) -> None:
        """Register a named navigation flow."""
        self._flows[name] = flow

    def get_flow(self, name: str) -> Optional[NavigationFlow]:
        """Get a registered flow by name."""
        return self._flows.get(name)

    async def execute_flow(
        self,
        page: Any,
        flow: NavigationFlow,
    ) -> NavResult:
        """
        Execute a navigation flow on a Playwright page.

        Args:
            page: Playwright page object
            flow: NavigationFlow to execute

        Returns:
            NavResult with execution details
        """
        self._history = []
        steps_completed = 0

        for i, step in enumerate(flow.steps):
            try:
                await page.goto(step.url, wait_until="domcontentloaded", timeout=step.wait_timeout)
                self._history.append(step.url)

                if step.wait_for:
                    await page.wait_for_selector(step.wait_for, timeout=step.wait_timeout)

                if step.expected_title:
                    title = await page.title()
                    if step.expected_title.lower() not in title.lower():
                        pass

                steps_completed += 1

            except Exception as e:
                if flow.on_error:
                    flow.on_error(i, str(e))
                return NavResult(
                    success=False,
                    current_url=page.url if page else "",
                    current_title=await page.title() if page else "",
                    steps_completed=steps_completed,
                    error=f"Step {i+1} failed: {e}",
                )

        if flow.on_complete:
            await flow.on_complete()

        return NavResult(
            success=True,
            current_url=page.url if page else "",
            current_title=await page.title() if page else "",
            steps_completed=steps_completed,
        )

    async def follow_links(
        self,
        page: Any,
        start_url: str,
        link_selector: str,
        max_depth: int = 3,
        max_links: int = 10,
    ) -> list[dict[str, str]]:
        """
        Follow links from a starting page up to max_depth levels.

        Args:
            page: Playwright page
            start_url: Starting URL
            link_selector: CSS selector for links to follow
            max_depth: Maximum link depth
            max_links: Maximum links per page

        Returns:
            List of visited URLs with metadata
        """
        visited: list[dict[str, str]] = []
        visited_urls: set[str] = set()
        to_visit: list[tuple[str, int]] = [(start_url, 0)]

        while to_visit:
            url, depth = to_visit.pop(0)
            if depth > max_depth or len(visited) >= max_links:
                continue
            if url in visited_urls:
                continue

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                visited_urls.add(url)

                title = await page.title()
                visited.append({"url": url, "title": title, "depth": depth})

                links = await page.query_selector_all(link_selector)
                for link_elem in links[:max_links]:
                    href = await link_elem.get_attribute("href")
                    if href:
                        full_url = urllib.parse.urljoin(url, href)
                        if full_url not in visited_urls:
                            to_visit.append((full_url, depth + 1))

            except Exception:
                continue

        return visited

    async def extract_breadcrumbs(self, page: Any) -> list[dict[str, str]]:
        """Extract breadcrumb navigation from current page."""
        breadcrumbs: list[dict[str, str]] = []

        selectors = [
            "nav.breadcrumb a",
            ".breadcrumbs a",
            "[aria-label='Breadcrumb'] a",
            "ol.breadcrumb a",
            ".breadcrumb a",
        ]

        for sel in selectors:
            try:
                elements = await page.query_selector_all(sel)
                for elem in elements:
                    text = await elem.text_content()
                    href = await elem.get_attribute("href")
                    if text and href:
                        breadcrumbs.append({"label": text.strip(), "url": href})
                if breadcrumbs:
                    break
            except Exception:
                continue

        return breadcrumbs

    async def get_sitemap(self, page: Any, base_url: str) -> list[str]:
        """Extract URLs from sitemap.xml if available."""
        sitemap_url = urllib.parse.urljoin(base_url, "/sitemap.xml")
        try:
            await page.goto(sitemap_url, timeout=10000)
            content = await page.content()
            urls = re.findall(r"<loc>(.*?)</loc>", content)
            return urls
        except Exception:
            return []

    async def wait_for_url_pattern(
        self,
        page: Any,
        pattern: str,
        timeout: float = 10000,
    ) -> bool:
        """Wait for URL to match a regex pattern."""
        import re
        compiled = re.compile(pattern)
        start = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start) * 1000 < timeout:
            if compiled.search(page.url):
                return True
            await asyncio.sleep(0.1)
        return False

    def get_history(self) -> list[str]:
        """Get URLs visited in last flow."""
        return list(self._history)

    def build_pagination_urls(
        self,
        base_url: str,
        start_page: int = 1,
        end_page: int = 10,
        page_param: str = "page",
    ) -> list[str]:
        """Build a list of pagination URLs."""
        parsed = urllib.parse.urlparse(base_url)
        queries = urllib.parse.parse_qsl(parsed.query)
        urls: list[str] = []

        for page_num in range(start_page, end_page + 1):
            new_queries = [(k, v) if k != page_param else (page_param, str(page_num)) for k, v in queries]
            if page_param not in [q[0] for q in queries]:
                new_queries.append((page_param, str(page_num)))
            new_query = urllib.parse.urlencode(new_queries)
            url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, ""))
            urls.append(url)

        return urls

    def extract_pagination_links(self, page: Any) -> list[str]:
        """Extract pagination links from current page."""
        links: list[str] = []

        selectors = [
            "nav.pagination a",
            ".pagination a",
            "ul.pagination a",
            "[class*='pagination'] a",
        ]

        for sel in selectors:
            try:
                elements = await page.query_selector_all(sel)
                for elem in elements:
                    href = await elem.get_attribute("href")
                    if href:
                        links.append(href)
                if links:
                    break
            except Exception:
                continue

        return links


if __name__ == "__main__":
    print("NavigationWalker module loaded")
