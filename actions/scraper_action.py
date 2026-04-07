"""Web scraper action module for rabai_autoclick.

This module provides web scraping capabilities including HTML parsing,
CSS selection, XPath queries, and content extraction.

Example:
    >>> action = WebScraperAction()
    >>> result = action.execute(url="https://example.com")
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse


@dataclass
class ScrapedContent:
    """Represents scraped content from a webpage."""
    url: str
    title: Optional[str] = None
    text: Optional[str] = None
    html: Optional[str] = None
    links: list[str] = field(default_factory=list)
    images: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class SelectorConfig:
    """Configuration for content selection."""
    css_selectors: list[str] = field(default_factory=list)
    xpath_selectors: list[str] = field(default_factory=list)
    attribute: Optional[str] = None
    multiple: bool = False


class WebScraperAction:
    """Web scraping action with multiple extraction strategies.

    Supports CSS selectors, XPath queries, regex patterns, and
    attribute extraction. Handles relative URLs and image sources.

    Example:
        >>> action = WebScraperAction()
        >>> result = action.execute(
        ...     url="https://news.ycombinator.com",
        ...     selectors=["a.story-link"],
        ...     extract="href"
        ... )
    """

    def __init__(self) -> None:
        """Initialize the web scraper."""
        self._session: Optional[Any] = None

    def execute(
        self,
        url: str,
        selectors: Optional[list[str]] = None,
        extract: str = "text",
        headers: Optional[dict[str, str]] = None,
        timeout: int = 30,
    ) -> ScrapedContent:
        """Execute web scraping operation.

        Args:
            url: Target URL to scrape.
            selectors: CSS selectors to extract specific elements.
            extract: What to extract ('text', 'html', 'href', 'src').
            headers: Custom HTTP headers.
            timeout: Request timeout in seconds.

        Returns:
            ScrapedContent object with extracted data.

        Raises:
            ValueError: If URL is invalid or extraction fails.
            requests.RequestException: If HTTP request fails.
        """
        import requests

        if not url or not url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid URL: {url}")

        default_headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36",
        }
        if headers:
            default_headers.update(headers)

        try:
            response = requests.get(
                url,
                headers=default_headers,
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.RequestException as e:
            raise requests.RequestException(f"Failed to fetch {url}: {e}") from e

        html = response.text
        parsed = urlparse(url)

        content = ScrapedContent(
            url=url,
            html=html,
        )

        # Simple title extraction
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if title_match:
            content.title = title_match.group(1).strip()

        # Extract links
        link_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>', re.IGNORECASE)
        for match in link_pattern.finditer(html):
            href = match.group(1)
            if href.startswith("/"):
                href = urljoin(url, href)
            if href.startswith("http"):
                content.links.append(href)

        # Extract images
        img_pattern = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)
        for match in img_pattern.finditer(html):
            src = match.group(1)
            if src.startswith("/"):
                src = urljoin(url, src)
            if src.startswith("http"):
                content.images.append(src)

        # Extract metadata
        meta_pattern = re.compile(
            r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']+)["\']',
            re.IGNORECASE,
        )
        for match in meta_pattern.finditer(html):
            content.metadata[match.group(1)] = match.group(2)

        # Extract text
        text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        content.text = text

        return content

    def extract_with_selectors(
        self,
        html: str,
        config: SelectorConfig,
    ) -> list[str]:
        """Extract content using selector configuration.

        Args:
            html: HTML content to parse.
            config: Selector configuration.

        Returns:
            List of extracted values.
        """
        results: list[str] = []

        if not html:
            return results

        for selector in config.css_selectors or []:
            pattern = self._css_to_regex(selector)
            matches = pattern.finditer(html)
            for match in matches:
                if config.attribute and config.multiple:
                    results.append(match.group(config.attribute))
                elif config.attribute:
                    results.append(match.group(config.attribute))
                    break
                elif config.multiple:
                    results.append(match.group(0))
                else:
                    results.append(match.group(0))
                    break

        return results

    def _css_to_regex(self, selector: str) -> re.Pattern:
        """Convert simple CSS selector to regex pattern.

        Args:
            selector: CSS selector string.

        Returns:
            Compiled regex pattern.
        """
        tag_match = re.match(r"^([a-zA-Z0-9]+)", selector)
        tag = tag_match.group(1) if tag_match else "div"

        class_match = re.search(r"\.([a-zA-Z0-9_-]+)", selector)
        id_match = re.search(r"#([a-zA-Z0-9_-]+)", selector)

        pattern_parts = [rf"<{tag}"]
        if id_match:
            pattern_parts.append(rf'[^>]*id=["\'](?:{id_match.group(1)})["\']')
        if class_match:
            pattern_parts.append(rf'[^>]*class=["\'](?:[^"\']*\s)?{class_match.group(1)}(?:\s[^"\']*)?["\']')
        pattern_parts.append(r"[^>]*>([^<]*)</{tag}>")

        pattern = "".join(pattern_parts)
        return re.compile(pattern, re.IGNORECASE | re.DOTALL)
