"""Sitemap parser action for extracting URLs from sitemaps.

This module provides sitemap XML parsing with support for
URL extraction, changefreq detection, and priority parsing.

Example:
    >>> action = SitemapAction()
    >>> result = action.execute(url="https://example.com/sitemap.xml")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class SitemapURL:
    """Represents a URL entry from sitemap."""
    loc: str
    lastmod: Optional[str] = None
    changefreq: Optional[str] = None
    priority: Optional[float] = None


@dataclass
class SitemapResult:
    """Sitemap parsing result."""
    urls: list[SitemapURL] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)
    url_count: int = 0


class SitemapAction:
    """Sitemap parsing action.

    Extracts URLs from XML sitemaps with support for
    sitemap indexes and nested sitemaps.

    Example:
        >>> action = SitemapAction()
        >>> result = action.execute(
        ...     url="https://example.com/sitemap.xml",
        ...     recursive=True
        ... )
    """

    def __init__(self) -> None:
        """Initialize sitemap action."""
        self._last_result: Optional[SitemapResult] = None

    def execute(
        self,
        url: str,
        recursive: bool = False,
        filter_pattern: Optional[str] = None,
        max_depth: int = 2,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute sitemap parsing.

        Args:
            url: Sitemap URL.
            recursive: Whether to follow sitemap index.
            filter_pattern: Optional regex to filter URLs.
            max_depth: Maximum recursion depth.
            **kwargs: Additional parameters.

        Returns:
            Parsing result dictionary.

        Raises:
            ValueError: If URL is invalid.
        """
        try:
            import requests
        except ImportError:
            return {
                "success": False,
                "error": "requests not installed. Run: pip install requests",
            }

        if not url or not url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid sitemap URL: {url}")

        result: dict[str, Any] = {"success": True, "url": url}

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Check if it's a sitemap index
            if "<sitemapindex" in response.text.lower():
                sitemaps = self._parse_sitemap_index(response.text)
                result["is_index"] = True
                result["sitemaps"] = sitemaps

                if recursive and len(sitemaps) <= max_depth * 10:
                    all_urls = []
                    for sitemap_url in sitemaps[:max_depth * 10]:
                        sub_result = self._fetch_sitemap(sitemap_url)
                        if sub_result:
                            all_urls.extend(sub_result)
                    result["urls"] = all_urls
                else:
                    result["url_count"] = len(sitemaps)
            else:
                urls = self._parse_sitemap(response.text)
                result["is_index"] = False
                result["urls"] = urls

            # Apply filter
            if filter_pattern:
                import re
                pattern = re.compile(filter_pattern)
                result["urls"] = [u for u in result["urls"] if pattern.search(u)]

            result["url_count"] = len(result["urls"])

            self._last_result = SitemapResult(
                urls=[SitemapURL(**u) if isinstance(u, dict) else u for u in result["urls"]],
            )

        except requests.RequestException as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _parse_sitemap_index(self, xml: str) -> list[str]:
        """Parse sitemap index to get sitemap URLs.

        Args:
            xml: XML content.

        Returns:
            List of sitemap URLs.
        """
        import re
        pattern = re.compile(r"<sitemap>.*?<loc>(.*?)</loc>.*?</sitemap>", re.DOTALL | re.IGNORECASE)
        return [m.group(1) for m in pattern.finditer(xml)]

    def _parse_sitemap(self, xml: str) -> list[dict[str, Any]]:
        """Parse sitemap XML to extract URLs.

        Args:
            xml: XML content.

        Returns:
            List of URL dictionaries.
        """
        import re

        urls = []
        # Match url elements
        url_pattern = re.compile(
            r"<url>(.*?)</url>",
            re.DOTALL | re.IGNORECASE,
        )

        for url_match in url_pattern.finditer(xml):
            url_content = url_match.group(1)

            loc_match = re.search(r"<loc>(.*?)</loc>", url_content, re.IGNORECASE)
            if not loc_match:
                continue

            url_data: dict[str, Any] = {
                "loc": loc_match.group(1).strip(),
            }

            # Optional elements
            lastmod = re.search(r"<lastmod>(.*?)</lastmod>", url_content, re.IGNORECASE)
            if lastmod:
                url_data["lastmod"] = last_match.group(1).strip()

            changefreq = re.search(r"<changefreq>(.*?)</changefreq>", url_content, re.IGNORECASE)
            if changefreq:
                url_data["changefreq"] = changefreq.group(1).strip()

            priority = re.search(r"<priority>(.*?)</priority>", url_content, re.IGNORECASE)
            if priority:
                try:
                    url_data["priority"] = float(priority.group(1).strip())
                except ValueError:
                    pass

            urls.append(url_data)

        return urls

    def _fetch_sitemap(self, url: str) -> list[str]:
        """Fetch and parse a sitemap URL.

        Args:
            url: Sitemap URL.

        Returns:
            List of URLs from the sitemap.
        """
        try:
            import requests
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            urls = self._parse_sitemap(response.text)
            return [u["loc"] for u in urls]
        except Exception:
            return []

    def filter_by_pattern(self, pattern: str) -> list[str]:
        """Filter last result URLs by pattern.

        Args:
            pattern: Regex pattern.

        Returns:
            Filtered URLs.
        """
        if not self._last_result:
            return []

        import re
        regex = re.compile(pattern)
        return [u.loc for u in self._last_result.urls if regex.search(u.loc)]

    def filter_by_date(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[str]:
        """Filter URLs by lastmod date.

        Args:
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).

        Returns:
            Filtered URLs.
        """
        if not self._last_result:
            return []

        from datetime import datetime

        results = []
        for url in self._last_result.urls:
            if not url.lastmod:
                continue

            try:
                mod_date = datetime.fromisoformat(url.lastmod[:10])

                if start_date:
                    start = datetime.fromisoformat(start_date)
                    if mod_date < start:
                        continue

                if end_date:
                    end = datetime.fromisoformat(end_date)
                    if mod_date > end:
                        continue

                results.append(url.loc)
            except ValueError:
                continue

        return results

    def get_priority_groups(self) -> dict[str, list[str]]:
        """Group URLs by priority.

        Returns:
            Dictionary of priority -> URLs.
        """
        if not self._last_result:
            return {}

        groups: dict[str, list[str]] = {
            "high": [],
            "medium": [],
            "low": [],
            "none": [],
        }

        for url in self._last_result.urls:
            if url.priority is None:
                groups["none"].append(url.loc)
            elif url.priority >= 0.8:
                groups["high"].append(url.loc)
            elif url.priority >= 0.5:
                groups["medium"].append(url.loc)
            else:
                groups["low"].append(url.loc)

        return groups

    def export_to_csv(self, path: str) -> dict[str, Any]:
        """Export URLs to CSV.

        Args:
            path: Output path.

        Returns:
            Result dictionary.
        """
        if not self._last_result:
            return {"success": False, "error": "No data to export"}

        import csv

        try:
            with open(path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["URL", "Last Modified", "Change Frequency", "Priority"])
                for url in self._last_result.urls:
                    writer.writerow([
                        url.loc,
                        url.lastmod or "",
                        url.changefreq or "",
                        url.priority or "",
                    ])
            return {"success": True, "exported": len(self._last_result.urls), "path": path}
        except Exception as e:
            return {"success": False, "error": str(e)}
