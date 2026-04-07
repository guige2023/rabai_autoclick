"""
URL Resolver Action Module.

Resolves, normalizes, and validates URLs. Handles relative URLs,
redirect chains, query parameter manipulation, and URL parsing.

Example:
    >>> from url_resolver_action import URLResolver
    >>> resolver = URLResolver()
    >>> resolved = resolver.resolve("/products?id=5", base="https://example.com")
    >>> parsed = resolver.parse("https://example.com/path?q=test")
"""
from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ParsedURL:
    """Parsed URL components."""
    scheme: str
    netloc: str
    path: str
    params: str
    query: dict[str, str]
    fragment: str


@dataclass
class ResolvedURL:
    """Resolved URL with metadata."""
    url: str
    final_url: str
    redirects: list[str]
    status: int


class URLResolver:
    """Resolve and manipulate URLs."""

    def __init__(self):
        self._redirect_cache: dict[str, list[str]] = {}

    def parse(self, url: str) -> ParsedURL:
        """Parse URL into components."""
        parsed = urllib.parse.urlparse(url)
        query = dict(urllib.parse.parse_qsl(parsed.query))
        return ParsedURL(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            params=parsed.params,
            query=query,
            fragment=parsed.fragment,
        )

    def build(
        self,
        scheme: Optional[str] = None,
        netloc: Optional[str] = None,
        path: Optional[str] = None,
        params: Optional[str] = None,
        query: Optional[dict[str, Any]] = None,
        fragment: Optional[str] = None,
    ) -> str:
        """Build URL from components."""
        scheme = scheme or "https"
        netloc = netloc or ""
        path = path or "/"
        params = params or ""
        if query is not None:
            query_str = urllib.parse.urlencode(query)
        else:
            query_str = ""
        fragment = fragment or ""

        netloc_and_path = netloc + path
        url = urllib.parse.urlunparse((scheme, netloc_and_path, "", params, query_str, fragment))
        return url

    def resolve(self, url: str, base: str = "") -> str:
        """
        Resolve relative URL against base URL.

        Args:
            url: URL to resolve (may be relative)
            base: Base URL for resolution

        Returns:
            Absolute URL string
        """
        if url.startswith(("http://", "https://", "//")):
            if url.startswith("//"):
                return "https:" + url
            return url

        if base:
            return urllib.parse.urljoin(base, url)
        elif url.startswith("/"):
            return url
        return url

    def normalize(self, url: str, remove_fragments: bool = True) -> str:
        """Normalize URL: lowercase, sort query params, remove empty parts."""
        parsed = urllib.parse.urlparse(url.lower().strip())
        query = dict(sorted(urllib.parse.parse_qsl(parsed.query)))
        query_str = urllib.parse.urlencode(query)
        normalized = urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip("/"),
            parsed.params,
            query_str,
            "" if remove_fragments else parsed.fragment,
        ))
        return normalized

    def add_params(self, url: str, params: dict[str, Any]) -> str:
        """Add query parameters to URL."""
        parsed = urllib.parse.urlparse(url)
        existing = dict(urllib.parse.parse_qsl(parsed.query))
        existing.update({k: str(v) for k, v in params.items()})
        query_str = urllib.parse.urlencode(existing)
        return urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query_str,
            parsed.fragment,
        ))

    def remove_params(self, url: str, params: list[str]) -> str:
        """Remove query parameters from URL."""
        parsed = urllib.parse.urlparse(url)
        existing = dict(urllib.parse.parse_qsl(parsed.query))
        for p in params:
            existing.pop(p, None)
        query_str = urllib.parse.urlencode(existing)
        return urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            query_str,
            parsed.fragment,
        ))

    def get_param(self, url: str, param: str) -> Optional[str]:
        """Get single query parameter value."""
        parsed = urllib.parse.urlparse(url)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        return params.get(param)

    def set_param(self, url: str, param: str, value: Any) -> str:
        """Set single query parameter."""
        return self.add_params(url, {param: value})

    def is_valid(self, url: str) -> bool:
        """Check if URL is valid."""
        try:
            result = urllib.parse.urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    def get_domain(self, url: str) -> str:
        """Extract domain from URL."""
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc

    def get_tld(self, url: str) -> str:
        """Extract top-level domain."""
        domain = self.get_domain(url)
        parts = domain.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return domain

    def get_subdomain(self, url: str) -> Optional[str]:
        """Extract subdomain if present."""
        domain = self.get_domain(url)
        parts = domain.split(".")
        if len(parts) > 2:
            return ".".join(parts[:-2])
        return None

    def paths_equal(self, url1: str, url2: str) -> bool:
        """Check if two URLs have the same path (ignoring query)."""
        p1 = urllib.parse.urlparse(url1)
        p2 = urllib.parse.urlparse(url2)
        return p1.path == p2.path and p1.netloc == p2.netloc

    def is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        return self.get_domain(url1) == self.get_domain(url2)

    def extract_links(
        self,
        html: str,
        base_url: str = "",
    ) -> list[str]:
        """Extract all links from HTML and resolve them."""
        from html_parser_action import HTMLParserAction, CSSSelector
        parser = HTMLParserAction()
        doc = parser.parse_string(html)
        sel = CSSSelector("a")
        links: list[str] = []
        for elem in sel.match_all(doc.root):
            href = elem.get_attribute("href")
            if href:
                resolved = self.resolve(href, base_url)
                if self.is_valid(resolved):
                    links.append(resolved)
        return links

    def follow_redirects(
        self,
        url: str,
        timeout: float = 10.0,
    ) -> ResolvedURL:
        """Follow redirect chain and return final URL."""
        import urllib.request
        redirects: list[str] = []
        current = url
        max_redirects = 10
        status = 200

        try:
            for _ in range(max_redirects):
                req = urllib.request.Request(current, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    status = resp.status
                    location = resp.headers.get("Location")
                    if location:
                        redirects.append(current)
                        current = self.resolve(location, current)
                        if current in redirects:
                            break
                    else:
                        break
        except urllib.request.HTTPError as e:
            status = e.code
        except Exception:
            pass

        return ResolvedURL(
            url=url,
            final_url=current,
            redirects=redirects,
            status=status,
        )

    def slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug."""
        text = text.lower().strip()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s_-]+", "-", text)
        text = text.strip("-")
        return text

    def unslugify(self, slug: str) -> str:
        """Convert URL slug to human-readable text."""
        return slug.replace("-", " ").replace("_", " ").strip()


if __name__ == "__main__":
    resolver = URLResolver()

    url = "https://example.com/products?id=123&sort=asc"
    parsed = resolver.parse(url)
    print(f"Scheme: {parsed.scheme}, Domain: {parsed.netloc}")
    print(f"Path: {parsed.path}")
    print(f"Params: {parsed.query}")

    resolved = resolver.resolve("/about", base="https://example.com/shop")
    print(f"Resolved: {resolved}")

    normalized = resolver.normalize("HTTPS://EXAMPLE.COM//Path//?A=1&B=2")
    print(f"Normalized: {normalized}")

    with_params = resolver.add_params("https://example.com/search", {"q": "test", "page": 1})
    print(f"With params: {with_params}")
