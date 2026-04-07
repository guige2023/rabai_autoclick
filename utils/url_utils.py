"""URL utilities: parsing, building, query parameter handling, and redirects."""

from __future__ import annotations

import urllib.parse as urlparse
from dataclasses import dataclass
from typing import Any

__all__ = [
    "URLParser",
    "URLBuilder",
    "parse_query_params",
    "build_query_string",
    "extract_domain",
    "normalize_url",
]


def parse_query_params(url: str) -> dict[str, str]:
    """Parse query parameters from URL into a dict."""
    parsed = urlparse.urlparse(url)
    return dict(urlparse.parse_qsl(parsed.query))


def build_query_string(params: dict[str, Any], doseq: bool = False) -> str:
    """Build a query string from a dict."""
    encoded = urlparse.urlencode(params, doseq=doseq)
    return encoded


def extract_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse.urlparse(url)
    return parsed.netloc


def normalize_url(url: str, trailing_slash: bool = False) -> str:
    """Normalize URL: lowercase, remove fragments, optionally add/remove trailing slash."""
    parsed = urlparse.urlparse(url.lower())
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path
    if trailing_slash and not path.endswith("/"):
        path += "/"
    elif not trailing_slash and path.endswith("/"):
        path = path.rstrip("/")
    normalized = urlparse.urlunparse((scheme, netloc, path, "", "", ""))
    return normalized


@dataclass
class URLParser:
    """Parse a URL into its components."""
    url: str

    @property
    def scheme(self) -> str:
        return urlparse.urlparse(self.url).scheme

    @property
    def netloc(self) -> str:
        return urlparse.urlparse(self.url).netloc

    @property
    def domain(self) -> str:
        netloc = self.netloc
        parts = netloc.split(":")[0].split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else netloc

    @property
    def subdomain(self) -> str:
        netloc = self.netloc.split(":")[0]
        parts = netloc.split(".")
        if len(parts) > 2:
            return parts[0]
        return ""

    @property
    def port(self) -> int | None:
        netloc = self.netloc
        if ":" in netloc:
            try:
                return int(netloc.split(":")[-1])
            except ValueError:
                return None
        return None

    @property
    def path(self) -> str:
        return urlparse.urlparse(self.url).path

    @property
    def query(self) -> dict[str, str]:
        return parse_query_params(self.url)

    @property
    def fragment(self) -> str:
        return urlparse.urlparse(self.url).fragment

    @property
    def is_secure(self) -> bool:
        return self.scheme == "https"

    def with_scheme(self, scheme: str) -> str:
        parsed = urlparse.urlparse(self.url)
        return urlparse.urlunparse((scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

    def with_domain(self, domain: str) -> str:
        parsed = urlparse.urlparse(self.url)
        netloc = f"{domain}{':' + str(self.port) if self.port else ''}"
        return urlparse.urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment))

    def with_path(self, path: str) -> str:
        parsed = urlparse.urlparse(self.url)
        return urlparse.urlunparse((parsed.scheme, parsed.netloc, path, parsed.params, parsed.query, parsed.fragment))

    def with_query(self, **params: Any) -> str:
        parsed = urlparse.urlparse(self.url)
        new_query = urlparse.urlencode(params)
        return urlparse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


class URLBuilder:
    """Build URLs with fluent interface."""

    def __init__(self, base: str = "") -> None:
        self._scheme = ""
        self._netloc = ""
        self._path = "/"
        self._query: dict[str, Any] = {}
        self._fragment = ""
        if base:
            self._parse(base)

    def _parse(self, url: str) -> None:
        parsed = urlparse.urlparse(url)
        self._scheme = parsed.scheme
        self._netloc = parsed.netloc
        self._path = parsed.path or "/"
        self._query = dict(urlparse.parse_qsl(parsed.query))
        self._fragment = parsed.fragment

    def scheme(self, scheme: str) -> "URLBuilder":
        self._scheme = scheme
        return self

    def netloc(self, netloc: str) -> "URLBuilder":
        self._netloc = netloc
        return self

    def path(self, path: str) -> "URLBuilder":
        self._path = "/" + path.strip("/")
        return self

    def query(self, **params: Any) -> "URLBuilder":
        self._query.update(params)
        return self

    def fragment(self, fragment: str) -> "URLBuilder":
        self._fragment = fragment
        return self

    def build(self) -> str:
        query = urlparse.urlencode(self._query) if self._query else ""
        return urlparse.urlunparse((
            self._scheme,
            self._netloc,
            self._path,
            "",
            query,
            self._fragment,
        ))
