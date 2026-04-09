"""
URL automation module.

Provides URL parsing, manipulation, and automation for
navigating and interacting with web resources.

Author: Aito Auto Agent
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)


class UrlComponent(Enum):
    """URL component types."""
    SCHEME = auto()
    NETLOC = auto()
    PATH = auto()
    PARAMS = auto()
    QUERY = auto()
    FRAGMENT = auto()


@dataclass
class UrlParts:
    """Parsed URL components."""
    scheme: str
    netloc: str
    path: str
    params: str
    query: dict[str, list[str]]
    fragment: str

    @property
    def host(self) -> str:
        """Get host from netloc."""
        return self.netloc.split(":")[0] if self.netloc else ""

    @property
    def port(self) -> Optional[int]:
        """Get port from netloc."""
        if ":" in self.netloc:
            try:
                return int(self.netloc.split(":")[1])
            except (IndexError, ValueError):
                return None
        return None

    @property
    def origin(self) -> str:
        """Get origin (scheme + host)."""
        port_part = f":{self.port}" if self.port else ""
        return f"{self.scheme}://{self.host}{port_part}"

    def to_string(self) -> str:
        """Convert back to URL string."""
        query_string = urlencode(self.query, doseq=True)
        netloc = self.netloc

        return urlunparse((
            self.scheme,
            netloc,
            self.path,
            self.params,
            query_string,
            self.fragment
        ))


class UrlAutomator:
    """
    URL parsing and manipulation automation.

    Example:
        automator = UrlAutomator()

        # Parse URL
        parts = automator.parse("https://example.com/path?query=value")

        # Modify query params
        parts.query["page"] = ["1"]
        new_url = parts.to_string()

        # Build URL from parts
        url = automator.build(scheme="https", host="example.com", path="/api/users")
    """

    def parse(self, url: str) -> UrlParts:
        """
        Parse URL into components.

        Args:
            url: URL to parse

        Returns:
            UrlParts object
        """
        parsed = urlparse(url)

        query_dict = parse_qs(parsed.query, keep_blank_values=True)

        return UrlParts(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            params=parsed.params,
            query=query_dict,
            fragment=parsed.fragment
        )

    def build(
        self,
        scheme: str = "https",
        host: str = "",
        path: str = "/",
        port: Optional[int] = None,
        query: Optional[dict[str, any]] = None,
        fragment: str = ""
    ) -> str:
        """
        Build URL from components.

        Args:
            scheme: URL scheme
            host: Hostname
            path: URL path
            port: Optional port
            query: Query parameters
            fragment: URL fragment

        Returns:
            Constructed URL string
        """
        netloc = host
        if port:
            netloc = f"{host}:{port}"

        query_string = ""
        if query:
            flat_query = {}
            for key, value in query.items():
                if isinstance(value, list):
                    flat_query[key] = value
                else:
                    flat_query[key] = [value]
            query_string = urlencode(flat_query, doseq=True)

        return urlunparse((
            scheme,
            netloc,
            path,
            "",
            query_string,
            fragment
        ))

    def add_params(
        self,
        url: str,
        params: dict[str, any]
    ) -> str:
        """
        Add query parameters to URL.

        Args:
            url: Base URL
            params: Parameters to add

        Returns:
            URL with added parameters
        """
        parts = self.parse(url)

        for key, value in params.items():
            if key in parts.query:
                if isinstance(parts.query[key], list):
                    if isinstance(value, list):
                        parts.query[key].extend(value)
                    else:
                        parts.query[key].append(value)
                else:
                    parts.query[key] = [parts.query[key], value]
            else:
                parts.query[key] = [value] if not isinstance(value, list) else value

        return parts.to_string()

    def remove_params(
        self,
        url: str,
        params: list[str]
    ) -> str:
        """
        Remove query parameters from URL.

        Args:
            url: Base URL
            params: Parameter names to remove

        Returns:
            URL with parameters removed
        """
        parts = self.parse(url)

        for param in params:
            parts.query.pop(param, None)

        return parts.to_string()

    def update_params(
        self,
        url: str,
        params: dict[str, any]
    ) -> str:
        """
        Update query parameters in URL.

        Args:
            url: Base URL
            params: Parameters to update

        Returns:
            URL with updated parameters
        """
        parts = self.parse(url)

        for key, value in params.items():
            parts.query[key] = [value] if not isinstance(value, list) else value

        return parts.to_string()

    def get_fragment_params(self, url: str) -> dict[str, str]:
        """
        Get parameters from URL fragment.

        Args:
            url: URL with fragment

        Returns:
            Dict of fragment parameters
        """
        parts = self.parse(url)
        return dict(parse_qsl(parts.fragment))

    def set_fragment_params(
        self,
        url: str,
        params: dict[str, any]
    ) -> str:
        """
        Set parameters in URL fragment.

        Args:
            url: Base URL
            params: Parameters to set

        Returns:
            URL with fragment parameters
        """
        parts = self.parse(url)
        parts.fragment = urlencode(params)
        return parts.to_string()

    def join(self, base: str, relative: str) -> str:
        """
        Join base URL with relative path.

        Args:
            base: Base URL
            relative: Relative path

        Returns:
            Joined URL
        """
        return urljoin(base, relative)

    def normalize(self, url: str) -> str:
        """
        Normalize URL by removing empty components.

        Args:
            url: URL to normalize

        Returns:
            Normalized URL
        """
        parts = self.parse(url)

        parts.path = parts.path or "/"

        if parts.scheme == "https" and parts.port == 443:
            parts.netloc = parts.host
        elif parts.scheme == "http" and parts.port == 80:
            parts.netloc = parts.host

        return parts.to_string()

    def is_absolute(self, url: str) -> bool:
        """Check if URL is absolute."""
        return bool(urlparse(url).netloc)

    def get_domain(self, url: str) -> str:
        """Get domain from URL."""
        parts = self.parse(url)
        return parts.host

    def get_subdomain(self, url: str) -> Optional[str]:
        """Get subdomain from URL."""
        parts = self.parse(url)
        host = parts.host

        if "." in host:
            parts_list = host.split(".")
            if len(parts_list) > 2:
                return ".".join(parts_list[:-2])
        return None

    def encode_value(self, value: str) -> str:
        """URL encode a value."""
        return quote(value, safe="")

    def decode_value(self, value: str) -> str:
        """URL decode a value."""
        return unquote(value)

    def encode_path(self, path: str) -> str:
        """Encode path segment."""
        return quote(path, safe="/")

    def validate(self, url: str) -> tuple[bool, Optional[str]]:
        """
        Validate URL format.

        Args:
            url: URL to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            parts = self.parse(url)

            if not parts.scheme:
                return False, "Missing scheme"

            if parts.scheme not in ("http", "https", "ftp", "file"):
                return False, f"Invalid scheme: {parts.scheme}"

            if not parts.host:
                return False, "Missing host"

            return True, None

        except Exception as e:
            return False, str(e)


class UrlShortener:
    """
    URL shortening utilities.

    Example:
        shortener = UrlShortener()

        # Create short URL
        short = shortener.shorten("https://example.com/very/long/path")
    """

    def __init__(self):
        self._cache: dict[str, str] = {}
        self._reverse: dict[str, str] = {}

    def shorten(self, url: str, max_length: int = 20) -> str:
        """
        Create a short URL representation.

        Args:
            url: URL to shorten
            max_length: Maximum length of short URL

        Returns:
            Shortened URL string
        """
        import hashlib

        if url in self._reverse:
            return self._reverse[url]

        short_id = hashlib.urlsafe_b64encode(
            hashlib.sha256(url.encode()).digest()
        )[:max_length].decode()

        short_url = f"/s/{short_id}"

        self._cache[short_url] = url
        self._reverse[url] = short_url

        return short_url

    def expand(self, short_url: str) -> Optional[str]:
        """Expand short URL to original."""
        return self._cache.get(short_url)


def create_url_automator() -> UrlAutomator:
    """Factory to create a UrlAutomator."""
    return UrlAutomator()


def create_shortener() -> UrlShortener:
    """Factory to create a UrlShortener."""
    return UrlShortener()
