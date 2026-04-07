"""URL utilities for RabAI AutoClick.

Provides:
- URL parsing and building
- Query parameter handling
- URL validation
"""

import re
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class URL:
    """Represents a parsed URL.

    Provides easy access to URL components and query parameters.
    """

    scheme: str
    host: str
    port: Optional[int]
    path: str
    query: Dict[str, str]
    fragment: str

    @property
    def netloc(self) -> str:
        """Get network location (host:port)."""
        if self.port:
            return f"{self.host}:{self.port}"
        return self.host

    @property
    def query_string(self) -> str:
        """Get query string without leading '?'."""
        return urllib.parse.urlencode(self.query)

    def __str__(self) -> str:
        """Get full URL string."""
        url = f"{self.scheme}://{self.netloc}{self.path}"
        if self.query:
            url += f"?{self.query_string}"
        if self.fragment:
            url += f"#{self.fragment}"
        return url

    @classmethod
    def parse(cls, url_str: str) -> 'URL':
        """Parse URL string.

        Args:
            url_str: URL to parse.

        Returns:
            URL object.
        """
        parsed = urllib.parse.urlparse(url_str)

        query_dict = dict(urllib.parse.parse_qsl(parsed.query))

        port = None
        if parsed.port:
            port = parsed.port

        return cls(
            scheme=parsed.scheme,
            host=parsed.hostname or "",
            port=port,
            path=parsed.path,
            query=query_dict,
            fragment=parsed.fragment,
        )


def build_url(
    scheme: str = "https",
    host: str = "",
    port: Optional[int] = None,
    path: str = "",
    query: Optional[Dict[str, Any]] = None,
    fragment: str = "",
) -> str:
    """Build URL from components.

    Args:
        scheme: URL scheme (http, https, etc.).
        host: Hostname.
        port: Optional port.
        path: URL path.
        query: Query parameters dict.
        fragment: URL fragment.

    Returns:
        Built URL string.
    """
    url = f"{scheme}://{host}"
    if port:
        url += f":{port}"
    url += path

    if query:
        url += "?" + urllib.parse.urlencode(query)

    if fragment:
        url += f"#{fragment}"

    return url


def get_query_params(url: str) -> Dict[str, str]:
    """Extract query parameters from URL.

    Args:
        url: URL string.

    Returns:
        Dict of query parameters.
    """
    parsed = urllib.parse.urlparse(url)
    return dict(urllib.parse.parse_qsl(parsed.query))


def add_query_params(
    url: str,
    params: Dict[str, Any],
) -> str:
    """Add query parameters to URL.

    Args:
        url: Base URL.
        params: Parameters to add.

    Returns:
        URL with added parameters.
    """
    parsed = urllib.parse.urlparse(url)
    existing = dict(urllib.parse.parse_qsl(parsed.query))
    existing.update(params)

    new_query = urllib.parse.urlencode(existing)
    return parsed._replace(query=new_query).geturl()


def remove_query_params(url: str, *keys: str) -> str:
    """Remove query parameters from URL.

    Args:
        url: URL with parameters.
        *keys: Parameter names to remove.

    Returns:
        URL with parameters removed.
    """
    parsed = urllib.parse.urlparse(url)
    existing = dict(urllib.parse.parse_qsl(parsed.query))

    for key in keys:
        existing.pop(key, None)

    new_query = urllib.parse.urlencode(existing)
    return parsed._replace(query=new_query).geturl()


def is_valid_url(url: str) -> bool:
    """Check if URL is valid.

    Args:
        url: URL string to validate.

    Returns:
        True if valid URL.
    """
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_absolute_url(url: str) -> bool:
    """Check if URL is absolute.

    Args:
        url: URL to check.

    Returns:
        True if absolute URL.
    """
    return bool(urllib.parse.urlparse(url).netloc)


def join_url(base: str, *parts: str) -> str:
    """Join URL parts.

    Args:
        base: Base URL.
        *parts: Path parts to join.

    Returns:
        Joined URL.
    """
    result = base.rstrip('/')

    for part in parts:
        if not part:
            continue
        part = part.strip('/')
        result += '/' + part

    return result


def normalize_url(url: str) -> str:
    """Normalize URL.

    - Removes default ports
    - Adds trailing slash to path if missing
    - Lowercases scheme and host

    Args:
        url: URL to normalize.

    Returns:
        Normalized URL.
    """
    parsed = urllib.parse.urlparse(url)

    # Remove default ports
    netloc = parsed.netloc
    if parsed.port and parsed.scheme in ('http', 'https'):
        default_port = 443 if parsed.scheme == 'https' else 80
        if parsed.port == default_port:
            netloc = parsed.hostname or ''

    # Lowercase scheme and host
    netloc = netloc.lower()

    # Add trailing slash to path if empty
    path = parsed.path or '/'

    return urllib.parse.urlunparse((
        parsed.scheme.lower(),
        netloc,
        path,
        parsed.params,
        parsed.query,
        parsed.fragment,
    ))


def encode_url_component(value: str) -> str:
    """Encode URL component.

    Args:
        value: Value to encode.

    Returns:
        URL-encoded value.
    """
    return urllib.parse.quote_plus(value)


def decode_url_component(value: str) -> str:
    """Decode URL component.

    Args:
        value: Value to decode.

    Returns:
        URL-decoded value.
    """
    return urllib.parse.unquote_plus(value)


def extract_domain(url: str) -> Optional[str]:
    """Extract domain from URL.

    Args:
        url: URL to extract from.

    Returns:
        Domain or None.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.hostname
    except Exception:
        return None


def is_same_domain(url1: str, url2: str) -> bool:
    """Check if URLs are from same domain.

    Args:
        url1: First URL.
        url2: Second URL.

    Returns:
        True if same domain.
    """
    domain1 = extract_domain(url1)
    domain2 = extract_domain(url2)

    if not domain1 or not domain2:
        return False

    return domain1.lower() == domain2.lower()