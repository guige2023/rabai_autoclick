"""Network utilities for RabAI AutoClick.

Provides:
- URL parsing and manipulation
- HTTP request helpers
- Network reachability checks
- IP address utilities
"""

from __future__ import annotations

import ipaddress
import socket
import urllib.parse
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)


def parse_url(url: str) -> urllib.parse.ParseResult:
    """Parse a URL into components.

    Args:
        url: URL to parse.

    Returns:
        ParseResult with scheme, netloc, path, etc.
    """
    return urllib.parse.urlparse(url)


def build_url(
    scheme: str,
    netloc: str,
    path: str = "",
    params: str = "",
    query: Optional[Dict[str, str]] = None,
    fragment: str = "",
) -> str:
    """Build a URL from components.

    Args:
        scheme: URL scheme (e.g., 'https').
        netloc: Network location (e.g., 'example.com:8080').
        path: Path component.
        params: Parameters.
        query: Query parameters dict.
        fragment: Fragment identifier.

    Returns:
        Complete URL string.
    """
    if query:
        query_str = urllib.parse.urlencode(query)
    else:
        query_str = ""
    return urllib.parse.urlunparse(
        (scheme, netloc, path, params, query_str, fragment)
    )


def add_query_params(
    url: str,
    params: Dict[str, str],
) -> str:
    """Add query parameters to a URL.

    Args:
        url: Base URL.
        params: Parameters to add.

    Returns:
        URL with added parameters.
    """
    parsed = parse_url(url)
    existing = urllib.parse.parse_qs(parsed.query)
    existing.update(params)
    new_query = urllib.parse.urlencode(existing, doseq=True)
    return build_url(
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        urllib.parse.parse_qs(new_query),
        parsed.fragment,
    )


def is_valid_ipv4(ip: str) -> bool:
    """Check if a string is a valid IPv4 address.

    Args:
        ip: IP address string.

    Returns:
        True if valid IPv4.
    """
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ValueError:
        return False


def is_valid_ipv6(ip: str) -> bool:
    """Check if a string is a valid IPv6 address.

    Args:
        ip: IP address string.

    Returns:
        True if valid IPv6.
    """
    try:
        ipaddress.IPv6Address(ip)
        return True
    except ValueError:
        return False


def is_private_ip(ip: str) -> bool:
    """Check if an IP address is private.

    Args:
        ip: IP address string.

    Returns:
        True if IP is in private range.
    """
    try:
        addr = ipaddress.ip_address(ip)
        return addr.is_private
    except ValueError:
        return False


def resolve_hostname(hostname: str) -> Optional[str]:
    """Resolve a hostname to an IP address.

    Args:
        hostname: Hostname to resolve.

    Returns:
        IP address string or None if resolution fails.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def is_port_open(
    host: str,
    port: int,
    timeout: float = 2.0,
) -> bool:
    """Check if a TCP port is open on a host.

    Args:
        host: Hostname or IP.
        port: Port number.
        timeout: Connection timeout in seconds.

    Returns:
        True if port is open.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True
    except (socket.error, OSError):
        return False
    finally:
        sock.close()


def get_local_ip() -> str:
    """Get the local machine's IP address.

    Returns:
        Local IP address string.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except Exception:
        return "127.0.0.1"


def cidr_to_ips(cidr: str) -> List[str]:
    """Generate IP addresses from a CIDR notation.

    Args:
        cidr: CIDR notation (e.g., '192.168.1.0/24').

    Returns:
        List of IP address strings.
    """
    try:
        network = ipaddress.ip_network(cidr, strict=False)
        return [str(ip) for ip in network.hosts()]
    except ValueError:
        return []


def url_encode(text: str, safe: str = "") -> str:
    """URL-encode a string.

    Args:
        text: Text to encode.
        safe: Characters that should not be encoded.

    Returns:
        URL-encoded string.
    """
    return urllib.parse.quote(text, safe=safe)


def url_decode(text: str) -> str:
    """URL-decode a string.

    Args:
        text: URL-encoded text.

    Returns:
        Decoded string.
    """
    return urllib.parse.unquote(text)


__all__ = [
    "parse_url",
    "build_url",
    "add_query_params",
    "is_valid_ipv4",
    "is_valid_ipv6",
    "is_private_ip",
    "resolve_hostname",
    "is_port_open",
    "get_local_ip",
    "cidr_to_ips",
    "url_encode",
    "url_decode",
]
