"""Network utilities v2 for RabAI AutoClick.

Provides:
- HTTP request helpers
- URL parsing and building
- Network checking utilities
- Port scanning helpers
"""

import ipaddress
import socket
import ssl
import urllib.parse
from typing import (
    Any,
    Dict,
    Optional,
    Union,
)


def is_valid_ipv4(ip: str) -> bool:
    """Check if string is a valid IPv4 address.

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
    """Check if string is a valid IPv6 address.

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


def is_valid_ip(ip: str) -> bool:
    """Check if string is a valid IP address.

    Args:
        ip: IP address string.

    Returns:
        True if valid IP (v4 or v6).
    """
    return is_valid_ipv4(ip) or is_valid_ipv6(ip)


def is_private_ip(ip: str) -> bool:
    """Check if IP is a private/internal IP.

    Args:
        ip: IP address string.

    Returns:
        True if private IP.
    """
    try:
        addr = ipaddress.ip_address(ip)
        return addr.is_private or addr.is_loopback or addr.is_reserved
    except ValueError:
        return False


def parse_url(url: str) -> Dict[str, Any]:
    """Parse a URL into components.

    Args:
        url: URL to parse.

    Returns:
        Dict with scheme, netloc, path, params, query, fragment, etc.
    """
    parsed = urllib.parse.urlparse(url)
    return {
        "scheme": parsed.scheme,
        "netloc": parsed.netloc,
        "hostname": parsed.hostname,
        "port": parsed.port,
        "path": parsed.path,
        "params": parsed.params,
        "query": parsed.query,
        "fragment": parsed.fragment,
        "username": parsed.username,
        "password": parsed.password,
    }


def build_url(
    scheme: str,
    host: str,
    path: str = "",
    port: Optional[int] = None,
    params: Optional[str] = None,
    query: Optional[Dict[str, str]] = None,
    fragment: Optional[str] = None,
) -> str:
    """Build a URL from components.

    Args:
        scheme: URL scheme (http, https).
        host: Hostname.
        path: URL path.
        port: Optional port.
        params: Optional params.
        query: Optional query dict.
        fragment: Optional fragment.

    Returns:
        Full URL string.
    """
    netloc = host
    if port:
        netloc = f"{host}:{port}"

    if query:
        query_str = urllib.parse.urlencode(query)
    else:
        query_str = ""

    return urllib.parse.urlunparse((
        scheme,
        netloc,
        path,
        params or "",
        query_str,
        fragment or "",
    ))


def get_hostname() -> str:
    """Get local hostname.

    Returns:
        Hostname string.
    """
    return socket.gethostname()


def get_fqdn() -> str:
    """Get fully qualified domain name.

    Returns:
        FQDN string.
    """
    return socket.getfqdn()


def resolve_hostname(hostname: str) -> Optional[str]:
    """Resolve hostname to IP address.

    Args:
        hostname: Hostname to resolve.

    Returns:
        IP address or None.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def resolve_host(ip_or_name: str) -> Optional[str]:
    """Resolve IP or hostname (reverse lookup if IP).

    Args:
        ip_or_name: IP or hostname.

    Returns:
        Resolved hostname or IP.
    """
    try:
        if is_valid_ip(ip_or_name):
            return socket.gethostbyaddr(ip_or_name)[0]
        return socket.gethostbyname(ip_or_name)
    except (socket.gaierror, socket.herror):
        return None


def check_port_open(
    host: str,
    port: int,
    timeout: float = 2.0,
) -> bool:
    """Check if a port is open on a host.

    Args:
        host: Hostname or IP.
        port: Port number.
        timeout: Connection timeout.

    Returns:
        True if port is open.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def get_local_ip() -> str:
    """Get local IP address.

    Returns:
        Local IP address.
    """
    try:
        # Create a dummy connection to determine local IP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        local_ip = sock.getsockname()[0]
        sock.close()
        return local_ip
    except Exception:
        return "127.0.0.1"


def get_public_ip() -> Optional[str]:
    """Get public IP address.

    Returns:
        Public IP or None.
    """
    try:
        import urllib.request
        response = urllib.request.urlopen("https://api.ipify.org", timeout=5)
        return response.read().decode().strip()
    except Exception:
        return None


def is_port_in_use(port: int, host: str = "localhost") -> bool:
    """Check if a port is in use.

    Args:
        port: Port number.
        host: Host to check.

    Returns:
        True if port is in use.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def find_free_port(
    start: int = 8000,
    end: int = 9000,
    host: str = "localhost",
) -> Optional[int]:
    """Find a free port in a range.

    Args:
        start: Start of range.
        end: End of range.
        host: Host to check.

    Returns:
        Free port number or None.
    """
    for port in range(start, end + 1):
        if not is_port_in_use(port, host):
            return port
    return None


def split_host_port(
    host_port: str,
    default_port: int = 80,
) -> tuple[str, int]:
    """Split host:port string.

    Args:
        host_port: Host:port string.
        default_port: Default port if not specified.

    Returns:
        Tuple of (host, port).
    """
    if ":" in host_port:
        host, port_str = host_port.rsplit(":", 1)
        try:
            port = int(port_str)
        except ValueError:
            port = default_port
    else:
        host = host_port
        port = default_port
    return host, port


def make_http_request(
    method: str,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = None,
    body: Optional[bytes] = None,
    timeout: float = 10.0,
    verify_ssl: bool = True,
) -> tuple[int, Dict[str, str], bytes]:
    """Make an HTTP request.

    Args:
        method: HTTP method.
        url: Target URL.
        headers: Optional headers.
        body: Optional request body.
        timeout: Request timeout.
        verify_ssl: Verify SSL certificates.

    Returns:
        Tuple of (status_code, response_headers, body).
    """
    import urllib.request
    import urllib.error

    req = urllib.request.Request(url, data=body, method=method)
    if headers:
        for key, value in headers.items():
            req.add_header(key, value)

    context = None
    if not verify_ssl:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as response:
            return (
                response.status,
                dict(response.headers),
                response.read(),
            )
    except urllib.error.HTTPError as e:
        return (
            e.code,
            dict(e.headers),
            e.read() if hasattr(e, "read") else b"",
        )
    except urllib.error.URLError as e:
        raise


def get_content_type(url: str) -> Optional[str]:
    """Get content type of a URL without downloading.

    Args:
        url: URL to check.

    Returns:
        Content type or None.
    """
    try:
        import urllib.request
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.headers.get("Content-Type")
    except Exception:
        return None


def parse_query_string(query: str) -> Dict[str, str]:
    """Parse a URL query string.

    Args:
        query: Query string (without leading ?).

    Returns:
        Dict of parameters.
    """
    return dict(urllib.parse.parse_qsl(query))


def build_query_string(params: Dict[str, Any]) -> str:
    """Build a URL query string.

    Args:
        params: Dict of parameters.

    Returns:
        Query string.
    """
    return urllib.parse.urlencode(params)
