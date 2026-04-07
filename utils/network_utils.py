"""Network utilities for RabAI AutoClick.

Provides:
- Network helpers
- URL utilities
- HTTP helpers
"""

import json
import re
import socket
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple


def is_valid_ipv4(address: str) -> bool:
    """Check if string is valid IPv4 address.

    Args:
        address: Address to check.

    Returns:
        True if valid IPv4.
    """
    pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
    if not re.match(pattern, address):
        return False
    parts = address.split('.')
    return all(0 <= int(part) <= 255 for part in parts)


def is_valid_ipv6(address: str) -> bool:
    """Check if string is valid IPv6 address.

    Args:
        address: Address to check.

    Returns:
        True if valid IPv6.
    """
    try:
        socket.inet_pton(socket.AF_INET6, address)
        return True
    except socket.error:
        return False


def is_valid_port(port: int) -> bool:
    """Check if port number is valid.

    Args:
        port: Port to check.

    Returns:
        True if valid port.
    """
    return 0 <= port <= 65535


def parse_url(url: str) -> Dict[str, str]:
    """Parse URL into components.

    Args:
        url: URL to parse.

    Returns:
        Dictionary with URL components.
    """
    parsed = urllib.parse.urlparse(url)
    return {
        "scheme": parsed.scheme,
        "netloc": parsed.netloc,
        "hostname": parsed.hostname or "",
        "port": parsed.port or 0,
        "path": parsed.path,
        "query": parsed.query,
        "fragment": parsed.fragment,
    }


def build_url(scheme: str, host: str, path: str = "", port: Optional[int] = None, query: Optional[Dict[str, str]] = None) -> str:
    """Build URL from components.

    Args:
        scheme: URL scheme (http, https).
        host: Hostname.
        path: URL path.
        port: Optional port.
        query: Optional query parameters.

    Returns:
        Built URL string.
    """
    if port:
        netloc = f"{host}:{port}"
    else:
        netloc = host

    url = f"{scheme}://{netloc}{path}"

    if query:
        query_string = urllib.parse.urlencode(query)
        url = f"{url}?{query_string}"

    return url


def parse_query_string(query: str) -> Dict[str, str]:
    """Parse query string into dict.

    Args:
        query: Query string.

    Returns:
        Dictionary of parameters.
    """
    return dict(urllib.parse.parse_qsl(query))


def build_query_string(params: Dict[str, Any]) -> str:
    """Build query string from dict.

    Args:
        params: Dictionary of parameters.

    Returns:
        Query string.
    """
    return urllib.parse.urlencode(params)


def url_encode(text: str) -> str:
    """URL encode text.

    Args:
        text: Text to encode.

    Returns:
        URL encoded string.
    """
    return urllib.parse.quote(text)


def url_decode(text: str) -> str:
    """URL decode text.

    Args:
        text: Text to decode.

    Returns:
        URL decoded string.
    """
    return urllib.parse.unquote(text)


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
        IP address or None if resolution fails.
    """
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        return None


def reverse_dns_lookup(ip: str) -> Optional[str]:
    """Perform reverse DNS lookup.

    Args:
        ip: IP address.

    Returns:
        Hostname or None if lookup fails.
    """
    try:
        return socket.gethostbyaddr(ip)[0]
    except (socket.herror, socket.gaierror):
        return None


def is_reachable(host: str, port: int, timeout: float = 3.0) -> bool:
    """Check if host:port is reachable.

    Args:
        host: Hostname or IP.
        port: Port number.
        timeout: Connection timeout.

    Returns:
        True if reachable.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        return True
    except (socket.error, socket.timeout):
        return False
    finally:
        sock.close()


def get_local_ip() -> str:
    """Get local IP address.

    Returns:
        Local IP address.
    """
    try:
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
        Public IP or None if fetching fails.
    """
    try:
        import urllib.request
        response = urllib.request.urlopen("https://api.ipify.org", timeout=5)
        return response.read().decode()
    except Exception:
        return None


def is_private_ip(ip: str) -> bool:
    """Check if IP is private.

    Args:
        ip: IP address.

    Returns:
        True if private IP.
    """
    if not is_valid_ipv4(ip):
        return False

    parts = [int(p) for p in ip.split('.')]

    # 10.0.0.0/8
    if parts[0] == 10:
        return True

    # 172.16.0.0/12
    if parts[0] == 172 and 16 <= parts[1] <= 31:
        return True

    # 192.168.0.0/16
    if parts[0] == 192 and parts[1] == 168:
        return True

    # 127.0.0.0/8
    if parts[0] == 127:
        return True

    return False


def is_loopback_ip(ip: str) -> bool:
    """Check if IP is loopback.

    Args:
        ip: IP address.

    Returns:
        True if loopback.
    """
    if not is_valid_ipv4(ip):
        return False
    return ip.startswith("127.")


def parse_http_headers(headers_text: str) -> Dict[str, str]:
    """Parse HTTP headers text.

    Args:
        headers_text: Raw headers text.

    Returns:
        Dictionary of headers.
    """
    headers = {}
    for line in headers_text.strip().split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            headers[key.strip()] = value.strip()
    return headers


def format_http_headers(headers: Dict[str, str]) -> str:
    """Format headers dict to HTTP format.

    Args:
        headers: Headers dictionary.

    Returns:
        Formatted headers string.
    """
    return '\n'.join(f"{key}: {value}" for key, value in headers.items())


def encode_json(data: Any) -> str:
    """Encode data as JSON.

    Args:
        data: Data to encode.

    Returns:
        JSON string.
    """
    return json.dumps(data)


def decode_json(text: str) -> Any:
    """Decode JSON to Python object.

    Args:
        text: JSON string.

    Returns:
        Python object or None if decode fails.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def is_valid_url(url: str) -> bool:
    """Check if string is valid URL.

    Args:
        url: URL to check.

    Returns:
        True if valid URL.
    """
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_ssl_port(port: int) -> bool:
    """Check if port is commonly used for SSL.

    Args:
        port: Port to check.

    Returns:
        True if common SSL port.
    """
    return port in (443, 8443, 9443)


def get_content_type(file_path: str) -> str:
    """Guess content type from file extension.

    Args:
        file_path: File path.

    Returns:
        MIME content type.
    """
    ext_map = {
        '.html': 'text/html',
        '.htm': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
        '.xml': 'application/xml',
        '.txt': 'text/plain',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.pdf': 'application/pdf',
        '.zip': 'application/zip',
        '.gz': 'application/gzip',
        '.tar': 'application/x-tar',
    }
    import os.path
    ext = os.path.splitext(file_path)[1].lower()
    return ext_map.get(ext, 'application/octet-stream')


def join_url_parts(parts: List[str]) -> str:
    """Join URL parts properly.

    Args:
        parts: URL path parts.

    Returns:
        Joined URL path.
    """
    return '/'.join(p.strip('/') for p in parts if p)


def normalize_url_path(path: str) -> str:
    """Normalize URL path.

    Args:
        path: URL path.

    Returns:
        Normalized path.
    """
    if not path:
        return '/'
    is_absolute = path.startswith('/')
    parts = path.split('/')
    result = []
    for part in parts:
        if part == '..':
            if result:
                result.pop()
        elif part != '.' and part != '':
            result.append(part)
    normalized = '/'.join(result)
    if is_absolute:
        normalized = '/' + normalized
    return normalized or '/'


def ip_to_int(ip: str) -> Optional[int]:
    """Convert IP address to integer.

    Args:
        ip: IP address string.

    Returns:
        Integer representation or None.
    """
    if not is_valid_ipv4(ip):
        return None
    parts = [int(p) for p in ip.split('.')]
    return (parts[0] << 24) + (parts[1] << 16) + (parts[2] << 8) + parts[3]


def int_to_ip(ip_int: int) -> str:
    """Convert integer to IP address.

    Args:
        ip_int: Integer representation.

    Returns:
        IP address string.
    """
    return f"{(ip_int >> 24) & 0xFF}.{(ip_int >> 16) & 0xFF}.{(ip_int >> 8) & 0xFF}.{ip_int & 0xFF}"


def cidr_to_netmask(cidr: int) -> str:
    """Convert CIDR prefix length to netmask.

    Args:
        cidr: CIDR prefix (0-32).

    Returns:
        Netmask string.
    """
    if not 0 <= cidr <= 32:
        return "0.0.0.0"
    mask = (0xFFFFFFFF << (32 - cidr)) & 0xFFFFFFFF
    return int_to_ip(mask)


def netmask_to_cidr(netmask: str) -> Optional[int]:
    """Convert netmask to CIDR prefix.

    Args:
        netmask: Netmask string.

    Returns:
        CIDR prefix or None.
    """
    if not is_valid_ipv4(netmask):
        return None
    mask_int = ip_to_int(netmask)
    if mask_int is None:
        return None
    cidr = 0
    while mask_int & 0x80000000:
        cidr += 1
        mask_int <<= 1
    return cidr
