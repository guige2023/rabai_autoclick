"""Network utilities for RabAI AutoClick.

Provides:
- HTTP helpers
- Request utilities
"""

import json
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


@dataclass
class HTTPResponse:
    """HTTP response container."""
    status_code: int
    body: str
    headers: Dict[str, str]

    @property
    def ok(self) -> bool:
        """Check if response was successful."""
        return 200 <= self.status_code < 300

    def json(self) -> Optional[Any]:
        """Parse response body as JSON."""
        try:
            return json.loads(self.body)
        except Exception:
            return None


def http_get(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
) -> Optional[HTTPResponse]:
    """Perform HTTP GET request.

    Args:
        url: URL to request.
        headers: Optional headers.
        timeout: Request timeout.

    Returns:
        HTTPResponse or None on error.
    """
    try:
        request = Request(url)
        request.add_header('User-Agent', 'RabAI-AutoClick/22.0')

        if headers:
            for key, value in headers.items():
                request.add_header(key, value)

        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode('utf-8')
            headers_dict = dict(response.headers)

            return HTTPResponse(
                status_code=response.status,
                body=body,
                headers=headers_dict,
            )
    except Exception:
        return None


def http_post(
    url: str,
    data: Optional[Dict[str, Any]] = None,
    json_data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 10,
) -> Optional[HTTPResponse]:
    """Perform HTTP POST request.

    Args:
        url: URL to request.
        data: Form data.
        json_data: JSON data.
        headers: Optional headers.
        timeout: Request timeout.

    Returns:
        HTTPResponse or None on error.
    """
    try:
        request = Request(url)
        request.add_header('User-Agent', 'RabAI-AutoClick/22.0')

        if headers:
            for key, value in headers.items():
                request.add_header(key, value)

        if json_data is not None:
            request.add_header('Content-Type', 'application/json')
            body = json.dumps(json_data).encode('utf-8')
        elif data is not None:
            body = urllib.parse.urlencode(data).encode('utf-8')
        else:
            body = None

        request.data = body

        with urlopen(request, timeout=timeout) as response:
            response_body = response.read().decode('utf-8')
            headers_dict = dict(response.headers)

            return HTTPResponse(
                status_code=response.status,
                body=response_body,
                headers=headers_dict,
            )
    except Exception:
        return None


def check_internet() -> bool:
    """Check internet connectivity.

    Returns:
        True if internet is reachable.
    """
    try:
        with urlopen("https://www.google.com", timeout=5) as response:
            return response.status == 200
    except Exception:
        return False


def check_url(url: str, timeout: int = 5) -> bool:
    """Check if URL is accessible.

    Args:
        url: URL to check.
        timeout: Request timeout.

    Returns:
        True if URL is accessible.
    """
    try:
        request = Request(url)
        with urlopen(request, timeout=timeout) as response:
            return 200 <= response.status < 400
    except Exception:
        return False