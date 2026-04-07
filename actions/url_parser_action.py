"""URL parser action for URL manipulation and analysis.

This module provides URL parsing, building, and manipulation
with support for query parameters and path components.

Example:
    >>> action = URLParserAction()
    >>> result = action.execute(operation="parse", url="https://example.com/path?a=1")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


@dataclass
class URLComponents:
    """Parsed URL components."""
    scheme: str
    netloc: str
    path: str
    params: str
    query: dict[str, list[str]]
    fragment: str


class URLParserAction:
    """URL parsing and manipulation action.

    Provides URL parsing, building, and modification
    with query parameter handling.

    Example:
        >>> action = URLParserAction()
        >>> result = action.execute(
        ...     operation="build",
        ...     scheme="https",
        ...     host="example.com",
        ...     path="/page",
        ...     params={"a": "1"}
        ... )
    """

    def __init__(self) -> None:
        """Initialize URL parser."""
        self._last_components: Optional[URLComponents] = None

    def execute(
        self,
        operation: str,
        url: Optional[str] = None,
        scheme: Optional[str] = None,
        host: Optional[str] = None,
        path: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
        query: Optional[dict[str, Any]] = None,
        fragment: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute URL operation.

        Args:
            operation: Operation (parse, build, add_param, remove_param, etc.).
            url: URL to parse.
            scheme: URL scheme.
            host: Hostname.
            path: URL path.
            params: Query parameters.
            query: Alternative query params.
            fragment: URL fragment.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "parse":
            if not url:
                raise ValueError("url required for 'parse'")
            result.update(self._parse_url(url))

        elif op == "build":
            result.update(self._build_url(scheme, host, path, query or params, fragment))

        elif op == "get_param":
            if not url:
                raise ValueError("url required")
            param = kwargs.get("param")
            if not param:
                raise ValueError("param required")
            result.update(self._get_param(url, param))

        elif op == "add_param":
            if not url:
                raise ValueError("url required")
            key = kwargs.get("key")
            value = kwargs.get("value")
            if not key:
                raise ValueError("key and value required")
            result.update(self._add_param(url, key, value))

        elif op == "remove_param":
            if not url:
                raise ValueError("url required")
            key = kwargs.get("key")
            if not key:
                raise ValueError("key required")
            result.update(self._remove_param(url, key))

        elif op == "update_param":
            if not url:
                raise ValueError("url required")
            key = kwargs.get("key")
            value = kwargs.get("value")
            if not key:
                raise ValueError("key and value required")
            result.update(self._add_param(url, key, value))

        elif op == "get_path":
            if not url:
                raise ValueError("url required")
            result["path"] = urlparse(url).path

        elif op == "get_host":
            if not url:
                raise ValueError("url required")
            result["host"] = urlparse(url).netloc

        elif op == "is_absolute":
            if not url:
                raise ValueError("url required")
            result["is_absolute"] = bool(urlparse(url).netloc)

        elif op == "join":
            base = kwargs.get("base")
            if not base:
                raise ValueError("base required")
            result["url"] = self._join_urls(base, url)

        elif op == "normalize":
            if not url:
                raise ValueError("url required")
            result["url"] = self._normalize_url(url)

        elif op == "encode":
            if not url:
                raise ValueError("url required")
            result["encoded"] = urlencode(parse_qs(urlparse(url).query), doseq=True)

        elif op == "decode":
            if not url:
                raise ValueError("url required")
            result["decoded"] = dict(parse_qs(urlparse(url).query))

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _parse_url(self, url: str) -> dict[str, Any]:
        """Parse URL into components.

        Args:
            url: URL to parse.

        Returns:
            Result dictionary.
        """
        parsed = urlparse(url)
        query_dict = parse_qs(parsed.query, keep_blank_values=True)

        components = URLComponents(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            params=parsed.params,
            query=query_dict,
            fragment=parsed.fragment,
        )
        self._last_components = components

        return {
            "components": {
                "scheme": parsed.scheme,
                "netloc": parsed.netloc,
                "path": parsed.path,
                "params": parsed.params,
                "query": query_dict,
                "fragment": parsed.fragment,
            },
            "host": parsed.netloc,
            "port": self._extract_port(parsed),
            "path": parsed.path,
            "query_string": parsed.query,
        }

    def _extract_port(self, parsed) -> Optional[int]:
        """Extract port from parsed URL.

        Args:
            parsed: urlparse result.

        Returns:
            Port number or None.
        """
        import re
        match = re.search(r":(\d+)", parsed.netloc)
        if match:
            return int(match.group(1))
        return None

    def _build_url(
        self,
        scheme: Optional[str],
        host: Optional[str],
        path: Optional[str],
        query: Optional[dict[str, Any]],
        fragment: Optional[str],
    ) -> dict[str, Any]:
        """Build URL from components.

        Args:
            scheme: URL scheme.
            host: Hostname.
            path: URL path.
            query: Query parameters.
            fragment: URL fragment.

        Returns:
            Result dictionary.
        """
        scheme = scheme or "https"
        host = host or ""
        path = path or "/"
        query = query or {}

        # Convert query values to lists
        query_list = {k: v if isinstance(v, list) else [v] for k, v in query.items()}

        netloc = host
        query_string = urlencode(query_list, doseq=True)

        reconstructed = urlunparse((
            scheme,
            netloc,
            path,
            "",  # params
            query_string,
            fragment or "",
        ))

        return {"url": reconstructed}

    def _get_param(self, url: str, param: str) -> dict[str, Any]:
        """Get query parameter value.

        Args:
            url: URL to extract from.
            param: Parameter name.

        Returns:
            Result dictionary.
        """
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if param in query:
            values = query[param]
            return {
                "param": param,
                "value": values[0] if len(values) == 1 else values,
                "found": True,
            }
        return {"param": param, "found": False}

    def _add_param(self, url: str, key: str, value: Any) -> dict[str, Any]:
        """Add or update query parameter.

        Args:
            url: URL to modify.
            key: Parameter name.
            value: Parameter value.

        Returns:
            Result dictionary.
        """
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        query[key] = [value]

        new_query = urlencode(query, doseq=True)
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        ))

        return {"url": new_url, "added": True, "key": key}

    def _remove_param(self, url: str, key: str) -> dict[str, Any]:
        """Remove query parameter.

        Args:
            url: URL to modify.
            key: Parameter to remove.

        Returns:
            Result dictionary.
        """
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        if key in query:
            del query[key]

        new_query = urlencode(query, doseq=True)
        new_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        ))

        return {"url": new_url, "removed": True, "key": key}

    def _join_urls(self, base: str, path: str) -> str:
        """Join base URL with path.

        Args:
            base: Base URL.
            path: Path to join.

        Returns:
            Joined URL.
        """
        if path.startswith(("http://", "https://")):
            return path

        if base.endswith("/"):
            base = base[:-1]

        if path.startswith("/"):
            parsed = urlparse(base)
            return f"{parsed.scheme}://{parsed.netloc}{path}"

        return f"{base}/{path}"

    def _normalize_url(self, url: str) -> str:
        """Normalize URL.

        Args:
            url: URL to normalize.

        Returns:
            Normalized URL.
        """
        parsed = urlparse(url)

        # Remove default ports
        netloc = parsed.netloc
        if parsed.scheme == "http" and ":80" in netloc:
            netloc = netloc.replace(":80", "")
        elif parsed.scheme == "https" and ":443" in netloc:
            netloc = netloc.replace(":443", "")

        # Remove trailing slash from path (except for root)
        path = parsed.path.rstrip("/") or "/"

        return urlunparse((
            parsed.scheme,
            netloc,
            path,
            parsed.params,
            parsed.query,
            "",  # Remove fragment
        ))
