"""
Response Parser Action Module.

Parses and normalizes HTTP API responses across formats:
JSON, XML, HTML, plain text. Handles pagination links,
rate limit headers, and error responses.

Example:
    >>> from response_parser_action import ResponseParser
    >>> parser = ResponseParser()
    >>> data = parser.parse_json(response.text)
    >>> next_page = parser.get_next_link(response.headers)
"""
from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ParsedResponse:
    """Normalized response wrapper."""
    data: Any
    content_type: str
    status: int
    next_page: Optional[str] = None
    total_count: Optional[int] = None
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[int] = None
    errors: list[str] = None


class ResponseParser:
    """Parse HTTP responses into structured data."""

    def __init__(self):
        self._last_content_type = ""

    def parse(
        self,
        content: bytes,
        content_type: str,
        status: int = 200,
    ) -> ParsedResponse:
        """
        Auto-detect and parse response content.

        Args:
            content: Raw response bytes
            content_type: Content-Type header value
            status: HTTP status code

        Returns:
            ParsedResponse with normalized data
        """
        self._last_content_type = content_type
        text = content.decode("utf-8", errors="replace")

        ct_lower = content_type.lower()
        if "json" in ct_lower:
            return self.parse_json(text, status)
        elif "xml" in ct_lower:
            return self.parse_xml(text, status)
        elif "html" in ct_lower:
            return self.parse_html(text, status)
        else:
            return ParsedResponse(data=text, content_type=content_type, status=status)

    def parse_json(
        self,
        text: str,
        status: int = 200,
    ) -> ParsedResponse:
        """Parse JSON response."""
        try:
            data = json.loads(text)
            return ParsedResponse(
                data=data,
                content_type="application/json",
                status=status,
            )
        except json.JSONDecodeError as e:
            return ParsedResponse(
                data=None,
                content_type="application/json",
                status=status,
                errors=[f"JSON parse error: {e}"],
            )

    def parse_xml(
        self,
        text: str,
        status: int = 200,
    ) -> ParsedResponse:
        """Parse XML response to dict."""
        try:
            root = ET.fromstring(text)
            data = self._xml_to_dict(root)
            return ParsedResponse(
                data=data,
                content_type="application/xml",
                status=status,
            )
        except ET.ParseError as e:
            return ParsedResponse(
                data=None,
                content_type="application/xml",
                status=status,
                errors=[f"XML parse error: {e}"],
            )

    def _xml_to_dict(self, elem: ET.Element) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if elem.attrib:
            result["@attributes"] = dict(elem.attrib)
        if elem.text and elem.text.strip():
            if len(elem) == 0:
                return elem.text.strip()
            result["#text"] = elem.text.strip()
        for child in elem:
            child_data = self._xml_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data
        return result

    def parse_html(
        self,
        text: str,
        status: int = 200,
    ) -> ParsedResponse:
        """Parse HTML response (extract text)."""
        from html_parser_action import HTMLParserAction
        parser = HTMLParserAction()
        doc = parser.parse_string(text)
        text_content = parser.strip_tags(text)
        return ParsedResponse(
            data={"text": text_content, "html": text},
            content_type="text/html",
            status=status,
        )

    def get_next_link(self, headers: dict[str, str]) -> Optional[str]:
        """Extract next page URL from Link header."""
        link_header = headers.get("Link") or headers.get("link", "")
        if not link_header:
            return None
        matches = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', link_header)
        for url, rel in matches:
            if rel.lower() == "next":
                return url
        return None

    def get_total_count(self, headers: dict[str, str], body: Any = None) -> Optional[int]:
        """Get total count from various header/body locations."""
        total = headers.get("X-Total-Count") or headers.get("Total-Count") or headers.get("total")
        if total:
            try:
                return int(total)
            except ValueError:
                pass
        if isinstance(body, dict):
            for key in ("total", "total_count", "totalCount", "count", "metadata.total"):
                if key in body:
                    try:
                        return int(body[key])
                    except (ValueError, TypeError):
                        pass
        return None

    def get_rate_limit_info(self, headers: dict[str, str]) -> tuple[Optional[int], Optional[int]]:
        """Extract rate limit info from headers."""
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("X-Rate-Limit-Remaining")
        reset = headers.get("X-RateLimit-Reset") or headers.get("X-Rate-Limit-Reset")
        if remaining:
            try:
                remaining = int(remaining)
            except ValueError:
                remaining = None
        if reset:
            try:
                reset = int(reset)
            except ValueError:
                reset = None
        return remaining, reset

    def normalize_error(self, content: bytes, content_type: str, status: int) -> dict[str, Any]:
        """Normalize error response to consistent format."""
        text = content.decode("utf-8", errors="replace")
        error: dict[str, Any] = {"status": status, "raw": text}

        if "json" in content_type.lower():
            try:
                data = json.loads(text)
                if isinstance(data, dict):
                    error["code"] = data.get("code") or data.get("error") or data.get("message", "")
                    error["message"] = data.get("message") or data.get("error_description", "")
                    error["details"] = data.get("details") or data.get("errors", {})
                else:
                    error["message"] = str(data)
            except json.JSONDecodeError:
                error["message"] = text[:500]
        else:
            error["message"] = text[:500]

        return error

    def extract_pagination_urls(self, link_header: str) -> dict[str, str]:
        """Extract all pagination URLs from Link header."""
        urls: dict[str, str] = {}
        matches = re.findall(r'<([^>]+)>;\s*rel="([^"]+)"', link_header)
        for url, rel in matches:
            urls[rel.lower()] = url
        return urls

    def unwrap_response(
        self,
        data: Any,
        path: str = "",
    ) -> Any:
        """
        Unwrap nested response envelope.

        Args:
            data: Response data
            path: Dot-notation path to extract (e.g., "data.result.items")

        Returns:
            Unwrapped data
        """
        if not path:
            return data
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                if -len(current) <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None
        return current

    def extract_list(
        self,
        data: Any,
        list_path: str = "items",
        total_path: Optional[str] = None,
    ) -> tuple[list, Optional[int]]:
        """Extract list from response, handling common envelope patterns."""
        unwrapped = self.unwrap_response(data, list_path)
        if isinstance(unwrapped, list):
            total = None
            if total_path:
                total_val = self.unwrap_response(data, total_path)
                if isinstance(total_val, int):
                    total = total_val
            return unwrapped, total
        return [], None

    def is_error_status(self, status: int) -> bool:
        """Check if status code indicates error."""
        return status >= 400

    def is_success_status(self, status: int) -> bool:
        """Check if status code indicates success."""
        return 200 <= status < 300

    def is_redirect_status(self, status: int) -> bool:
        """Check if status code is a redirect."""
        return 300 <= status < 400


if __name__ == "__main__":
    parser = ResponseParser()

    import urllib.parse
    test_link = '<https://api.example.com/page=2>; rel="next", <https://api.example.com/page=1>; rel="prev", <https://api.example.com/page=3>; rel="last"'
    urls = parser.extract_pagination_urls(test_link)
    print(f"Next: {urls.get('next')}")
    print(f"Prev: {urls.get('prev')}")
    print(f"Last: {urls.get('last')}")

    test_json = '{"data": {"items": [{"id": 1}, {"id": 2}], "total": 100}}'
    parsed = parser.parse_json(test_json)
    print(f"Items: {parser.extract_list(parsed.data, 'data.items', 'data.total')}")
