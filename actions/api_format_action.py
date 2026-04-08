"""
API Format Action - Formats API requests and responses.

This module provides request/response formatting including
header management, content type handling, and data transformation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class FormatConfig:
    """Configuration for API formatting."""
    content_type: str = "application/json"
    accept: str = "application/json"
    default_encoding: str = "utf-8"
    add_timestamp: bool = False
    add_trace_id: bool = False


@dataclass
class FormattedRequest:
    """A formatted API request."""
    method: str
    url: str
    headers: dict[str, str]
    params: dict[str, Any]
    body: Any | None
    encoded: str | None = None


@dataclass
class FormattedResponse:
    """A formatted API response."""
    status_code: int
    headers: dict[str, str]
    data: Any
    formatted_data: str | None = None


class RequestFormatter:
    """Formats API requests."""
    
    def __init__(self, config: FormatConfig | None = None) -> None:
        self.config = config or FormatConfig()
    
    def format_request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> FormattedRequest:
        """Format an API request."""
        final_headers = self._build_headers(headers)
        final_params = self._add_params(params)
        final_body = self._format_body(body)
        
        return FormattedRequest(
            method=method,
            url=url,
            headers=final_headers,
            params=final_params,
            body=final_body,
        )
    
    def _build_headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        """Build request headers."""
        result = dict(headers) if headers else {}
        if self.config.add_trace_id:
            import uuid
            result["X-Trace-ID"] = str(uuid.uuid4())
        return result
    
    def _add_params(self, params: dict[str, Any] | None) -> dict[str, Any]:
        """Add timestamp to params if configured."""
        if self.config.add_timestamp and params:
            import time
            params["_timestamp"] = int(time.time())
        return params or {}
    
    def _format_body(self, body: Any) -> Any:
        """Format request body."""
        if body is None:
            return None
        if isinstance(body, (dict, list)):
            return body
        return body


class ResponseFormatter:
    """Formats API responses."""
    
    def __init__(self, config: FormatConfig | None = None) -> None:
        self.config = config or FormatConfig()
    
    def format_response(
        self,
        status_code: int,
        headers: dict[str, str],
        data: Any,
    ) -> FormattedResponse:
        """Format an API response."""
        formatted = self._format_data(data)
        return FormattedResponse(
            status_code=status_code,
            headers=headers,
            data=data,
            formatted_data=formatted,
        )
    
    def _format_data(self, data: Any) -> str | None:
        """Format data as string."""
        if self.config.content_type == "application/json":
            try:
                return json.dumps(data, indent=2, ensure_ascii=False, default=str)
            except Exception:
                return str(data)
        return str(data)


class APIFormatAction:
    """API format action for automation workflows."""
    
    def __init__(self, content_type: str = "application/json") -> None:
        self.config = FormatConfig(content_type=content_type)
        self.request_formatter = RequestFormatter(self.config)
        self.response_formatter = ResponseFormatter(self.config)
    
    def format_request(self, method: str, url: str, **kwargs) -> FormattedRequest:
        """Format an API request."""
        return self.request_formatter.format_request(method, url, **kwargs)
    
    def format_response(self, status_code: int, headers: dict, data: Any) -> FormattedResponse:
        """Format an API response."""
        return self.response_formatter.format_response(status_code, headers, data)


__all__ = ["FormatConfig", "FormattedRequest", "FormattedResponse", "RequestFormatter", "ResponseFormatter", "APIFormatAction"]
