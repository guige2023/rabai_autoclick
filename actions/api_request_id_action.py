"""API request ID tracking and propagation.

This module provides request ID handling:
- Request ID generation
- Header propagation
- Logging integration
- Distributed tracing support

Example:
    >>> from actions.api_request_id_action import RequestIDMiddleware
    >>> middleware = RequestIDMiddleware()
    >>> request_id = middleware.get_or_create_request_id(request)
"""

from __future__ import annotations

import uuid
import logging
from typing import Any, Optional
from dataclasses import dataclass
import threading

logger = logging.getLogger(__name__)

DEFAULT_REQUEST_ID_HEADER = "X-Request-ID"
DEFAULT_TRACE_ID_HEADER = "X-Trace-ID"
DEFAULT_SPAN_ID_HEADER = "X-Span-ID"


class RequestIDContext:
    """Thread-local request ID context."""

    def __init__(self) -> None:
        self._local = threading.local()

    def set_request_id(self, request_id: str) -> None:
        """Set the request ID for this context."""
        self._local.request_id = request_id

    def get_request_id(self) -> Optional[str]:
        """Get the request ID for this context."""
        return getattr(self._local, "request_id", None)

    def clear(self) -> None:
        """Clear the context."""
        self._local.request_id = None


class RequestIDGenerator:
    """Generate unique request IDs.

    Example:
        >>> generator = RequestIDGenerator()
        >>> request_id = generator.generate()
    """

    def __init__(self, prefix: str = "") -> None:
        self.prefix = prefix
        self._counter = 0
        self._counter_lock = threading.Lock()

    def generate(self) -> str:
        """Generate a new unique request ID.

        Returns:
            A unique request ID string.
        """
        unique_id = str(uuid.uuid4())
        with self._counter_lock:
            self._counter += 1
            counter = self._counter
        if self.prefix:
            return f"{self.prefix}-{unique_id}-{counter}"
        return f"{unique_id}-{counter}"


class RequestIDManager:
    """Manage request IDs across an application.

    Example:
        >>> manager = RequestIDManager()
        >>> request_id = manager.get_or_create_request_id(request)
        >>> manager.add_to_logging(request_id)
    """

    def __init__(
        self,
        request_id_header: str = DEFAULT_REQUEST_ID_HEADER,
        trace_id_header: str = DEFAULT_TRACE_ID_HEADER,
        generate: bool = True,
    ) -> None:
        self.request_id_header = request_id_header
        self.trace_id_header = trace_id_header
        self._context = RequestIDContext()
        self._generator = RequestIDGenerator() if generate else None
        self._request_ids: dict[str, str] = {}

    def get_or_create_request_id(
        self,
        request: dict[str, Any],
    ) -> str:
        """Get existing request ID or create a new one.

        Args:
            request: Request dictionary.

        Returns:
            Request ID string.
        """
        headers = request.get("headers", {})
        request_id = headers.get(self.request_id_header)
        if not request_id and self._generator:
            request_id = self._generator.generate()
        if request_id:
            self._context.set_request_id(request_id)
            self._request_ids[request_id] = request_id
        return request_id or ""

    def get_current_request_id(self) -> Optional[str]:
        """Get the current request ID from context.

        Returns:
            Current request ID or None.
        """
        return self._context.get_request_id()

    def propagate_to_headers(
        self,
        headers: dict[str, str],
        request_id: Optional[str] = None,
    ) -> dict[str, str]:
        """Add request ID to headers for propagation.

        Args:
            headers: Headers dictionary.
            request_id: Optional specific request ID.

        Returns:
            Headers with request ID added.
        """
        rid = request_id or self.get_current_request_id()
        if rid:
            headers[self.request_id_header] = rid
        return headers

    def generate_trace_context(self) -> dict[str, str]:
        """Generate trace context for downstream calls.

        Returns:
            Dictionary of trace headers.
        """
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:16]
        return {
            self.trace_id_header: trace_id,
            self.span_id_header: span_id,
        }


class RequestIDMiddleware:
    """Middleware for automatic request ID handling.

    Example:
        >>> middleware = RequestIDMiddleware()
        >>> response = middleware.process(request)
    """

    def __init__(
        self,
        manager: Optional[RequestIDManager] = None,
    ) -> None:
        self.manager = manager or RequestIDManager()

    def process_request(self, request: dict[str, Any]) -> dict[str, Any]:
        """Process incoming request.

        Args:
            request: Request dictionary.

        Returns:
            Modified request with request ID.
        """
        request_id = self.manager.get_or_create_request_id(request)
        if request_id:
            request["request_id"] = request_id
            logger.info(f"Request {request_id}: Processing {request.get('path', 'unknown')}")
        return request

    def process_response(
        self,
        response: dict[str, Any],
        request_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Process outgoing response.

        Args:
            response: Response dictionary.
            request_id: Request ID to add.

        Returns:
            Modified response with request ID.
        """
        rid = request_id or self.manager.get_current_request_id()
        if rid:
            response["headers"] = self.manager.propagate_to_headers(
                response.get("headers", {}),
                rid,
            )
        return response


def get_request_id(request: dict[str, Any]) -> Optional[str]:
    """Quick extract request ID from request.

    Args:
        request: Request dictionary.

    Returns:
        Request ID or None.
    """
    return request.get("headers", {}).get(DEFAULT_REQUEST_ID_HEADER)


def set_request_id(request: dict[str, Any], request_id: str) -> None:
    """Quick set request ID in request.

    Args:
        request: Request dictionary.
        request_id: Request ID to set.
    """
    if "headers" not in request:
        request["headers"] = {}
    request["headers"][DEFAULT_REQUEST_ID_HEADER] = request_id
    request["request_id"] = request_id
