"""API Request ID Propagator.

This module provides request ID generation and propagation:
- Unique request ID generation
- Context propagation across async calls
- Request ID logging integration
- Response header injection

Example:
    >>> from actions.api_request_id_action import RequestIDPropagator
    >>> propagator = RequestIDPropagator()
    >>> request_id = propagator.get_or_create_request_id()
"""

from __future__ import annotations

import uuid
import threading
import logging
from contextvars import ContextVar
from typing import Optional, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


@dataclass
class RequestContext:
    """Request context with ID and metadata."""
    request_id: str
    parent_id: Optional[str] = None
    root_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    metadata: dict = None


class RequestIDPropagator:
    """Generates and propagates request IDs across service calls."""

    def __init__(
        self,
        header_name: str = "X-Request-ID",
        propagate_header: str = "X-Request-ID",
    ) -> None:
        """Initialize the propagator.

        Args:
            header_name: Header name for request ID.
            propagate_header: Header name for incoming request ID propagation.
        """
        self._header_name = header_name
        self._propagate_header = propagate_header
        self._lock = threading.Lock()
        self._counters: dict[str, int] = {}
        self._contexts: dict[str, RequestContext] = {}

    def get_or_create_request_id(
        self,
        incoming_id: Optional[str] = None,
    ) -> str:
        """Get existing request ID or create a new one.

        Args:
            incoming_id: ID from incoming request header.

        Returns:
            The request ID.
        """
        existing = _request_id_var.get()
        if existing:
            return existing

        request_id = incoming_id or self._generate_request_id()
        _request_id_var.set(request_id)
        return request_id

    def create_request_context(
        self,
        request_id: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> RequestContext:
        """Create a new request context.

        Args:
            request_id: Request ID. None = generate new.
            parent_id: Parent request ID for tracing.

        Returns:
            The created RequestContext.
        """
        request_id = request_id or self._generate_request_id()
        ctx = RequestContext(
            request_id=request_id,
            parent_id=parent_id,
            root_id=parent_id or request_id,
            metadata={},
        )

        with self._lock:
            self._contexts[request_id] = ctx

        return ctx

    def get_context(self, request_id: str) -> Optional[RequestContext]:
        """Get request context by ID."""
        with self._lock:
            return self._contexts.get(request_id)

    def generate_span_id(self) -> str:
        """Generate a new span ID for distributed tracing."""
        return uuid.uuid4().hex[:16]

    def set_context_value(self, request_id: str, key: str, value: any) -> None:
        """Set a metadata value in the request context.

        Args:
            request_id: Request ID.
            key: Metadata key.
            value: Metadata value.
        """
        with self._lock:
            ctx = self._contexts.get(request_id)
            if ctx and ctx.metadata is not None:
                ctx.metadata[key] = value

    def inject_into_headers(
        self,
        headers: dict[str, str],
        request_id: Optional[str] = None,
    ) -> dict[str, str]:
        """Inject request ID into response headers.

        Args:
            headers: Response headers dict.
            request_id: Request ID. None = get from context.

        Returns:
            Headers with request ID added.
        """
        rid = request_id or _request_id_var.get() or self._generate_request_id()
        headers[self._header_name] = rid
        return headers

    def extract_from_headers(self, headers: dict[str, str]) -> Optional[str]:
        """Extract request ID from incoming request headers.

        Args:
            headers: Request headers dict.

        Returns:
            Request ID if found, None otherwise.
        """
        return headers.get(self._propagate_header)

    def _generate_request_id(self) -> str:
        """Generate a unique request ID."""
        return f"req_{uuid.uuid4().hex[:24]}"

    def clear_context(self, request_id: str) -> None:
        """Clear a request context.

        Args:
            request_id: Request ID to clear.
        """
        with self._lock:
            self._contexts.pop(request_id, None)

    def list_active_contexts(self) -> list[RequestContext]:
        """List all active request contexts."""
        with self._lock:
            return list(self._contexts.values())
