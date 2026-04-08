"""
API Adapter Action Module.

Provides adapters to normalize different API response formats
 into a standard structure with error handling.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ResponseStyle(Enum):
    """Style of API response."""
    STANDARD = "standard"
    WRAPPED = "wrapped"
    PAGINATED = "paginated"
    HAL = "hal"
    GRAPHQL = "graphql"
    CUSTOM = "custom"


@dataclass
class NormalizedResponse:
    """Normalized API response."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    status_code: int = 200
    metadata: dict[str, Any] = field(default_factory=dict)


class APIAdapterAction:
    """
    API response adapter for multiple formats.

    Normalizes responses from various API styles into a
    consistent format with automatic error extraction.

    Example:
        adapter = APIAdapterAction(response_style=ResponseStyle.WRAPPED)
        adapter.set_data_path("response.data")
        adapter.set_error_path("response.error")
        normalized = adapter.adapt(raw_response)
    """

    def __init__(
        self,
        response_style: ResponseStyle = ResponseStyle.STANDARD,
    ) -> None:
        self.response_style = response_style
        self._data_path: Optional[str] = None
        self._error_path: Optional[str] = None
        self._status_path: Optional[str] = None
        self._success_field: Optional[str] = None
        self._custom_parser: Optional[Callable[[Any], NormalizedResponse]] = None

    def set_data_path(self, path: str) -> "APIAdapterAction":
        """Set the path to extract data from response."""
        self._data_path = path
        return self

    def set_error_path(self, path: str) -> "APIAdapterAction":
        """Set the path to extract errors from response."""
        self._error_path = path
        return self

    def set_status_path(self, path: str) -> "APIAdapterAction":
        """Set the path to extract status from response."""
        self._status_path = path
        return self

    def set_custom_parser(
        self,
        parser_func: Callable[[Any], NormalizedResponse],
    ) -> "APIAdapterAction":
        """Set a custom response parser."""
        self._custom_parser = parser_func
        self.response_style = ResponseStyle.CUSTOM
        return self

    def adapt(
        self,
        response: Any,
        status_code: int = 200,
    ) -> NormalizedResponse:
        """Normalize an API response."""
        if self._custom_parser:
            return self._custom_parser(response)

        if self.response_style == ResponseStyle.STANDARD:
            return self._adapt_standard(response, status_code)

        elif self.response_style == ResponseStyle.WRAPPED:
            return self._adapt_wrapped(response, status_code)

        elif self.response_style == ResponseStyle.PAGINATED:
            return self._adapt_paginated(response, status_code)

        elif self.response_style == ResponseStyle.HAL:
            return self._adapt_hal(response, status_code)

        elif self.response_style == ResponseStyle.GRAPHQL:
            return self._adapt_graphql(response, status_code)

        return NormalizedResponse(
            success=True,
            data=response,
            status_code=status_code,
        )

    def _adapt_standard(
        self,
        response: Any,
        status_code: int,
    ) -> NormalizedResponse:
        """Adapt standard REST response."""
        if isinstance(response, dict):
            data = response
            error = None

            if self._error_path:
                error = self._extract_path(response, self._error_path)
            elif "error" in response:
                error = response["error"]
            elif "message" in response and status_code >= 400:
                error = response["message"]

            success = status_code < 400 and error is None

            return NormalizedResponse(
                success=success,
                data=data,
                error=error,
                status_code=status_code,
            )

        return NormalizedResponse(
            success=status_code < 400,
            data=response,
            status_code=status_code,
        )

    def _adapt_wrapped(
        self,
        response: Any,
        status_code: int,
    ) -> NormalizedResponse:
        """Adapt wrapped response (e.g., {data: ..., error: ...})."""
        if not isinstance(response, dict):
            return NormalizedResponse(
                success=status_code < 400,
                data=response,
                status_code=status_code,
            )

        data = response
        error = None

        if self._data_path:
            data = self._extract_path(response, self._data_path)
        elif "data" in response:
            data = response["data"]

        if self._error_path:
            error = self._extract_path(response, self._error_path)
        elif "error" in response:
            error = response["error"]

        success = status_code < 400 and error is None

        return NormalizedResponse(
            success=success,
            data=data,
            error=error,
            status_code=status_code,
        )

    def _adapt_paginated(
        self,
        response: Any,
        status_code: int,
    ) -> NormalizedResponse:
        """Adapt paginated response."""
        if not isinstance(response, dict):
            return NormalizedResponse(
                success=status_code < 400,
                data=response,
                status_code=status_code,
            )

        metadata: dict[str, Any] = {}

        if "total" in response:
            metadata["total"] = response["total"]
        if "page" in response:
            metadata["page"] = response["page"]
        if "per_page" in response or "pageSize" in response:
            metadata["page_size"] = response.get("per_page") or response.get("pageSize")

        data = response.get("data") or response.get("items") or response

        return NormalizedResponse(
            success=status_code < 400,
            data=data,
            status_code=status_code,
            metadata=metadata,
        )

    def _adapt_hal(
        self,
        response: Any,
        status_code: int,
    ) -> NormalizedResponse:
        """Adapt HAL (Hypertext Application Language) response."""
        if not isinstance(response, dict):
            return NormalizedResponse(
                success=status_code < 400,
                data=response,
                status_code=status_code,
            )

        data = {k: v for k, v in response.items() if k not in ("_links", "_embedded")}

        embedded = response.get("_embedded", {})
        links = response.get("_links", {})

        metadata: dict[str, Any] = {"_links": links}
        if embedded:
            metadata["_embedded"] = embedded

        return NormalizedResponse(
            success=status_code < 400,
            data=data,
            status_code=status_code,
            metadata=metadata,
        )

    def _adapt_graphql(
        self,
        response: Any,
        status_code: int,
    ) -> NormalizedResponse:
        """Adapt GraphQL response."""
        if not isinstance(response, dict):
            return NormalizedResponse(
                success=status_code < 400,
                data=response,
                status_code=status_code,
            )

        if "errors" in response:
            error_messages = [e.get("message") for e in response["errors"]]
            return NormalizedResponse(
                success=False,
                data=response.get("data"),
                error="; ".join(filter(None, error_messages)),
                status_code=status_code,
            )

        return NormalizedResponse(
            success=True,
            data=response.get("data"),
            status_code=status_code,
        )

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract value from nested data using dot notation."""
        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None

            if current is None:
                return None

        return current
