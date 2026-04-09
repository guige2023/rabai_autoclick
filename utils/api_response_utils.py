"""Utilities for API response handling, parsing, and transformation.

This module provides utilities for standardizing API response formats,
error handling, pagination, and data extraction.
"""

from __future__ import annotations

from typing import Any, TypeVar, Generic, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ResponseStatus(Enum):
    """Standard API response status codes."""
    SUCCESS = "success"
    ERROR = "error"
    PARTIAL = "partial"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    UNAUTHORIZED = "unauthorized"
    NOT_FOUND = "not_found"
    VALIDATION_ERROR = "validation_error"
    SERVER_ERROR = "server_error"


@dataclass
class ApiResponse(Generic[T]):
    """Standardized API response container.

    Attributes:
        status: Response status code.
        data: Response payload data.
        error: Error message if status is not SUCCESS.
        metadata: Additional response metadata.
        timestamp: Unix timestamp of response.
    """
    status: ResponseStatus
    data: T | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    @property
    def is_success(self) -> bool:
        """Check if response indicates success."""
        return self.status == ResponseStatus.SUCCESS

    @property
    def is_error(self) -> bool:
        """Check if response indicates an error."""
        return self.status in (ResponseStatus.ERROR, ResponseStatus.SERVER_ERROR)

    def get_or_raise(self) -> T:
        """Get data or raise exception if error.

        Returns:
            The response data.

        Raises:
            ApiResponseError: If response status is not SUCCESS.
        """
        if not self.is_success or self.data is None:
            raise ApiResponseError(
                f"API request failed: {self.error or self.status.value}"
            )
        return self.data

    def map(self, fn: Callable[[T], T]) -> ApiResponse[T]:
        """Apply transformation function to response data.

        Args:
            fn: Transformation function to apply.

        Returns:
            New response with transformed data.
        """
        if self.data is not None:
            return ApiResponse(
                status=self.status,
                data=fn(self.data),
                error=self.error,
                metadata=self.metadata,
                timestamp=self.timestamp
            )
        return self


class ApiResponseError(Exception):
    """Exception raised when API response indicates failure."""

    def __init__(self, message: str, status: ResponseStatus = ResponseStatus.ERROR) -> None:
        """Initialize API response error.

        Args:
            message: Error description.
            status: Response status that caused this error.
        """
        super().__init__(message)
        self.status = status


@dataclass
class PaginatedResponse(Generic[T]):
    """Paginated API response container.

    Attributes:
        items: List of items in current page.
        page: Current page number (0-indexed).
        page_size: Number of items per page.
        total_items: Total number of items across all pages.
        total_pages: Total number of pages.
        has_next: Whether there is a next page.
        has_previous: Whether there is a previous page.
    """
    items: list[T]
    page: int
    page_size: int
    total_items: int
    total_pages: int
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def has_next(self) -> bool:
        """Check if there is a next page."""
        return self.page < self.total_pages - 1

    @property
    def has_previous(self) -> bool:
        """Check if there is a previous page."""
        return self.page > 0

    @property
    def start_index(self) -> int:
        """0-based index of first item in this page."""
        return self.page * self.page_size

    @property
    def end_index(self) -> int:
        """0-based index of last item in this page (exclusive)."""
        return min((self.page + 1) * self.page_size, self.total_items)


def parse_json_response(data: dict[str, Any]) -> ApiResponse[Any]:
    """Parse a raw JSON dict into a standardized ApiResponse.

    Expects dict with optional keys: 'status', 'data', 'error', 'metadata'.

    Args:
        data: Raw JSON dictionary.

    Returns:
        Standardized ApiResponse.
    """
    status_str = data.get("status", "success")
    try:
        status = ResponseStatus(status_str)
    except ValueError:
        logger.warning(f"Unknown status '{status_str}', defaulting to ERROR")
        status = ResponseStatus.ERROR

    return ApiResponse(
        status=status,
        data=data.get("data"),
        error=data.get("error"),
        metadata=data.get("metadata", {}),
        timestamp=data.get("timestamp", time.time())
    )


def paginate(items: list[T], page: int, page_size: int) -> PaginatedResponse[T]:
    """Create a paginated response from a list of items.

    Args:
        items: Full list of items.
        page: Page number (0-indexed).
        page_size: Number of items per page.

    Returns:
        Paginated response with requested page.
    """
    total_items = len(items)
    total_pages = max(1, (total_items + page_size - 1) // page_size)

    page = max(0, min(page, total_pages - 1))

    start = page * page_size
    end = min(start + page_size, total_items)

    return PaginatedResponse(
        items=items[start:end],
        page=page,
        page_size=page_size,
        total_items=total_items,
        total_pages=total_pages
    )


def extract_field(data: dict[str, Any], path: str, default: T = None) -> T:
    """Extract a nested field from a dictionary using dot notation.

    Args:
        data: Source dictionary.
        path: Dot-separated field path (e.g., 'result.user.name').
        default: Default value if path not found.

    Returns:
        Value at path or default.
    """
    keys = path.split(".")
    current: Any = data

    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        elif hasattr(current, key):
            current = getattr(current, key)
        else:
            return default

        if current is None:
            return default

    return current  # type: ignore


def flatten_dict(data: dict[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Flatten a nested dictionary into dot-notation keys.

    Args:
        data: Nested dictionary to flatten.
        parent_key: Prefix for keys in this level.
        sep: Separator for nested keys.

    Returns:
        Flattened dictionary.
    """
    items: dict[str, Any] = {}

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.update(flatten_dict(value, new_key, sep))
        else:
            items[new_key] = value

    return items


def retry_on_status(
    statuses: list[ResponseStatus] | None = None,
    max_retries: int = 3,
    backoff_base: float = 1.0
) -> Callable:
    """Decorator factory for retrying on specific API response statuses.

    Args:
        statuses: List of statuses that trigger retry. Defaults to [RATE_LIMITED, TIMEOUT].
        max_retries: Maximum number of retry attempts.
        backoff_base: Base delay in seconds for exponential backoff.

    Returns:
        Decorator function.
    """
    if statuses is None:
        statuses = [ResponseStatus.RATE_LIMITED, ResponseStatus.TIMEOUT]

    def decorator(func: Callable[..., ApiResponse[T]]) -> Callable[..., ApiResponse[T]]:
        """Decorator that retries function on specified statuses."""

        def wrapper(*args: Any, **kwargs: Any) -> ApiResponse[T]:
            last_response: ApiResponse[T] | None = None

            for attempt in range(max_retries + 1):
                response = func(*args, **kwargs)
                last_response = response

                if response.is_success:
                    return response

                if response.status not in statuses:
                    return response

                if attempt < max_retries:
                    delay = backoff_base * (2 ** attempt)
                    logger.debug(f"Retrying after {delay}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)

            return last_response  # type: ignore

        return wrapper  # type: ignore

    return decorator
