"""API Error Translator and Formatter.

This module provides API error handling and formatting:
- Error code mapping
- Human-readable messages
- Error response formatting
- Error tracking and aggregation

Example:
    >>> from actions.api_error_translator_action import ErrorTranslator
    >>> translator = ErrorTranslator()
    >>> translator.register_error("AUTH_001", "Invalid token", status=401)
    >>> response = translator.translate(ValueError("Invalid token"))
"""

from __future__ import annotations

import logging
import threading
import traceback
import time
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorSeverity(Enum):
    """Error severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ErrorDefinition:
    """Definition of an API error."""
    code: str
    message: str
    status: int = 500
    severity: ErrorSeverity = ErrorSeverity.ERROR
    details: str = ""
    retryable: bool = False


@dataclass
class APIError:
    """A formatted API error response."""
    code: str
    message: str
    status: int
    severity: str
    timestamp: float = field(default_factory=time.time)
    request_id: Optional[str] = None
    details: Optional[str] = None
    help_url: Optional[str] = None


class ErrorTranslator:
    """Translates and formats API errors."""

    def __init__(self, include_traceback: bool = False) -> None:
        """Initialize the error translator.

        Args:
            include_traceback: Whether to include traceback in responses.
        """
        self._errors: dict[str, ErrorDefinition] = {}
        self._exception_mapping: dict[type, str] = {}
        self._include_traceback = include_traceback
        self._lock = threading.RLock()
        self._stats: dict[str, int] = {}
        self._error_log: list[APIError] = []
        self._max_log_size = 1000

        self._register_defaults()

    def _register_defaults(self) -> None:
        """Register default error definitions."""
        defaults = [
            ErrorDefinition("ERR_400", "Bad Request", 400, ErrorSeverity.WARNING),
            ErrorDefinition("ERR_401", "Unauthorized", 401, ErrorSeverity.WARNING),
            ErrorDefinition("ERR_403", "Forbidden", 403, ErrorSeverity.WARNING),
            ErrorDefinition("ERR_404", "Not Found", 404, ErrorSeverity.INFO),
            ErrorDefinition("ERR_409", "Conflict", 409, ErrorSeverity.WARNING),
            ErrorDefinition("ERR_422", "Validation Error", 422, ErrorSeverity.WARNING),
            ErrorDefinition("ERR_429", "Too Many Requests", 429, ErrorSeverity.WARNING, retryable=True),
            ErrorDefinition("ERR_500", "Internal Server Error", 500, ErrorSeverity.CRITICAL),
            ErrorDefinition("ERR_502", "Bad Gateway", 502, ErrorSeverity.ERROR, retryable=True),
            ErrorDefinition("ERR_503", "Service Unavailable", 503, ErrorSeverity.CRITICAL, retryable=True),
            ErrorDefinition("ERR_504", "Gateway Timeout", 504, ErrorSeverity.ERROR, retryable=True),
        ]
        for err in defaults:
            self.register_error(err)

    def register_error(
        self,
        error: ErrorDefinition,
    ) -> None:
        """Register an error definition.

        Args:
            error: ErrorDefinition to register.
        """
        with self._lock:
            self._errors[error.code] = error
            logger.info("Registered error: %s (%d)", error.code, error.status)

    def register_exception_mapping(
        self,
        exception_type: type,
        error_code: str,
    ) -> None:
        """Map an exception type to an error code.

        Args:
            exception_type: Exception class.
            error_code: Error code to map to.
        """
        with self._lock:
            self._exception_mapping[exception_type] = error_code

    def translate(
        self,
        exc: Exception,
        request_id: Optional[str] = None,
        custom_message: Optional[str] = None,
    ) -> APIError:
        """Translate an exception to an API error.

        Args:
            exc: The exception to translate.
            request_id: Optional request ID for tracking.
            custom_message: Override error message.

        Returns:
            Formatted APIError.
        """
        error_code = self._find_error_code(exc)
        error_def = self._errors.get(error_code)

        if error_def:
            api_error = APIError(
                code=error_def.code,
                message=custom_message or error_def.message,
                status=error_def.status,
                severity=error_def.severity.value,
                request_id=request_id,
                details=error_def.details if error_def.details else None,
            )
        else:
            api_error = APIError(
                code="ERR_INTERNAL",
                message=custom_message or str(exc) or "An unexpected error occurred",
                status=500,
                severity=ErrorSeverity.ERROR.value,
                request_id=request_id,
            )

        if self._include_traceback:
            api_error.details = traceback.format_exc()

        self._log_error(api_error)
        self._update_stats(error_code)

        return api_error

    def translate_error_code(
        self,
        error_code: str,
        request_id: Optional[str] = None,
        **kwargs: Any,
    ) -> APIError:
        """Translate an error code to an API error.

        Args:
            error_code: The error code.
            request_id: Optional request ID.
            **kwargs: Additional fields for the error message.

        Returns:
            Formatted APIError.
        """
        error_def = self._errors.get(error_code)

        if error_def:
            message = error_def.message
            if kwargs:
                message = message.format(**kwargs)

            api_error = APIError(
                code=error_def.code,
                message=message,
                status=error_def.status,
                severity=error_def.severity.value,
                request_id=request_id,
                details=error_def.details if error_def.details else None,
            )
        else:
            api_error = APIError(
                code=error_code,
                message="Unknown error",
                status=500,
                severity=ErrorSeverity.ERROR.value,
                request_id=request_id,
            )

        self._log_error(api_error)
        self._update_stats(error_code)

        return api_error

    def _find_error_code(self, exc: Exception) -> Optional[str]:
        """Find the error code for an exception."""
        exc_type = type(exc)
        return self._exception_mapping.get(exc_type)

    def _log_error(self, error: APIError) -> None:
        """Log an error for tracking."""
        self._error_log.append(error)
        if len(self._error_log) > self._max_log_size:
            self._error_log = self._error_log[-self._max_log_size // 2:]

    def _update_stats(self, error_code: str) -> None:
        """Update error statistics."""
        with self._lock:
            self._stats[error_code] = self._stats.get(error_code, 0) + 1

    def get_recent_errors(self, limit: int = 50) -> list[APIError]:
        """Get recent errors.

        Args:
            limit: Maximum errors to return.

        Returns:
            List of recent APIError objects.
        """
        with self._lock:
            return list(reversed(self._error_log[-limit:]))

    def get_error_stats(self) -> dict[str, int]:
        """Get error statistics."""
        with self._lock:
            return dict(self._stats)

    def is_retryable(self, error_code: str) -> bool:
        """Check if an error is retryable.

        Args:
            error_code: The error code.

        Returns:
            True if the error should be retried.
        """
        error_def = self._errors.get(error_code)
        return error_def.retryable if error_def else False
