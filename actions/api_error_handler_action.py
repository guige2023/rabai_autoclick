# Copyright (c) 2024. coded by claude
"""API Error Handler Action Module.

Handles API errors with support for error classification,
recovery strategies, and user-friendly error messages.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NOT_FOUND = "not_found"
    RATE_LIMIT = "rate_limit"
    SERVER_ERROR = "server_error"
    NETWORK_ERROR = "network_error"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass
class APIError:
    category: ErrorCategory
    code: str
    message: str
    status_code: int
    details: Optional[Dict[str, Any]] = None
    retryable: bool = False


@dataclass
class ErrorResponse:
    success: bool = False
    error: APIError
    user_message: str


class APIErrorHandler:
    ERROR_MAPPING: Dict[str, tuple] = {
        "VALIDATION_ERROR": (ErrorCategory.VALIDATION, 400, True),
        "AUTH_REQUIRED": (ErrorCategory.AUTHENTICATION, 401, False),
        "FORBIDDEN": (ErrorCategory.AUTHORIZATION, 403, False),
        "NOT_FOUND": (ErrorCategory.NOT_FOUND, 404, False),
        "RATE_LIMIT_EXCEEDED": (ErrorCategory.RATE_LIMIT, 429, True),
        "SERVER_ERROR": (ErrorCategory.SERVER_ERROR, 500, True),
        "NETWORK_ERROR": (ErrorCategory.NETWORK_ERROR, 503, True),
        "TIMEOUT": (ErrorCategory.TIMEOUT, 504, True),
    }

    def __init__(self):
        self._custom_handlers: Dict[ErrorCategory, Callable] = {}
        self._fallback_handler: Optional[Callable] = None

    def register_handler(self, category: ErrorCategory, handler: Callable) -> None:
        self._custom_handlers[category] = handler

    def set_fallback_handler(self, handler: Callable) -> None:
        self._fallback_handler = handler

    def handle_error(self, exception: Exception, context: Optional[Dict[str, Any]] = None) -> ErrorResponse:
        error = self._classify_exception(exception)
        handler = self._custom_handlers.get(error.category, self._fallback_handler)
        if handler:
            try:
                user_message = handler(error, context)
            except Exception:
                user_message = self._get_default_message(error)
        else:
            user_message = self._get_default_message(error)
        return ErrorResponse(success=False, error=error, user_message=user_message)

    def _classify_exception(self, exception: Exception) -> APIError:
        error_str = str(exception).upper()
        for error_code, (category, status, retryable) in self.ERROR_MAPPING.items():
            if error_code in error_str:
                return APIError(
                    category=category,
                    code=error_code,
                    message=str(exception),
                    status_code=status,
                    retryable=retryable,
                )
        return APIError(
            category=ErrorCategory.UNKNOWN,
            code="UNKNOWN_ERROR",
            message=str(exception),
            status_code=500,
            retryable=False,
        )

    def _get_default_message(self, error: APIError) -> str:
        messages = {
            ErrorCategory.VALIDATION: "The request contains invalid data. Please check your input.",
            ErrorCategory.AUTHENTICATION: "Authentication is required. Please provide valid credentials.",
            ErrorCategory.AUTHORIZATION: "You do not have permission to perform this action.",
            ErrorCategory.NOT_FOUND: "The requested resource was not found.",
            ErrorCategory.RATE_LIMIT: "Too many requests. Please wait a moment and try again.",
            ErrorCategory.SERVER_ERROR: "An unexpected error occurred. Please try again later.",
            ErrorCategory.NETWORK_ERROR: "Unable to connect. Please check your network connection.",
            ErrorCategory.TIMEOUT: "The request timed out. Please try again.",
            ErrorCategory.UNKNOWN: "An unexpected error occurred. Please try again.",
        }
        return messages.get(error.category, "An error occurred.")

    def is_retryable(self, error: APIError) -> bool:
        return error.retryable
