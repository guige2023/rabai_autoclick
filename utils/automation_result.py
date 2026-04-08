"""Automation result types and utilities for UI automation.

Provides standardized result types for automation actions,
including success/failure status, error handling, and retry support.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ResultStatus(Enum):
    """Status of an automation result."""
    SUCCESS = auto()
    FAILURE = auto()
    SKIPPED = auto()
    PENDING = auto()
    TIMEOUT = auto()


@dataclass
class AutomationError:
    """An error from an automation action.

    Attributes:
        code: Error code string.
        message: Human-readable error message.
        details: Additional error details.
        recoverable: Whether this error can be retried.
    """
    code: str
    message: str
    details: Optional[str] = None
    recoverable: bool = False


@dataclass
class AutomationResult:
    """Result of an automation action.

    Attributes:
        result_id: Unique identifier for this result.
        status: The status of the result.
        value: The return value if successful.
        error: Error information if failed.
        elapsed_ms: Time taken in milliseconds.
        retry_count: Number of retries performed.
        timestamp: When the result was created.
        metadata: Additional result metadata.
    """
    status: ResultStatus
    result_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    value: Any = None
    error: Optional[AutomationError] = None
    elapsed_ms: float = 0.0
    retry_count: int = 0
    timestamp: float = field(default_factory=lambda: __import__("time").time())
    metadata: dict = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Return True if status is SUCCESS."""
        return self.status == ResultStatus.SUCCESS

    @property
    def is_failure(self) -> bool:
        """Return True if status indicates failure."""
        return self.status in (ResultStatus.FAILURE, ResultStatus.TIMEOUT)

    @property
    def error_message(self) -> str:
        """Return error message or empty string."""
        return self.error.message if self.error else ""

    def with_retry(self, count: int) -> AutomationResult:
        """Return a copy with updated retry count."""
        result = AutomationResult(
            status=self.status,
            value=self.value,
            error=self.error,
            elapsed_ms=self.elapsed_ms,
            retry_count=count,
            metadata=self.metadata.copy(),
        )
        return result


class ResultBuilder:
    """Builder for creating AutomationResult objects."""

    def __init__(self) -> None:
        """Initialize with default values."""
        self._status: ResultStatus = ResultStatus.SUCCESS
        self._value: Any = None
        self._error: Optional[AutomationError] = None
        self._elapsed_ms: float = 0.0
        self._retry_count: int = 0
        self._metadata: dict = {}

    def success(self, value: Any = None) -> ResultBuilder:
        """Set status to SUCCESS with optional value."""
        self._status = ResultStatus.SUCCESS
        self._value = value
        return self

    def failure(
        self,
        code: str = "FAILURE",
        message: str = "",
        recoverable: bool = False,
    ) -> ResultBuilder:
        """Set status to FAILURE with error info."""
        self._status = ResultStatus.FAILURE
        self._error = AutomationError(
            code=code, message=message, recoverable=recoverable
        )
        return self

    def timeout(self, message: str = "Operation timed out") -> ResultBuilder:
        """Set status to TIMEOUT."""
        self._status = ResultStatus.TIMEOUT
        self._error = AutomationError(code="TIMEOUT", message=message)
        return self

    def skipped(self, reason: str = "") -> ResultBuilder:
        """Set status to SKIPPED."""
        self._status = ResultStatus.SKIPPED
        self._metadata["skip_reason"] = reason
        return self

    def elapsed(self, ms: float) -> ResultBuilder:
        """Set elapsed time."""
        self._elapsed_ms = ms
        return self

    def retries(self, count: int) -> ResultBuilder:
        """Set retry count."""
        self._retry_count = count
        return self

    def metadata(self, key: str, value: Any) -> ResultBuilder:
        """Add metadata."""
        self._metadata[key] = value
        return self

    def build(self) -> AutomationResult:
        """Build the result object."""
        return AutomationResult(
            status=self._status,
            value=self._value,
            error=self._error,
            elapsed_ms=self._elapsed_ms,
            retry_count=self._retry_count,
            metadata=self._metadata,
        )


# Utility functions
def success_result(value: Any = None) -> AutomationResult:
    """Create a quick success result."""
    return ResultBuilder().success(value).build()


def failure_result(
    code: str = "FAILURE",
    message: str = "",
    recoverable: bool = False,
) -> AutomationResult:
    """Create a quick failure result."""
    return ResultBuilder().failure(code, message, recoverable).build()


def timeout_result(message: str = "Operation timed out") -> AutomationResult:
    """Create a quick timeout result."""
    return ResultBuilder().timeout(message).build()
