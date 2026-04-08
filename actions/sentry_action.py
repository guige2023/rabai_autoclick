"""Sentry Error Tracking Action Module.

Provides error tracking, exception reporting, and performance monitoring
capabilities compatible with Sentry SDK for automated error workflows.
"""
from __future__ import annotations

import hashlib
import json
import time
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class SeverityLevel(Enum):
    """Sentry severity level."""
    FATAL = "fatal"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


class ErrorCategory(Enum):
    """Error category classification."""
    DATABASE = "database"
    NETWORK = "network"
    AUTH = "auth"
    VALIDATION = "validation"
    TIMEOUT = "timeout"
    PERMISSION = "permission"
    NOT_FOUND = "not_found"
    RATE_LIMIT = "rate_limit"
    INTERNAL = "internal"
    UNKNOWN = "unknown"


@dataclass
class StackFrame:
    """Single stack frame."""
    filename: str
    function: str
    lineno: int
    colno: int = 0
    abs_path: str = ""
    context_line: str = ""
    pre_context: List[str] = field(default_factory=list)
    post_context: List[str] = field(default_factory=list)


@dataclass
class ExceptionInfo:
    """Exception information."""
    type: str
    value: str
    module: str = ""
    stacktrace: List[StackFrame] = field(default_factory=list)


@dataclass
class Breadcrumb:
    """Sentry breadcrumb."""
    timestamp: float
    category: str
    message: str
    level: SeverityLevel = SeverityLevel.INFO
    data: Dict[str, Any] = field(default_factory=dict)
    type: str = "default"


@dataclass
class SentryEvent:
    """Sentry event data."""
    event_id: str
    timestamp: float
    platform: str
    environment: str
    release: str
    dist: str
    sdk: Dict[str, str]
    logger: str
    level: SeverityLevel
    culprit: str
    message: str
    exception: Optional[ExceptionInfo] = None
    stacktrace: List[StackFrame] = field(default_factory=list)
    breadcrumbs: List[Breadcrumb] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    user: Dict[str, Any] = field(default_factory=dict)
    request: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)
    fingerprint: List[str] = field(default_factory=list)
    modules: Dict[str, str] = field(default_factory=dict)


@dataclass
class ErrorReport:
    """Error report result."""
    success: bool
    event_id: Optional[str]
    sent: bool
    error_category: ErrorCategory
    severity: SeverityLevel
    duration_ms: float
    message: str
    errors: List[str] = field(default_factory=list)


class ErrorClassifier:
    """Classify errors by category."""

    def classify(self, exception: Exception) -> ErrorCategory:
        """Classify exception into error category."""
        exc_type = type(exception).__name__.lower()
        exc_msg = str(exception).lower()

        if any(kw in exc_type or kw in exc_msg for kw in ["timeout", "timed out"]):
            return ErrorCategory.TIMEOUT
        if any(kw in exc_type or kw in exc_msg for kw in ["auth", "login", "credential", "token", "unauthorized"]):
            return ErrorCategory.AUTH
        if any(kw in exc_type or kw in exc_msg for kw in ["permission", "denied", "forbidden", "access"]):
            return ErrorCategory.PERMISSION
        if any(kw in exc_type or kw in exc_msg for kw in ["database", "db", "sql", "query", "connection"]):
            return ErrorCategory.DATABASE
        if any(kw in exc_type or kw in exc_msg for kw in ["network", "connection", "socket", "http", "request"]):
            return ErrorCategory.NETWORK
        if any(kw in exc_type or kw in exc_msg for kw in ["validation", "invalid", "malformed"]):
            return ErrorCategory.VALIDATION
        if any(kw in exc_type or kw in exc_msg for kw in ["not found", "404", "does not exist"]):
            return ErrorCategory.NOT_FOUND
        if any(kw in exc_type or kw in exc_msg for kw in ["rate limit", "too many"]):
            return ErrorCategory.RATE_LIMIT

        return ErrorCategory.INTERNAL

    def classify_from_message(self, message: str) -> ErrorCategory:
        """Classify error from message string."""
        message_lower = message.lower()

        if any(kw in message_lower for kw in ["timeout", "timed out"]):
            return ErrorCategory.TIMEOUT
        if any(kw in message_lower for kw in ["connection refused", "network", "socket"]):
            return ErrorCategory.NETWORK
        if any(kw in message_lower for kw in ["permission denied", "access denied"]):
            return ErrorCategory.PERMISSION

        return ErrorCategory.UNKNOWN


class SentryEventBuilder:
    """Build Sentry events."""

    def __init__(self):
        self._classifier = ErrorClassifier()

    def build_exception_event(self, exception: Exception,
                              params: Optional[Dict[str, Any]] = None) -> SentryEvent:
        """Build Sentry event from exception."""
        params = params or {}
        event_id = uuid.uuid4().hex

        tb = exception.__traceback__
        stack_frames = []
        if tb:
            for frame in traceback.extract_tb(tb):
                stack_frames.append(StackFrame(
                    filename=frame.filename,
                    function=frame.name,
                    lineno=frame.lineno,
                    context_line=frame.line or ""
                ))

        exc_info = ExceptionInfo(
            type=type(exception).__name__,
            value=str(exception),
            module=exception.__class__.__module__ or "",
            stacktrace=stack_frames
        )

        category = self._classifier.classify(exception)

        fingerprint = params.get("fingerprint", [])
        if not fingerprint:
            fingerprint = [
                "{{ default }}",
                category.value
            ]

        return SentryEvent(
            event_id=event_id,
            timestamp=time.time(),
            platform="python",
            environment=params.get("environment", "production"),
            release=params.get("release", "1.0.0"),
            dist=params.get("dist", ""),
            sdk={"name": "rabai-autoclick", "version": "1.0.0"},
            logger="rabai.autoclick",
            level=SeverityLevel.ERROR,
            culprit=f"{exc_info.module}.{exc_info.type}" if exc_info.module else exc_info.type,
            message=str(exception),
            exception=exc_info,
            stacktrace=stack_frames,
            breadcrumbs=params.get("breadcrumbs", []),
            tags={
                "category": category.value,
                "handled": "false"
            },
            user=params.get("user", {}),
            request=params.get("request", {}),
            extra=params.get("extra", {}),
            fingerprint=fingerprint,
            modules=params.get("modules", {})
        )

    def build_message_event(self, message: str, level: SeverityLevel = SeverityLevel.ERROR,
                            params: Optional[Dict[str, Any]] = None) -> SentryEvent:
        """Build Sentry event from message."""
        params = params or {}
        event_id = uuid.uuid4().hex
        category = self._classifier.classify_from_message(message)

        return SentryEvent(
            event_id=event_id,
            timestamp=time.time(),
            platform="python",
            environment=params.get("environment", "production"),
            release=params.get("release", "1.0.0"),
            dist=params.get("dist", ""),
            sdk={"name": "rabai-autoclick", "version": "1.0.0"},
            logger=params.get("logger", "rabai.autoclick"),
            level=level,
            culprit=params.get("culprit", message[:100]),
            message=message,
            breadcrumbs=params.get("breadcrumbs", []),
            tags={
                "category": category.value
            },
            user=params.get("user", {}),
            extra=params.get("extra", {})
        )

    def serialize_event(self, event: SentryEvent) -> Dict[str, Any]:
        """Serialize Sentry event to dict."""
        def serialize_stacktrace(frames: List[StackFrame]) -> Dict[str, Any]:
            return {
                "frames": [
                    {
                        "filename": f.filename,
                        "function": f.function,
                        "lineno": f.lineno,
                        "colno": f.colno,
                        "abs_path": f.abs_path,
                        "context_line": f.context_line,
                        "pre_context": f.pre_context,
                        "post_context": f.post_context
                    }
                    for f in frames
                ]
            }

        result = {
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "platform": event.platform,
            "environment": event.environment,
            "release": event.release,
            "dist": event.dist,
            "sdk": event.sdk,
            "logger": event.logger,
            "level": event.level.value,
            "culprit": event.culprit,
            "message": event.message,
            "tags": event.tags,
            "user": event.user,
            "request": event.request,
            "extra": event.extra,
            "fingerprint": event.fingerprint,
            "modules": event.modules
        }

        if event.exception:
            result["exception"] = {
                "values": [
                    {
                        "type": event.exception.type,
                        "value": event.exception.value,
                        "module": event.exception.module,
                        "stacktrace": serialize_stacktrace(event.exception.stacktrace)
                    }
                ]
            }

        if event.stacktrace:
            result["stacktrace"] = serialize_stacktrace(event.stacktrace)

        if event.breadcrumbs:
            result["breadcrumbs"] = {
                "values": [
                    {
                        "timestamp": b.timestamp,
                        "category": b.category,
                        "message": b.message,
                        "level": b.level.value,
                        "type": b.type,
                        "data": b.data
                    }
                    for b in event.breadcrumbs
                ]
            }

        return result


class SentryAction:
    """Sentry error tracking and monitoring action.

    Example:
        action = SentryAction(
            dsn="https://key@sentry.io/project",
            environment="production"
        )

        action.capture_exception(exc)
        action.capture_message("Failed to process", level="warning")

        breadcrumb = action.add_breadcrumb("User clicked button")
        action.capture_exception(exc, breadcrumbs=[breadcrumb])
    """

    def __init__(self, dsn: Optional[str] = None,
                 environment: str = "production",
                 release: str = "1.0.0"):
        """Initialize Sentry action.

        Args:
            dsn: Sentry DSN URL
            environment: Environment name
            release: Release version
        """
        self._dsn = dsn
        self._environment = environment
        self._release = release
        self._builder = SentryEventBuilder()
        self._breadcrumbs: List[Breadcrumb] = []
        self._event_count = 0

    def add_breadcrumb(self, message: str, category: str = "default",
                       level: SeverityLevel = SeverityLevel.INFO,
                       data: Optional[Dict[str, Any]] = None) -> Breadcrumb:
        """Add breadcrumb to current context.

        Args:
            message: Breadcrumb message
            category: Breadcrumb category
            level: Severity level
            data: Additional data

        Returns:
            Created breadcrumb
        """
        crumb = Breadcrumb(
            timestamp=time.time(),
            category=category,
            message=message,
            level=level,
            data=data or {}
        )
        self._breadcrumbs.append(crumb)
        return crumb

    def clear_breadcrumbs(self) -> None:
        """Clear all breadcrumbs."""
        self._breadcrumbs.clear()

    def capture_exception(self, exception: Exception,
                          params: Optional[Dict[str, Any]] = None) -> ErrorReport:
        """Capture exception and send to Sentry.

        Args:
            exception: Exception to capture
            params: Additional parameters

        Returns:
            ErrorReport with capture status
        """
        start = time.time()
        params = params or {}

        params["environment"] = params.get("environment", self._environment)
        params["release"] = params.get("release", self._release)
        params["breadcrumbs"] = params.get("breadcrumbs", self._breadcrumbs.copy())

        try:
            event = self._builder.build_exception_event(exception, params)
            event_dict = self._builder.serialize_event(event)

            sent = self._send_event(event_dict)

            self._event_count += 1

            return ErrorReport(
                success=True,
                event_id=event.event_id,
                sent=sent,
                error_category=self._builder._classifier.classify(exception),
                severity=event.level,
                duration_ms=(time.time() - start) * 1000,
                message=f"Captured exception: {type(exception).__name__}"
            )

        except Exception as e:
            return ErrorReport(
                success=False,
                event_id=None,
                sent=False,
                error_category=ErrorCategory.UNKNOWN,
                severity=SeverityLevel.ERROR,
                duration_ms=(time.time() - start) * 1000,
                message="",
                errors=[str(e)]
            )

    def capture_message(self, message: str,
                        level: SeverityLevel = SeverityLevel.ERROR,
                        params: Optional[Dict[str, Any]] = None) -> ErrorReport:
        """Capture message and send to Sentry.

        Args:
            message: Message to capture
            level: Severity level
            params: Additional parameters

        Returns:
            ErrorReport with capture status
        """
        start = time.time()
        params = params or {}

        params["environment"] = params.get("environment", self._environment)
        params["release"] = params.get("release", self._release)
        params["breadcrumbs"] = params.get("breadcrumbs", self._breadcrumbs.copy())

        try:
            event = self._builder.build_message_event(message, level, params)
            event_dict = self._builder.serialize_event(event)

            sent = self._send_event(event_dict)

            self._event_count += 1

            return ErrorReport(
                success=True,
                event_id=event.event_id,
                sent=sent,
                error_category=ErrorCategory.INTERNAL,
                severity=level,
                duration_ms=(time.time() - start) * 1000,
                message=f"Captured message: {message[:50]}..."
            )

        except Exception as e:
            return ErrorReport(
                success=False,
                event_id=None,
                sent=False,
                error_category=ErrorCategory.UNKNOWN,
                severity=level,
                duration_ms=(time.time() - start) * 1000,
                message="",
                errors=[str(e)]
            )

    def _send_event(self, event: Dict[str, Any]) -> bool:
        """Send event to Sentry (simulated).

        In production, this would send to Sentry API.
        """
        return True

    def get_event_count(self) -> int:
        """Get number of events captured."""
        return self._event_count

    def get_breadcrumbs(self) -> List[Breadcrumb]:
        """Get current breadcrumbs."""
        return self._breadcrumbs.copy()


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Sentry error tracking action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "capture_exception", "capture_message", "add_breadcrumb",
                         "clear_breadcrumbs", "get_stats"
            - exception: Exception object (for capture_exception)
            - message: Message string (for capture_message)
            - level: Severity level (fatal, error, warning, info, debug)
            - dsn: Sentry DSN URL
            - environment: Environment name
            - release: Release version
            - category: Breadcrumb category (for add_breadcrumb)
            - data: Additional data (for add_breadcrumb)

    Returns:
        Dict with success, event_id, error_category, severity, message
    """
    operation = params.get("operation", "capture_exception")

    try:
        dsn = params.get("dsn")
        environment = params.get("environment", "production")
        release = params.get("release", "1.0.0")

        action = SentryAction(dsn=dsn, environment=environment, release=release)

        level_str = params.get("level", "error")
        try:
            level = SeverityLevel(level_str)
        except ValueError:
            level = SeverityLevel.ERROR

        if operation == "capture_exception":
            exception = params.get("exception")
            if exception is None:
                return {"success": False, "message": "exception required"}

            result = action.capture_exception(exception, params)
            return {
                "success": result.success,
                "event_id": result.event_id,
                "sent": result.sent,
                "error_category": result.error_category.value,
                "severity": result.severity.value,
                "duration_ms": result.duration_ms,
                "errors": result.errors,
                "message": result.message
            }

        elif operation == "capture_message":
            message = params.get("message", "")
            if not message:
                return {"success": False, "message": "message required"}

            result = action.capture_message(message, level, params)
            return {
                "success": result.success,
                "event_id": result.event_id,
                "sent": result.sent,
                "severity": result.severity.value,
                "duration_ms": result.duration_ms,
                "errors": result.errors,
                "message": result.message
            }

        elif operation == "add_breadcrumb":
            message = params.get("message", "")
            category = params.get("category", "default")
            data = params.get("data", {})
            crumb = action.add_breadcrumb(message, category, level, data)
            return {
                "success": True,
                "timestamp": crumb.timestamp,
                "message": f"Breadcrumb added: {message[:30]}..."
            }

        elif operation == "clear_breadcrumbs":
            action.clear_breadcrumbs()
            return {"success": True, "message": "Breadcrumbs cleared"}

        elif operation == "get_stats":
            return {
                "success": True,
                "event_count": action.get_event_count(),
                "breadcrumb_count": len(action.get_breadcrumbs()),
                "message": "Stats retrieved"
            }

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Sentry error: {str(e)}"}
