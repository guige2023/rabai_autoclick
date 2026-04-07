"""Observability utilities: structured logging, context propagation, and health checks."""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "ObsContext",
    "StructuredLogger",
    "HealthCheck",
    "HealthCheckRegistry",
    "ServiceMesh",
]


_trace_id: ContextVar[str] = ContextVar("trace_id", default="")
_span_id: ContextVar[str] = ContextVar("span_id", default="")


@dataclass
class ObsContext:
    """Observability context for tracing."""

    trace_id: str
    span_id: str
    user_id: str = ""
    session_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def current(cls) -> "ObsContext":
        """Get current context from context vars."""
        return cls(
            trace_id=_trace_id.get(),
            span_id=_span_id.get(),
        )

    def attach(self) -> None:
        """Attach this context to the current execution."""
        _trace_id.set(self.trace_id)
        _span_id.set(self.span_id)

    @classmethod
    def new(cls) -> "ObsContext":
        """Create a new context with generated IDs."""
        import uuid
        ctx = cls(
            trace_id=uuid.uuid4().hex[:16],
            span_id=uuid.uuid4().hex[:8],
        )
        ctx.attach()
        return ctx

    def child(self, name: str) -> "ObsContext":
        """Create a child context."""
        import uuid
        return ObsContext(
            trace_id=self.trace_id,
            span_id=uuid.uuid4().hex[:8],
            user_id=self.user_id,
            session_id=self.session_id,
            metadata={**self.metadata, "parent_span": self.span_id, "event": name},
        )


class StructuredLogger:
    """Structured JSON logger for observability."""

    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
    ) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)

    def _build_record(
        self,
        level: str,
        message: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Build a structured log record."""
        ctx = ObsContext.current()
        record: dict[str, Any] = {
            "timestamp": time.time(),
            "level": level,
            "message": message,
            "logger": self.logger.name,
            "trace_id": ctx.trace_id,
            "span_id": ctx.span_id,
        }
        if ctx.user_id:
            record["user_id"] = ctx.user_id
        if ctx.session_id:
            record["session_id"] = ctx.session_id
        record.update(kwargs)
        return record

    def debug(self, message: str, **kwargs: Any) -> None:
        self.logger.debug(json.dumps(self._build_record("DEBUG", message, **kwargs)))

    def info(self, message: str, **kwargs: Any) -> None:
        self.logger.info(json.dumps(self._build_record("INFO", message, **kwargs)))

    def warning(self, message: str, **kwargs: Any) -> None:
        self.logger.warning(json.dumps(self._build_record("WARNING", message, **kwargs)))

    def error(self, message: str, **kwargs: Any) -> None:
        self.logger.error(json.dumps(self._build_record("ERROR", message, **kwargs)))

    def critical(self, message: str, **kwargs: Any) -> None:
        self.logger.critical(json.dumps(self._build_record("CRITICAL", message, **kwargs)))

    def metric(
        self,
        name: str,
        value: float,
        unit: str = "",
        tags: dict[str, str] | None = None,
    ) -> None:
        """Log a metric observation."""
        metric_record = {
            "timestamp": time.time(),
            "type": "metric",
            "name": name,
            "value": value,
            "unit": unit,
            "tags": tags or {},
        }
        self.logger.info(json.dumps(metric_record))


@dataclass
class HealthCheck:
    """A single health check."""

    name: str
    check_fn: callable
    timeout: float = 5.0
    critical: bool = True


class HealthCheckRegistry:
    """Registry and runner for health checks."""

    def __init__(self) -> None:
        self._checks: list[HealthCheck] = []
        self._lock = threading.Lock()

    def register(
        self,
        name: str,
        check_fn: callable,
        critical: bool = True,
        timeout: float = 5.0,
    ) -> None:
        with self._lock:
            self._checks.append(HealthCheck(name, check_fn, timeout, critical))

    def run(self) -> dict[str, Any]:
        """Run all health checks and return results."""
        results: dict[str, Any] = {
            "status": "healthy",
            "timestamp": time.time(),
            "checks": {},
        }
        any_failed = False
        any_critical_failed = False

        for check in self._checks:
            try:
                start = time.time()
                result = check.check_fn()
                elapsed = time.time() - start
                status = "pass" if result else "fail"
            except Exception as e:
                elapsed = time.time() - start
                result = False
                status = "error"
                result_error = str(e)

            results["checks"][check.name] = {
                "status": status,
                "elapsed_ms": elapsed * 1000,
            }
            if status != "pass":
                any_failed = True
                if check.critical:
                    any_critical_failed = True

        if any_critical_failed:
            results["status"] = "unhealthy"
        elif any_failed:
            results["status"] = "degraded"
        else:
            results["status"] = "healthy"

        return results


class ServiceMesh:
    """Service mesh utilities for distributed tracing and service discovery."""

    def __init__(self) -> None:
        self._services: dict[str, dict[str, Any]] = {}

    def register(
        self,
        name: str,
        host: str,
        port: int,
        tags: dict[str, str] | None = None,
    ) -> None:
        self._services[name] = {
            "host": host,
            "port": port,
            "tags": tags or {},
            "registered_at": time.time(),
            "healthy": True,
        }

    def get_endpoint(self, name: str) -> str | None:
        """Get service endpoint URL."""
        svc = self._services.get(name)
        if svc and svc.get("healthy"):
            return f"http://{svc['host']}:{svc['port']}"
        return None

    def set_healthy(self, name: str, healthy: bool) -> None:
        if name in self._services:
            self._services[name]["healthy"] = healthy

    def list_services(self) -> dict[str, dict[str, Any]]:
        return dict(self._services)
