"""
OpenTelemetry Observability Utilities.

Helpers for instrumenting Python applications with OpenTelemetry,
managing traces, metrics, and logs, and exporting to various
backends (Jaeger, Zipkin, OTLP collectors, Prometheus).

Author: rabai_autoclick
License: MIT
"""

import os
import time
import logging
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "unknown-service")
OTEL_EXPORTER_OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
OTEL_RESOURCE_ATTRIBUTES = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")


# --------------------------------------------------------------------------- #
# Tracer Setup (requires opentelemetry-api installed)
# --------------------------------------------------------------------------- #

def get_tracer(name: str = __name__) -> Any:
    """
    Return a tracer instance for the given module name.

    This is a lazy initialization that defers the actual tracer
    creation until the first call, allowing the module to be
    imported without requiring all OTel dependencies upfront.

    Returns:
        An opentelemetry Tracer, or a NoOpTracer if OTel is not installed.
    """
    try:
        from opentelemetry import trace
        return trace.get_tracer(name)
    except ImportError:
        logging.warning("opentelemetry-api not installed, using no-op tracer")
        return NoOpTracer()


class NoOpSpan:
    """No-operation span that drops all data."""
    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_attributes(self, attrs: dict[str, Any]) -> None:
        pass

    def add_event(self, name: str, attributes: Optional[dict[str, Any]] = None) -> None:
        pass

    def record_exception(self, exc: Exception, attributes: Optional[dict[str, Any]] = None) -> None:
        pass

    def set_status(self, status: str, description: str = "") -> None:
        pass

    def end(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.end()


class NoOpTracer:
    """No-operation tracer that returns no-op spans."""
    def start_as_current_span(self, name: str, **kwargs) -> NoOpSpan:
        return NoOpSpan()

    def start_span(self, name: str, **kwargs) -> NoOpSpan:
        return NoOpSpan()


# --------------------------------------------------------------------------- #
# Context Managers
# --------------------------------------------------------------------------- #

def trace(name: str, attributes: Optional[dict[str, Any]] = None):
    """
    Context manager for creating a span around a block of code.

    Usage:
        with trace("my-operation", {"key": "value"}):
            do_work()
    """
    tracer = get_tracer()
    return tracer.start_as_current_span(name, attributes=attributes)


def timed(name: Optional[str] = None):
    """
    Decorator to automatically time a function's execution.

    Usage:
        @timed("my-function")
        def my_function():
            ...
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            span_name = name or f"{func.__module__}.{func.__qualname__}"
            with trace(span_name):
                start = time.monotonic()
                try:
                    return func(*args, **kwargs)
                finally:
                    pass
        return wrapper
    return decorator


# --------------------------------------------------------------------------- #
# Span Helpers
# --------------------------------------------------------------------------- #

def add_span_attributes(attributes: dict[str, Any]) -> None:
    """Add attributes to the current active span."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.set_attributes(attributes)
    except Exception:
        pass


def record_exception(exc: Exception, attributes: Optional[dict[str, Any]] = None) -> None:
    """Record an exception on the current active span."""
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        if span:
            span.record_exception(exc)
            if attributes:
                span.set_attributes(attributes)
    except Exception:
        pass


def set_span_status(error: bool = False, description: str = "") -> None:
    """Set the status of the current active span."""
    try:
        from opentelemetry import trace
        from opentelemetry.trace import Status, StatusCode
        span = trace.get_current_span()
        if span:
            code = StatusCode.ERROR if error else StatusCode.OK
            span.set_status(Status(code, description))
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

_meter_provider = None
_meter = None


def get_meter(name: str = __name__) -> Any:
    """Return a meter for creating instruments."""
    global _meter
    if _meter is not None:
        return _meter
    try:
        from opentelemetry import metrics
        global _meter_provider
        # In a real app, initialize MeterProvider with an exporter here
        _meter = metrics.get_meter(name)
        return _meter
    except ImportError:
        logging.warning("opentelemetry-metrics not installed")
        return NoOpMeter()


class NoOpMeter:
    """No-op meter."""
    def create_counter(self, name: str, **kwargs):
        return NoOpCounter()

    def create_histogram(self, name: str, **kwargs):
        return NoOpHistogram()

    def create_up_down_counter(self, name: str, **kwargs):
        return NoOpUpDownCounter()


class NoOpCounter:
    def add(self, amount: float, attributes: Optional[dict[str, Any]] = None) -> None:
        pass


class NoOpHistogram:
    def record(self, amount: float, attributes: Optional[dict[str, Any]] = None) -> None:
        pass


class NoOpUpDownCounter:
    def add(self, amount: float, attributes: Optional[dict[str, Any]] = None) -> None:
        pass


# --------------------------------------------------------------------------- #
# OTLP Exporter Setup
# --------------------------------------------------------------------------- #

def setup_otlp_exporter(
    service_name: Optional[str] = None,
    endpoint: Optional[str] = None,
    insecure: bool = True,
) -> None:
    """
    Initialize the global OTLP span exporter.

    Args:
        service_name: Name of this service.
        endpoint: OTLP collector gRPC endpoint.
        insecure: Use plaintext (no TLS) for the collector connection.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        service = service_name or OTEL_SERVICE_NAME
        resource = Resource.create({"service.name": service})
        provider = TracerProvider(resource=resource)

        exporter = OTLPSpanExporter(
            endpoint=endpoint or OTEL_EXPORTER_OTLP_ENDPOINT,
            insecure=insecure,
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
    except ImportError as exc:
        logging.warning(f"OpenTelemetry dependencies not installed: {exc}")


def setup_batched_logging() -> None:
    """Configure OpenTelemetry logging to batch exports."""
    try:
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        logging.getLogger("opentelemetry").setLevel(logging.WARNING)
    except ImportError:
        pass


# --------------------------------------------------------------------------- #
# Resource Attributes
# --------------------------------------------------------------------------- #

def get_resource_attributes() -> dict[str, str]:
    """
    Parse OTEL_RESOURCE_ATTRIBUTES (key=value,key=value) into a dict.

    Returns:
        Dict of resource attribute key-value pairs.
    """
    attrs: dict[str, str] = {"service.name": OTEL_SERVICE_NAME}
    if not OTEL_RESOURCE_ATTRIBUTES:
        return attrs
    for pair in OTEL_RESOURCE_ATTRIBUTES.split(","):
        if "=" in pair:
            k, v = pair.split("=", 1)
            attrs[k.strip()] = v.strip()
    return attrs


# --------------------------------------------------------------------------- #
# Logging Correlation
# --------------------------------------------------------------------------- #

def inject_trace_context(logger: Optional[logging.Logger] = None) -> dict[str, str]:
    """
    Inject current trace/span IDs into a dict for log correlation.

    This allows structured logs to include trace context for
    correlation with distributed traces.

    Returns:
        Dict with trace_id, span_id, trace_flags.
    """
    result: dict[str, str] = {}
    try:
        from opentelemetry import trace
        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            result["trace_id"] = format(ctx.trace_id, "032x")
            result["span_id"] = format(ctx.span_id, "016x")
            result["trace_flags"] = format(ctx.trace_flags, "02x")
    except Exception:
        pass
    return result


class LogRecordFactory(logging.Logger):
    """Custom log record factory that injects OTel trace context."""
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
        record = super().makeRecord(name, level, fn, lno, msg, args, exc_info, func, extra)
        ctx = inject_trace_context()
        for k, v in ctx.items():
            setattr(record, k, v)
        return record
