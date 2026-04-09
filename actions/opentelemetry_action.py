"""OpenTelemetry action module for RabAI AutoClick.

Provides OpenTelemetry tracing and metrics operations:
- OTelTracer: Create and manage distributed traces
- OTelMeter: Record metrics (counters, gauges, histograms)
- OTelSpan: Span creation and annotation
- OTelExporter: Export telemetry data
- OTelContext: Context propagation utilities
"""

from __future__ import annotations

import time
import sys
import os
import json
import threading
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from contextvars import ContextVar
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False


_tracer: Optional[Any] = None
_meter: Optional[Any] = None
_initialized: bool = False
_init_lock = threading.Lock()


class OTelInitAction(BaseAction):
    """Initialize OpenTelemetry SDK."""
    action_type = "otel_init"
    display_name = "OpenTelemetry初始化"
    description = "初始化OpenTelemetry追踪和指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not OTEL_AVAILABLE:
            return ActionResult(success=False, message="opentelemetry not installed: pip install opentelemetry-api opentelemetry-sdk")

        global _tracer, _meter, _initialized

        try:
            service_name = params.get("service_name", "rabai_autoclick")
            service_version = params.get("service_version", "1.0.0")
            export_to_console = params.get("export_to_console", True)
            exporter_endpoint = params.get("exporter_endpoint", None)

            with _init_lock:
                if _initialized:
                    return ActionResult(success=True, message="Already initialized", data={"initialized": True})

                resource = Resource.create({
                    SERVICE_NAME: service_name,
                    "service.version": service_version,
                    "deployment.environment": params.get("environment", "development"),
                })

                tracer_provider = TracerProvider(resource=resource)
                if export_to_console:
                    span_processor = BatchSpanProcessor(ConsoleSpanExporter())
                    tracer_provider.add_span_processor(span_processor)

                trace.set_tracer_provider(tracer_provider)
                _tracer = trace.get_tracer(__name__)

                metric_reader = PeriodicExportingMetricReader(
                    ConsoleMetricExporter() if export_to_console else None,
                    export_interval_millis=params.get("export_interval_ms", 60000)
                )
                meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
                metrics.set_meter_provider(meter_provider)
                _meter = metrics.get_meter(__name__)

                _initialized = True

            return ActionResult(
                success=True,
                message=f"OpenTelemetry initialized for {service_name}",
                data={"service_name": service_name, "initialized": True}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Init error: {str(e)}")


class OTelTracerAction(BaseAction):
    """Create and manage distributed traces."""
    action_type = "otel_tracer"
    display_name = "OpenTelemetry追踪"
    description = "创建和管理分布式追踪"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not OTEL_AVAILABLE:
            return ActionResult(success=False, message="opentelemetry not installed")

        global _tracer
        if not _initialized:
            init_action = OTelInitAction()
            init_result = init_action.execute(context, {"service_name": params.get("service_name", "rabai")})
            if not init_result.success:
                return init_result

        try:
            operation = params.get("operation", "")
            attributes = params.get("attributes", {})
            parent_span = params.get("parent_span", None)
            start_time = params.get("start_time", None)
            end_on_execute = params.get("end_on_execute", False)

            if not operation:
                return ActionResult(success=False, message="operation name is required")

            span_name = params.get("span_name", operation)
            kind = params.get("kind", "INTERNAL")

            span_kind_map = {
                "INTERNAL": trace.SpanKind.INTERNAL,
                "SERVER": trace.SpanKind.SERVER,
                "CLIENT": trace.SpanKind.CLIENT,
                "PRODUCER": trace.SpanKind.PRODUCER,
                "CONSUMER": trace.SpanKind.CONSUMER,
            }
            span_kind = span_kind_map.get(kind, trace.SpanKind.INTERNAL)

            with _tracer.start_as_current_span(
                span_name,
                kind=span_kind,
                start_time=start_time,
            ) as span:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value) if value is not None else "")

                if end_on_execute:
                    span.set_status(Status(StatusCode.OK))
                    span.end()
                    return ActionResult(success=True, message=f"Traced: {operation}")

                return ActionResult(
                    success=True,
                    message=f"Span created: {operation}",
                    data={"operation": operation, "span_name": span_name, "span_id": str(span.get_span_context().span_id)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Tracing error: {str(e)}")


class OTelSpanAction(BaseAction):
    """Create and annotate individual spans."""
    action_type = "otel_span"
    display_name = "OpenTelemetry Span"
    description = "创建和注解Span"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not OTEL_AVAILABLE:
            return ActionResult(success=False, message="opentelemetry not installed")

        global _tracer
        if not _tracer:
            return ActionResult(success=False, message="Tracer not initialized. Call otel_init first.")

        try:
            span_name = params.get("span_name", "unnamed-span")
            event_name = params.get("event_name", None)
            attributes = params.get("attributes", {})
            set_status = params.get("status", None)
            add_event = params.get("add_event", False)
            record_exception = params.get("record_exception", None)

            with _tracer.start_as_current_span(span_name) as span:
                for key, value in attributes.items():
                    span.set_attribute(key, str(value) if value is not None else "")

                if event_name:
                    span.add_event(event_name, attributes=attributes)

                if set_status:
                    status_map = {"ok": StatusCode.OK, "error": StatusCode.ERROR}
                    span.set_status(Status(status_map.get(set_status, StatusCode.OK)))

                if record_exception and isinstance(record_exception, dict):
                    exc_type = record_exception.get("type", "Exception")
                    exc_msg = record_exception.get("message", "")
                    span.record_exception(Exception(f"{exc_type}: {exc_msg}"))

                span_id = str(span.get_span_context().span_id)
                trace_id = str(span.get_span_context().trace_id)

                return ActionResult(
                    success=True,
                    message=f"Span: {span_name}",
                    data={"span_id": span_id, "trace_id": trace_id, "span_name": span_name}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Span error: {str(e)}")


class OTelMetricsAction(BaseAction):
    """Record metrics using OpenTelemetry."""
    action_type = "otel_metrics"
    display_name = "OpenTelemetry指标"
    description = "记录OpenTelemetry指标数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not OTEL_AVAILABLE:
            return ActionResult(success=False, message="opentelemetry not installed")

        global _meter
        if not _meter:
            return ActionResult(success=False, message="Meter not initialized. Call otel_init first.")

        try:
            metric_name = params.get("metric_name", "")
            metric_type = params.get("metric_type", "counter")
            value = params.get("value", 1)
            attributes = params.get("attributes", {})

            if not metric_name:
                return ActionResult(success=False, message="metric_name is required")

            metric_types = {
                "counter": lambda: _meter.create_counter(metric_name),
                "histogram": lambda: _meter.create_histogram(metric_name),
                "gauge": lambda: _meter.create_observable_gauge(
                    metric_name,
                    callbacks=[lambda options: [metrics.Observation(value, attributes)]],
                ),
                "up_down_counter": lambda: _meter.create_up_down_counter(metric_name),
            }

            if metric_type not in metric_types:
                return ActionResult(success=False, message=f"Invalid type. Use: {list(metric_types.keys())}")

            instrument = metric_types[metric_type]()

            if metric_type == "counter":
                instrument.add(value, attributes)
            elif metric_type == "histogram":
                instrument.record(value, attributes)
            elif metric_type == "up_down_counter":
                instrument.add(value, attributes)

            return ActionResult(
                success=True,
                message=f"Metric recorded: {metric_name}={value}",
                data={"metric_name": metric_name, "metric_type": metric_type, "value": value, "attributes": attributes}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics error: {str(e)}")


class OTelContextPropAction(BaseAction):
    """Context propagation for distributed tracing."""
    action_type = "otel_context_prop"
    display_name = "OpenTelemetry上下文传播"
    description = "分布式追踪上下文传播"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        if not OTEL_AVAILABLE:
            return ActionResult(success=False, message="opentelemetry not installed")

        try:
            operation = params.get("operation", "inject")
            carrier = params.get("carrier", {})
            trace_context = params.get("trace_context", {})

            propagator = TraceContextTextMapPropagator()

            if operation == "inject":
                with trace.get_tracer(__name__).start_as_current_span("context_inject") as span:
                    carrier.clear()
                    propagator.inject(carrier)
                    return ActionResult(
                        success=True,
                        message="Context injected into carrier",
                        data={"carrier": carrier, "trace_id": str(span.get_span_context().trace_id)}
                    )

            elif operation == "extract":
                try:
                    ctx = propagator.extract(carrier=trace_context)
                    with trace.use_context(ctx):
                        return ActionResult(
                            success=True,
                            message="Context extracted from carrier",
                            data={"carrier": trace_context}
                        )
                except Exception:
                    return ActionResult(success=True, message="Using default context", data={})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Context propagation error: {str(e)}")
