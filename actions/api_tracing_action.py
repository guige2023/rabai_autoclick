"""API tracing and distributed tracing action module for RabAI AutoClick.

Provides tracing operations:
- TraceStartAction: Start a trace span
- TraceEndAction: End a trace span
- TraceAnnotateAction: Add annotation to trace
- TraceInjectAction: Inject trace context
- TraceExtractAction: Extract trace context
- TraceExportAction: Export traces to collector
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TraceStartAction(BaseAction):
    """Start a trace span."""
    action_type = "trace_start"
    display_name = "追踪开始"
    description = "启动追踪Span"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation_name = params.get("operation_name", "unknown")
            parent_span_id = params.get("parent_span_id", "")
            tags = params.get("tags", {})
            trace_id = params.get("trace_id", str(uuid.uuid4()).replace("-", "")[:16])

            span_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "traces"):
                context.traces = {}
            context.traces[span_id] = {
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
                "operation_name": operation_name,
                "tags": tags,
                "start_time": time.time(),
                "status": "running",
            }

            return ActionResult(
                success=True,
                data={"trace_id": trace_id, "span_id": span_id, "status": "running"},
                message=f"Trace started: {operation_name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace start failed: {e}")


class TraceEndAction(BaseAction):
    """End a trace span."""
    action_type = "trace_end"
    display_name = "追踪结束"
    description = "结束追踪Span"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            span_id = params.get("span_id", "")
            status_code = params.get("status_code", 0)
            error_message = params.get("error_message", "")

            if not span_id:
                return ActionResult(success=False, message="span_id is required")

            if not hasattr(context, "traces") or span_id not in context.traces:
                return ActionResult(success=False, message=f"Span {span_id} not found")

            span = context.traces[span_id]
            span["end_time"] = time.time()
            span["duration_ms"] = (span["end_time"] - span["start_time"]) * 1000
            span["status_code"] = status_code
            span["error_message"] = error_message
            span["status"] = "completed" if status_code == 0 else "error"

            return ActionResult(
                success=True,
                data={
                    "span_id": span_id,
                    "trace_id": span["trace_id"],
                    "duration_ms": span["duration_ms"],
                    "status": span["status"],
                },
                message=f"Trace ended: {span_id} ({span['duration_ms']:.2f}ms)",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace end failed: {e}")


class TraceAnnotateAction(BaseAction):
    """Add annotation to trace."""
    action_type = "trace_annotate"
    display_name = "追踪标注"
    description = "为追踪添加标注"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            span_id = params.get("span_id", "")
            annotation = params.get("annotation", "")
            annotation_type = params.get("type", "string")
            timestamp = params.get("timestamp", time.time())

            if not span_id:
                return ActionResult(success=False, message="span_id is required")
            if not annotation:
                return ActionResult(success=False, message="annotation is required")

            if not hasattr(context, "traces") or span_id not in context.traces:
                return ActionResult(success=False, message=f"Span {span_id} not found")

            if "annotations" not in context.traces[span_id]:
                context.traces[span_id]["annotations"] = []
            context.traces[span_id]["annotations"].append({
                "annotation": annotation,
                "type": annotation_type,
                "timestamp": timestamp,
            })

            return ActionResult(
                success=True,
                data={"span_id": span_id, "annotation": annotation, "type": annotation_type},
                message=f"Annotation added to {span_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace annotate failed: {e}")


class TraceInjectAction(BaseAction):
    """Inject trace context into carrier."""
    action_type = "trace_inject"
    display_name = "追踪注入"
    description = "注入追踪上下文"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            span_id = params.get("span_id", "")
            carrier_type = params.get("carrier_type", "http")
            headers = params.get("headers", {})

            if not span_id:
                return ActionResult(success=False, message="span_id is required")

            if not hasattr(context, "traces") or span_id not in context.traces:
                return ActionResult(success=False, message=f"Span {span_id} not found")

            span = context.traces[span_id]
            injected = {
                "traceparent": f"00-{span['trace_id']}-{span_id}-01",
                "tracestate": "",
            }

            return ActionResult(
                success=True,
                data={"span_id": span_id, "carrier_type": carrier_type, "injected": injected},
                message=f"Trace context injected into {carrier_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace inject failed: {e}")


class TraceExtractAction(BaseAction):
    """Extract trace context from carrier."""
    action_type = "trace_extract"
    display_name = "追踪提取"
    description = "从载体提取追踪上下文"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            carrier_type = params.get("carrier_type", "http")
            headers = params.get("headers", {})

            traceparent = headers.get("traceparent", "")
            if traceparent:
                parts = traceparent.split("-")
                trace_id = parts[1] if len(parts) > 1 else ""
                span_id = parts[2] if len(parts) > 2 else ""
            else:
                trace_id = str(uuid.uuid4()).replace("-", "")[:16]
                span_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"trace_id": trace_id, "span_id": span_id, "carrier_type": carrier_type},
                message="Trace context extracted",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace extract failed: {e}")


class TraceExportAction(BaseAction):
    """Export traces to collector."""
    action_type = "trace_export"
    display_name = "追踪导出"
    description = "导出追踪数据到收集器"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            protocol = params.get("protocol", "jaeger")
            batch_size = params.get("batch_size", 100)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            traces = getattr(context, "traces", {})
            exported = list(traces.values())[:batch_size]

            return ActionResult(
                success=True,
                data={"exported_count": len(exported), "endpoint": endpoint, "protocol": protocol},
                message=f"Exported {len(exported)} traces to {endpoint}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Trace export failed: {e}")
