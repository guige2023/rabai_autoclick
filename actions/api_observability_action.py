"""API observability action module for RabAI AutoClick.

Provides request tracing, metrics collection, and structured logging
for API operations. Supports OpenTelemetry-style spans.
"""

import time
import json
import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from core.base_action import BaseAction, ActionResult


class ApiTracingAction(BaseAction):
    """Trace API requests with timing and metadata.
    
    Generates trace IDs and spans for distributed tracing.
    Captures request/response details and timing breakdown.
    """
    action_type = "api_tracing"
    display_name = "API追踪"
    description = "追踪API请求的时序和元数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute traced API request.
        
        Args:
            context: Execution context.
            params: Dict with keys: url, method, headers, body,
                   trace_id, parent_span_id, tags.
        
        Returns:
            ActionResult with trace context and response data.
        """
        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        headers = params.get("headers", {})
        body = params.get("body")
        trace_id = params.get("trace_id") or uuid.uuid4().hex[:16]
        parent_span_id = params.get("parent_span_id")
        tags = params.get("tags", {})
        
        if not url:
            return ActionResult(success=False, message="URL is required")
        
        start_time = time.time()
        dns_time = 0
        connect_time = 0
        tls_time = 0
        send_time = 0
        wait_time = 0
        receive_time = 0
        
        span_id = uuid.uuid4().hex[:8]
        
        trace_context = {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "start_time": datetime.utcnow().isoformat() + "Z"
        }
        
        try:
            import urllib.request
            
            req_headers = {str(k): str(v) for k, v in headers.items()}
            req_headers["X-Trace-ID"] = trace_id
            req_headers["X-Span-ID"] = span_id
            
            request_body = None
            if body:
                if isinstance(body, dict):
                    request_body = json.dumps(body).encode()
                elif isinstance(body, str):
                    request_body = body.encode()
                elif isinstance(body, bytes):
                    request_body = body
                
                req_headers["Content-Type"] = req_headers.get("Content-Type", "application/json")
            
            send_time = time.time()
            req = urllib.request.Request(url, data=request_body, headers=req_headers, method=method)
            
            wait_start = time.time()
            with urllib.request.urlopen(req, timeout=30) as response:
                wait_time = time.time() - wait_start
                
                receive_start = time.time()
                response_body = response.read()
                response_headers = dict(response.headers)
                status_code = response.status
                receive_time = time.time() - receive_start
            
            total_time = time.time() - start_time
            
            trace_result = {
                "trace_context": trace_context,
                "request": {
                    "url": url,
                    "method": method,
                    "headers": req_headers
                },
                "response": {
                    "status_code": status_code,
                    "headers": response_headers
                },
                "timing": {
                    "total_ms": round(total_time * 1000, 2),
                    "send_ms": round(send_time * 1000, 2),
                    "wait_ms": round(wait_time * 1000, 2),
                    "receive_ms": round(receive_time * 1000, 2)
                },
                "tags": tags
            }
            
            try:
                trace_result["response"]["body"] = json.loads(response_body.decode())
            except Exception:
                trace_result["response"]["body"] = response_body.decode("utf-8", errors="replace")
            
            return ActionResult(
                success=status_code < 400,
                message=f"Traced {method} {url} -> {status_code} ({total_time*1000:.1f}ms)",
                data=trace_result
            )
        except Exception as e:
            total_time = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Traced request failed: {e}",
                data={
                    "trace_context": trace_context,
                    "error": str(e),
                    "timing": {"total_ms": round(total_time * 1000, 2)}
                }
            )


class ApiMetricsAction(BaseAction):
    """Collect and aggregate API operation metrics.
    
    Tracks request counts, latencies, error rates, and
    size metrics. Supports histogram bucketing.
    """
    action_type = "api_metrics"
    display_name = "API指标"
    description = "收集和聚合API操作指标"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Record or retrieve API metrics.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation, metric_name, value,
                   labels, histogram_buckets, reset.
        
        Returns:
            ActionResult with metric values or confirmation.
        """
        operation = params.get("operation", "record")
        metric_name = params.get("metric_name", "")
        value = params.get("value")
        labels = params.get("labels", {})
        histogram_buckets = params.get("histogram_buckets", [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000])
        reset = params.get("reset", False)
        
        if not metric_name:
            return ActionResult(success=False, message="metric_name is required")
        
        try:
            metrics_store = getattr(context, "_metrics_store", {})
            
            if reset:
                metrics_store = {}
                setattr(context, "_metrics_store", metrics_store)
                return ActionResult(success=True, message=f"Metrics reset for {metric_name}")
            
            if metric_name not in metrics_store:
                metrics_store[metric_name] = {
                    "count": 0,
                    "sum": 0,
                    "values": [],
                    "labels": []
                }
            
            metric = metrics_store[metric_name]
            
            if operation == "record":
                if value is None:
                    return ActionResult(success=False, message="value is required for record operation")
                
                metric["count"] += 1
                metric["sum"] += float(value)
                metric["values"].append(float(value))
                metric["labels"].append(labels)
                
                histogram_buckets_default = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
                bucket_idx = 0
                for i, bucket in enumerate(histogram_buckets_default):
                    if float(value) <= bucket:
                        bucket_idx = i
                        break
                    bucket_idx = i + 1
                
                return ActionResult(
                    success=True,
                    message=f"Recorded {metric_name}={value}",
                    data={"recorded": True, "metric": metric_name}
                )
            
            elif operation == "get":
                count = metric["count"]
                avg = metric["sum"] / count if count > 0 else 0
                values = metric["values"]
                
                sorted_values = sorted(values)
                p50 = sorted_values[int(len(sorted_values) * 0.5)] if sorted_values else 0
                p95 = sorted_values[int(len(sorted_values) * 0.95)] if sorted_values else 0
                p99 = sorted_values[int(len(sorted_values) * 0.99)] if sorted_values else 0
                
                return ActionResult(
                    success=True,
                    message=f"Retrieved metrics for {metric_name}",
                    data={
                        "metric": metric_name,
                        "count": count,
                        "sum": metric["sum"],
                        "avg": avg,
                        "min": min(values) if values else 0,
                        "max": max(values) if values else 0,
                        "p50": p50,
                        "p95": p95,
                        "p99": p99
                    }
                )
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Metrics operation failed: {e}")


class ApiStructuredLogAction(BaseAction):
    """Emit structured JSON logs for API operations.
    
    Creates machine-readable log entries with severity levels,
    timestamps, and contextual metadata.
    """
    action_type = "api_structured_log"
    display_name = "API结构化日志"
    description = "生成API操作的JSON结构化日志"
    VALID_LEVELS = ["debug", "info", "warn", "error", "critical"]
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Emit structured log entry.
        
        Args:
            context: Execution context.
            params: Dict with keys: level, message, event, metadata,
                   trace_id, span_id, timestamp.
        
        Returns:
            ActionResult confirming log emission.
        """
        level = params.get("level", "info").lower()
        message = params.get("message", "")
        event = params.get("event")
        metadata = params.get("metadata", {})
        trace_id = params.get("trace_id")
        span_id = params.get("span_id")
        timestamp = params.get("timestamp") or datetime.utcnow().isoformat() + "Z"
        
        valid, msg = self.validate_in(level, self.VALID_LEVELS, "level")
        if not valid:
            return ActionResult(success=False, message=msg)
        
        if not message and not event:
            return ActionResult(success=False, message="Either message or event is required")
        
        log_entry = {
            "timestamp": timestamp,
            "level": level.upper(),
            "message": message,
            "event": event,
            "metadata": metadata,
            "trace_id": trace_id,
            "span_id": span_id,
            "service": "rabai_autoclick"
        }
        
        log_line = json.dumps(log_entry, default=str)
        
        import sys
        log_target = sys.stderr if level in ("warn", "error", "critical") else sys.stdout
        print(log_line, file=log_target)
        
        return ActionResult(
            success=True,
            message=f"Logged [{level.upper()}] {event or message}",
            data={"log_entry": log_entry}
        )
