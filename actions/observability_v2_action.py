"""Observability v2 action module for RabAI AutoClick.

Provides enhanced observability including distributed tracing,
structured logging, and health check endpoints.
"""

import time
import json
import uuid
import traceback
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StructuredLoggerAction(BaseAction):
    """Structured logging with context propagation.
    
    Provides JSON-structured logging with automatic
    context enrichment and log levels.
    """
    action_type = "structured_logger"
    display_name = "结构化日志"
    description = "结构化日志记录"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Emit structured log.
        
        Args:
            context: Execution context.
            params: Dict with keys: level (debug|info|warn|error),
                   message, data, source, include_context.
        
        Returns:
            ActionResult with log status.
        """
        level = params.get('level', 'info')
        message = params.get('message', '')
        data = params.get('data', {})
        source = params.get('source', 'action')
        include_context = params.get('include_context', True)
        start_time = time.time()

        log_entry = {
            'timestamp': time.time(),
            'level': level.upper(),
            'message': message,
            'source': source,
            'trace_id': getattr(context, '_trace_id', None) or str(uuid.uuid4())[:8],
            'span_id': getattr(context, '_span_id', None) or str(uuid.uuid4())[:8],
        }

        if include_context and hasattr(context, '_log_context'):
            log_entry['context'] = context._log_context

        if data:
            log_entry['data'] = data

        if not hasattr(context, '_log_buffer'):
            context._log_buffer = []
        context._log_buffer.append(log_entry)

        max_buffer = 1000
        if len(context._log_buffer) > max_buffer:
            context._log_buffer = context._log_buffer[-max_buffer:]

        log_output = json.dumps(log_entry)
        if level == 'error':
            print(f"[ERROR] {log_output}")
        elif level == 'warn':
            print(f"[WARN] {log_output}")
        elif level == 'debug':
            print(f"[DEBUG] {log_output}")
        else:
            print(f"[INFO] {log_output}")

        return ActionResult(
            success=True,
            message=f"Logged: [{level.upper()}] {message}",
            data={
                'logged': True,
                'trace_id': log_entry['trace_id'],
                'buffer_size': len(context._log_buffer)
            }
        )


class DistributedTracerAction(BaseAction):
    """Distributed tracing for workflow execution.
    
    Tracks request flow across multiple actions
    with timing and causality tracking.
    """
    action_type = "distributed_tracer"
    display_name = "分布式追踪"
    description = "工作流执行的分布式追踪"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manage distributed trace.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (start_span|end_span|annotate|get_trace),
                   span_name, parent_span_id, annotations.
        
        Returns:
            ActionResult with trace data.
        """
        operation = params.get('operation', 'start_span')
        span_name = params.get('span_name', '')
        parent_span_id = params.get('parent_span_id')
        annotations = params.get('annotations', {})
        start_time = time.time()

        if not hasattr(context, '_traces'):
            context._traces = {}
            context._spans = {}
            context._trace_id = str(uuid.uuid4())
            context._span_counter = 0

        if operation == 'start_span':
            context._span_counter += 1
            span_id = f"span_{context._span_counter}"
            span = {
                'span_id': span_id,
                'name': span_name,
                'parent_id': parent_span_id or getattr(context, '_root_span_id', None),
                'start_time': time.time(),
                'end_time': None,
                'duration': None,
                'annotations': {},
                'events': [],
                'trace_id': context._trace_id
            }
            context._spans[span_id] = span
            if not parent_span_id:
                context._root_span_id = span_id

            return ActionResult(
                success=True,
                message=f"Started span: {span_name} ({span_id})",
                data={
                    'span_id': span_id,
                    'trace_id': context._trace_id,
                    'span_name': span_name
                }
            )

        elif operation == 'end_span':
            span_id = params.get('span_id', '')
            if span_id and span_id in context._spans:
                span = context._spans[span_id]
                span['end_time'] = time.time()
                span['duration'] = span['end_time'] - span['start_time']
                return ActionResult(
                    success=True,
                    message=f"Ended span: {span_id} ({span['duration']:.4f}s)",
                    data={
                        'span_id': span_id,
                        'duration_ms': round(span['duration'] * 1000, 2)
                    }
                )
            return ActionResult(success=False, message=f"Span not found: {span_id}")

        elif operation == 'annotate':
            span_id = params.get('span_id', context._root_span_id or '')
            if span_id and span_id in context._spans:
                context._spans[span_id]['annotations'].update(annotations)
                return ActionResult(
                    success=True,
                    message=f"Annotated span: {span_id}",
                    data={'annotations': annotations}
                )
            return ActionResult(success=False, message=f"Span not found: {span_id}")

        elif operation == 'get_trace':
            spans = list(context._spans.values())
            total_duration = sum(s.get('duration', 0) or 0 for s in spans)
            return ActionResult(
                success=True,
                message=f"Trace {context._trace_id}: {len(spans)} spans",
                data={
                    'trace_id': context._trace_id,
                    'spans': spans,
                    'span_count': len(spans),
                    'total_duration_ms': round(total_duration * 1000, 2)
                }
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")


class HealthCheckAction(BaseAction):
    """Health check endpoint for monitoring.
    
    Provides health status reporting with
    configurable checks and status aggregation.
    """
    action_type = "health_check"
    display_name = "健康检查"
    description = "监控系统健康状态"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform or register health check.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (check|register|report),
                   check_name, check_func, tags.
        
        Returns:
            ActionResult with health status.
        """
        operation = params.get('operation', 'check')
        check_name = params.get('check_name', 'default')
        tags = params.get('tags', [])
        start_time = time.time()

        if not hasattr(context, '_health_checks'):
            context._health_checks = {}

        if operation == 'register':
            context._health_checks[check_name] = {
                'name': check_name,
                'tags': tags,
                'registered_at': time.time(),
                'last_check': None,
                'last_status': None
            }
            return ActionResult(
                success=True,
                message=f"Registered health check: {check_name}",
                data={'check_name': check_name, 'tags': tags}
            )

        elif operation == 'report':
            status = params.get('status', 'healthy')
            context._health_checks[check_name] = {
                'name': check_name,
                'tags': tags,
                'last_check': time.time(),
                'last_status': status
            }
            return ActionResult(
                success=True,
                message=f"Reported status for '{check_name}': {status}",
                data={'check_name': check_name, 'status': status}
            )

        elif operation == 'check':
            checks = context._health_checks
            results = []
            healthy_count = 0

            for name, check in checks.items():
                last_status = check.get('last_status', 'unknown')
                results.append({
                    'name': name,
                    'status': last_status,
                    'tags': check.get('tags', []),
                    'last_check': check.get('last_check')
                })
                if last_status == 'healthy':
                    healthy_count += 1

            overall_status = 'healthy'
            if any(r['status'] == 'unhealthy' for r in results):
                overall_status = 'unhealthy'
            elif any(r['status'] == 'degraded' for r in results):
                overall_status = 'degraded'

            return ActionResult(
                success=overall_status == 'healthy',
                message=f"Health check: {healthy_count}/{len(results)} healthy ({overall_status})",
                data={
                    'overall_status': overall_status,
                    'checks': results,
                    'healthy_count': healthy_count,
                    'total_count': len(results)
                }
            )

        return ActionResult(success=False, message=f"Unknown operation: {operation}")
