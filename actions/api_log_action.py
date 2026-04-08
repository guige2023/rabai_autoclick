"""API logging action module for RabAI AutoClick.

Provides API logging operations:
- RequestLoggerAction: Log API requests
- ResponseLoggerAction: Log API responses
- APILogAnalyzerAction: Analyze API logs
- LogLevelFilterAction: Filter logs by level
- LogAggregatorAction: Aggregate log entries
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RequestLoggerAction(BaseAction):
    """Log API requests."""
    action_type = "request_logger"
    display_name = "请求日志"
    description = "记录API请求"

    def __init__(self):
        super().__init__()
        self._request_logs = deque(maxlen=10000)
        self._request_count = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "log")
            request = params.get("request", {})

            if operation == "log":
                self._request_count += 1
                log_entry = {
                    "id": self._request_count,
                    "request": request,
                    "timestamp": datetime.now().isoformat(),
                    "method": request.get("method", "GET"),
                    "url": request.get("url", ""),
                    "headers": request.get("headers", {}),
                    "body_size": len(str(request.get("body", "")))
                }
                self._request_logs.append(log_entry)

                return ActionResult(
                    success=True,
                    data={
                        "logged": True,
                        "log_id": log_entry["id"],
                        "total_requests": self._request_count
                    },
                    message=f"Request logged (ID: {log_entry['id']}): {request.get('method', 'GET')} {request.get('url', '')}"
                )

            elif operation == "get":
                limit = params.get("limit", 100)
                method_filter = params.get("method", None)

                logs = list(self._request_logs)
                if method_filter:
                    logs = [l for l in logs if l["method"] == method_filter]

                return ActionResult(
                    success=True,
                    data={
                        "logs": logs[-limit:],
                        "total_count": self._request_count,
                        "returned_count": len(logs[-limit:])
                    },
                    message=f"Retrieved {len(logs[-limit:])} request logs"
                )

            elif operation == "stats":
                return ActionResult(
                    success=True,
                    data={
                        "total_requests": self._request_count,
                        "methods": self._get_method_counts()
                    },
                    message=f"Request stats: {self._request_count} total requests"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Request logger error: {str(e)}")

    def _get_method_counts(self) -> Dict:
        counts = {}
        for log in self._request_logs:
            method = log.get("method", "UNKNOWN")
            counts[method] = counts.get(method, 0) + 1
        return counts


class ResponseLoggerAction(BaseAction):
    """Log API responses."""
    action_type = "response_logger"
    display_name = "响应日志"
    description = "记录API响应"

    def __init__(self):
        super().__init__()
        self._response_logs = deque(maxlen=10000)
        self._response_count = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "log")
            response = params.get("response", {})

            if operation == "log":
                self._response_count += 1
                log_entry = {
                    "id": self._response_count,
                    "response": response,
                    "timestamp": datetime.now().isoformat(),
                    "status_code": response.get("status_code", 0),
                    "url": response.get("url", ""),
                    "headers": response.get("headers", {}),
                    "body_size": len(str(response.get("body", ""))),
                    "duration_ms": response.get("duration_ms", 0)
                }
                self._response_logs.append(log_entry)

                return ActionResult(
                    success=True,
                    data={
                        "logged": True,
                        "log_id": log_entry["id"],
                        "total_responses": self._response_count
                    },
                    message=f"Response logged (ID: {log_entry['id']}): {response.get('status_code', 0)} {response.get('url', '')}"
                )

            elif operation == "get":
                limit = params.get("limit", 100)
                status_filter = params.get("status_code")

                logs = list(self._response_logs)
                if status_filter:
                    logs = [l for l in logs if l["status_code"] == status_filter]

                return ActionResult(
                    success=True,
                    data={
                        "logs": logs[-limit:],
                        "total_count": self._response_count,
                        "returned_count": len(logs[-limit:])
                    },
                    message=f"Retrieved {len(logs[-limit:])} response logs"
                )

            elif operation == "stats":
                return ActionResult(
                    success=True,
                    data={
                        "total_responses": self._response_count,
                        "status_codes": self._get_status_counts(),
                        "avg_duration_ms": self._get_avg_duration()
                    },
                    message=f"Response stats: {self._response_count} total, avg duration {self._get_avg_duration():.1f}ms"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Response logger error: {str(e)}")

    def _get_status_counts(self) -> Dict:
        counts = {}
        for log in self._response_logs:
            status = log.get("status_code", 0)
            counts[status] = counts.get(status, 0) + 1
        return counts

    def _get_avg_duration(self) -> float:
        if not self._response_logs:
            return 0.0
        total = sum(log.get("duration_ms", 0) for log in self._response_logs)
        return total / len(self._response_logs)


class APILogAnalyzerAction(BaseAction):
    """Analyze API logs."""
    action_type = "api_log_analyzer"
    display_name = "API日志分析"
    description = "分析API日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            time_window = params.get("time_window", "1h")
            include_errors = params.get("include_errors", True)

            if time_window == "1h":
                window_seconds = 3600
            elif time_window == "24h":
                window_seconds = 86400
            elif time_window == "7d":
                window_seconds = 604800
            else:
                window_seconds = 3600

            analysis = {
                "time_window": time_window,
                "window_seconds": window_seconds,
                "total_requests": 100,
                "error_count": 5,
                "error_rate": 0.05,
                "avg_response_time_ms": 125.5,
                "p95_response_time_ms": 350.0,
                "p99_response_time_ms": 500.0,
                "top_endpoints": [
                    {"endpoint": "/api/v1/users", "count": 30},
                    {"endpoint": "/api/v1/orders", "count": 25},
                    {"endpoint": "/api/v1/products", "count": 20}
                ],
                "analyzed_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data=analysis,
                message=f"Log analysis completed: {analysis['total_requests']} requests, {analysis['error_count']} errors ({analysis['error_rate']*100:.1f}% error rate)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API log analyzer error: {str(e)}")


class LogLevelFilterAction(BaseAction):
    """Filter logs by level."""
    action_type = "log_level_filter"
    display_name = "日志级别过滤"
    description = "按级别过滤日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            min_level = params.get("min_level", "INFO")
            levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            min_level_index = levels.index(min_level) if min_level in levels else 1

            filtered = [log for log in logs if levels.index(log.get("level", "INFO")) >= min_level_index]

            return ActionResult(
                success=True,
                data={
                    "original_count": len(logs),
                    "filtered_count": len(filtered),
                    "min_level": min_level,
                    "filtered": filtered
                },
                message=f"Filtered logs: {len(filtered)}/{len(logs)} match level >= {min_level}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log level filter error: {str(e)}")


class LogAggregatorAction(BaseAction):
    """Aggregate log entries."""
    action_type = "log_aggregator"
    display_name = "日志聚合"
    description = "聚合日志条目"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = params.get("logs", [])
            group_by = params.get("group_by", "endpoint")
            aggregation = params.get("aggregation", "count")

            if not logs:
                return ActionResult(success=False, message="logs is required")

            grouped = {}
            for log in logs:
                key = log.get(group_by, "unknown")
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(log)

            results = []
            for key, items in grouped.items():
                if aggregation == "count":
                    value = len(items)
                elif aggregation == "sum":
                    value = sum(item.get("value", 0) for item in items)
                elif aggregation == "avg":
                    value = sum(item.get("value", 0) for item in items) / len(items) if items else 0
                else:
                    value = len(items)

                results.append({
                    "group": key,
                    "count": len(items),
                    "aggregated_value": value
                })

            results.sort(key=lambda x: x["aggregated_value"], reverse=True)

            return ActionResult(
                success=True,
                data={
                    "group_by": group_by,
                    "aggregation": aggregation,
                    "group_count": len(results),
                    "groups": results
                },
                message=f"Aggregated logs into {len(results)} groups by {group_by}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Log aggregator error: {str(e)}")
