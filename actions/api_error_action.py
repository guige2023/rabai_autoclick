"""API error handling action module for RabAI AutoClick.

Provides API error handling operations:
- ErrorHandlerAction: Handle API errors
- ErrorRecoveryAction: Recover from errors
- ErrorFormatterAction: Format error responses
- ErrorClassifierAction: Classify error types
- ErrorLoggerAction: Log API errors
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ErrorHandlerAction(BaseAction):
    """Handle API errors."""
    action_type = "error_handler"
    display_name = "错误处理"
    description = "处理API错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", None)
            error_type = params.get("error_type", "unknown")
            handler_strategy = params.get("strategy", "log_and_return")
            fallback_value = params.get("fallback_value", None)

            if error is None:
                return ActionResult(success=False, message="error is required")

            error_info = {
                "error_type": error_type,
                "error_message": str(error),
                "handled_at": datetime.now().isoformat(),
                "strategy": handler_strategy
            }

            if handler_strategy == "log_and_return":
                return ActionResult(
                    success=True,
                    data={
                        **error_info,
                        "fallback_used": True,
                        "fallback_value": fallback_value
                    },
                    message=f"Error handled with fallback: {str(error)[:50]}"
                )

            elif handler_strategy == "retry":
                max_retries = params.get("max_retries", 3)
                return ActionResult(
                    success=True,
                    data={
                        **error_info,
                        "max_retries": max_retries,
                        "retry_recommended": True
                    },
                    message=f"Error requires retry: {str(error)[:50]}"
                )

            elif handler_strategy == "fail":
                return ActionResult(
                    success=False,
                    data=error_info,
                    message=f"Error handling failed: {str(error)[:50]}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown strategy: {handler_strategy}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error handler error: {str(e)}")


class ErrorRecoveryAction(BaseAction):
    """Recover from API errors."""
    action_type = "error_recovery"
    display_name = "错误恢复"
    description = "从API错误恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", None)
            recovery_steps = params.get("recovery_steps", [])
            max_recovery_time = params.get("max_recovery_time", 60)

            if error is None:
                return ActionResult(success=False, message="error is required")

            recovery_attempted = []
            recovery_successful = False

            for step in recovery_steps:
                step_name = step.get("name", "unknown")
                step_type = step.get("type", "action")

                recovery_attempted.append({
                    "step": step_name,
                    "type": step_type,
                    "attempted": True,
                    "successful": step_type == "action"
                })

                if step_type == "action":
                    recovery_successful = True
                    break

            return ActionResult(
                success=recovery_successful,
                data={
                    "error": str(error),
                    "recovery_steps_attempted": len(recovery_attempted),
                    "recovery_successful": recovery_successful,
                    "recovery_details": recovery_attempted,
                    "max_recovery_time": max_recovery_time
                },
                message=f"Recovery {'successful' if recovery_successful else 'failed'} after {len(recovery_attempted)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error recovery error: {str(e)}")


class ErrorFormatterAction(BaseAction):
    """Format error responses."""
    action_type = "error_formatter"
    display_name = "错误格式化"
    description = "格式化错误响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", None)
            format_type = params.get("format_type", "standard")
            include_stack_trace = params.get("include_stack_trace", False)
            locale = params.get("locale", "en")

            if error is None:
                return ActionResult(success=False, message="error is required")

            if format_type == "standard":
                formatted = {
                    "error": {
                        "code": "API_ERROR",
                        "message": str(error),
                        "timestamp": datetime.now().isoformat()
                    }
                }
            elif format_type == "detailed":
                formatted = {
                    "error": {
                        "code": "API_ERROR",
                        "type": type(error).__name__,
                        "message": str(error),
                        "timestamp": datetime.now().isoformat(),
                        "context": params.get("context", {})
                    }
                }
            elif format_type == "rest":
                formatted = {
                    "status": "error",
                    "message": str(error),
                    "timestamp": datetime.now().isoformat()
                }
            elif format_type == "graphql":
                formatted = {
                    "errors": [{"message": str(error), "extensions": {"code": "INTERNAL_ERROR"}}],
                    "data": None
                }
            else:
                formatted = {"error": str(error)}

            return ActionResult(
                success=True,
                data={
                    "format_type": format_type,
                    "formatted_error": formatted,
                    "locale": locale
                },
                message=f"Error formatted as {format_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error formatter error: {str(e)}")


class ErrorClassifierAction(BaseAction):
    """Classify API error types."""
    action_type = "error_classifier"
    display_name = "错误分类"
    description = "分类API错误类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error", None)
            status_code = params.get("status_code", 500)
            error_message = params.get("error_message", "")

            if error is None and not error_message:
                return ActionResult(success=False, message="error or error_message is required")

            error_str = str(error) if error else error_message

            if status_code == 400:
                category = "client_error"
                subcategory = "bad_request"
                severity = "warning"
            elif status_code == 401:
                category = "client_error"
                subcategory = "authentication_required"
                severity = "high"
            elif status_code == 403:
                category = "client_error"
                subcategory = "forbidden"
                severity = "high"
            elif status_code == 404:
                category = "client_error"
                subcategory = "not_found"
                severity = "warning"
            elif status_code == 429:
                category = "client_error"
                subcategory = "rate_limit_exceeded"
                severity = "warning"
            elif status_code >= 500:
                category = "server_error"
                subcategory = "internal_error"
                severity = "critical"
            else:
                category = "unknown"
                subcategory = "unclassified"
                severity = "medium"

            if "timeout" in error_str.lower():
                subcategory = "timeout"
                severity = "high"
            elif "connection" in error_str.lower():
                subcategory = "connection_error"
                severity = "high"
            elif "validation" in error_str.lower():
                subcategory = "validation_error"
                severity = "warning"

            return ActionResult(
                success=True,
                data={
                    "category": category,
                    "subcategory": subcategory,
                    "severity": severity,
                    "status_code": status_code,
                    "retry_recommended": category == "server_error" or subcategory in ["timeout", "connection_error"]
                },
                message=f"Error classified: {category}/{subcategory} (severity: {severity})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error classifier error: {str(e)}")


class ErrorLoggerAction(BaseAction):
    """Log API errors."""
    action_type = "error_logger"
    display_name = "错误日志"
    description = "记录API错误"

    def __init__(self):
        super().__init__()
        self._error_log = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "log")
            error = params.get("error", None)
            error_type = params.get("error_type", "unknown")
            include_context = params.get("include_context", True)

            if operation == "log":
                if error is None:
                    return ActionResult(success=False, message="error is required for log operation")

                log_entry = {
                    "id": len(self._error_log) + 1,
                    "error": str(error),
                    "error_type": error_type,
                    "timestamp": datetime.now().isoformat(),
                    "context": params.get("context", {}) if include_context else {}
                }

                self._error_log.append(log_entry)

                return ActionResult(
                    success=True,
                    data={
                        "logged": True,
                        "log_id": log_entry["id"],
                        "total_errors": len(self._error_log)
                    },
                    message=f"Error logged (ID: {log_entry['id']})"
                )

            elif operation == "get":
                limit = params.get("limit", 100)
                error_type_filter = params.get("error_type_filter", None)

                filtered = self._error_log
                if error_type_filter:
                    filtered = [e for e in filtered if e["error_type"] == error_type_filter]

                return ActionResult(
                    success=True,
                    data={
                        "errors": filtered[-limit:],
                        "total_count": len(self._error_log),
                        "returned_count": len(filtered[-limit:])
                    },
                    message=f"Retrieved {min(limit, len(filtered))} errors"
                )

            elif operation == "clear":
                cleared_count = len(self._error_log)
                self._error_log = []
                return ActionResult(
                    success=True,
                    data={"cleared_count": cleared_count},
                    message=f"Cleared {cleared_count} error logs"
                )

            elif operation == "stats":
                if not self._error_log:
                    return ActionResult(
                        success=True,
                        data={"total_errors": 0, "by_type": {}},
                        message="No errors logged"
                    )

                by_type = {}
                for entry in self._error_log:
                    et = entry["error_type"]
                    by_type[et] = by_type.get(et, 0) + 1

                return ActionResult(
                    success=True,
                    data={
                        "total_errors": len(self._error_log),
                        "by_type": by_type,
                        "oldest": self._error_log[0]["timestamp"] if self._error_log else None,
                        "newest": self._error_log[-1]["timestamp"] if self._error_log else None
                    },
                    message=f"Error stats: {len(self._error_log)} total, {len(by_type)} types"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error logger error: {str(e)}")
