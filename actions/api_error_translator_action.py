"""API error translator action module for RabAI AutoClick.

Provides error translation for API operations:
- ApiErrorTranslatorAction: Translate API errors to standard format
- ApiErrorClassifierAction: Classify API errors by type
- ApiErrorFormatterAction: Format API errors for display
- ApiErrorRecoverySuggestionAction: Suggest recovery from API errors
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ErrorCategory(Enum) if False else object:
    """Error categories."""
    TRANSIENT = "transient"
    CLIENT = "client"
    SERVER = "server"
    AUTH = "auth"
    RATE_LIMIT = "rate_limit"
    NETWORK = "network"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ApiErrorTranslatorAction(BaseAction):
    """Translate API errors to standard format."""
    action_type = "api_error_translator"
    display_name = "API错误翻译"
    description = "将API错误转换为标准格式"

    def __init__(self):
        super().__init__()
        self._error_mappings: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error")
            api_provider = params.get("api_provider", "generic")
            include_suggestion = params.get("include_suggestion", True)

            if not error:
                return ActionResult(success=False, message="error is required")

            translated = self._translate_error(error, api_provider)

            if include_suggestion:
                suggestion = self._get_recovery_suggestion(translated)
                translated["suggestion"] = suggestion

            return ActionResult(
                success=True,
                message=f"Translated error: {translated.get('code', 'unknown')}",
                data=translated
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Error translator error: {e}")

    def _translate_error(self, error: Any, provider: str) -> Dict[str, Any]:
        """Translate an error to standard format."""
        error_str = str(error)
        code = self._extract_error_code(error)
        status = self._extract_http_status(error)

        category = self._classify_error(code, status, error_str)
        severity = self._get_severity(category)

        return {
            "code": code,
            "message": self._extract_message(error),
            "status": status,
            "category": category,
            "severity": severity,
            "provider": provider,
            "original_error": error_str,
            "timestamp": datetime.now().isoformat(),
        }

    def _extract_error_code(self, error: Any) -> str:
        """Extract error code from error object."""
        if isinstance(error, dict):
            return str(error.get("code", error.get("error_code", "UNKNOWN")))
        elif hasattr(error, "code"):
            return str(error.code)
        return "UNKNOWN"

    def _extract_http_status(self, error: Any) -> Optional[int]:
        """Extract HTTP status code."""
        if isinstance(error, dict):
            return error.get("status") or error.get("status_code")
        elif hasattr(error, "code"):
            if hasattr(error, "reason"):
                return getattr(error, "code", None)
            return error.code if isinstance(error.code, int) else None
        return None

    def _extract_message(self, error: Any) -> str:
        """Extract error message."""
        if isinstance(error, dict):
            return error.get("message", error.get("msg", error.get("error", str(error))))
        elif hasattr(error, "reason"):
            return str(error.reason)
        return str(error)

    def _classify_error(self, code: str, status: Optional[int], message: str) -> str:
        """Classify error into category."""
        code_str = str(code).upper()
        msg_lower = message.lower()

        if status in (401, 403) or "AUTH" in code_str or "token" in msg_lower or "credential" in msg_lower:
            return "auth"
        elif status == 429 or "rate" in msg_lower or "quota" in msg_lower:
            return "rate_limit"
        elif status in (408, 504) or "timeout" in msg_lower or "timed out" in msg_lower:
            return "timeout"
        elif status in (502, 503, 504) or "unavailable" in msg_lower or "backend" in msg_lower:
            return "server"
        elif status and status >= 500:
            return "server"
        elif status and status >= 400:
            return "client"
        elif "network" in msg_lower or "connection" in msg_lower or "dns" in msg_lower:
            return "network"
        return "unknown"

    def _get_severity(self, category: str) -> str:
        """Get severity level for category."""
        severity_map = {
            "transient": "warning",
            "network": "warning",
            "timeout": "warning",
            "rate_limit": "warning",
            "auth": "error",
            "client": "info",
            "server": "error",
            "unknown": "info",
        }
        return severity_map.get(category, "info")

    def _get_recovery_suggestion(self, translated: Dict[str, Any]) -> str:
        """Get recovery suggestion for error."""
        category = translated.get("category", "unknown")
        suggestions = {
            "auth": "Check your API credentials and ensure the token is valid and not expired.",
            "rate_limit": "Implement exponential backoff and respect rate limits. Consider caching responses.",
            "timeout": "Increase the timeout value or check network connectivity.",
            "server": "The server is experiencing issues. Retry with backoff after a delay.",
            "client": "Fix the request parameters. Check the API documentation for correct usage.",
            "network": "Check your internet connection and DNS settings.",
            "unknown": "Retry the request. If the issue persists, check the API status page.",
        }
        return suggestions.get(category, "Retry the request.")


class ApiErrorClassifierAction(BaseAction):
    """Classify API errors by type."""
    action_type = "api_error_classifier"
    display_name = "API错误分类"
    description = "按类型分类API错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            errors = params.get("errors", [])
            classify_individually = params.get("classify_individually", True)

            if not errors:
                return ActionResult(success=False, message="errors list is required")

            if classify_individually:
                classified = []
                for err in errors:
                    code = self._get_code(err)
                    status = self._get_status(err)
                    msg = self._get_message(err)
                    category = self._classify(code, status, msg)
                    classified.append({"error": err, "category": category, "code": code, "status": status})
            else:
                categories = {}
                for err in errors:
                    code = self._get_code(err)
                    status = self._get_status(err)
                    msg = self._get_message(err)
                    cat = self._classify(code, status, msg)
                    if cat not in categories:
                        categories[cat] = []
                    categories[cat].append(err)
                classified = [{"category": cat, "count": len(errs), "errors": errs} for cat, errs in categories.items()]

            return ActionResult(success=True, message=f"Classified {len(errors)} errors", data={"classified": classified, "total": len(errors)})
        except Exception as e:
            return ActionResult(success=False, message=f"Error classifier error: {e}")

    def _get_code(self, err: Any) -> str:
        if isinstance(err, dict):
            return str(err.get("code", err.get("error_code", "")))
        return ""

    def _get_status(self, err: Any) -> Optional[int]:
        if isinstance(err, dict):
            return err.get("status") or err.get("status_code")
        return None

    def _get_message(self, err: Any) -> str:
        if isinstance(err, dict):
            return str(err.get("message", err.get("msg", err.get("error", ""))))
        return str(err)

    def _classify(self, code: str, status: Optional[int], message: str) -> str:
        code_str = str(code).upper()
        msg_lower = message.lower()
        if status in (401, 403) or "AUTH" in code_str:
            return "auth"
        elif status == 429 or "rate" in msg_lower:
            return "rate_limit"
        elif status in (408, 504) or "timeout" in msg_lower:
            return "timeout"
        elif status and status >= 500:
            return "server"
        elif status and status >= 400:
            return "client"
        elif "network" in msg_lower or "connection" in msg_lower:
            return "network"
        return "unknown"


class ApiErrorFormatterAction(BaseAction):
    """Format API errors for display."""
    action_type = "api_error_formatter"
    display_name = "API错误格式化"
    description = "格式化API错误用于显示"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error")
            format_type = params.get("format_type", "readable")
            include_trace = params.get("include_trace", False)

            if not error:
                return ActionResult(success=False, message="error is required")

            if format_type == "readable":
                formatted = self._format_readable(error)
            elif format_type == "json":
                import json
                formatted = json.dumps(error, indent=2, default=str)
            elif format_type == "short":
                formatted = self._format_short(error)
            elif format_type == "detailed":
                formatted = self._format_detailed(error)
            else:
                formatted = str(error)

            return ActionResult(success=True, message="Error formatted", data={"formatted": formatted, "format": format_type})
        except Exception as e:
            return ActionResult(success=False, message=f"Error formatter error: {e}")

    def _format_readable(self, error: Any) -> str:
        parts = []
        if isinstance(error, dict):
            if "message" in error:
                parts.append(f"Error: {error['message']}")
            if "code" in error:
                parts.append(f"Code: {error['code']}")
            if "status" in error:
                parts.append(f"Status: {error['status']}")
        else:
            parts.append(f"Error: {error}")
        return " | ".join(parts)

    def _format_short(self, error: Any) -> str:
        if isinstance(error, dict):
            code = error.get("code", "UNKNOWN")
            msg = error.get("message", str(error))[:50]
            return f"[{code}] {msg}"
        return str(error)[:80]

    def _format_detailed(self, error: Any) -> str:
        lines = ["=" * 60, "DETAILED ERROR REPORT", "=" * 60]
        if isinstance(error, dict):
            for k, v in error.items():
                lines.append(f"  {k}: {v}")
        else:
            lines.append(f"  {error}")
        lines.append("=" * 60)
        return "\n".join(lines)


class ApiErrorRecoverySuggestionAction(BaseAction):
    """Suggest recovery actions for API errors."""
    action_type = "api_error_recovery_suggestion"
    display_name = "API错误恢复建议"
    description = "为API错误提供恢复建议"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            error = params.get("error")
            retry_config = params.get("retry_config", {})

            if not error:
                return ActionResult(success=False, message="error is required")

            suggestions = self._get_suggestions(error, retry_config)

            return ActionResult(
                success=True,
                message=f"Generated {len(suggestions)} suggestions",
                data={"suggestions": suggestions, "error": str(error)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Recovery suggestion error: {e}")

    def _get_suggestions(self, error: Any, retry_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate recovery suggestions."""
        suggestions = []
        error_str = str(error).lower()

        suggestions.append({"action": "log", "description": "Log the error for debugging", "priority": "low"})

        if "timeout" in error_str or "timed out" in error_str:
            suggestions.append({"action": "retry", "description": "Retry with increased timeout", "priority": "high", "timeout": 60})
            suggestions.append({"action": "check", "description": "Check network connectivity", "priority": "medium"})

        if "rate" in error_str or "429" in error_str:
            suggestions.append({"action": "backoff", "description": "Implement exponential backoff", "priority": "high"})
            suggestions.append({"action": "wait", "description": "Wait before retrying", "priority": "high", "wait_seconds": 60})

        if "auth" in error_str or "401" in error_str or "403" in error_str:
            suggestions.append({"action": "refresh_token", "description": "Refresh or regenerate authentication token", "priority": "high"})
            suggestions.append({"action": "check_permissions", "description": "Verify API key/permissions are correct", "priority": "medium"})

        if "500" in error_str or "502" in error_str or "503" in error_str:
            suggestions.append({"action": "retry", "description": "Retry after delay (server error)", "priority": "medium", "wait_seconds": 30})
            suggestions.append({"action": "check_status", "description": "Check API status page", "priority": "low"})

        if "connection" in error_str or "network" in error_str:
            suggestions.append({"action": "check_network", "description": "Verify internet connection", "priority": "high"})
            suggestions.append({"action": "retry", "description": "Retry the connection", "priority": "medium"})

        if not suggestions:
            suggestions.append({"action": "retry", "description": "Retry the request", "priority": "medium"})

        return suggestions


from enum import Enum
