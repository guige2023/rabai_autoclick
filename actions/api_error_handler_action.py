"""API Error Handler Action Module. Handles API errors with categorization."""
import sys, os, time
from typing import Any, Optional
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class ErrorCategory:
    name: str; http_codes: list; retryable: bool; severity: str

@dataclass
class ErrorDecision:
    category: str; retryable: bool; retry_after_seconds: Optional[float]
    fallback_action: Optional[str]; should_alert: bool; severity: str; message: str

class APIErrorHandlerAction(BaseAction):
    action_type = "api_error_handler"; display_name = "API错误处理"
    description = "处理API错误"
    def __init__(self) -> None:
        super().__init__()
        self._cats = {"auth": ErrorCategory("auth", [401,403], False, "high"),
                      "rate_limit": ErrorCategory("rate_limit", [429], True, "medium"),
                      "client_error": ErrorCategory("client_error", [400], False, "medium"),
                      "server_error": ErrorCategory("server_error", [500,502,503,504], True, "high"),
                      "timeout": ErrorCategory("timeout", [0], True, "medium"),
                      "network": ErrorCategory("network", [-1], True, "high")}
        self._error_counts = {}
    def _categorize(self, status_code: int, error_message: str) -> ErrorCategory:
        if status_code == 0 or "timeout" in error_message.lower(): return self._cats["timeout"]
        if "connection" in error_message.lower() or "network" in error_message.lower(): return self._cats["network"]
        for cat in self._cats.values():
            if status_code in cat.http_codes: return cat
        return ErrorCategory("unknown", [status_code], False, "medium")
    def execute(self, context: Any, params: dict) -> ActionResult:
        status_code = params.get("status_code", 0)
        error_message = params.get("error_message", "")
        retry_count = params.get("retry_count", 0)
        max_retries = params.get("max_retries", 3)
        endpoint = params.get("endpoint", "unknown")
        cat = self._categorize(status_code, error_message)
        timestamp = time.time()
        key = f"{endpoint}:{cat.name}"
        self._error_counts.setdefault(key, []).append(timestamp)
        cutoff = timestamp - 300
        self._error_counts[key] = [t for t in self._error_counts[key] if t > cutoff]
        retry_after = None
        if cat.name == "rate_limit": retry_after = params.get("retry_after_seconds", 60.0)
        elif cat.retryable and retry_count < max_retries:
            retry_after = params.get("base_delay", 1.0) * (2**retry_count)
        retryable = cat.retryable and retry_count < max_retries
        should_alert = cat.severity in ("high","critical") or len(self._error_counts[key]) >= 5
        fallback_action = None
        if not retryable or retry_count >= max_retries:
            fallback_action = {"auth": "notify_admin", "rate_limit": "use_cache"}.get(cat.name, "circuit_open")
        decision = ErrorDecision(category=cat.name, retryable=retryable, retry_after_seconds=retry_after,
                                fallback_action=fallback_action, should_alert=should_alert,
                                severity=cat.severity, message=f"HTTP {status_code}: {error_message}")
        alert_info = {}
        if should_alert and params.get("alert_channels"):
            alert_info = {"channels": params["alert_channels"], "message": f"API Error: {cat.name} on {endpoint}"}
        return ActionResult(success=retryable, message=f"'{cat.name}': {'retry' if retryable else 'no retry'} ({retry_count+1}/{max_retries})",
                          data={"decision": vars(decision), "alert": alert_info if alert_info else None})
