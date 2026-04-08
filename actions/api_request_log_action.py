"""
API Request Log Action Module.

Logs API requests with full request/response details,
searchable history, and analytics export.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class RequestLogEntry:
    """A logged API request."""
    id: str
    timestamp: float
    method: str
    path: str
    status_code: int
    latency_ms: float
    request_size: int
    response_size: int
    client_ip: str
    error: Optional[str] = None


class APIRequestLogAction(BaseAction):
    """Log API requests for auditing and analytics."""

    def __init__(self) -> None:
        super().__init__("api_request_log")
        self._logs: list[RequestLogEntry] = []
        self._max_logs = 10000

    def execute(self, context: dict, params: dict) -> dict:
        """
        Log or query API requests.

        Args:
            context: Execution context
            params: Parameters:
                - action: log or query
                - request: Request data for logging
                - filters: Query filters
                - limit: Max results for query

        Returns:
            For log: confirmation
            For query: list of log entries
        """
        import time
        import hashlib

        action = params.get("action", "log")

        if action == "log":
            request = params.get("request", {})
            timestamp = time.time()

            entry = RequestLogEntry(
                id=hashlib.md5(f"{timestamp}{request.get('path', '')}".encode()).hexdigest()[:12],
                timestamp=timestamp,
                method=request.get("method", "GET"),
                path=request.get("path", ""),
                status_code=request.get("status_code", 200),
                latency_ms=request.get("latency_ms", 0),
                request_size=request.get("request_size", 0),
                response_size=request.get("response_size", 0),
                client_ip=request.get("client_ip", ""),
                error=request.get("error")
            )

            self._logs.append(entry)
            if len(self._logs) > self._max_logs:
                self._logs = self._logs[-self._max_logs:]

            return {"logged": True, "log_id": entry.id}

        elif action == "query":
            filters = params.get("filters", {})
            limit = params.get("limit", 100)

            results = list(self._logs)
            if "method" in filters:
                results = [r for r in results if r.method == filters["method"]]
            if "path" in filters:
                results = [r for r in results if filters["path"] in r.path]
            if "status_code" in filters:
                results = [r for r in results if r.status_code == filters["status_code"]]
            if "from_timestamp" in filters:
                results = [r for r in results if r.timestamp >= filters["from_timestamp"]]
            if "to_timestamp" in filters:
                results = [r for r in results if r.timestamp <= filters["to_timestamp"]]

            return {
                "count": len(results),
                "logs": [vars(r) for r in results[-limit:]]
            }

        elif action == "export":
            return {
                "exported": len(self._logs),
                "logs": [vars(r) for r in self._logs]
            }

        return {"error": f"Unknown action: {action}"}

    def get_stats(self) -> dict[str, Any]:
        """Get log statistics."""
        if not self._logs:
            return {"total": 0, "avg_latency_ms": 0, "error_rate": 0}
        total = len(self._logs)
        errors = sum(1 for r in self._logs if r.error or r.status_code >= 400)
        avg_latency = sum(r.latency_ms for r in self._logs) / total
        return {
            "total": total,
            "avg_latency_ms": avg_latency,
            "error_rate": errors / total if total > 0 else 0
        }
