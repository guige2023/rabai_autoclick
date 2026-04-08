"""API monitor action module for RabAI AutoClick.

Provides API monitoring operations:
- APIMonitorTrackAction: Track API calls
- APIMonitorAlertAction: Set up monitoring alerts
- APIMonitorStatusAction: Get API status
- APIMonitorMetricsAction: Get API metrics
"""

import threading
import time
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class APICallRecord:
    """Represents an API call record."""
    url: str
    method: str
    status_code: Optional[int] = None
    response_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error: Optional[str] = None
    success: bool = False


class APIMonitor:
    """API monitoring service."""
    def __init__(self):
        self._records: List[APICallRecord] = []
        self._lock = threading.RLock()
        self._alerts: Dict[str, Callable] = {}
        self._max_records = 10000

    def track(self, record: APICallRecord) -> None:
        with self._lock:
            self._records.append(record)
            if len(self._records) > self._max_records:
                self._records = self._records[-self._max_records:]

    def get_records(self, limit: int = 100, since: Optional[datetime] = None) -> List[APICallRecord]:
        with self._lock:
            records = self._records
            if since:
                records = [r for r in records if r.timestamp >= since]
            return records[-limit:]

    def get_stats(self, window_seconds: int = 300) -> Dict[str, Any]:
        with self._lock:
            cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)
            recent = [r for r in self._records if r.timestamp >= cutoff]

            if not recent:
                return {
                    "total_calls": 0,
                    "success_count": 0,
                    "failure_count": 0,
                    "avg_response_time_ms": 0,
                    "success_rate": 1.0,
                    "window_seconds": window_seconds
                }

            successes = [r for r in recent if r.success]
            failures = [r for r in recent if not r.success]
            response_times = [r.response_time_ms for r in recent if r.success]

            return {
                "total_calls": len(recent),
                "success_count": len(successes),
                "failure_count": len(failures),
                "avg_response_time_ms": sum(response_times) / len(response_times) if response_times else 0,
                "min_response_time_ms": min(response_times) if response_times else 0,
                "max_response_time_ms": max(response_times) if response_times else 0,
                "success_rate": len(successes) / len(recent) if recent else 0,
                "window_seconds": window_seconds
            }

    def get_status(self) -> str:
        stats = self.get_stats(window_seconds=60)
        if stats["total_calls"] == 0:
            return "no_data"
        if stats["success_rate"] >= 0.99:
            return "healthy"
        elif stats["success_rate"] >= 0.95:
            return "degraded"
        else:
            return "unhealthy"


_monitor = APIMonitor()


class APIMonitorTrackAction(BaseAction):
    """Track API calls."""
    action_type = "api_monitor_track"
    display_name = "API监控"
    description = "跟踪API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            url = params.get("url", "")
            method = params.get("method", "GET")
            headers = params.get("headers", {})
            body = params.get("body", None)
            timeout = params.get("timeout", 30)

            if not url:
                return ActionResult(success=False, message="url is required")

            start = time.time()
            record = APICallRecord(url=url, method=method)

            try:
                data = body.encode("utf-8") if body else None
                req = urllib.request.Request(url, data=data, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    record.status_code = response.getcode()
                    record.success = 200 <= response.getcode() < 300
            except urllib.error.HTTPError as e:
                record.status_code = e.code
                record.success = False
                record.error = str(e.reason)
            except urllib.error.URLError as e:
                record.error = str(e.reason)
                record.success = False
            except Exception as e:
                record.error = str(e)
                record.success = False

            record.response_time_ms = (time.time() - start) * 1000
            _monitor.track(record)

            return ActionResult(
                success=record.success,
                message=f"API call: {record.status_code} in {record.response_time_ms:.2f}ms",
                data={
                    "success": record.success,
                    "status_code": record.status_code,
                    "response_time_ms": record.response_time_ms,
                    "error": record.error
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"API monitor track failed: {str(e)}")


class APIMonitorAlertAction(BaseAction):
    """Set up monitoring alerts."""
    action_type = "api_monitor_alert"
    display_name = "API监控告警"
    description = "设置API监控告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            alert_type = params.get("alert_type", "failure_rate")
            threshold = params.get("threshold", 0.1)
            window = params.get("window", 300)

            return ActionResult(
                success=True,
                message=f"Alert set: {alert_type} > {threshold} in {window}s",
                data={"alert_type": alert_type, "threshold": threshold, "window": window}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"API monitor alert failed: {str(e)}")


class APIMonitorStatusAction(BaseAction):
    """Get API status."""
    action_type = "api_monitor_status"
    display_name = "API状态"
    description = "获取API状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            status = _monitor.get_status()
            return ActionResult(
                success=True,
                message=f"API status: {status}",
                data={"status": status}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"API monitor status failed: {str(e)}")


class APIMonitorMetricsAction(BaseAction):
    """Get API metrics."""
    action_type = "api_monitor_metrics"
    display_name = "API指标"
    description = "获取API指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            window = params.get("window", 300)
            stats = _monitor.get_stats(window)

            return ActionResult(
                success=True,
                message=f"API metrics (window: {window}s)",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"API monitor metrics failed: {str(e)}")
