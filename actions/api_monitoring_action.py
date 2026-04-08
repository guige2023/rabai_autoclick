"""API monitoring action module for RabAI AutoClick.

Provides API monitoring operations:
- MonitorCreateAction: Create monitor
- MonitorTrackAction: Track API metrics
- MonitorAlertAction: Set up alerts
- MonitorDashboardAction: Get dashboard data
- MonitorDowntimeAction: Track downtime
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MonitorCreateAction(BaseAction):
    """Create an API monitor."""
    action_type = "monitor_create"
    display_name = "创建监控"
    description = "创建API监控"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            target_url = params.get("target_url", "")
            interval = params.get("interval", 60)

            if not name:
                return ActionResult(success=False, message="name is required")

            monitor_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "api_monitors"):
                context.api_monitors = {}
            context.api_monitors[monitor_id] = {
                "monitor_id": monitor_id,
                "name": name,
                "target_url": target_url,
                "interval": interval,
                "status": "active",
                "created_at": time.time(),
                "checks": 0,
                "failures": 0,
            }

            return ActionResult(
                success=True,
                data={"monitor_id": monitor_id, "name": name, "target_url": target_url},
                message=f"Monitor {monitor_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor create failed: {e}")


class MonitorTrackAction(BaseAction):
    """Track API metrics."""
    action_type = "monitor_track"
    display_name = "监控追踪"
    description = "追踪API指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            monitor_id = params.get("monitor_id", "")
            latency_ms = params.get("latency_ms", 0)
            status_code = params.get("status_code", 200)
            timestamp = params.get("timestamp", time.time())

            if not monitor_id:
                return ActionResult(success=False, message="monitor_id is required")

            monitors = getattr(context, "api_monitors", {})
            if monitor_id not in monitors:
                return ActionResult(success=False, message=f"Monitor {monitor_id} not found")

            monitor = monitors[monitor_id]
            monitor["checks"] += 1
            if status_code >= 400:
                monitor["failures"] += 1

            uptime = (monitor["checks"] - monitor["failures"]) / monitor["checks"] if monitor["checks"] > 0 else 0

            return ActionResult(
                success=True,
                data={"monitor_id": monitor_id, "latency_ms": latency_ms, "status_code": status_code, "uptime": uptime},
                message=f"Tracked check for {monitor_id}: {status_code} in {latency_ms}ms",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor track failed: {e}")


class MonitorAlertAction(BaseAction):
    """Set up monitoring alerts."""
    action_type = "monitor_alert"
    display_name = "监控告警"
    description = "设置监控告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            monitor_id = params.get("monitor_id", "")
            condition = params.get("condition", {})
            severity = params.get("severity", "warning")

            if not monitor_id:
                return ActionResult(success=False, message="monitor_id is required")

            alert_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "monitor_alerts"):
                context.monitor_alerts = {}
            context.monitor_alerts[alert_id] = {
                "alert_id": alert_id,
                "monitor_id": monitor_id,
                "condition": condition,
                "severity": severity,
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"alert_id": alert_id, "monitor_id": monitor_id, "severity": severity},
                message=f"Alert {alert_id} created for monitor {monitor_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor alert failed: {e}")


class MonitorDashboardAction(BaseAction):
    """Get monitoring dashboard data."""
    action_type = "monitor_dashboard"
    display_name = "监控仪表板"
    description = "获取监控仪表板数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            monitors = getattr(context, "api_monitors", {})

            summary = []
            for mid, m in monitors.items():
                uptime = (m["checks"] - m["failures"]) / m["checks"] if m["checks"] > 0 else 0
                summary.append({
                    "monitor_id": mid,
                    "name": m["name"],
                    "status": m["status"],
                    "checks": m["checks"],
                    "failures": m["failures"],
                    "uptime_pct": round(uptime * 100, 2),
                })

            return ActionResult(
                success=True,
                data={"monitors": summary, "total_monitors": len(summary)},
                message=f"Dashboard: {len(summary)} monitors",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor dashboard failed: {e}")


class MonitorDowntimeAction(BaseAction):
    """Track downtime events."""
    action_type = "monitor_downtime"
    display_name = "监控宕机"
    description = "追踪宕机事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            monitor_id = params.get("monitor_id", "")
            downtime_id = str(uuid.uuid4())[:8]
            started_at = params.get("started_at", time.time())
            resolved = params.get("resolved", False)
            resolved_at = time.time() if resolved else None

            if not monitor_id:
                return ActionResult(success=False, message="monitor_id is required")

            if not hasattr(context, "downtime_events"):
                context.downtime_events = []
            context.downtime_events.append({
                "downtime_id": downtime_id,
                "monitor_id": monitor_id,
                "started_at": started_at,
                "resolved": resolved,
                "resolved_at": resolved_at,
            })

            return ActionResult(
                success=True,
                data={"downtime_id": downtime_id, "monitor_id": monitor_id, "resolved": resolved},
                message=f"Downtime {downtime_id}: {'resolved' if resolved else 'ongoing'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor downtime failed: {e}")
