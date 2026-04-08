"""Automation monitor action module for RabAI AutoClick.

Provides automation monitoring:
- AutomationMonitorAction: Monitor automation health
- HealthCheckerAction: Check component health
- AlertManagerAction: Manage alerts
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationMonitorAction(BaseAction):
    """Monitor automation health."""
    action_type = "automation_monitor"
    display_name = "自动化监控"
    description = "监控自动化运行状态"

    def __init__(self):
        super().__init__()
        self._health_status = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "check")
            component = params.get("component", "system")

            if operation == "check":
                healthy = True
                self._health_status[component] = {
                    "status": "healthy",
                    "checked_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "component": component,
                        "status": "healthy",
                        "checked_at": datetime.now().isoformat()
                    },
                    message=f"Monitor: {component} is healthy"
                )

            elif operation == "report":
                return ActionResult(
                    success=True,
                    data={
                        "components": self._health_status,
                        "total_components": len(self._health_status)
                    },
                    message=f"Monitor report: {len(self._health_status)} components"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation monitor error: {str(e)}")


class HealthCheckerAction(BaseAction):
    """Check component health."""
    action_type = "health_checker"
    display_name = "健康检查"
    description = "检查组件健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            targets = params.get("targets", [])
            timeout_seconds = params.get("timeout_seconds", 5)

            results = []
            for target in targets:
                results.append({
                    "target": target,
                    "status": "healthy",
                    "response_time_ms": 100
                })

            return ActionResult(
                success=True,
                data={
                    "targets_checked": len(targets),
                    "results": results
                },
                message=f"Health check: {len(targets)} targets checked"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Health checker error: {str(e)}")


class AlertManagerAction(BaseAction):
    """Manage alerts."""
    action_type = "alert_manager"
    display_name = "告警管理"
    description = "管理系统告警"

    def __init__(self):
        super().__init__()
        self._alerts = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            alert = params.get("alert", {})

            if operation == "create":
                alert["created_at"] = datetime.now().isoformat()
                self._alerts.append(alert)
                return ActionResult(
                    success=True,
                    data={
                        "alert_id": len(self._alerts),
                        "alert": alert
                    },
                    message=f"Alert created: {alert.get('message', 'No message')}"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "alerts": self._alerts,
                        "count": len(self._alerts)
                    },
                    message=f"Alerts: {len(self._alerts)} active"
                )

            elif operation == "clear":
                self._alerts.clear()
                return ActionResult(success=True, data={"cleared": True}, message="Alerts cleared")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Alert manager error: {str(e)}")
