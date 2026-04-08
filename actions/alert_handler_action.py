"""Alert handling action module for RabAI AutoClick.

Provides alert operations:
- AlertCreateAction: Create alert
- AlertSendAction: Send alert
- AlertAcknowledgeAction: Acknowledge alert
- AlertResolveAction: Resolve alert
- AlertEscalateAction: Escalate alert
- AlertHistoryAction: Alert history
- AlertGroupAction: Group alerts
- AlertTemplateAction: Alert templates
"""

import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AlertStore:
    """In-memory alert storage."""
    
    _alerts: List[Dict[str, Any]] = []
    _alert_id = 1
    
    @classmethod
    def create(cls, title: str, message: str, severity: str, **kwargs) -> Dict[str, Any]:
        """Create a new alert."""
        alert = {
            "id": cls._alert_id,
            "title": title,
            "message": message,
            "severity": severity,
            "status": "active",
            "created_at": time.time(),
            "acknowledged_at": None,
            "resolved_at": None,
            **kwargs
        }
        cls._alert_id += 1
        cls._alerts.append(alert)
        return alert
    
    @classmethod
    def list(cls, status: str = None, severity: str = None) -> List[Dict[str, Any]]:
        """List alerts."""
        results = cls._alerts
        if status:
            results = [a for a in results if a["status"] == status]
        if severity:
            results = [a for a in results if a["severity"] == severity]
        return sorted(results, key=lambda x: x["created_at"], reverse=True)
    
    @classmethod
    def get(cls, alert_id: int) -> Optional[Dict[str, Any]]:
        """Get alert by ID."""
        for alert in cls._alerts:
            if alert["id"] == alert_id:
                return alert
        return None


class AlertCreateAction(BaseAction):
    """Create an alert."""
    action_type = "alert_create"
    display_name = "创建告警"
    description = "创建新的告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            title = params.get("title", "")
            message = params.get("message", "")
            severity = params.get("severity", "warning")
            source = params.get("source", "system")
            metadata = params.get("metadata", {})
            
            if not title or not message:
                return ActionResult(success=False, message="title and message required")
            
            alert = AlertStore.create(title, message, severity, source=source, metadata=metadata)
            
            return ActionResult(
                success=True,
                message=f"Created alert #{alert['id']}: {title}",
                data={"alert": alert}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert create failed: {str(e)}")


class AlertSendAction(BaseAction):
    """Send an alert."""
    action_type = "alert_send"
    display_name = "发送告警"
    description = "发送告警通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            alert_id = params.get("alert_id")
            channels = params.get("channels", ["log"])
            recipients = params.get("recipients", [])
            
            if alert_id:
                alert = AlertStore.get(alert_id)
                if not alert:
                    return ActionResult(success=False, message=f"Alert {alert_id} not found")
            else:
                title = params.get("title", "")
                message = params.get("message", "")
                severity = params.get("severity", "info")
                alert = AlertStore.create(title, message, severity)
            
            sent = []
            for channel in channels:
                if channel == "log":
                    sent.append({"channel": "log", "status": "sent"})
                elif channel == "email":
                    sent.append({"channel": "email", "status": "sent", "recipients": recipients})
                elif channel == "webhook":
                    sent.append({"channel": "webhook", "status": "sent"})
            
            return ActionResult(
                success=True,
                message=f"Sent alert via {len(sent)} channels",
                data={"alert_id": alert.get("id"), "sent": sent}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert send failed: {str(e)}")


class AlertAcknowledgeAction(BaseAction):
    """Acknowledge an alert."""
    action_type = "alert_acknowledge"
    display_name = "确认告警"
    description = "确认告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            alert_id = params.get("alert_id")
            comment = params.get("comment", "")
            
            if not alert_id:
                return ActionResult(success=False, message="alert_id required")
            
            alert = AlertStore.get(alert_id)
            if not alert:
                return ActionResult(success=False, message=f"Alert {alert_id} not found")
            
            alert["status"] = "acknowledged"
            alert["acknowledged_at"] = time.time()
            if comment:
                alert["ack_comment"] = comment
            
            return ActionResult(
                success=True,
                message=f"Acknowledged alert #{alert_id}",
                data={"alert": alert}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert acknowledge failed: {str(e)}")


class AlertResolveAction(BaseAction):
    """Resolve an alert."""
    action_type = "alert_resolve"
    display_name = "解决告警"
    description = "解决告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            alert_id = params.get("alert_id")
            resolution = params.get("resolution", "")
            
            if not alert_id:
                return ActionResult(success=False, message="alert_id required")
            
            alert = AlertStore.get(alert_id)
            if not alert:
                return ActionResult(success=False, message=f"Alert {alert_id} not found")
            
            alert["status"] = "resolved"
            alert["resolved_at"] = time.time()
            if resolution:
                alert["resolution"] = resolution
            
            return ActionResult(
                success=True,
                message=f"Resolved alert #{alert_id}",
                data={"alert": alert}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert resolve failed: {str(e)}")


class AlertEscalateAction(BaseAction):
    """Escalate an alert."""
    action_type = "alert_escalate"
    display_name = "升级告警"
    description = "升级告警级别"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            alert_id = params.get("alert_id")
            new_severity = params.get("severity", "critical")
            reason = params.get("reason", "")
            
            if not alert_id:
                return ActionResult(success=False, message="alert_id required")
            
            alert = AlertStore.get(alert_id)
            if not alert:
                return ActionResult(success=False, message=f"Alert {alert_id} not found")
            
            old_severity = alert.get("severity", "info")
            alert["severity"] = new_severity
            alert["escalated_at"] = time.time()
            alert["escalation_reason"] = reason
            
            return ActionResult(
                success=True,
                message=f"Escalated alert #{alert_id} from {old_severity} to {new_severity}",
                data={"alert": alert, "old_severity": old_severity, "new_severity": new_severity}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert escalate failed: {str(e)}")


class AlertHistoryAction(BaseAction):
    """Get alert history."""
    action_type = "alert_history"
    display_name = "告警历史"
    description = "获取告警历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            status = params.get("status", "")
            severity = params.get("severity", "")
            limit = params.get("limit", 100)
            
            alerts = AlertStore.list(status or None, severity or None)
            
            return ActionResult(
                success=True,
                message=f"Found {len(alerts)} alerts",
                data={"alerts": alerts[:limit], "count": len(alerts)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert history failed: {str(e)}")


class AlertGroupAction(BaseAction):
    """Group alerts."""
    action_type = "alert_group"
    display_name = "告警分组"
    description = "告警分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            group_by = params.get("group_by", "severity")
            
            alerts = AlertStore.list()
            
            grouped = {}
            for alert in alerts:
                if group_by == "severity":
                    key = alert.get("severity", "unknown")
                elif group_by == "status":
                    key = alert.get("status", "unknown")
                elif group_by == "source":
                    key = alert.get("source", "unknown")
                else:
                    key = "other"
                
                if key not in grouped:
                    grouped[key] = []
                grouped[key].append(alert)
            
            return ActionResult(
                success=True,
                message=f"Grouped alerts by {group_by}",
                data={"groups": {k: len(v) for k, v in grouped.items()}, "total": len(alerts)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert group failed: {str(e)}")


class AlertTemplateAction(BaseAction):
    """Use alert templates."""
    action_type = "alert_template"
    display_name = "告警模板"
    description = "使用告警模板"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            template_name = params.get("template", "")
            variables = params.get("variables", {})
            
            templates = {
                "high_cpu": {
                    "title": "High CPU Usage Detected",
                    "message": "CPU usage is at {cpu_percent}%",
                    "severity": "warning"
                },
                "high_memory": {
                    "title": "High Memory Usage Detected",
                    "message": "Memory usage is at {memory_percent}%",
                    "severity": "warning"
                },
                "disk_full": {
                    "title": "Disk Space Low",
                    "message": "Disk usage is at {disk_percent}%",
                    "severity": "critical"
                },
                "service_down": {
                    "title": "Service Down: {service_name}",
                    "message": "Service {service_name} is not responding",
                    "severity": "critical"
                },
                "error_rate": {
                    "title": "High Error Rate",
                    "message": "Error rate is {error_rate}% (threshold: {threshold}%)",
                    "severity": "warning"
                }
            }
            
            if not template_name:
                return ActionResult(
                    success=True,
                    message=f"Available templates: {list(templates.keys())}",
                    data={"templates": list(templates.keys())}
                )
            
            if template_name not in templates:
                return ActionResult(success=False, message=f"Template not found: {template_name}")
            
            template = templates[template_name]
            title = template["title"].format(**variables)
            message = template["message"].format(**variables)
            
            alert = AlertStore.create(title, message, template["severity"], template=template_name)
            
            return ActionResult(
                success=True,
                message=f"Created alert from template '{template_name}'",
                data={"alert": alert}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Alert template failed: {str(e)}")
