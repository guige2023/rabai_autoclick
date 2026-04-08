"""Audit logging action module for RabAI AutoClick.

Provides audit logging operations:
- AuditLogAction: Log audit event
- AuditQueryAction: Query audit logs
- AuditReportAction: Generate audit report
- AuditAlertAction: Audit-based alerting
- AuditExportAction: Export audit logs
- AuditRetentionAction: Manage retention
- AuditSearchAction: Search audit logs
- AuditSummaryAction: Audit summary
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AuditLogStore:
    """Audit log storage."""
    
    _logs: List[Dict[str, Any]] = []
    _max_size = 10000
    
    @classmethod
    def add(cls, log: Dict[str, Any]) -> None:
        cls._logs.append(log)
        if len(cls._logs) > cls._max_size:
            cls._logs = cls._logs[-cls._max_size:]
    
    @classmethod
    def query(cls, filters: Dict[str, Any] = None, limit: int = 1000) -> List[Dict[str, Any]]:
        if filters is None:
            return cls._logs[-limit:]
        
        results = cls._logs
        if "user" in filters:
            results = [r for r in results if r.get("user") == filters["user"]]
        if "action" in filters:
            results = [r for r in results if filters["action"] in r.get("action", "")]
        if "resource" in filters:
            results = [r for r in results if filters["resource"] in r.get("resource", "")]
        if "start_time" in filters:
            results = [r for r in results if r.get("timestamp", 0) >= filters["start_time"]]
        if "end_time" in filters:
            results = [r for r in results if r.get("timestamp", 0) <= filters["end_time"]]
        
        return results[-limit:]
    
    @classmethod
    def clear(cls) -> None:
        cls._logs.clear()


class AuditLogAction(BaseAction):
    """Log an audit event."""
    action_type = "audit_log"
    display_name = "审计日志"
    description = "记录审计事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            resource = params.get("resource", "")
            user = params.get("user", "system")
            result = params.get("result", "success")
            details = params.get("details", {})
            ip_address = params.get("ip_address", "")
            
            if not action:
                return ActionResult(success=False, message="action required")
            
            log_entry = {
                "id": len(AuditLogStore._logs) + 1,
                "timestamp": time.time(),
                "action": action,
                "resource": resource,
                "user": user,
                "result": result,
                "details": details,
                "ip_address": ip_address
            }
            
            AuditLogStore.add(log_entry)
            
            return ActionResult(
                success=True,
                message=f"Logged audit event: {action}",
                data={"log": log_entry}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit log failed: {str(e)}")


class AuditQueryAction(BaseAction):
    """Query audit logs."""
    action_type = "audit_query"
    display_name = "审计查询"
    description = "查询审计日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            user = params.get("user", "")
            action = params.get("action", "")
            resource = params.get("resource", "")
            start_time = params.get("start_time")
            end_time = params.get("end_time")
            limit = params.get("limit", 100)
            
            filters = {}
            if user:
                filters["user"] = user
            if action:
                filters["action"] = action
            if resource:
                filters["resource"] = resource
            if start_time:
                if isinstance(start_time, str):
                    filters["start_time"] = datetime.fromisoformat(start_time).timestamp()
                else:
                    filters["start_time"] = start_time
            if end_time:
                if isinstance(end_time, str):
                    filters["end_time"] = datetime.fromisoformat(end_time).timestamp()
                else:
                    filters["end_time"] = end_time
            
            results = AuditLogStore.query(filters, limit)
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} audit logs",
                data={"logs": results, "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit query failed: {str(e)}")


class AuditReportAction(BaseAction):
    """Generate audit report."""
    action_type = "audit_report"
    display_name = "审计报告"
    description = "生成审计报告"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            start_date = params.get("start_date", "")
            end_date = params.get("end_date", "")
            group_by = params.get("group_by", "action")
            
            filters = {}
            if start_date:
                filters["start_time"] = datetime.fromisoformat(start_date).timestamp()
            if end_date:
                filters["end_time"] = datetime.fromisoformat(end_date).timestamp()
            
            logs = AuditLogStore.query(filters, 10000)
            
            from collections import Counter
            if group_by == "action":
                groups = Counter(log.get("action", "") for log in logs)
            elif group_by == "user":
                groups = Counter(log.get("user", "") for log in logs)
            elif group_by == "resource":
                groups = Counter(log.get("resource", "") for log in logs)
            else:
                groups = Counter()
            
            total = len(logs)
            success = sum(1 for log in logs if log.get("result") == "success")
            failed = sum(1 for log in logs if log.get("result") == "failure")
            
            report = {
                "generated_at": datetime.now().isoformat(),
                "period": {"start": start_date, "end": end_date},
                "total_events": total,
                "success_count": success,
                "failure_count": failed,
                "group_by": group_by,
                "groups": dict(groups.most_common(20))
            }
            
            return ActionResult(
                success=True,
                message=f"Audit report: {total} events",
                data={"report": report}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit report failed: {str(e)}")


class AuditAlertAction(BaseAction):
    """Alert on audit events."""
    action_type = "audit_alert"
    display_name = "审计告警"
    description = "审计事件告警"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action_pattern = params.get("action_pattern", "")
            threshold = params.get("threshold", 10)
            window = params.get("window", 3600)
            
            if not action_pattern:
                return ActionResult(success=False, message="action_pattern required")
            
            cutoff = time.time() - window
            logs = [log for log in AuditLogStore._logs if log.get("timestamp", 0) >= cutoff]
            
            matching = [log for log in logs if action_pattern in log.get("action", "")]
            
            alerts = []
            if len(matching) >= threshold:
                alerts.append({
                    "pattern": action_pattern,
                    "count": len(matching),
                    "threshold": threshold,
                    "window": window,
                    "triggered": True
                })
            
            return ActionResult(
                success=len(alerts) == 0,
                message=f"Alert {'triggered' if alerts else 'not triggered'}: {len(matching)} matches",
                data={"alerts": alerts, "matching_count": len(matching)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit alert failed: {str(e)}")


class AuditExportAction(BaseAction):
    """Export audit logs."""
    action_type = "audit_export"
    display_name = "审计导出"
    description = "导出审计日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            output_path = params.get("output_path", "/tmp/audit_export.json")
            format = params.get("format", "json")
            limit = params.get("limit", 10000)
            
            logs = AuditLogStore.query(limit=limit)
            
            if format == "json":
                with open(output_path, "w") as f:
                    json.dump(logs, f, indent=2)
            elif format == "csv":
                import csv
                with open(output_path, "w", newline="") as f:
                    if logs:
                        writer = csv.DictWriter(f, fieldnames=logs[0].keys())
                        writer.writeheader()
                        writer.writerows(logs)
            
            return ActionResult(
                success=True,
                message=f"Exported {len(logs)} audit logs to {output_path}",
                data={"output_path": output_path, "count": len(logs), "format": format}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit export failed: {str(e)}")


class AuditRetentionAction(BaseAction):
    """Manage audit log retention."""
    action_type = "audit_retention"
    display_name = "审计保留"
    description = "管理审计日志保留期"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            retention_days = params.get("retention_days", 90)
            action = params.get("action", "check")
            
            cutoff = time.time() - (retention_days * 86400)
            
            if action == "clean":
                original_count = len(AuditLogStore._logs)
                AuditLogStore._logs = [log for log in AuditLogStore._logs if log.get("timestamp", 0) >= cutoff]
                deleted = original_count - len(AuditLogStore._logs)
                
                return ActionResult(
                    success=True,
                    message=f"Cleaned {deleted} old audit logs",
                    data={"deleted": deleted, "remaining": len(AuditLogStore._logs), "retention_days": retention_days}
                )
            
            elif action == "check":
                old_logs = [log for log in AuditLogStore._logs if log.get("timestamp", 0) < cutoff]
                
                return ActionResult(
                    success=True,
                    message=f"Found {len(old_logs)} logs older than {retention_days} days",
                    data={"old_count": len(old_logs), "total_count": len(AuditLogStore._logs), "retention_days": retention_days}
                )
            
            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Audit retention failed: {str(e)}")


class AuditSearchAction(BaseAction):
    """Search audit logs."""
    action_type = "audit_search"
    display_name = "审计搜索"
    description = "搜索审计日志"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", "")
            field = params.get("field", "action")
            limit = params.get("limit", 100)
            
            if not query:
                return ActionResult(success=False, message="query required")
            
            results = []
            for log in reversed(AuditLogStore._logs):
                if field in log and query.lower() in str(log[field]).lower():
                    results.append(log)
                    if len(results) >= limit:
                        break
            
            return ActionResult(
                success=True,
                message=f"Found {len(results)} matches for '{query}'",
                data={"results": results, "count": len(results), "query": query}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit search failed: {str(e)}")


class AuditSummaryAction(BaseAction):
    """Get audit summary."""
    action_type = "audit_summary"
    display_name = "审计摘要"
    description = "获取审计摘要"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            logs = AuditLogStore._logs
            
            from collections import Counter
            
            users = Counter(log.get("user", "") for log in logs)
            actions = Counter(log.get("action", "") for log in logs)
            resources = Counter(log.get("resource", "") for log in logs)
            
            last_24h = sum(1 for log in logs if time.time() - log.get("timestamp", 0) < 86400)
            last_hour = sum(1 for log in logs if time.time() - log.get("timestamp", 0) < 3600)
            
            summary = {
                "total_logs": len(logs),
                "last_24h": last_24h,
                "last_hour": last_hour,
                "top_users": dict(users.most_common(10)),
                "top_actions": dict(actions.most_common(10)),
                "top_resources": dict(resources.most_common(10))
            }
            
            return ActionResult(
                success=True,
                message=f"Audit summary: {len(logs)} total logs",
                data={"summary": summary}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Audit summary failed: {str(e)}")
