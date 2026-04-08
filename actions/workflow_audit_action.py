"""
Workflow Audit Action Module.

Audits workflow executions: tracks all actions, captures snapshots,
logs state changes, and generates compliance reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class AuditEntry:
    """A single audit log entry."""
    timestamp: float
    workflow_id: str
    action: str
    actor: str
    details: dict[str, Any]
    result: str  # success, failure, skipped
    duration_ms: float


@dataclass
class AuditReport:
    """Audit report for a workflow."""
    workflow_id: str
    total_entries: int
    success_count: int
    failure_count: int
    skipped_count: int
    entries: list[AuditEntry]
    duration_ms: float


class WorkflowAuditAction(BaseAction):
    """Audit workflow executions for compliance."""

    def __init__(self) -> None:
        super().__init__("workflow_audit")
        self._entries: list[AuditEntry] = []
        self._snapshots: dict[str, list[dict[str, Any]]] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Record an audit entry or generate report.

        Args:
            context: Execution context
            params: Parameters:
                - action: "log" or "report"
                - workflow_id: Workflow identifier
                - action_name: Name of action being audited
                - actor: Who performed the action
                - details: Additional details
                - result: success, failure, skipped
                - duration_ms: Action duration
                - snapshot: Optional state snapshot
                - report_since: Filter entries since timestamp

        Returns:
            For log action: confirmation
            For report action: AuditReport
        """
        import time

        action = params.get("action", "log")
        workflow_id = params.get("workflow_id", "default")
        timestamp = time.time()

        if action == "log":
            entry = AuditEntry(
                timestamp=timestamp,
                workflow_id=workflow_id,
                action=params.get("action_name", ""),
                actor=params.get("actor", "system"),
                details=params.get("details", {}),
                result=params.get("result", "success"),
                duration_ms=params.get("duration_ms", 0)
            )
            self._entries.append(entry)

            snapshot = params.get("snapshot")
            if snapshot:
                if workflow_id not in self._snapshots:
                    self._snapshots[workflow_id] = []
                self._snapshots[workflow_id].append({
                    "timestamp": timestamp,
                    "snapshot": snapshot
                })

            return {"logged": True, "entry_count": len(self._entries)}

        elif action == "report":
            report_since = params.get("report_since", 0)
            entries_filtered = [e for e in self._entries if e.workflow_id == workflow_id and e.timestamp >= report_since]
            success_count = sum(1 for e in entries_filtered if e.result == "success")
            failure_count = sum(1 for e in entries_filtered if e.result == "failure")
            skipped_count = sum(1 for e in entries_filtered if e.result == "skipped")
            total_duration = sum(e.duration_ms for e in entries_filtered)

            return AuditReport(
                workflow_id=workflow_id,
                total_entries=len(entries_filtered),
                success_count=success_count,
                failure_count=failure_count,
                skipped_count=skipped_count,
                entries=entries_filtered,
                duration_ms=total_duration
            )

        return {"error": f"Unknown action: {action}"}

    def get_entries(self, workflow_id: Optional[str] = None, limit: int = 100) -> list[AuditEntry]:
        """Get audit entries."""
        entries = self._entries
        if workflow_id:
            entries = [e for e in entries if e.workflow_id == workflow_id]
        return entries[-limit:]

    def get_snapshots(self, workflow_id: str) -> list[dict[str, Any]]:
        """Get state snapshots for a workflow."""
        return self._snapshots.get(workflow_id, [])

    def search(self, query: str, limit: int = 50) -> list[AuditEntry]:
        """Search audit entries by action or details."""
        results = []
        for entry in self._entries:
            if query.lower() in entry.action.lower():
                results.append(entry)
            elif query.lower() in str(entry.details).lower():
                results.append(entry)
            if len(results) >= limit:
                break
        return results
