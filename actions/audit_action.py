"""Audit action module for RabAI AutoClick.

Provides audit logging utilities:
- AuditLogger: Structured audit logging
- AuditTrail: Track action history
- AuditFormatter: Format audit records
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import threading
import json
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class AuditRecord:
    """Audit log record."""
    record_id: str
    timestamp: str
    action: str
    user: str
    resource: str
    result: str
    metadata: Dict[str, Any] = field(default_factory=dict)


class AuditLogger:
    """Thread-safe audit logger."""

    def __init__(self, app_name: str = "rabai"):
        self.app_name = app_name
        self._records: List[AuditRecord] = []
        self._lock = threading.RLock()
        self._max_records = 10000

    def log(self, action: str, user: str, resource: str, result: str, metadata: Optional[Dict[str, Any]] = None) -> str:
        """Log an audit record."""
        with self._lock:
            record_id = str(uuid.uuid4())
            record = AuditRecord(
                record_id=record_id,
                timestamp=datetime.now().isoformat(),
                action=action,
                user=user,
                resource=resource,
                result=result,
                metadata=metadata or {},
            )
            self._records.append(record)

            if len(self._records) > self._max_records:
                self._records = self._records[-self._max_records:]

            return record_id

    def query(
        self,
        action: Optional[str] = None,
        user: Optional[str] = None,
        resource: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 100,
    ) -> List[AuditRecord]:
        """Query audit records."""
        with self._lock:
            results = self._records

            if action:
                results = [r for r in results if r.action == action]
            if user:
                results = [r for r in results if r.user == user]
            if resource:
                results = [r for r in results if r.resource == resource]
            if start_time:
                results = [r for r in results if r.timestamp >= start_time]
            if end_time:
                results = [r for r in results if r.timestamp <= end_time]

            return results[-limit:]

    def get_record(self, record_id: str) -> Optional[AuditRecord]:
        """Get a specific record."""
        with self._lock:
            for record in reversed(self._records):
                if record.record_id == record_id:
                    return record
            return None

    def count(self) -> int:
        """Get total record count."""
        with self._lock:
            return len(self._records)

    def clear(self) -> None:
        """Clear all records."""
        with self._lock:
            self._records.clear()


class AuditFormatter:
    """Format audit records."""

    @staticmethod
    def to_json(record: AuditRecord) -> str:
        """Format as JSON."""
        return json.dumps({
            "record_id": record.record_id,
            "timestamp": record.timestamp,
            "action": record.action,
            "user": record.user,
            "resource": record.resource,
            "result": record.result,
            "metadata": record.metadata,
        }, indent=2)

    @staticmethod
    def to_text(record: AuditRecord) -> str:
        """Format as text."""
        lines = [
            f"ID: {record.record_id}",
            f"Time: {record.timestamp}",
            f"Action: {record.action}",
            f"User: {record.user}",
            f"Resource: {record.resource}",
            f"Result: {record.result}",
        ]
        if record.metadata:
            lines.append(f"Metadata: {json.dumps(record.metadata)}")
        return "\n".join(lines)


class AuditAction(BaseAction):
    """Audit logging action."""
    action_type = "audit"
    display_name = "审计日志"
    description = "操作审计"

    def __init__(self):
        super().__init__()
        self._logger = AuditLogger()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "log")

            if operation == "log":
                return self._log(params)
            elif operation == "query":
                return self._query(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "count":
                return self._count(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "export":
                return self._export(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Audit error: {str(e)}")

    def _log(self, params: Dict[str, Any]) -> ActionResult:
        """Log an audit record."""
        action = params.get("action")
        user = params.get("user", "system")
        resource = params.get("resource")
        result = params.get("result", "success")
        metadata = params.get("metadata")

        if not action or not resource:
            return ActionResult(success=False, message="action and resource are required")

        record_id = self._logger.log(action, user, resource, result, metadata)

        return ActionResult(success=True, message=f"Logged: {record_id}", data={"record_id": record_id})

    def _query(self, params: Dict[str, Any]) -> ActionResult:
        """Query audit records."""
        action = params.get("action")
        user = params.get("user")
        resource = params.get("resource")
        start_time = params.get("start_time")
        end_time = params.get("end_time")
        limit = params.get("limit", 100)

        records = self._logger.query(
            action=action,
            user=user,
            resource=resource,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

        formatted = [AuditFormatter.to_json(r) for r in records]

        return ActionResult(success=True, message=f"Found {len(records)} records", data={"count": len(records), "records": formatted})

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get a specific record."""
        record_id = params.get("record_id")

        if not record_id:
            return ActionResult(success=False, message="record_id is required")

        record = self._logger.get_record(record_id)

        if not record:
            return ActionResult(success=False, message="Record not found")

        return ActionResult(success=True, message="Record found", data={"record": AuditFormatter.to_json(record)})

    def _count(self, params: Dict[str, Any]) -> ActionResult:
        """Get record count."""
        count = self._logger.count()
        return ActionResult(success=True, message=f"Total records: {count}", data={"count": count})

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear all records."""
        self._logger.clear()
        return ActionResult(success=True, message="Audit log cleared")

    def _export(self, params: Dict[str, Any]) -> ActionResult:
        """Export all records as JSON."""
        format_type = params.get("format", "json")

        records = self._logger.query(limit=self._logger.count())

        if format_type == "json":
            data = [AuditFormatter.to_json(r) for r in records]
        else:
            data = [AuditFormatter.to_text(r) for r in records]

        return ActionResult(success=True, message=f"Exported {len(data)} records", data={"records": data})
