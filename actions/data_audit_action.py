"""
Data Audit Action.

Provides data audit trail functionality.
Supports:
- Operation logging
- Change tracking
- Compliance reporting
- Access logging
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import threading
import logging
import json
import uuid

logger = logging.getLogger(__name__)


class AuditOperation(Enum):
    """Audit operation types."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    EXPORT = "export"
    IMPORT = "import"
    GRANT = "grant"
    REVOKE = "revoke"


@dataclass
class AuditEntry:
    """Audit log entry."""
    entry_id: str
    timestamp: datetime
    user_id: str
    operation: AuditOperation
    resource_type: str
    resource_id: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    status: str = "success"
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "operation": self.operation.value,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "status": self.status,
            "ip_address": self.ip_address,
            "metadata": self.metadata
        }


class DataAuditAction:
    """
    Data Audit Action.
    
    Provides audit trail functionality with support for:
    - Operation logging
    - Change tracking
    - Query and filtering
    - Compliance reports
    """
    
    def __init__(self, retention_days: int = 365):
        """
        Initialize the Data Audit Action.
        
        Args:
            retention_days: How long to retain audit logs
        """
        self.retention_days = retention_days
        self._entries: List[AuditEntry] = []
        self._lock = threading.RLock()
    
    def log(
        self,
        user_id: str,
        operation: AuditOperation,
        resource_type: str,
        resource_id: str,
        old_value: Optional[Any] = None,
        new_value: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        status: str = "success",
        error_message: Optional[str] = None
    ) -> AuditEntry:
        """
        Log an audit entry.
        
        Args:
            user_id: User performing the operation
            operation: Type of operation
            resource_type: Type of resource
            resource_id: ID of the resource
            old_value: Previous value (for updates/deletes)
            new_value: New value (for creates/updates)
            metadata: Additional metadata
            ip_address: Client IP address
            user_agent: Client user agent
            status: Operation status
            error_message: Error message if failed
        
        Returns:
            Created AuditEntry
        """
        entry = AuditEntry(
            entry_id=str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            user_id=user_id,
            operation=operation,
            resource_type=resource_type,
            resource_id=resource_id,
            old_value=old_value,
            new_value=new_value,
            metadata=metadata or {},
            ip_address=ip_address,
            user_agent=user_agent,
            status=status,
            error_message=error_message
        )
        
        with self._lock:
            self._entries.append(entry)
        
        logger.info(
            f"AUDIT: {user_id} {operation.value} {resource_type}/{resource_id}"
        )
        
        return entry
    
    def query(
        self,
        user_id: Optional[str] = None,
        operation: Optional[AuditOperation] = None,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[AuditEntry]:
        """
        Query audit entries.
        
        Args:
            user_id: Filter by user
            operation: Filter by operation type
            resource_type: Filter by resource type
            resource_id: Filter by resource ID
            start_time: Filter by start time
            end_time: Filter by end time
            status: Filter by status
            limit: Maximum results
        
        Returns:
            List of matching AuditEntries
        """
        with self._lock:
            results = self._entries.copy()
        
        if user_id:
            results = [e for e in results if e.user_id == user_id]
        if operation:
            results = [e for e in results if e.operation == operation]
        if resource_type:
            results = [e for e in results if e.resource_type == resource_type]
        if resource_id:
            results = [e for e in results if e.resource_id == resource_id]
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]
        if status:
            results = [e for e in results if e.status == status]
        
        return results[-limit:]
    
    def get_user_activity(
        self,
        user_id: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get activity summary for a user."""
        start_time = datetime.utcnow() - timedelta(days=days)
        entries = self.query(user_id=user_id, start_time=start_time, limit=10000)
        
        operations = {}
        resources = set()
        
        for entry in entries:
            operations[entry.operation.value] = operations.get(entry.operation.value, 0) + 1
            resources.add(f"{entry.resource_type}/{entry.resource_id}")
        
        return {
            "user_id": user_id,
            "period_days": days,
            "total_operations": len(entries),
            "operations_by_type": operations,
            "unique_resources": len(resources),
            "last_activity": entries[-1].timestamp.isoformat() if entries else None
        }
    
    def get_compliance_report(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Generate a compliance report."""
        entries = self.query(start_time=start_time, end_time=end_time, limit=100000)
        
        operations = {}
        users = set()
        resources = set()
        failures = []
        
        for entry in entries:
            operations[entry.operation.value] = operations.get(entry.operation.value, 0) + 1
            users.add(entry.user_id)
            resources.add(f"{entry.resource_type}/{entry.resource_id}")
            
            if entry.status == "failure":
                failures.append(entry.to_dict())
        
        return {
            "period": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "total_entries": len(entries),
            "unique_users": len(users),
            "unique_resources": len(resources),
            "operations_by_type": operations,
            "failure_count": len(failures),
            "failures": failures[:100]  # Limit to first 100
        }
    
    def prune_old_entries(self) -> int:
        """Remove entries older than retention period."""
        cutoff = datetime.utcnow() - timedelta(days=self.retention_days)
        
        with self._lock:
            before = len(self._entries)
            self._entries = [e for e in self._entries if e.timestamp >= cutoff]
            after = len(self._entries)
        
        pruned = before - after
        logger.info(f"Pruned {pruned} audit entries")
        return pruned
    
    def get_stats(self) -> Dict[str, Any]:
        """Get audit statistics."""
        with self._lock:
            total = len(self._entries)
            
            operations = {}
            for entry in self._entries:
                op = entry.operation.value
                operations[op] = operations.get(op, 0) + 1
            
            return {
                "total_entries": total,
                "operations_by_type": operations,
                "retention_days": self.retention_days
            }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    audit = DataAuditAction(retention_days=30)
    
    # Log some actions
    audit.log(
        user_id="user123",
        operation=AuditOperation.CREATE,
        resource_type="order",
        resource_id="ORD-001",
        new_value={"items": ["item1"]},
        ip_address="192.168.1.1"
    )
    
    audit.log(
        user_id="user123",
        operation=AuditOperation.UPDATE,
        resource_type="order",
        resource_id="ORD-001",
        old_value={"status": "pending"},
        new_value={"status": "shipped"},
        ip_address="192.168.1.1"
    )
    
    audit.log(
        user_id="admin",
        operation=AuditOperation.LOGIN,
        resource_type="system",
        resource_id="auth",
        ip_address="10.0.0.1"
    )
    
    # Query
    entries = audit.query(resource_type="order")
    print(f"Order entries: {len(entries)}")
    
    # User activity
    activity = audit.get_user_activity("user123")
    print(f"User activity: {json.dumps(activity, indent=2)}")
    
    # Stats
    print(f"Stats: {json.dumps(audit.get_stats(), indent=2)}")
