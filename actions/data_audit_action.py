"""
Data Audit Action Module.

Data auditing and compliance tracking with field-level
change tracking, PII detection, and audit report generation.
"""

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Audit event types."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    EXPORT = "export"
    ACCESS = "access"


class PIIFieldType(Enum):
    """PII field type classifications."""
    NAME = "name"
    EMAIL = "email"
    PHONE = "phone"
    ADDRESS = "address"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    PASSWORD = "password"
    BANK_ACCOUNT = "bank_account"
    CUSTOM = "custom"


@dataclass
class AuditEntry:
    """
    Single audit entry.

    Attributes:
        entry_id: Unique entry identifier.
        event_type: Type of audit event.
        entity_type: Type of entity affected.
        entity_id: ID of entity affected.
        user: User who performed action.
        timestamp: When event occurred.
        changes: Dict of field changes.
        pii_accessed: Whether PII was accessed.
        metadata: Additional metadata.
    """
    entry_id: str
    event_type: AuditEventType
    entity_type: str
    entity_id: str
    user: str
    timestamp: float = field(default_factory=time.time, init=False)
    changes: dict = field(default_factory=dict)
    pii_accessed: bool = False
    pii_fields: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class PIIField:
    """PII field definition."""
    name: str
    field_type: PIIFieldType
    sensitivity_level: int = 1
    anonymize_func: Optional[Callable[[str], str]] = None


@dataclass
class AuditReport:
    """Audit report summary."""
    total_events: int
    by_type: dict
    by_user: dict
    pii_access_count: int
    time_range: tuple


class DataAuditAction:
    """
    Data auditing and compliance tracking.

    Example:
        auditor = DataAuditAction()
        auditor.register_pii_field("email", PIIFieldType.EMAIL)
        auditor.log_event(AuditEventType.UPDATE, "user", "123", changes=delta)
    """

    def __init__(self):
        """Initialize data audit action."""
        self._entries: list[AuditEntry] = []
        self._pii_fields: dict[str, PIIField] = {}
        self._entity_index: dict[str, list[int]] = {}
        self._max_entries = 100000

    def register_pii_field(
        self,
        field_name: str,
        field_type: PIIFieldType,
        sensitivity_level: int = 1,
        anonymize_func: Optional[Callable[[str], str]] = None
    ) -> PIIField:
        """
        Register a PII field.

        Args:
            field_name: Field name pattern.
            field_type: Type of PII.
            sensitivity_level: Sensitivity 1-5.
            anonymize_func: Optional anonymization function.

        Returns:
            Created PIIField.
        """
        pii_field = PIIField(
            name=field_name,
            field_type=field_type,
            sensitivity_level=sensitivity_level,
            anonymize_func=anonymize_func
        )

        self._pii_fields[field_name] = pii_field
        logger.debug(f"Registered PII field: {field_name}")
        return pii_field

    def log_event(
        self,
        event_type: AuditEventType,
        entity_type: str,
        entity_id: str,
        user: str = "system",
        changes: Optional[dict] = None,
        metadata: Optional[dict] = None
    ) -> AuditEntry:
        """
        Log an audit event.

        Args:
            event_type: Type of event.
            entity_type: Entity type.
            entity_id: Entity ID.
            user: User performing action.
            changes: Dict of changes.
            metadata: Additional metadata.

        Returns:
            Created AuditEntry.
        """
        import uuid

        entry = AuditEntry(
            entry_id=str(uuid.uuid4())[:12],
            event_type=event_type,
            entity_type=entity_type,
            entity_id=entity_id,
            user=user,
            changes=changes or {},
            metadata=metadata or {}
        )

        entry.pii_accessed, entry.pii_fields = self._detect_pii(changes or {})

        self._entries.append(entry)

        key = f"{entity_type}:{entity_id}"
        if key not in self._entity_index:
            self._entity_index[key] = []
        self._entity_index[key].append(len(self._entries) - 1)

        if len(self._entries) > self._max_entries:
            self._compact()

        logger.debug(f"Audit logged: {event_type.value} on {key} by {user}")
        return entry

    def _detect_pii(self, data: dict) -> tuple[bool, list[str]]:
        """Detect PII fields in data."""
        pii_fields = []
        has_pii = False

        for key, value in data.items():
            if key.lower() in self._pii_fields:
                pii_fields.append(key)
                has_pii = True
                continue

            key_lower = key.lower()
            if any(pattern in key_lower for pattern in ["email", "phone", "ssn", "password", "address"]):
                pii_fields.append(key)
                has_pii = True

        return has_pii, pii_fields

    def anonymize_value(self, value: str, field_type: PIIFieldType) -> str:
        """
        Anonymize a PII value.

        Args:
            value: Value to anonymize.
            field_type: Type of PII.

        Returns:
            Anonymized string.
        """
        if not value:
            return "***"

        if field_type == PIIFieldType.EMAIL:
            parts = value.split("@")
            if len(parts) == 2:
                return f"{parts[0][0]}***@{parts[1]}"

        elif field_type == PIIFieldType.PHONE:
            return f"***-***-{value[-4:]}"

        elif field_type == PIIFieldType.SSN:
            return f"***-**-{value[-4:]}"

        elif field_type == PIIFieldType.CREDIT_CARD:
            return f"****-****-****-{value[-4:]}"

        elif field_type == PIIFieldType.NAME:
            return value[0] + "***"

        return hashlib.sha256(value.encode()).hexdigest()[:8]

    def get_entity_history(
        self,
        entity_type: str,
        entity_id: str,
        limit: int = 100
    ) -> list[AuditEntry]:
        """
        Get audit history for an entity.

        Args:
            entity_type: Entity type.
            entity_id: Entity ID.
            limit: Maximum entries to return.

        Returns:
            List of AuditEntries.
        """
        key = f"{entity_type}:{entity_id}"
        indices = self._entity_index.get(key, [])

        entries = []
        for idx in reversed(indices[-limit:]):
            if idx < len(self._entries):
                entries.append(self._entries[idx])

        return entries

    def get_user_activity(
        self,
        user: str,
        limit: int = 100
    ) -> list[AuditEntry]:
        """
        Get audit entries for a user.

        Args:
            user: Username.
            limit: Maximum entries.

        Returns:
            List of AuditEntries.
        """
        user_entries = [e for e in reversed(self._entries) if e.user == user]
        return user_entries[:limit]

    def get_pii_access_log(
        self,
        since: Optional[float] = None
    ) -> list[AuditEntry]:
        """
        Get entries where PII was accessed.

        Args:
            since: Timestamp to filter from.

        Returns:
            List of AuditEntries with PII access.
        """
        entries = [e for e in self._entries if e.pii_accessed]

        if since:
            entries = [e for e in entries if e.timestamp >= since]

        return entries

    def generate_report(
        self,
        since: Optional[float] = None,
        until: Optional[float] = None
    ) -> AuditReport:
        """
        Generate audit report.

        Args:
            since: Start timestamp.
            until: End timestamp.

        Returns:
            AuditReport summary.
        """
        entries = self._entries

        if since:
            entries = [e for e in entries if e.timestamp >= since]
        if until:
            entries = [e for e in entries if e.timestamp <= until]

        by_type: dict = {}
        by_user: dict = {}
        pii_count = 0

        for entry in entries:
            event_type = entry.event_type.value
            by_type[event_type] = by_type.get(event_type, 0) + 1

            by_user[entry.user] = by_user.get(entry.user, 0) + 1

            if entry.pii_accessed:
                pii_count += 1

        timestamps = [e.timestamp for e in entries] if entries else [time.time()]

        return AuditReport(
            total_events=len(entries),
            by_type=by_type,
            by_user=by_user,
            pii_access_count=pii_count,
            time_range=(min(timestamps), max(timestamps)) if timestamps else (0, 0)
        )

    def verify_integrity(
        self,
        entity_type: str,
        entity_id: str,
        expected_hash: str
    ) -> bool:
        """
        Verify entity data integrity.

        Args:
            entity_type: Entity type.
            entity_id: Entity ID.
            expected_hash: Expected hash value.

        Returns:
            True if integrity verified.
        """
        history = self.get_entity_history(entity_type, entity_id)

        if not history:
            return False

        combined = "|".join(str(e.timestamp) for e in history)
        actual_hash = hashlib.sha256(combined.encode()).hexdigest()

        return actual_hash == expected_hash

    def export_audit_log(
        self,
        path: str,
        format: str = "json",
        since: Optional[float] = None
    ) -> None:
        """
        Export audit log to file.

        Args:
            path: Output file path.
            format: Export format (json/csv).
            since: Export entries since timestamp.
        """
        entries = self._entries

        if since:
            entries = [e for e in entries if e.timestamp >= since]

        if format == "json":
            import json
            data = [
                {
                    "entry_id": e.entry_id,
                    "event_type": e.event_type.value,
                    "entity_type": e.entity_type,
                    "entity_id": e.entity_id,
                    "user": e.user,
                    "timestamp": e.timestamp,
                    "changes": e.changes,
                    "pii_accessed": e.pii_accessed,
                    "metadata": e.metadata
                }
                for e in entries
            ]
            with open(path, "w") as f:
                json.dump(data, f, indent=2)

        logger.info(f"Exported {len(entries)} audit entries to {path}")

    def _compact(self) -> None:
        """Compact old entries."""
        keep = self._max_entries // 2
        self._entries = self._entries[-keep:]
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Rebuild entity index after compaction."""
        self._entity_index.clear()

        for i, entry in enumerate(self._entries):
            key = f"{entry.entity_type}:{entry.entity_id}"
            if key not in self._entity_index:
                self._entity_index[key] = []
            self._entity_index[key].append(i)

    def get_stats(self) -> dict:
        """Get audit statistics."""
        return {
            "total_entries": len(self._entries),
            "pii_fields_registered": len(self._pii_fields),
            "entities_tracked": len(self._entity_index),
            "max_entries": self._max_entries
        }

    def clear(self) -> None:
        """Clear all audit entries."""
        self._entries.clear()
        self._entity_index.clear()
