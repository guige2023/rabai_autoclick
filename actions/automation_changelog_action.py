"""
Automation Changelog Action Module.

Change tracking and audit logging for automation workflows
with diff generation and change history.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Type of change."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    ROLLBACK = "rollback"


@dataclass
class ChangeRecord:
    """
    Single change record.

    Attributes:
        change_id: Unique change identifier.
        entity_type: Type of entity changed.
        entity_id: ID of changed entity.
        change_type: Type of change.
        old_value: Previous value (for update/delete).
        new_value: New value (for create/update).
        diff: Computed diff between old and new.
        timestamp: When change occurred.
        user: User who made the change.
        reason: Optional reason for change.
    """
    change_id: str
    entity_type: str
    entity_id: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    diff: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time, init=False)
    user: str = "system"
    reason: str = ""


@dataclass
class ChangelogStats:
    """Changelog statistics."""
    total_changes: int
    by_type: dict
    by_entity: dict
    recent_changes: int


class AutomationChangelogAction:
    """
    Tracks and logs changes for automation workflows.

    Example:
        changelog = AutomationChangelogAction()
        changelog.record_change("config", "app", ChangeType.UPDATE, old_val, new_val)
        history = changelog.get_history(entity_type="config", entity_id="app")
    """

    def __init__(self, max_records: int = 10000):
        """
        Initialize changelog action.

        Args:
            max_records: Maximum records to keep in memory.
        """
        self.max_records = max_records
        self._changes: list[ChangeRecord] = []
        self._entity_index: dict[str, list[int]] = {}

    def record_change(
        self,
        entity_type: str,
        entity_id: str,
        change_type: ChangeType,
        old_value: Any = None,
        new_value: Any = None,
        user: str = "system",
        reason: str = ""
    ) -> ChangeRecord:
        """
        Record a change.

        Args:
            entity_type: Type of entity.
            entity_id: ID of entity.
            change_type: Type of change.
            old_value: Previous value.
            new_value: New value.
            user: User who made change.
            reason: Reason for change.

        Returns:
            Created ChangeRecord.
        """
        import uuid

        record = ChangeRecord(
            change_id=str(uuid.uuid4())[:8],
            entity_type=entity_type,
            entity_id=entity_id,
            change_type=change_type,
            old_value=old_value,
            new_value=new_value,
            user=user,
            reason=reason
        )

        record.diff = self._compute_diff(old_value, new_value)

        self._changes.append(record)

        key = f"{entity_type}:{entity_id}"
        if key not in self._entity_index:
            self._entity_index[key] = []
        self._entity_index[key].append(len(self._changes) - 1)

        if len(self._changes) > self.max_records:
            self._compact()

        logger.debug(f"Recorded change: {change_type.value} on {key}")
        return record

    def get_history(
        self,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        change_type: Optional[ChangeType] = None,
        user: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100
    ) -> list[ChangeRecord]:
        """
        Get change history with filters.

        Args:
            entity_type: Filter by entity type.
            entity_id: Filter by entity ID.
            change_type: Filter by change type.
            user: Filter by user.
            since: Filter by timestamp.
            limit: Maximum records to return.

        Returns:
            List of matching ChangeRecords.
        """
        results = self._changes

        if entity_type or entity_id:
            key = f"{entity_type or ''}:{entity_id or ''}".rstrip(":")
            indices = self._entity_index.get(key, [])
            results = [self._changes[i] for i in indices if i < len(self._changes)]

        if change_type:
            results = [r for r in results if r.change_type == change_type]

        if user:
            results = [r for r in results if r.user == user]

        if since:
            results = [r for r in results if r.timestamp >= since]

        return results[-limit:]

    def get_entity_version(
        self,
        entity_type: str,
        entity_id: str,
        version: int
    ) -> Optional[Any]:
        """
        Get historical version of an entity.

        Args:
            entity_type: Entity type.
            entity_id: Entity ID.
            version: Version number (1 = oldest).

        Returns:
            Entity value at that version, or None.
        """
        history = self.get_history(entity_type, entity_id, limit=version * 2)

        creates = [r for r in history if r.change_type == ChangeType.CREATE]
        updates = [r for r in history if r.change_type == ChangeType.UPDATE]
        deletes = [r for r in history if r.change_type == ChangeType.DELETE]

        all_changes = sorted(
            creates + updates + deletes,
            key=lambda r: r.timestamp
        )

        if version > len(all_changes):
            return None

        target = all_changes[version - 1]

        if target.change_type == ChangeType.CREATE:
            return target.new_value
        elif target.change_type == ChangeType.UPDATE:
            return target.new_value
        elif target.change_type == ChangeType.DELETE:
            return target.old_value

        return None

    def rollback(
        self,
        entity_type: str,
        entity_id: str,
        version: int,
        user: str = "system"
    ) -> Optional[ChangeRecord]:
        """
        Rollback entity to previous version.

        Args:
            entity_type: Entity type.
            entity_id: Entity ID.
            version: Version to rollback to.
            user: User performing rollback.

        Returns:
            ChangeRecord for rollback, or None.
        """
        target_version = self.get_entity_version(entity_type, entity_id, version)

        if target_version is None:
            logger.error(f"Cannot rollback: version {version} not found")
            return None

        current = self._get_current_value(entity_type, entity_id)

        return self.record_change(
            entity_type=entity_type,
            entity_id=entity_id,
            change_type=ChangeType.ROLLBACK,
            old_value=current,
            new_value=target_version,
            user=user,
            reason=f"Rollback to version {version}"
        )

    def _get_current_value(self, entity_type: str, entity_id: str) -> Optional[Any]:
        """Get current value of entity from history."""
        history = self.get_history(entity_type, entity_id)

        creates = [r for r in history if r.change_type == ChangeType.CREATE]
        updates = [r for r in history if r.change_type == ChangeType.UPDATE]
        deletes = [r for r in history if r.change_type == ChangeType.DELETE]

        if creates:
            latest_create = max(creates, key=lambda r: r.timestamp)

            if deletes and max(deletes, key=lambda r: r.timestamp).timestamp > latest_create.timestamp:
                return None

            latest_update = max(updates, key=lambda r: r.timestamp) if updates else None

            if latest_update and latest_update.timestamp > latest_create.timestamp:
                return latest_update.new_value

            return latest_create.new_value

        return None

    def _compute_diff(self, old_value: Any, new_value: Any) -> dict:
        """Compute diff between old and new values."""
        diff = {"changed": [], "added": [], "removed": []}

        if old_value is None:
            diff["added"] = self._flatten(new_value) if new_value else []
            return diff

        if new_value is None:
            diff["removed"] = self._flatten(old_value) if old_value else []
            return diff

        old_flat = self._flatten(old_value)
        new_flat = self._flatten(new_value)

        old_keys = set(old_flat.keys())
        new_keys = set(new_flat.keys())

        for key in old_keys & new_keys:
            if old_flat[key] != new_flat[key]:
                diff["changed"].append({
                    "key": key,
                    "old": old_flat[key],
                    "new": new_flat[key]
                })

        diff["added"] = [{"key": k, "value": new_flat[k]} for k in new_keys - old_keys]
        diff["removed"] = [{"key": k, "value": old_flat[k]} for k in old_keys - new_keys]

        return diff

    def _flatten(self, data: Any, prefix: str = "") -> dict:
        """Flatten nested data structure."""
        result = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}.{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    result.update(self._flatten(value, new_key))
                else:
                    result[new_key] = value

        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}[{i}]"
                if isinstance(item, (dict, list)):
                    result.update(self._flatten(item, new_key))
                else:
                    result[new_key] = item

        else:
            result[prefix] = data

        return result

    def _compact(self) -> None:
        """Compact old records to stay under max_records."""
        keep = self.max_records // 2
        self._changes = self._changes[-keep:]
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Rebuild entity index after compaction."""
        self._entity_index.clear()

        for i, record in enumerate(self._changes):
            key = f"{record.entity_type}:{record.entity_id}"
            if key not in self._entity_index:
                self._entity_index[key] = []
            self._entity_index[key].append(i)

    def get_stats(self) -> ChangelogStats:
        """Get changelog statistics."""
        by_type = {}
        by_entity = {}

        for record in self._changes:
            ct = record.change_type.value
            by_type[ct] = by_type.get(ct, 0) + 1

            key = f"{record.entity_type}:{record.entity_id}"
            by_entity[key] = by_entity.get(key, 0) + 1

        recent_cutoff = time.time() - 86400
        recent = sum(1 for r in self._changes if r.timestamp >= recent_cutoff)

        return ChangelogStats(
            total_changes=len(self._changes),
            by_type=by_type,
            by_entity=by_entity,
            recent_changes=recent
        )

    def export(self, path: str) -> None:
        """Export changelog to JSON file."""
        data = [
            {
                "change_id": r.change_id,
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "change_type": r.change_type.value,
                "old_value": r.old_value,
                "new_value": r.new_value,
                "timestamp": r.timestamp,
                "user": r.user,
                "reason": r.reason
            }
            for r in self._changes
        ]

        with open(path, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Exported {len(data)} changelog records to {path}")

    def clear(self) -> None:
        """Clear all changelog records."""
        self._changes.clear()
        self._entity_index.clear()
