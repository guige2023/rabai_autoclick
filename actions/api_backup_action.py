"""API Backup Action Module.

Provides API data backup, restore, and disaster recovery
with versioning, compression, and integrity verification.
"""

import time
import hashlib
import threading
import sys
import os
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BackupSnapshot:
    """Backup snapshot metadata."""
    snapshot_id: str
    created_at: float
    size_bytes: int
    checksum: str
    resource_count: int
    compression: str
    retention_days: int
    tags: List[str]


@dataclass
class BackupEntry:
    """Individual backup entry."""
    resource_id: str
    resource_type: str
    data: Any
    backed_up_at: float
    version: int


class ApiBackupAction(BaseAction):
    """API Backup Manager.

    Manages API data backup, restore, and disaster recovery
    with versioning and integrity verification.
    """
    action_type = "api_backup"
    display_name = "API备份管理器"
    description = "API数据备份与恢复，支持版本控制和完整性验证"

    _backups: Dict[str, List[BackupSnapshot]] = {}
    _entries: Dict[str, Dict[str, BackupEntry]] = {}
    _lock = threading.RLock()
    _default_retention_days: int = 30

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute backup operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: str - 'backup', 'restore', 'list', 'delete',
                               'verify', 'cleanup', 'export', 'import'
                - resource_id: str - resource to backup
                - resource_type: str - type of resource
                - data: Any - data to backup
                - snapshot_id: str (optional) - specific snapshot ID
                - retention_days: int (optional) - retention period
                - compression: str (optional) - 'gzip', 'lz4', 'none'
                - tags: list (optional) - backup tags

        Returns:
            ActionResult with backup operation result.
        """
        start_time = time.time()
        operation = params.get('operation', 'backup')

        try:
            with self._lock:
                if operation == 'backup':
                    return self._create_backup(params, start_time)
                elif operation == 'restore':
                    return self._restore_backup(params, start_time)
                elif operation == 'list':
                    return self._list_backups(params, start_time)
                elif operation == 'delete':
                    return self._delete_backup(params, start_time)
                elif operation == 'verify':
                    return self._verify_backup(params, start_time)
                elif operation == 'cleanup':
                    return self._cleanup_old_backups(params, start_time)
                elif operation == 'export':
                    return self._export_backup(params, start_time)
                elif operation == 'import':
                    return self._import_backup(params, start_time)
                else:
                    return ActionResult(
                        success=False,
                        message=f"Unknown operation: {operation}",
                        duration=time.time() - start_time
                    )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Backup error: {str(e)}",
                duration=time.time() - start_time
            )

    def _create_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a backup snapshot."""
        resource_id = params.get('resource_id', 'default')
        resource_type = params.get('resource_type', 'generic')
        data = params.get('data', {})
        retention_days = params.get('retention_days', self._default_retention_days)
        compression = params.get('compression', 'gzip')
        tags = params.get('tags', [])

        snapshot_id = self._generate_snapshot_id(resource_id, data)
        checksum = self._compute_checksum(data)
        size_bytes = len(str(data))

        entry = BackupEntry(
            resource_id=resource_id,
            resource_type=resource_type,
            data=data,
            backed_up_at=time.time(),
            version=1
        )

        if resource_id not in self._entries:
            self._entries[resource_id] = {}
        if resource_type not in self._entries[resource_id]:
            self._entries[resource_id][resource_type] = {}
        self._entries[resource_id][resource_type][snapshot_id] = entry

        snapshot = BackupSnapshot(
            snapshot_id=snapshot_id,
            created_at=time.time(),
            size_bytes=size_bytes,
            checksum=checksum,
            resource_count=1,
            compression=compression,
            retention_days=retention_days,
            tags=tags
        )

        if resource_id not in self._backups:
            self._backups[resource_id] = []
        self._backups[resource_id].append(snapshot)

        return ActionResult(
            success=True,
            message=f"Backup created for {resource_id}",
            data={
                'snapshot_id': snapshot_id,
                'checksum': checksum,
                'size_bytes': size_bytes,
                'created_at': snapshot.created_at,
                'retention_days': retention_days,
            },
            duration=time.time() - start_time
        )

    def _restore_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Restore from a backup snapshot."""
        resource_id = params.get('resource_id', 'default')
        resource_type = params.get('resource_type', 'generic')
        snapshot_id = params.get('snapshot_id')

        if resource_id not in self._entries:
            return ActionResult(success=False, message=f"No backups found for {resource_id}", duration=time.time() - start_time)

        if resource_type not in self._entries[resource_id]:
            return ActionResult(success=False, message=f"No backups found for {resource_type}", duration=time.time() - start_time)

        if snapshot_id and snapshot_id in self._entries[resource_id][resource_type]:
            entry = self._entries[resource_id][resource_type][snapshot_id]
            return ActionResult(
                success=True,
                message=f"Restored from snapshot {snapshot_id}",
                data={'data': entry.data, 'resource_id': resource_id, 'snapshot_id': snapshot_id, 'version': entry.version},
                duration=time.time() - start_time
            )

        backups = self._backups.get(resource_id, [])
        if not backups:
            return ActionResult(success=False, message="No backups available", duration=time.time() - start_time)

        latest = backups[-1]
        entry = self._entries[resource_id][resource_type][latest.snapshot_id]

        return ActionResult(
            success=True,
            message=f"Restored latest backup for {resource_id}",
            data={'data': entry.data, 'resource_id': resource_id, 'snapshot_id': latest.snapshot_id, 'version': entry.version},
            duration=time.time() - start_time
        )

    def _list_backups(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all backups for a resource."""
        resource_id = params.get('resource_id')
        include_data = params.get('include_data', False)

        if resource_id:
            backups = self._backups.get(resource_id, [])
        else:
            all_backups = []
            for rid, snaps in self._backups.items():
                for snap in snaps:
                    all_backups.append((rid, snap))
            all_backups.sort(key=lambda x: x[1].created_at, reverse=True)
            backups = [s for _, s in all_backups[:100]]

        return ActionResult(
            success=True,
            message=f"Found {len(backups)} backup snapshots",
            data={
                'count': len(backups),
                'backups': [
                    {
                        'snapshot_id': s.snapshot_id,
                        'created_at': s.created_at,
                        'size_bytes': s.size_bytes,
                        'checksum': s.checksum,
                        'compression': s.compression,
                        'retention_days': s.retention_days,
                        'tags': s.tags,
                    } for s in backups
                ]
            },
            duration=time.time() - start_time
        )

    def _delete_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a backup snapshot."""
        resource_id = params.get('resource_id', 'default')
        resource_type = params.get('resource_type', 'generic')
        snapshot_id = params.get('snapshot_id')

        if resource_id not in self._backups:
            return ActionResult(success=False, message=f"No backups for {resource_id}", duration=time.time() - start_time)

        if snapshot_id:
            self._backups[resource_id] = [s for s in self._backups[resource_id] if s.snapshot_id != snapshot_id]
            if resource_id in self._entries and resource_type in self._entries[resource_id]:
                self._entries[resource_id][resource_type].pop(snapshot_id, None)
            return ActionResult(success=True, message=f"Deleted snapshot {snapshot_id}", data={'deleted': snapshot_id}, duration=time.time() - start_time)

        deleted = len(self._backups[resource_id])
        del self._backups[resource_id]
        if resource_id in self._entries:
            del self._entries[resource_id]

        return ActionResult(success=True, message=f"Deleted {deleted} backups for {resource_id}", data={'deleted_count': deleted}, duration=time.time() - start_time)

    def _verify_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Verify backup integrity."""
        resource_id = params.get('resource_id', 'default')
        snapshot_id = params.get('snapshot_id')

        if resource_id not in self._entries:
            return ActionResult(success=False, message=f"No backup data for {resource_id}", duration=time.time() - start_time)

        for resource_type, snapshots in self._entries[resource_id].items():
            if snapshot_id and snapshot_id in snapshots:
                entry = snapshots[snapshot_id]
                computed = self._compute_checksum(entry.data)
                backup_snap = next((s for s in self._backups.get(resource_id, []) if s.snapshot_id == snapshot_id), None)
                original_checksum = backup_snap.checksum if backup_snap else computed
                return ActionResult(
                    success=True,
                    message="Verification passed" if computed == original_checksum else "Checksum mismatch",
                    data={'verified': computed == original_checksum, 'computed_checksum': computed, 'original_checksum': original_checksum},
                    duration=time.time() - start_time
                )

        return ActionResult(success=False, message="Backup not found", duration=time.time() - start_time)

    def _cleanup_old_backups(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Clean up expired backups based on retention policy."""
        max_age_seconds = params.get('max_age_seconds', self._default_retention_days * 86400)
        cutoff = time.time() - max_age_seconds

        total_deleted = 0
        resources_cleaned = []

        for resource_id in list(self._backups.keys()):
            original_count = len(self._backups[resource_id])
            self._backups[resource_id] = [s for s in self._backups[resource_id] if s.created_at >= cutoff]
            deleted = original_count - len(self._backups[resource_id])

            if deleted > 0:
                total_deleted += deleted
                resources_cleaned.append(resource_id)

            if not self._backups[resource_id]:
                del self._backups[resource_id]
                if resource_id in self._entries:
                    del self._entries[resource_id]

        return ActionResult(
            success=True,
            message=f"Cleaned up {total_deleted} expired backups",
            data={'deleted_count': total_deleted, 'resources_cleaned': resources_cleaned},
            duration=time.time() - start_time
        )

    def _export_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Export backup metadata and data."""
        resource_id = params.get('resource_id', 'default')
        include_entries = params.get('include_entries', True)

        if resource_id not in self._backups:
            return ActionResult(success=False, message=f"No backups for {resource_id}", duration=time.time() - start_time)

        export_data = {
            'resource_id': resource_id,
            'exported_at': time.time(),
            'snapshots': [
                {'snapshot_id': s.snapshot_id, 'created_at': s.created_at,
                 'size_bytes': s.size_bytes, 'checksum': s.checksum, 'tags': s.tags}
                for s in self._backups[resource_id]
            ]
        }

        if include_entries and resource_id in self._entries:
            export_data['entries'] = self._entries[resource_id]

        return ActionResult(
            success=True,
            message=f"Exported backup for {resource_id}",
            data={'export_size_bytes': len(str(export_data)), 'resource_id': resource_id},
            duration=time.time() - start_time
        )

    def _import_backup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Import backup data."""
        imported_data = params.get('import_data', {})
        resource_id = imported_data.get('resource_id', 'default')
        snapshots = imported_data.get('snapshots', [])
        entries = imported_data.get('entries', {})

        if resource_id not in self._backups:
            self._backups[resource_id] = []
        if resource_id not in self._entries:
            self._entries[resource_id] = {}

        for snap_data in snapshots:
            from dataclasses import asdict
            snapshot = BackupSnapshot(**snap_data)
            self._backups[resource_id].append(snapshot)

        for rtype, snaps in entries.items():
            if rtype not in self._entries[resource_id]:
                self._entries[resource_id][rtype] = {}
            self._entries[resource_id][rtype].update(snaps)

        return ActionResult(
            success=True,
            message=f"Imported backup for {resource_id}",
            data={'imported_snapshots': len(snapshots), 'resource_id': resource_id},
            duration=time.time() - start_time
        )

    def _generate_snapshot_id(self, resource_id: str, data: Any) -> str:
        """Generate a unique snapshot ID."""
        content = f"{resource_id}:{str(data)}:{time.time()}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _compute_checksum(self, data: Any) -> str:
        """Compute checksum for data integrity."""
        content = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()
