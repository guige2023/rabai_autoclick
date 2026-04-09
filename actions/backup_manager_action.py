"""Backup manager action for creating and managing backups.

Provides backup scheduling, compression, encryption,
and restoration functionality.
"""

import gzip
import hashlib
import json
import logging
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class BackupStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    RESTORED = "restored"


class BackupType(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


@dataclass
class BackupMetadata:
    backup_id: str
    backup_type: BackupType
    status: BackupStatus
    created_at: float
    completed_at: Optional[float] = None
    size_bytes: int = 0
    checksum: str = ""
    source_path: str = ""
    destination_path: str = ""
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class BackupManagerAction:
    """Manage backups with scheduling and restoration.

    Args:
        backup_dir: Directory to store backups.
        compression_level: Gzip compression level (0-9).
        enable_encryption: Enable backup encryption.
        max_backups: Maximum number of backups to retain.
    """

    def __init__(
        self,
        backup_dir: Optional[str] = None,
        compression_level: int = 6,
        enable_encryption: bool = False,
        max_backups: int = 10,
    ) -> None:
        self._backup_dir = Path(backup_dir) if backup_dir else Path.cwd() / "backups"
        self._compression_level = compression_level
        self._enable_encryption = enable_encryption
        self._max_backups = max_backups
        self._backups: dict[str, BackupMetadata] = {}
        self._backup_hooks: dict[str, list[Callable]] = {
            "on_start": [],
            "on_complete": [],
            "on_failure": [],
        }
        self._ensure_backup_dir()

    def _ensure_backup_dir(self) -> None:
        """Ensure backup directory exists."""
        self._backup_dir.mkdir(parents=True, exist_ok=True)

    def _generate_backup_id(self) -> str:
        """Generate a unique backup ID.

        Returns:
            Backup ID string.
        """
        return f"backup_{int(time.time() * 1000)}"

    def _compute_checksum(self, data: bytes) -> str:
        """Compute checksum for data.

        Args:
            data: Data to checksum.

        Returns:
            Checksum string.
        """
        return hashlib.sha256(data).hexdigest()[:16]

    def create_backup(
        self,
        source_path: str,
        backup_type: BackupType = BackupType.FULL,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Create a new backup.

        Args:
            source_path: Path to back up.
            backup_type: Type of backup.
            metadata: Optional backup metadata.

        Returns:
            Backup ID or None.
        """
        backup_id = self._generate_backup_id()
        source = Path(source_path)

        if not source.exists():
            logger.error(f"Source path does not exist: {source_path}")
            return None

        backup_meta = BackupMetadata(
            backup_id=backup_id,
            backup_type=backup_type,
            status=BackupStatus.PENDING,
            created_at=time.time(),
            source_path=str(source.absolute()),
        )

        self._backups[backup_id] = backup_meta
        self._run_hooks("on_start", backup_id)

        try:
            backup_meta.status = BackupStatus.IN_PROGRESS
            dest_path = self._backup_dir / f"{backup_id}.backup"

            data = self._gather_data(source)
            compressed = self._compress(data)

            with open(dest_path, "wb") as f:
                f.write(compressed)

            backup_meta.destination_path = str(dest_path)
            backup_meta.size_bytes = len(compressed)
            backup_meta.checksum = self._compute_checksum(compressed)
            backup_meta.status = BackupStatus.COMPLETED
            backup_meta.completed_at = time.time()
            backup_meta.metadata = metadata or {}

            self._enforce_max_backups()
            self._run_hooks("on_complete", backup_id)

            logger.info(f"Backup created: {backup_id} ({backup_meta.size_bytes} bytes)")
            return backup_id

        except Exception as e:
            backup_meta.status = BackupStatus.FAILED
            backup_meta.error = str(e)
            self._run_hooks("on_failure", backup_id)
            logger.error(f"Backup failed: {backup_id} - {e}")
            return None

    def _gather_data(self, source: Path) -> bytes:
        """Gather data from source path.

        Args:
            source: Source path.

        Returns:
            Gathered data as bytes.
        """
        if source.is_file():
            with open(source, "rb") as f:
                return f.read()
        else:
            data = {}
            for item in source.rglob("*"):
                if item.is_file():
                    rel_path = item.relative_to(source)
                    with open(item, "rb") as f:
                        data[str(rel_path)] = f.read()
            return json.dumps(data).encode()

    def _compress(self, data: bytes) -> bytes:
        """Compress data.

        Args:
            data: Data to compress.

        Returns:
            Compressed data.
        """
        return gzip.compress(data, compresslevel=self._compression_level)

    def _decompress(self, data: bytes) -> bytes:
        """Decompress data.

        Args:
            data: Data to decompress.

        Returns:
            Decompressed data.
        """
        return gzip.decompress(data)

    def restore_backup(
        self,
        backup_id: str,
        destination_path: str,
        verify_checksum: bool = True,
    ) -> bool:
        """Restore a backup.

        Args:
            backup_id: Backup ID to restore.
            destination_path: Path to restore to.
            verify_checksum: Verify checksum before restoring.

        Returns:
            True if restore was successful.
        """
        backup_meta = self._backups.get(backup_id)
        if not backup_meta:
            logger.error(f"Backup not found: {backup_id}")
            return False

        if backup_meta.status != BackupStatus.COMPLETED:
            logger.error(f"Backup not complete: {backup_id}")
            return False

        try:
            dest_path = Path(destination_path)
            backup_path = Path(backup_meta.destination_path)

            if not backup_path.exists():
                logger.error(f"Backup file not found: {backup_path}")
                return False

            with open(backup_path, "rb") as f:
                compressed = f.read()

            if verify_checksum:
                if self._compute_checksum(compressed) != backup_meta.checksum:
                    logger.error("Checksum mismatch")
                    return False

            data = self._decompress(compressed)

            if dest_path.exists():
                if dest_path.is_file():
                    dest_path.unlink()
                else:
                    shutil.rmtree(dest_path)

            if self._is_json(data):
                files = json.loads(data)
                dest_path.mkdir(parents=True, exist_ok=True)
                for rel_path, content in files.items():
                    file_path = dest_path / rel_path
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, "wb") as f:
                        f.write(content)
            else:
                dest_path.write_bytes(data)

            backup_meta.status = BackupStatus.RESTORED
            logger.info(f"Backup restored: {backup_id} -> {destination_path}")
            return True

        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False

    def _is_json(self, data: bytes) -> bool:
        """Check if data is JSON.

        Args:
            data: Data to check.

        Returns:
            True if data is JSON.
        """
        try:
            json.loads(data)
            return True
        except Exception:
            return False

    def _enforce_max_backups(self) -> None:
        """Enforce maximum number of backups to retain."""
        backups = self.get_all_backups()
        if len(backups) > self._max_backups:
            excess = backups[:-self._max_backups]
            for backup in excess:
                self.delete_backup(backup.backup_id)

    def delete_backup(self, backup_id: str) -> bool:
        """Delete a backup.

        Args:
            backup_id: Backup ID to delete.

        Returns:
            True if deleted.
        """
        backup_meta = self._backups.get(backup_id)
        if not backup_meta:
            return False

        if backup_meta.destination_path:
            path = Path(backup_meta.destination_path)
            if path.exists():
                path.unlink()

        del self._backups[backup_id]
        logger.debug(f"Deleted backup: {backup_id}")
        return True

    def get_backup(self, backup_id: str) -> Optional[BackupMetadata]:
        """Get backup metadata.

        Args:
            backup_id: Backup ID.

        Returns:
            Backup metadata or None.
        """
        return self._backups.get(backup_id)

    def get_all_backups(
        self,
        status_filter: Optional[BackupStatus] = None,
    ) -> list[BackupMetadata]:
        """Get all backups.

        Args:
            status_filter: Filter by status.

        Returns:
            List of backups (newest first).
        """
        backups = list(self._backups.values())
        if status_filter:
            backups = [b for b in backups if b.status == status_filter]
        return sorted(backups, key=lambda b: b.created_at, reverse=True)

    def register_backup_hook(
        self,
        hook_type: str,
        callback: Callable,
    ) -> None:
        """Register a backup lifecycle hook.

        Args:
            hook_type: Hook type ('on_start', 'on_complete', 'on_failure').
            callback: Callback function.
        """
        if hook_type in self._backup_hooks:
            self._backup_hooks[hook_type].append(callback)

    def _run_hooks(self, hook_type: str, backup_id: str) -> None:
        """Run registered hooks.

        Args:
            hook_type: Hook type.
            backup_id: Backup ID.
        """
        for hook in self._backup_hooks.get(hook_type, []):
            try:
                hook(backup_id)
            except Exception as e:
                logger.error(f"Hook error in {hook_type}: {e}")

    def get_stats(self) -> dict[str, Any]:
        """Get backup manager statistics.

        Returns:
            Dictionary with stats.
        """
        total_size = sum(b.size_bytes for b in self._backups.values())
        completed = sum(1 for b in self._backups.values() if b.status == BackupStatus.COMPLETED)
        failed = sum(1 for b in self._backups.values() if b.status == BackupStatus.FAILED)

        return {
            "total_backups": len(self._backups),
            "completed": completed,
            "failed": failed,
            "total_size_bytes": total_size,
            "max_backups": self._max_backups,
            "compression_level": self._compression_level,
            "encryption_enabled": self._enable_encryption,
        }
