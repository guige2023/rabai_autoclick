"""
Backup and Disaster Recovery Utilities.

Provides utilities for managing backup schedules, verifying backup integrity,
and coordinating disaster recovery procedures across distributed systems.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tarfile
import tempfile
import zipfile
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional

import yaml


class BackupStatus(Enum):
    """Status of a backup operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"
    RESTORED = "restored"


class BackupType(Enum):
    """Types of backup operations."""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"
    SNAPSHOT = "snapshot"


class RecoveryPointObjective:
    """Represents a recovery point objective configuration."""

    def __init__(
        self,
        name: str,
        max_data_loss_seconds: int,
        backup_interval_seconds: int,
        retention_days: int,
    ) -> None:
        self.name = name
        self.max_data_loss_seconds = max_data_loss_seconds
        self.backup_interval_seconds = backup_interval_seconds
        self.retention_days = retention_days

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "max_data_loss_seconds": self.max_data_loss_seconds,
            "backup_interval_seconds": self.backup_interval_seconds,
            "retention_days": self.retention_days,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RecoveryPointObjective:
        return cls(
            name=data["name"],
            max_data_loss_seconds=data["max_data_loss_seconds"],
            backup_interval_seconds=data["backup_interval_seconds"],
            retention_days=data["retention_days"],
        )


class BackupManifest:
    """Manifest containing metadata about a backup."""

    def __init__(
        self,
        backup_id: str,
        backup_type: BackupType,
        source_paths: list[str],
        destination_path: str,
        status: BackupStatus,
        created_at: datetime,
        completed_at: Optional[datetime] = None,
        checksum: Optional[str] = None,
        size_bytes: int = 0,
        file_count: int = 0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        self.backup_id = backup_id
        self.backup_type = backup_type
        self.source_paths = source_paths
        self.destination_path = destination_path
        self.status = status
        self.created_at = created_at
        self.completed_at = completed_at
        self.checksum = checksum
        self.size_bytes = size_bytes
        self.file_count = file_count
        self.metadata = metadata or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "backup_type": self.backup_type.value,
            "source_paths": self.source_paths,
            "destination_path": self.destination_path,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "file_count": self.file_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackupManifest:
        return cls(
            backup_id=data["backup_id"],
            backup_type=BackupType(data["backup_type"]),
            source_paths=data["source_paths"],
            destination_path=data["destination_path"],
            status=BackupStatus(data["status"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            checksum=data.get("checksum"),
            size_bytes=data.get("size_bytes", 0),
            file_count=data.get("file_count", 0),
            metadata=data.get("metadata", {}),
        )


class BackupManager:
    """Manages backup operations for files and directories."""

    def __init__(
        self,
        storage_path: Path,
        manifest_db_path: Optional[Path] = None,
    ) -> None:
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.manifest_db_path = manifest_db_path or self.storage_path / "manifests.db"
        self._init_manifest_db()

    def _init_manifest_db(self) -> None:
        """Initialize the manifest database."""
        conn = sqlite3.connect(str(self.manifest_db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backups (
                backup_id TEXT PRIMARY KEY,
                manifest_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def compute_checksum(self, file_path: Path, algorithm: str = "sha256") -> str:
        """Compute checksum of a file."""
        hash_func = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_func.update(chunk)
        return hash_func.hexdigest()

    def create_backup(
        self,
        source_paths: list[str | Path],
        backup_name: Optional[str] = None,
        backup_type: BackupType = BackupType.FULL,
        compression: str = "gzip",
        exclude_patterns: Optional[list[str]] = None,
    ) -> BackupManifest:
        """Create a new backup from source paths."""
        backup_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]}"
        backup_name = backup_name or backup_id

        manifest = BackupManifest(
            backup_id=backup_id,
            backup_type=backup_type,
            source_paths=[str(p) for p in source_paths],
            destination_path=str(self.storage_path / f"{backup_name}.tar.gz"),
            status=BackupStatus.IN_PROGRESS,
            created_at=datetime.now(),
        )

        try:
            archive_path = self.storage_path / f"{backup_name}.tar.gz"
            exclude_patterns = exclude_patterns or []

            with tarfile.open(archive_path, f"w:{compression}") as tar:
                for source_path in source_paths:
                    source = Path(source_path)
                    if not source.exists():
                        raise FileNotFoundError(f"Source path not found: {source}")

                    for item in source.rglob("*"):
                        if item.is_file():
                            should_exclude = False
                            for pattern in exclude_patterns:
                                if pattern in str(item):
                                    should_exclude = True
                                    break
                            if not should_exclude:
                                tar.add(item, arcname=str(item.relative_to(source.parent)))

            manifest.checksum = self.compute_checksum(archive_path)
            manifest.size_bytes = archive_path.stat().st_size
            manifest.status = BackupStatus.COMPLETED
            manifest.completed_at = datetime.now()

            self._save_manifest(manifest)

        except Exception as e:
            manifest.status = BackupStatus.FAILED
            manifest.metadata["error"] = str(e)
            self._save_manifest(manifest)
            raise

        return manifest

    def restore_backup(
        self,
        backup_id: str,
        destination: Path,
        verify_checksum: bool = True,
    ) -> BackupManifest:
        """Restore a backup to a destination path."""
        manifest = self.get_manifest(backup_id)
        if not manifest:
            raise ValueError(f"Backup manifest not found: {backup_id}")

        archive_path = Path(manifest.destination_path)
        if not archive_path.exists():
            raise FileNotFoundError(f"Backup archive not found: {archive_path}")

        if verify_checksum:
            current_checksum = self.compute_checksum(archive_path)
            if current_checksum != manifest.checksum:
                raise ValueError("Backup checksum verification failed")

        destination.mkdir(parents=True, exist_ok=True)

        with tarfile.open(archive_path, "r:*") as tar:
            tar.extractall(destination)

        manifest.status = BackupStatus.RESTORED
        self._save_manifest(manifest)

        return manifest

    def verify_backup(self, backup_id: str) -> bool:
        """Verify the integrity of a backup."""
        manifest = self.get_manifest(backup_id)
        if not manifest:
            return False

        archive_path = Path(manifest.destination_path)
        if not archive_path.exists():
            return False

        current_checksum = self.compute_checksum(archive_path)
        if current_checksum != manifest.checksum:
            return False

        manifest.status = BackupStatus.VERIFIED
        self._save_manifest(manifest)
        return True

    def list_backups(
        self,
        status: Optional[BackupStatus] = None,
        limit: int = 100,
    ) -> list[BackupManifest]:
        """List backups, optionally filtered by status."""
        conn = sqlite3.connect(str(self.manifest_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if status:
            cursor.execute(
                "SELECT manifest_json FROM backups ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            rows = cursor.fetchall()
            manifests = []
            for row in rows:
                manifest = BackupManifest.from_dict(json.loads(row["manifest_json"]))
                if manifest.status == status:
                    manifests.append(manifest)
        else:
            cursor.execute(
                "SELECT manifest_json FROM backups ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            rows = cursor.fetchall()
            manifests = [BackupManifest.from_dict(json.loads(row["manifest_json"])) for row in rows]

        conn.close()
        return manifests

    def get_manifest(self, backup_id: str) -> Optional[BackupManifest]:
        """Get a backup manifest by ID."""
        conn = sqlite3.connect(str(self.manifest_db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT manifest_json FROM backups WHERE backup_id = ?", (backup_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return BackupManifest.from_dict(json.loads(row["manifest_json"]))
        return None

    def _save_manifest(self, manifest: BackupManifest) -> None:
        """Save a backup manifest to the database."""
        conn = sqlite3.connect(str(self.manifest_db_path))
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO backups (backup_id, manifest_json, created_at) VALUES (?, ?, ?)",
            (manifest.backup_id, json.dumps(manifest.to_dict()), datetime.now()),
        )
        conn.commit()
        conn.close()

    def cleanup_old_backups(self, retention_days: int = 30) -> int:
        """Remove backups older than retention period."""
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        backups = self.list_backups()
        removed_count = 0

        for manifest in backups:
            if manifest.created_at < cutoff_date:
                archive_path = Path(manifest.destination_path)
                if archive_path.exists():
                    archive_path.unlink()
                conn = sqlite3.connect(str(self.manifest_db_path))
                cursor = conn.cursor()
                cursor.execute("DELETE FROM backups WHERE backup_id = ?", (manifest.backup_id,))
                conn.commit()
                conn.close()
                removed_count += 1

        return removed_count
