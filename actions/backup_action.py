"""
Backup action for creating and managing data backups.

This module provides actions for creating backups, managing backup
retention policies, and restore operations.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class BackupType(Enum):
    """Types of backup operations."""
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class BackupStatus(Enum):
    """Status of a backup operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"


class CompressionType(Enum):
    """Compression types for backups."""
    NONE = "none"
    GZIP = "gzip"
    BZIP2 = "bzip2"
    XZ = "xz"
    ZIP = "zip"


@dataclass
class BackupMetadata:
    """Metadata for a backup."""
    id: str
    name: str
    backup_type: BackupType
    status: BackupStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    source_paths: List[str] = field(default_factory=list)
    destination_path: Optional[str] = None
    file_count: int = 0
    total_size_bytes: int = 0
    compressed_size_bytes: int = 0
    compression_ratio: float = 0.0
    checksum_md5: Optional[str] = None
    checksum_sha256: Optional[str] = None
    parent_backup_id: Optional[str] = None
    retention_days: Optional[int] = None
    tags: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    metadata_: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "backup_type": self.backup_type.value,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "source_paths": self.source_paths,
            "destination_path": self.destination_path,
            "file_count": self.file_count,
            "total_size_bytes": self.total_size_bytes,
            "compressed_size_bytes": self.compressed_size_bytes,
            "compression_ratio": self.compression_ratio,
            "checksum_md5": self.checksum_md5,
            "checksum_sha256": self.checksum_sha256,
            "parent_backup_id": self.parent_backup_id,
            "retention_days": self.retention_days,
            "tags": self.tags,
            "error_message": self.error_message,
            "metadata": self.metadata_,
        }


@dataclass
class BackupConfig:
    """Configuration for backup operations."""
    backup_type: BackupType = BackupType.FULL
    compression: CompressionType = CompressionType.GZIP
    compression_level: int = 6
    verify_after_backup: bool = True
    calculate_checksums: bool = True
    follow_symlinks: bool = False
    exclude_patterns: Optional[List[str]] = None
    include_patterns: Optional[List[str]] = None
    retention_days: Optional[int] = None
    max_backup_size: Optional[int] = None
    parallel_files: int = 4


class BackupManager:
    """
    Manages backup creation, retention, and restoration.

    Supports full, incremental, and differential backups with
    configurable compression and verification.
    """

    def __init__(self, storage_path: Optional[Union[str, Path]] = None):
        """
        Initialize the backup manager.

        Args:
            storage_path: Path to store backups.
        """
        self._storage_path = Path(storage_path) if storage_path else Path.home() / ".backups"
        self._storage_path.mkdir(parents=True, exist_ok=True)
        self._backups: Dict[str, BackupMetadata] = {}
        self._lock = threading.RLock()
        self._load_metadata()

    def create_backup(
        self,
        name: str,
        source_paths: List[Union[str, Path]],
        config: Optional[BackupConfig] = None,
        destination: Optional[Union[str, Path]] = None,
    ) -> BackupMetadata:
        """
        Create a new backup.

        Args:
            name: Name for the backup.
            source_paths: List of paths to back up.
            config: Backup configuration.
            destination: Optional custom destination path.

        Returns:
            BackupMetadata for the created backup.

        Raises:
            ValueError: If source paths are invalid.
            IOError: If backup creation fails.
        """
        config = config or BackupConfig()
        backup_id = str(uuid.uuid4())

        metadata = BackupMetadata(
            id=backup_id,
            name=name,
            backup_type=config.backup_type,
            status=BackupStatus.IN_PROGRESS,
            created_at=datetime.now(),
            source_paths=[str(p) for p in source_paths],
            retention_days=config.retention_days,
        )

        if destination:
            dest_path = Path(destination)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{name}_{timestamp}.tar.gz"
            dest_path = self._storage_path / backup_name

        metadata.destination_path = str(dest_path)

        try:
            total_size, file_count = self._calculate_size(source_paths)

            if config.max_backup_size and total_size > config.max_backup_size:
                raise ValueError(
                    f"Backup size {total_size} exceeds maximum {config.max_backup_size}"
                )

            metadata.total_size_bytes = total_size
            metadata.file_count = file_count

            archive_path = self._create_archive(
                source_paths,
                dest_path,
                config,
            )

            compressed_size = archive_path.stat().st_size
            metadata.compressed_size_bytes = compressed_size
            metadata.compression_ratio = (
                (total_size - compressed_size) / total_size * 100
                if total_size > 0 else 0
            )

            if config.calculate_checksums:
                checksums = self._calculate_checksums(archive_path)
                metadata.checksum_md5 = checksums["md5"]
                metadata.checksum_sha256 = checksums["sha256"]

            metadata.status = BackupStatus.COMPLETED
            metadata.completed_at = datetime.now()

            if config.verify_after_backup:
                if self._verify_backup(archive_path, config):
                    metadata.status = BackupStatus.VERIFIED
                else:
                    metadata.status = BackupStatus.COMPLETED

        except Exception as e:
            metadata.status = BackupStatus.FAILED
            metadata.error_message = str(e)
            raise

        finally:
            with self._lock:
                self._backups[backup_id] = metadata
                self._save_metadata()

        return metadata

    def _create_archive(
        self,
        source_paths: List[Union[str, Path]],
        dest_path: Path,
        config: BackupConfig,
    ) -> Path:
        """Create the backup archive."""
        mode_map = {
            CompressionType.NONE: "w",
            CompressionType.GZIP: "w:gz",
            CompressionType.BZIP2: "w:bz2",
            CompressionType.XZ: "w:xz",
        }

        mode = mode_map.get(config.compression, "w:gz")

        if config.compression == CompressionType.ZIP:
            return self._create_zip(source_paths, dest_path, config)
        else:
            return self._create_tar(source_paths, dest_path, mode, config)

    def _create_tar(
        self,
        source_paths: List[Union[str, Path]],
        dest_path: Path,
        mode: str,
        config: BackupConfig,
    ) -> Path:
        """Create a tar archive backup."""
        with tarfile.open(dest_path, mode) as tar:
            for source in source_paths:
                source = Path(source)
                if source.exists():
                    tar.add(
                        source,
                        arcname=source.name,
                        recursive=True,
                        filter=lambda x: None if self._should_exclude(x.name, config) else x,
                    )
        return dest_path

    def _create_zip(
        self,
        source_paths: List[Union[str, Path]],
        dest_path: Path,
        config: BackupConfig,
    ) -> Path:
        """Create a zip archive backup."""
        with zipfile.ZipFile(dest_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for source in source_paths:
                source = Path(source)
                if source.exists():
                    if source.is_file():
                        zf.write(source, source.name)
                    else:
                        for root, dirs, files in os.walk(source):
                            for file in files:
                                file_path = Path(root) / file
                                if not self._should_exclude(file, config):
                                    zf.write(file_path, file_path.relative_to(source.parent))
        return dest_path

    def _should_exclude(self, name: str, config: BackupConfig) -> bool:
        """Check if a file should be excluded."""
        if config.exclude_patterns:
            import fnmatch
            for pattern in config.exclude_patterns:
                if fnmatch.fnmatch(name, pattern):
                    return True
        return False

    def _calculate_size(self, paths: List[Union[str, Path]]) -> Tuple[int, int]:
        """Calculate total size and file count."""
        total_size = 0
        file_count = 0

        for path in paths:
            path = Path(path)
            if path.is_file():
                total_size += path.stat().st_size
                file_count += 1
            elif path.is_dir():
                for root, dirs, files in os.walk(path):
                    for f in files:
                        fpath = Path(root) / f
                        total_size += fpath.stat().st_size
                        file_count += 1

        return total_size, file_count

    def _calculate_checksums(self, path: Path) -> Dict[str, str]:
        """Calculate MD5 and SHA256 checksums."""
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()

        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
                sha256.update(chunk)

        return {
            "md5": md5.hexdigest(),
            "sha256": sha256.hexdigest(),
        }

    def _verify_backup(
        self,
        archive_path: Path,
        config: BackupConfig,
    ) -> bool:
        """Verify a backup archive."""
        try:
            if archive_path.suffix == ".zip":
                with zipfile.ZipFile(archive_path, "r") as zf:
                    return zf.testzip() is None
            else:
                with tarfile.open(archive_path, "r:*") as tar:
                    members = tar.getmembers()
                    return len(members) > 0
        except Exception:
            return False

    def restore_backup(
        self,
        backup_id: str,
        destination: Union[str, Path],
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Restore a backup to a destination.

        Args:
            backup_id: ID of the backup to restore.
            destination: Path to restore to.
            overwrite: Whether to overwrite existing files.

        Returns:
            Dictionary with restore results.
        """
        with self._lock:
            metadata = self._backups.get(backup_id)
            if not metadata:
                raise ValueError(f"Backup not found: {backup_id}")

            if metadata.status not in (BackupStatus.COMPLETED, BackupStatus.VERIFIED):
                raise ValueError(f"Backup not in restorable state: {metadata.status}")

        archive_path = Path(metadata.destination_path)
        if not archive_path.exists():
            raise FileNotFoundError(f"Backup archive not found: {archive_path}")

        dest_path = Path(destination)
        if dest_path.exists() and not overwrite:
            raise FileExistsError(f"Destination already exists: {dest_path}")

        dest_path.mkdir(parents=True, exist_ok=True)

        restored_count = 0
        errors = []

        try:
            if archive_path.suffix == ".zip":
                with zipfile.ZipFile(archive_path, "r") as zf:
                    zf.extractall(dest_path)
                    restored_count = len(zf.namelist())
            else:
                with tarfile.open(archive_path, "r:*") as tar:
                    tar.extractall(dest_path)
                    restored_count = len(tar.getmembers())

        except Exception as e:
            errors.append(str(e))

        return {
            "backup_id": backup_id,
            "destination": str(dest_path),
            "restored_count": restored_count,
            "errors": errors,
            "success": len(errors) == 0,
        }

    def list_backups(
        self,
        status: Optional[BackupStatus] = None,
        tag: Optional[str] = None,
    ) -> List[BackupMetadata]:
        """List backups, optionally filtered by status or tag."""
        with self._lock:
            backups = list(self._backups.values())

            if status:
                backups = [b for b in backups if b.status == status]

            if tag:
                backups = [b for b in backups if tag in b.tags]

            backups.sort(key=lambda b: b.created_at, reverse=True)
            return backups

    def get_backup(self, backup_id: str) -> Optional[BackupMetadata]:
        """Get backup metadata by ID."""
        with self._lock:
            return self._backups.get(backup_id)

    def delete_backup(self, backup_id: str, delete_files: bool = True) -> bool:
        """
        Delete a backup.

        Args:
            backup_id: ID of the backup to delete.
            delete_files: Whether to delete the backup archive file.

        Returns:
            True if deleted, False if not found.
        """
        with self._lock:
            metadata = self._backups.get(backup_id)
            if not metadata:
                return False

            if delete_files and metadata.destination_path:
                archive_path = Path(metadata.destination_path)
                if archive_path.exists():
                    archive_path.unlink()

            del self._backups[backup_id]
            self._save_metadata()

            return True

    def cleanup_old_backups(self, days: int) -> Dict[str, int]:
        """
        Delete backups older than specified days.

        Args:
            days: Delete backups older than this many days.

        Returns:
            Dictionary with cleanup statistics.
        """
        cutoff = datetime.now() - timedelta(days=days)

        with self._lock:
            to_delete = [
                bid for bid, meta in self._backups.items()
                if meta.created_at < cutoff
            ]

        deleted = 0
        errors = 0

        for bid in to_delete:
            try:
                if self.delete_backup(bid):
                    deleted += 1
            except Exception:
                errors += 1

        return {
            "deleted": deleted,
            "errors": errors,
            "total_checked": len(to_delete),
        }

    def _load_metadata(self) -> None:
        """Load backup metadata from disk."""
        metadata_file = self._storage_path / "backup_metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, "r") as f:
                    data = json.load(f)
                    for backup_data in data.values():
                        backup_data = backup_data.copy()
                        if "created_at" in backup_data and isinstance(backup_data["created_at"], str):
                            backup_data["created_at"] = datetime.fromisoformat(backup_data["created_at"])
                        if "completed_at" in backup_data and isinstance(backup_data.get("completed_at"), str):
                            backup_data["completed_at"] = datetime.fromisoformat(backup_data["completed_at"])
                        if "backup_type" in backup_data:
                            backup_data["backup_type"] = BackupType(backup_data["backup_type"])
                        if "status" in backup_data:
                            backup_data["status"] = BackupStatus(backup_data["status"])
                        self._backups[backup_data["id"]] = BackupMetadata(**backup_data)
            except Exception:
                pass

    def _save_metadata(self) -> None:
        """Save backup metadata to disk."""
        metadata_file = self._storage_path / "backup_metadata.json"
        try:
            data = {
                bid: meta.to_dict()
                for bid, meta in self._backups.items()
            }
            with open(metadata_file, "w") as f:
                json.dump(data, f)
        except Exception:
            pass


_default_manager: Optional[BackupManager] = None


def get_backup_manager() -> BackupManager:
    """Get or create the default backup manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = BackupManager()
    return _default_manager


def backup_create_action(
    name: str,
    source_paths: List[str],
    backup_type: str = "full",
    compression: str = "gzip",
    retention_days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Action function to create a backup.

    Args:
        name: Name for the backup.
        source_paths: List of paths to back up.
        backup_type: Type of backup (full, incremental, differential).
        compression: Compression type (none, gzip, bzip2, xz, zip).
        retention_days: Optional retention period.

    Returns:
        Dictionary with backup metadata.
    """
    config = BackupConfig(
        backup_type=BackupType(backup_type.lower()),
        compression=CompressionType(compression.lower()),
        retention_days=retention_days,
    )

    manager = get_backup_manager()
    metadata = manager.create_backup(name, source_paths, config)
    return metadata.to_dict()


def backup_restore_action(
    backup_id: str,
    destination: str,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Restore a backup."""
    manager = get_backup_manager()
    return manager.restore_backup(backup_id, destination, overwrite)


def backup_list_action(
    status: Optional[str] = None,
    tag: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List backups."""
    manager = get_backup_manager()
    status_enum = BackupStatus(status) if status else None
    backups = manager.list_backups(status_enum, tag)
    return [b.to_dict() for b in backups]


def backup_delete_action(
    backup_id: str,
    delete_files: bool = True,
) -> bool:
    """Delete a backup."""
    manager = get_backup_manager()
    return manager.delete_backup(backup_id, delete_files)
