"""
File storage action for managing local and remote file operations.

This module provides actions for file storage operations including
upload, download, copy, move, list, and metadata management.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class StorageType(Enum):
    """Types of file storage backends."""
    LOCAL = "local"
    TEMPORARY = "temporary"
    MEMORY = "memory"


class FileType(Enum):
    """Common file type categories."""
    TEXT = "text"
    BINARY = "binary"
    IMAGE = "image"
    VIDEO = "audio"
    ARCHIVE = "archive"
    DOCUMENT = "document"
    UNKNOWN = "unknown"


@dataclass
class FileMetadata:
    """Metadata for a stored file."""
    path: str
    name: str
    size_bytes: int
    file_type: str
    mime_type: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    accessed_at: Optional[datetime] = None
    checksum_md5: Optional[str] = None
    checksum_sha256: Optional[str] = None
    is_compressed: bool = False
    is_encrypted: bool = False
    permissions: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            "path": self.path,
            "name": self.name,
            "size_bytes": self.size_bytes,
            "file_type": self.file_type,
            "mime_type": self.mime_type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "accessed_at": self.accessed_at.isoformat() if self.accessed_at else None,
            "checksum_md5": self.checksum_md5,
            "checksum_sha256": self.checksum_sha256,
            "is_compressed": self.is_compressed,
            "is_encrypted": self.is_encrypted,
            "permissions": self.permissions,
        }


@dataclass
class StorageConfig:
    """Configuration for file storage operations."""
    storage_type: StorageType = StorageType.LOCAL
    base_path: Optional[str] = None
    max_file_size: Optional[int] = None
    allowed_extensions: Optional[List[str]] = None
    create_dirs: bool = True
    overwrite: bool = True
    compute_checksums: bool = True
    preserve_permissions: bool = True
    preserve_timestamps: bool = True


class FileStorage:
    """
    File storage manager for local and temporary file operations.

    Supports upload, download, copy, move, list, and metadata operations.
    """

    MIME_TYPE_MAP = {
        ".txt": "text/plain",
        ".html": "text/html",
        ".css": "text/css",
        ".js": "application/javascript",
        ".json": "application/json",
        ".xml": "application/xml",
        ".pdf": "application/pdf",
        ".zip": "application/zip",
        ".tar": "application/x-tar",
        ".gz": "application/gzip",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".svg": "image/svg+xml",
        ".mp4": "video/mp4",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    FILE_TYPE_MAP = {
        ".txt": FileType.TEXT,
        ".md": FileType.TEXT,
        ".csv": FileType.TEXT,
        ".log": FileType.TEXT,
        ".html": FileType.TEXT,
        ".css": FileType.TEXT,
        ".js": FileType.TEXT,
        ".json": FileType.TEXT,
        ".xml": FileType.TEXT,
        ".png": FileType.IMAGE,
        ".jpg": FileType.IMAGE,
        ".jpeg": FileType.IMAGE,
        ".gif": FileType.IMAGE,
        ".svg": FileType.IMAGE,
        ".webp": FileType.IMAGE,
        ".mp4": FileType.VIDEO,
        ".avi": FileType.VIDEO,
        ".mov": FileType.VIDEO,
        ".mp3": FileType.VIDEO,
        ".wav": FileType.VIDEO,
        ".zip": FileType.ARCHIVE,
        ".tar": FileType.ARCHIVE,
        ".gz": FileType.ARCHIVE,
        ".bz2": FileType.ARCHIVE,
        ".7z": FileType.ARCHIVE,
        ".pdf": FileType.DOCUMENT,
        ".doc": FileType.DOCUMENT,
        ".docx": FileType.DOCUMENT,
        ".xls": FileType.DOCUMENT,
        ".xlsx": FileType.DOCUMENT,
        ".ppt": FileType.DOCUMENT,
        ".pptx": FileType.DOCUMENT,
    }

    def __init__(self, config: Optional[StorageConfig] = None):
        """
        Initialize the file storage manager.

        Args:
            config: Optional storage configuration.
        """
        self.config = config or StorageConfig()

        if self.config.storage_type == StorageType.LOCAL:
            self._base_path = Path(self.config.base_path or os.getcwd())
        elif self.config.storage_type == StorageType.TEMPORARY:
            self._base_path = Path(tempfile.gettempdir()) / "file_storage"
        else:
            self._base_path = Path(self.config.base_path or os.getcwd())

        if self.config.create_dirs:
            self._base_path.mkdir(parents=True, exist_ok=True)

    def upload(
        self,
        source_path: Union[str, Path],
        dest_path: Optional[Union[str, Path]] = None,
        dest_name: Optional[str] = None,
    ) -> FileMetadata:
        """
        Upload a file to storage.

        Args:
            source_path: Source file path.
            dest_path: Destination path in storage.
            dest_name: Destination filename.

        Returns:
            FileMetadata for the uploaded file.

        Raises:
            FileNotFoundError: If source file doesn't exist.
            ValueError: If file type is not allowed.
        """
        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        if not source_path.is_file():
            raise ValueError(f"Source is not a file: {source_path}")

        if self.config.allowed_extensions:
            ext = source_path.suffix.lower()
            if ext not in self.config.allowed_extensions:
                raise ValueError(f"File extension {ext} not allowed")

        file_size = source_path.stat().st_size
        if self.config.max_file_size and file_size > self.config.max_file_size:
            raise ValueError(f"File size {file_size} exceeds maximum {self.config.max_file_size}")

        if dest_path:
            dest_full = self._base_path / dest_path
        else:
            dest_full = self._base_path

        dest_full.parent.mkdir(parents=True, exist_ok=True)

        if dest_name:
            dest_file = dest_full.parent / dest_name
        else:
            dest_file = dest_full / source_path.name

        if not self.config.overwrite and dest_file.exists():
            raise FileExistsError(f"Destination already exists: {dest_file}")

        shutil.copy2(source_path, dest_file)

        metadata = self._get_metadata(dest_file)

        if self.config.preserve_permissions:
            shutil.copystat(source_path, dest_file)

        return metadata

    def download(
        self,
        source_path: Union[str, Path],
        dest_path: Union[str, Path],
    ) -> FileMetadata:
        """
        Download a file from storage.

        Args:
            source_path: Source path in storage.
            dest_path: Destination path.

        Returns:
            FileMetadata for the downloaded file.
        """
        source_full = self._resolve_path(source_path)
        if not source_full.exists():
            raise FileNotFoundError(f"File not found in storage: {source_path}")

        dest_full = Path(dest_path)
        dest_full.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(source_full, dest_full)

        return self._get_metadata(dest_full)

    def copy(
        self,
        source_path: Union[str, Path],
        dest_path: Union[str, Path],
    ) -> FileMetadata:
        """
        Copy a file within storage.

        Args:
            source_path: Source path in storage.
            dest_path: Destination path in storage.

        Returns:
            FileMetadata for the copied file.
        """
        source_full = self._resolve_path(source_path)
        if not source_full.exists():
            raise FileNotFoundError(f"File not found: {source_path}")

        dest_full = self._resolve_path(dest_path)
        dest_full.parent.mkdir(parents=True, exist_ok=True)

        shutil.copy2(source_full, dest_full)

        return self._get_metadata(dest_full)

    def move(
        self,
        source_path: Union[str, Path],
        dest_path: Union[str, Path],
    ) -> FileMetadata:
        """
        Move a file within storage.

        Args:
            source_path: Source path in storage.
            dest_path: Destination path in storage.

        Returns:
            FileMetadata for the moved file.
        """
        source_full = self._resolve_path(source_path)
        if not source_full.exists():
            raise FileNotFoundError(f"File not found: {source_path}")

        dest_full = self._resolve_path(dest_path)
        dest_full.parent.mkdir(parents=True, exist_ok=True)

        shutil.move(str(source_full), str(dest_full))

        return self._get_metadata(dest_full)

    def delete(self, file_path: Union[str, Path]) -> bool:
        """
        Delete a file from storage.

        Args:
            file_path: Path to the file.

        Returns:
            True if deleted, False if not found.
        """
        full_path = self._resolve_path(file_path)
        if full_path.exists():
            full_path.unlink()
            return True
        return False

    def list(
        self,
        path: Union[str, Path] = ".",
        pattern: Optional[str] = None,
        recursive: bool = False,
    ) -> List[FileMetadata]:
        """
        List files in storage.

        Args:
            path: Path within storage to list.
            pattern: Optional glob pattern to filter files.
            recursive: Whether to list recursively.

        Returns:
            List of FileMetadata for matching files.
        """
        full_path = self._resolve_path(path)

        if not full_path.exists():
            return []

        if pattern:
            if recursive:
                files = list(full_path.rglob(pattern))
            else:
                files = list(full_path.glob(pattern))
        else:
            if recursive:
                files = [f for f in full_path.rglob("*") if f.is_file()]
            else:
                files = [f for f in full_path.iterdir() if f.is_file()]

        return [self._get_metadata(f) for f in files]

    def exists(self, file_path: Union[str, Path]) -> bool:
        """Check if a file exists in storage."""
        return self._resolve_path(file_path).exists()

    def get_metadata(self, file_path: Union[str, Path]) -> FileMetadata:
        """Get metadata for a file."""
        full_path = self._resolve_path(file_path)
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        return self._get_metadata(full_path)

    def _resolve_path(self, path: Union[str, Path]) -> Path:
        """Resolve a storage path to an absolute path."""
        path = Path(path)
        if path.is_absolute():
            return path
        return self._base_path / path

    def _get_metadata(self, file_path: Path) -> FileMetadata:
        """Get metadata for a file path."""
        stat = file_path.stat()

        ext = file_path.suffix.lower()
        file_type = self.FILE_TYPE_MAP.get(ext, FileType.UNKNOWN).value
        mime_type = self.MIME_TYPE_MAP.get(ext)

        checksums = {}
        if self.config.compute_checksums:
            checksums = self._compute_checksums(file_path)

        permissions = None
        if hasattr(stat, "st_mode"):
            permissions = oct(stat.st_mode)[-3:]

        return FileMetadata(
            path=str(file_path),
            name=file_path.name,
            size_bytes=stat.st_size,
            file_type=file_type,
            mime_type=mime_type,
            created_at=datetime.fromtimestamp(stat.st_ctime),
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            accessed_at=datetime.fromtimestamp(stat.st_atime),
            checksum_md5=checksums.get("md5"),
            checksum_sha256=checksums.get("sha256"),
            permissions=permissions,
        )

    def _compute_checksums(self, file_path: Path) -> Dict[str, str]:
        """Compute MD5 and SHA256 checksums for a file."""
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()

        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
                sha256.update(chunk)

        return {
            "md5": md5.hexdigest(),
            "sha256": sha256.hexdigest(),
        }


def file_upload_action(
    source_path: str,
    dest_path: Optional[str] = None,
    storage_type: str = "local",
    base_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Action function to upload a file to storage.

    Args:
        source_path: Source file path.
        dest_path: Destination path in storage.
        storage_type: Type of storage (local, temporary).
        base_path: Base path for storage.

    Returns:
        Dictionary with upload result and metadata.
    """
    storage_type_map = {
        "local": StorageType.LOCAL,
        "temporary": StorageType.TEMPORARY,
    }

    if storage_type.lower() not in storage_type_map:
        raise ValueError(f"Unknown storage type: {storage_type}")

    config = StorageConfig(
        storage_type=storage_type_map[storage_type.lower()],
        base_path=base_path,
    )

    storage = FileStorage(config)
    metadata = storage.upload(source_path, dest_path)

    return metadata.to_dict()


def file_download_action(
    source_path: str,
    dest_path: str,
    storage_type: str = "local",
    base_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Action function to download a file from storage.

    Args:
        source_path: Source path in storage.
        dest_path: Destination file path.
        storage_type: Type of storage.
        base_path: Base path for storage.

    Returns:
        Dictionary with download result and metadata.
    """
    config = StorageConfig(
        storage_type=StorageType.LOCAL if storage_type == "local" else StorageType.TEMPORARY,
        base_path=base_path,
    )

    storage = FileStorage(config)
    metadata = storage.download(source_path, dest_path)

    return metadata.to_dict()


def file_copy_action(
    source_path: str,
    dest_path: str,
    storage_type: str = "local",
    base_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Action function to copy a file within storage.

    Args:
        source_path: Source path in storage.
        dest_path: Destination path in storage.
        storage_type: Type of storage.
        base_path: Base path for storage.

    Returns:
        Dictionary with copy result and metadata.
    """
    config = StorageConfig(
        storage_type=StorageType.LOCAL if storage_type == "local" else StorageType.TEMPORARY,
        base_path=base_path,
    )

    storage = FileStorage(config)
    metadata = storage.copy(source_path, dest_path)

    return metadata.to_dict()


def file_list_action(
    path: str = ".",
    pattern: Optional[str] = None,
    recursive: bool = False,
    storage_type: str = "local",
    base_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Action function to list files in storage.

    Args:
        path: Path within storage to list.
        pattern: Optional glob pattern.
        recursive: Whether to list recursively.
        storage_type: Type of storage.
        base_path: Base path for storage.

    Returns:
        List of file metadata dictionaries.
    """
    config = StorageConfig(
        storage_type=StorageType.LOCAL if storage_type == "local" else StorageType.TEMPORARY,
        base_path=base_path,
    )

    storage = FileStorage(config)
    files = storage.list(path, pattern, recursive)

    return [f.to_dict() for f in files]
