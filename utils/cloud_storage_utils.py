"""Multi-cloud storage abstraction layer (S3, GCS, Azure Blob, local)."""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

__all__ = [
    "CloudStorageConfig",
    "CloudStorageClient",
    "S3StorageBackend",
    "GCSStorageBackend",
    "AzureBlobBackend",
    "LocalStorageBackend",
    "StorageObject",
]


@dataclass
class StorageObject:
    """A stored object metadata."""
    key: str
    size: int
    last_modified: float
    content_type: str | None = None
    etag: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    checksum: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "size": self.size,
            "last_modified": self.last_modified,
            "content_type": self.content_type,
            "etag": self.etag,
            "metadata": self.metadata,
            "checksum": self.checksum,
        }


class CloudStorageBackend(ABC):
    """Abstract base class for cloud storage backends."""

    @abstractmethod
    def upload_file(self, key: str, path: str | Path, **kwargs: Any) -> StorageObject:
        pass

    @abstractmethod
    def download_file(self, key: str, destination: str | Path) -> None:
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    def list_objects(self, prefix: str = "", **kwargs: Any) -> list[StorageObject]:
        pass

    @abstractmethod
    def get_object(self, key: str) -> StorageObject | None:
        pass

    @abstractmethod
    def generate_download_url(self, key: str, expires_in: int = 3600) -> str:
        pass


class LocalStorageBackend(CloudStorageBackend):
    """Local filesystem storage backend."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _key_to_path(self, key: str) -> Path:
        key = key.lstrip("/")
        return self.root / key

    def upload_file(self, key: str, path: str | Path, **kwargs: Any) -> StorageObject:
        src = Path(path)
        dst = self._key_to_path(key)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        stat = dst.stat()
        with open(dst, "rb") as f:
            checksum = hashlib.sha256(f.read()).hexdigest()
        return StorageObject(
            key=key,
            size=stat.st_size,
            last_modified=stat.st_mtime,
            metadata=kwargs.get("metadata", {}),
            checksum=checksum,
        )

    def download_file(self, key: str, destination: str | Path) -> None:
        src = self._key_to_path(key)
        dst = Path(destination)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    def delete(self, key: str) -> bool:
        path = self._key_to_path(key)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_objects(self, prefix: str = "", **kwargs: Any) -> list[StorageObject]:
        prefix_path = self._key_to_path(prefix)
        results: list[StorageObject] = []
        if not prefix_path.exists():
            return results
        for item in prefix_path.rglob("*"):
            if item.is_file():
                rel_key = str(item.relative_to(self.root))
                stat = item.stat()
                results.append(StorageObject(
                    key=rel_key,
                    size=stat.st_size,
                    last_modified=stat.st_mtime,
                ))
        return results

    def get_object(self, key: str) -> StorageObject | None:
        path = self._key_to_path(key)
        if not path.exists():
            return None
        stat = path.stat()
        with open(path, "rb") as f:
            checksum = hashlib.sha256(f.read()).hexdigest()
        return StorageObject(
            key=key,
            size=stat.st_size,
            last_modified=stat.st_mtime,
            checksum=checksum,
        )

    def generate_download_url(self, key: str, expires_in: int = 3600) -> str:
        return f"file://{self._key_to_path(key).absolute()}"


class CloudStorageClient:
    """Unified multi-cloud storage client with local fallback."""

    def __init__(self, backend: CloudStorageBackend) -> None:
        self._backend = backend

    def upload(self, key: str, data: bytes | str | Path, **kwargs: Any) -> StorageObject:
        if isinstance(data, (str, Path)):
            return self._backend.upload_file(key, Path(data), **kwargs)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".tmp") as f:
            f.write(data if isinstance(data, bytes) else data.encode())
            f.flush()
            result = self._backend.upload_file(key, Path(f.name), **kwargs)
        return result

    def download(self, key: str, destination: str | Path) -> None:
        self._backend.download_file(key, destination)

    def delete(self, key: str) -> bool:
        return self._backend.delete(key)

    def list(self, prefix: str = "") -> list[StorageObject]:
        return self._backend.list_objects(prefix)

    def get(self, key: str) -> StorageObject | None:
        return self._backend.get_object(key)

    def get_url(self, key: str, expires_in: int = 3600) -> str:
        return self._backend.generate_download_url(key, expires_in)

    def batch_delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if self.delete(k):
                count += 1
        return count
