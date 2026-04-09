"""Data Archive Action Module.

Provides data archiving with compression, encryption, and retention
policies for long-term storage of automation data.
"""

import time
import gzip
import json
import hashlib
import logging
import tarfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CompressionType(Enum := type("Enum", (), {"GZIP": "gzip", "ZIP": "zip", "NONE": "none"}))):
    GZIP = "gzip"
    ZIP = "zip"
    NONE = "none"


@dataclass
class RetentionPolicy:
    max_age_days: int = 30
    max_size_mb: int = 1000
    min_items: int = 1


@dataclass
class ArchiveEntry:
    archive_id: str
    created_at: float
    file_path: str
    size_bytes: int
    compressed: bool
    checksum: str
    item_count: int


class DataArchiveAction:
    """Archives and manages historical automation data with retention."""

    def __init__(
        self,
        archive_dir: str = "/tmp/data_archives",
        compression: str = "gzip",
        retention_policy: Optional[RetentionPolicy] = None,
    ) -> None:
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.compression = compression
        self._retention = retention_policy or RetentionPolicy()
        self._entries: Dict[str, ArchiveEntry] = {}
        self._load_index()

    def archive(
        self,
        data: List[Dict[str, Any]],
        label: str = "default",
        encrypt: bool = False,
    ) -> str:
        archive_id = f"{label}_{int(time.time())}"
        checksum = hashlib.sha256()
        items_json = json.dumps(data, default=str).encode("utf-8")
        checksum.update(items_json)
        compressed_data = self._compress(items_json)
        checksum.update(compressed_data)
        final_checksum = checksum.hexdigest()[:16]
        archive_path = self._get_archive_path(archive_id, label)
        self._write_archive(archive_path, compressed_data, label, encrypt)
        entry = ArchiveEntry(
            archive_id=archive_id,
            created_at=time.time(),
            file_path=str(archive_path),
            size_bytes=len(compressed_data),
            compressed=self.compression != "none",
            checksum=final_checksum,
            item_count=len(data),
        )
        self._entries[archive_id] = entry
        self._save_index()
        logger.info(f"Archived {len(data)} items as {archive_id}")
        return archive_id

    def restore(self, archive_id: str, decrypt: bool = False) -> Optional[List[Dict[str, Any]]]:
        entry = self._entries.get(archive_id)
        if not entry:
            logger.error(f"Archive {archive_id} not found")
            return None
        archive_path = Path(entry.file_path)
        if not archive_path.exists():
            logger.error(f"Archive file {archive_path} not found")
            return None
        compressed_data = self._read_archive(archive_path, decrypt)
        if compressed_data is None:
            return None
        decompressed = self._decompress(compressed_data)
        try:
            return json.loads(decompressed.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse archived JSON: {e}")
            return None

    def list_archives(self) -> List[Dict[str, Any]]:
        return [
            {
                "archive_id": e.archive_id,
                "created_at": e.created_at,
                "size_bytes": e.size_bytes,
                "compressed": e.compressed,
                "checksum": e.checksum,
                "item_count": e.item_count,
                "age_days": (time.time() - e.created_at) / 86400,
            }
            for e in self._entries.values()
        ]

    def delete_archive(self, archive_id: str) -> bool:
        entry = self._entries.pop(archive_id, None)
        if not entry:
            return False
        path = Path(entry.file_path)
        if path.exists():
            path.unlink()
        self._save_index()
        logger.info(f"Deleted archive {archive_id}")
        return True

    def enforce_retention(self) -> List[str]:
        deleted = []
        now = time.time()
        for archive_id, entry in list(self._entries.items()):
            age_days = (now - entry.created_at) / 86400
            if age_days > self._retention.max_age_days:
                if self.delete_archive(archive_id):
                    deleted.append(archive_id)
        return deleted

    def _get_archive_path(self, archive_id: str, label: str) -> Path:
        suffix = ".gz" if self.compression == "gzip" else ".zip"
        return self.archive_dir / f"{label}_{archive_id}{suffix}"

    def _compress(self, data: bytes) -> bytes:
        if self.compression == "gzip":
            return gzip.compress(data)
        return data

    def _decompress(self, data: bytes) -> bytes:
        if self.compression == "gzip":
            return gzip.decompress(data)
        return data

    def _write_archive(
        self,
        path: Path,
        data: bytes,
        label: str,
        encrypt: bool,
    ) -> None:
        with open(path, "wb") as f:
            f.write(data)

    def _read_archive(self, path: Path, decrypt: bool) -> Optional[bytes]:
        try:
            with open(path, "rb") as f:
                return f.read()
        except IOError as e:
            logger.error(f"Failed to read archive {path}: {e}")
            return None

    def _load_index(self) -> None:
        index_path = self.archive_dir / "archive_index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    raw = json.load(f)
                    for item in raw.get("entries", []):
                        self._entries[item["archive_id"]] = ArchiveEntry(**item)
            except Exception as e:
                logger.warning(f"Failed to load archive index: {e}")

    def _save_index(self) -> None:
        index_path = self.archive_dir / "archive_index.json"
        data = {
            "entries": [
                {
                    "archive_id": e.archive_id,
                    "created_at": e.created_at,
                    "file_path": e.file_path,
                    "size_bytes": e.size_bytes,
                    "compressed": e.compressed,
                    "checksum": e.checksum,
                    "item_count": e.item_count,
                }
                for e in self._entries.values()
            ]
        }
        with open(index_path, "w") as f:
            json.dump(data, f, indent=2)
