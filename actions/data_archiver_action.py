"""Data Archiver Action Module.

Provides data archival with compression, retention policies,
and incremental backup support.
"""
from __future__ import annotations

import gzip
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class ArchiveFormat(Enum):
    """Archive format."""
    JSON = "json"
    JSON_GZ = "json_gz"
    CSV = "csv"
    PARQUET = "parquet"


@dataclass
class ArchivePolicy:
    """Archive retention policy."""
    name: str
    retention_days: int
    archive_format: ArchiveFormat = ArchiveFormat.JSON_GZ
    compress: bool = True


@dataclass
class ArchiveEntry:
    """Archive entry metadata."""
    archive_id: str
    archive_path: str
    created_at: float
    size_bytes: int
    item_count: int
    policy_name: str


class DataArchiverAction:
    """Data archiver with retention policies.

    Example:
        archiver = DataArchiverAction("/data/archives")

        archiver.add_policy(ArchivePolicy(
            name="daily",
            retention_days=30
        ))

        await archiver.archive(data, "daily")
        await archiver.prune()
    """

    def __init__(self, archive_dir: str = "/tmp/archives") -> None:
        self.archive_dir = Path(archive_dir)
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        self._policies: Dict[str, ArchivePolicy] = {}
        self._archives: Dict[str, ArchiveEntry] = {}
        self._metadata_file = self.archive_dir / ".archive_metadata.json"
        self._load_metadata()

    def add_policy(self, policy: ArchivePolicy) -> None:
        """Add retention policy.

        Args:
            policy: ArchivePolicy to add
        """
        self._policies[policy.name] = policy

    async def archive(
        self,
        data: Any,
        policy_name: str,
        archive_id: Optional[str] = None,
    ) -> ArchiveEntry:
        """Archive data.

        Args:
            data: Data to archive
            policy_name: Policy name to use
            archive_id: Optional archive identifier

        Returns:
            ArchiveEntry with metadata
        """
        if policy_name not in self._policies:
            raise ValueError(f"Unknown policy: {policy_name}")

        policy = self._policies[policy_name]
        archive_id = archive_id or f"{policy_name}_{int(time.time())}"

        archive_path = self.archive_dir / f"{archive_id}.{self._get_extension(policy)}"

        serialized = self._serialize(data)
        content = serialized

        if policy.compress or policy.archive_format == ArchiveFormat.JSON_GZ:
            content = gzip.compress(serialized)

        with open(archive_path, "wb") as f:
            f.write(content if isinstance(content, bytes) else content.encode("utf-8"))

        entry = ArchiveEntry(
            archive_id=archive_id,
            archive_path=str(archive_path),
            created_at=time.time(),
            size_bytes=len(content),
            item_count=len(data) if isinstance(data, (list, dict)) else 1,
            policy_name=policy_name,
        )

        self._archives[archive_id] = entry
        self._save_metadata()

        return entry

    async def restore(self, archive_id: str) -> Any:
        """Restore archived data.

        Args:
            archive_id: Archive identifier

        Returns:
            Restored data
        """
        if archive_id not in self._archives:
            raise ValueError(f"Unknown archive: {archive_id}")

        entry = self._archives[archive_id]
        archive_path = Path(entry.archive_path)

        if not archive_path.exists():
            raise FileNotFoundError(f"Archive file not found: {archive_path}")

        with open(archive_path, "rb") as f:
            content = f.read()

        if archive_path.suffix == ".gz":
            content = gzip.decompress(content)

        return self._deserialize(content)

    async def prune(self, dry_run: bool = False) -> List[ArchiveEntry]:
        """Prune archives past retention period.

        Args:
            dry_run: If True, don't actually delete

        Returns:
            List of pruned (or would-be pruned) entries
        """
        pruned: List[ArchiveEntry] = []
        now = time.time()

        for archive_id, entry in list(self._archives.items()):
            policy = self._policies.get(entry.policy_name)
            if not policy:
                continue

            age_days = (now - entry.created_at) / 86400

            if age_days > policy.retention_days:
                pruned.append(entry)

                if not dry_run:
                    archive_path = Path(entry.archive_path)
                    if archive_path.exists():
                        archive_path.unlink()

                    del self._archives[archive_id]

        if not dry_run:
            self._save_metadata()

        return pruned

    def list_archives(
        self,
        policy_name: Optional[str] = None,
    ) -> List[ArchiveEntry]:
        """List archives.

        Args:
            policy_name: Filter by policy name

        Returns:
            List of ArchiveEntries
        """
        archives = list(self._archives.values())

        if policy_name:
            archives = [a for a in archives if a.policy_name == policy_name]

        return sorted(archives, key=lambda x: -x.created_at)

    def _get_extension(self, policy: ArchivePolicy) -> str:
        """Get file extension for format."""
        if policy.archive_format == ArchiveFormat.JSON:
            return "json"
        elif policy.archive_format == ArchiveFormat.JSON_GZ:
            return "json.gz"
        elif policy.archive_format == ArchiveFormat.CSV:
            return "csv"
        return "bin"

    def _serialize(self, data: Any) -> bytes:
        """Serialize data."""
        if isinstance(data, bytes):
            return data
        elif isinstance(data, str):
            return data.encode("utf-8")
        else:
            return json.dumps(data, indent=2, default=str).encode("utf-8")

    def _deserialize(self, content: bytes) -> Any:
        """Deserialize data."""
        try:
            return json.loads(content.decode("utf-8"))
        except:
            return content

    def _save_metadata(self) -> None:
        """Save metadata to file."""
        data = {
            "archives": {
                k: {
                    "archive_id": v.archive_id,
                    "archive_path": v.archive_path,
                    "created_at": v.created_at,
                    "size_bytes": v.size_bytes,
                    "item_count": v.item_count,
                    "policy_name": v.policy_name,
                }
                for k, v in self._archives.items()
            }
        }

        with open(self._metadata_file, "w") as f:
            json.dump(data, f)

    def _load_metadata(self) -> None:
        """Load metadata from file."""
        if not self._metadata_file.exists():
            return

        try:
            with open(self._metadata_file) as f:
                data = json.load(f)

            for k, v in data.get("archives", {}).items():
                self._archives[k] = ArchiveEntry(**v)

        except Exception as e:
            logger.error(f"Failed to load metadata: {e}")
