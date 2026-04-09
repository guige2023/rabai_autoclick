"""
Data Version Action Module.

Data versioning utilities for automation with support for tracking changes,
version history, and diff computation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Type of change made to data."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class DataVersion:
    """A single version of data."""
    version_id: str
    timestamp: float
    data_hash: str
    data_size: int
    change_type: ChangeType
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_version: Optional[str] = None


@dataclass
class ChangeDiff:
    """A diff between two data versions."""
    field_path: str
    old_value: Any = None
    new_value: Any = None
    change_type: ChangeType = ChangeType.UNCHANGED


@dataclass
class VersionHistory:
    """Complete version history for a dataset."""
    current_version: Optional[str]
    versions: List[DataVersion]
    total_changes: int = 0


class DataVersionAction:
    """
    Data versioning for automation.

    Tracks changes to data over time, computes diffs between versions,
    and manages version history.

    Example:
        versioner = DataVersionAction()

        v1_id = versioner.save_version(data={"users": [...]})
        v2_id = versioner.save_version(data={"users": [...], "roles": [...]})

        diffs = versioner.get_diff(v1_id, v2_id)
        history = versioner.get_history()
    """

    def __init__(self, key: str = "default") -> None:
        self.key = key
        self._versions: List[DataVersion] = []
        self._data_cache: Dict[str, Any] = {}
        self._current_version: Optional[str] = None

    def _compute_hash(self, data: Any) -> str:
        """Compute a hash for data."""
        if isinstance(data, dict):
            serialized = json.dumps(data, sort_keys=True, default=str)
        else:
            serialized = str(data)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

    def _compute_size(self, data: Any) -> int:
        """Compute size of data in bytes."""
        if isinstance(data, dict):
            return len(json.dumps(data, default=str).encode())
        return len(str(data).encode())

    def save_version(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> str:
        """Save a new version of data."""
        data_hash = self._compute_hash(data)
        data_size = self._compute_size(data)

        # Determine change type
        change_type = ChangeType.ADDED
        parent_version = None

        if self._current_version:
            parent_version = self._current_version
            current = self._versions[-1] if self._versions else None
            if current and current.data_hash == data_hash:
                change_type = ChangeType.UNCHANGED
            else:
                change_type = ChangeType.MODIFIED
        else:
            change_type = ChangeType.ADDED

        # Create version ID
        version_id = f"v{len(self._versions) + 1}-{data_hash}"

        version = DataVersion(
            version_id=version_id,
            timestamp=time.time(),
            data_hash=data_hash,
            data_size=data_size,
            change_type=change_type,
            metadata=metadata or {},
            parent_version=parent_version,
        )

        self._versions.append(version)
        self._data_cache[version_id] = data
        self._current_version = version_id

        logger.info(f"Saved version {version_id}: {change_type.value} ({data_size} bytes)")
        return version_id

    def get_version(self, version_id: str) -> Optional[Any]:
        """Get data for a specific version."""
        return self._data_cache.get(version_id)

    def get_latest_version(self) -> Optional[DataVersion]:
        """Get the most recent version."""
        return self._versions[-1] if self._versions else None

    def get_history(
        self,
        limit: Optional[int] = None,
    ) -> VersionHistory:
        """Get version history."""
        versions = self._versions[-limit:] if limit else self._versions
        return VersionHistory(
            current_version=self._current_version,
            versions=versions,
            total_changes=len(self._versions),
        )

    def get_diff(
        self,
        version_a: str,
        version_b: str,
    ) -> List[ChangeDiff]:
        """Compute diff between two versions."""
        data_a = self._data_cache.get(version_a)
        data_b = self._data_cache.get(version_b)

        if data_a is None or data_b is None:
            return []

        if not isinstance(data_a, dict) or not isinstance(data_b, dict):
            # Simple comparison for non-dict data
            if data_a != data_b:
                return [ChangeDiff(
                    field_path="root",
                    old_value=data_a,
                    new_value=data_b,
                    change_type=ChangeType.MODIFIED,
                )]
            return []

        return self._dict_diff("", data_a, data_b)

    def _dict_diff(
        self,
        prefix: str,
        old: Dict[str, Any],
        new: Dict[str, Any],
    ) -> List[ChangeDiff]:
        """Recursively compute dict diff."""
        diffs: List[ChangeDiff] = []
        all_keys = set(old.keys()) | set(new.keys())

        for key in sorted(all_keys):
            path = f"{prefix}.{key}" if prefix else key
            old_val = old.get(key)
            new_val = new.get(key)

            if key not in old:
                diffs.append(ChangeDiff(
                    field_path=path,
                    old_value=None,
                    new_value=new_val,
                    change_type=ChangeType.ADDED,
                ))
            elif key not in new:
                diffs.append(ChangeDiff(
                    field_path=path,
                    old_value=old_val,
                    new_value=None,
                    change_type=ChangeType.REMOVED,
                ))
            elif old_val != new_val:
                if isinstance(old_val, dict) and isinstance(new_val, dict):
                    diffs.extend(self._dict_diff(path, old_val, new_val))
                else:
                    diffs.append(ChangeDiff(
                        field_path=path,
                        old_value=old_val,
                        new_value=new_val,
                        change_type=ChangeType.MODIFIED,
                    ))

        return diffs

    def rollback_to(self, version_id: str) -> Optional[Any]:
        """Rollback to a specific version."""
        if version_id not in self._data_cache:
            logger.error(f"Version '{version_id}' not found")
            return None

        new_version_id = self.save_version(
            self._data_cache[version_id],
            metadata={"rollback_from": self._current_version},
            message=f"Rollback to {version_id}",
        )

        logger.info(f"Rolled back from {self._current_version} to {version_id}")
        return new_version_id

    def prune_versions(self, keep_last: int = 10) -> int:
        """Prune old versions, keeping only the most recent."""
        if len(self._versions) <= keep_last:
            return 0

        to_remove = self._versions[:-keep_last]
        removed = 0

        for version in to_remove:
            if version.version_id in self._data_cache:
                del self._data_cache[version.version_id]
            removed += 1

        self._versions = self._versions[-keep_last:]
        logger.info(f"Pruned {removed} old versions, kept {keep_last}")
        return removed
