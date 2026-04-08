"""
Data Snapshot Action Module.

Creates point-in-time snapshots of data structures,
supports versioning, restoration, and diff analysis.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
import logging
import copy
import time
import hashlib
import json

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    """Point-in-time data snapshot."""
    version: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    label: str = ""
    checksum: str = ""


@dataclass
class SnapshotMetadata:
    """Metadata for a snapshot."""
    version: str
    timestamp: float
    label: str
    size_bytes: int
    checksum: str
    data_type: str


class DataSnapshotAction:
    """
    Creates and manages data snapshots with versioning.

    Supports labeled snapshots, automatic versioning,
    and restoration with optional integrity checking.

    Example:
        snap = DataSnapshotAction()
        snap.save(my_data, label="before_update")
        # ... modify data ...
        restored = snap.restore("before_update")
    """

    def __init__(
        self,
        max_snapshots: int = 50,
        auto_version: bool = True,
    ) -> None:
        self.max_snapshots = max_snapshots
        self.auto_version = auto_version
        self._snapshots: dict[str, Snapshot] = {}
        self._version_counter: int = 0

    def save(
        self,
        data: Any,
        version: Optional[str] = None,
        label: str = "",
    ) -> str:
        """Save a snapshot of data."""
        version = version or self._generate_version()
        data_copy = self._deep_copy(data)

        try:
            serialized = json.dumps(data_copy, sort_keys=True, default=str)
            checksum = hashlib.sha256(serialized.encode()).hexdigest()[:16]
        except Exception:
            checksum = ""

        snapshot = Snapshot(
            version=version,
            data=data_copy,
            label=label,
            checksum=checksum,
        )

        self._snapshots[version] = snapshot
        self._enforce_max()

        logger.debug("Snapshot saved: %s (label=%s)", version, label)
        return version

    def restore(self, version: str) -> Optional[Any]:
        """Restore data from a snapshot."""
        if version not in self._snapshots:
            logger.warning("Snapshot not found: %s", version)
            return None

        snapshot = self._snapshots[version]
        return self._deep_copy(snapshot.data)

    def get_metadata(self, version: str) -> Optional[SnapshotMetadata]:
        """Get metadata for a snapshot without loading data."""
        if version not in self._snapshots:
            return None

        snap = self._snapshots[version]
        data_str = self._serialize_for_size(snap.data)

        return SnapshotMetadata(
            version=snap.version,
            timestamp=snap.timestamp,
            label=snap.label,
            size_bytes=len(data_str),
            checksum=snap.checksum,
            data_type=type(snap.data).__name__,
        )

    def delete(self, version: str) -> bool:
        """Delete a specific snapshot."""
        if version in self._snapshots:
            del self._snapshots[version]
            return True
        return False

    def list_versions(self) -> list[str]:
        """List all snapshot versions."""
        return sorted(
            self._snapshots.keys(),
            key=lambda v: self._snapshots[v].timestamp,
            reverse=True,
        )

    def find_by_label(self, label: str) -> list[str]:
        """Find snapshots by label prefix."""
        return [
            v for v, s in self._snapshots.items()
            if s.label.startswith(label)
        ]

    def get_latest(self) -> Optional[Any]:
        """Get the most recent snapshot."""
        versions = self.list_versions()
        if not versions:
            return None
        return self.restore(versions[0])

    def diff(
        self,
        version_a: str,
        version_b: str,
    ) -> Optional[dict[str, Any]]:
        """Compare two snapshots."""
        snap_a = self._snapshots.get(version_a)
        snap_b = self._snapshots.get(version_b)

        if not snap_a or not snap_b:
            return None

        return {
            "version_a": version_a,
            "version_b": version_b,
            "size_a": len(self._serialize_for_size(snap_a.data)),
            "size_b": len(self._serialize_for_size(snap_b.data)),
            "checksum_a": snap_a.checksum,
            "checksum_b": snap_b.checksum,
            "same_checksum": snap_a.checksum == snap_b.checksum,
        }

    def _generate_version(self) -> str:
        """Generate next version string."""
        self._version_counter += 1
        return f"v{self._version_counter:04d}_{int(time.time())}"

    def _enforce_max(self) -> None:
        """Remove oldest snapshots when exceeding max."""
        if len(self._snapshots) > self.max_snapshots:
            sorted_versions = self.list_versions()
            to_remove = sorted_versions[self.max_snapshots:]
            for v in to_remove:
                del self._snapshots[v]

    def _deep_copy(self, data: Any) -> Any:
        """Create a deep copy of data."""
        try:
            return copy.deepcopy(data)
        except Exception:
            return data

    def _serialize_for_size(self, data: Any) -> str:
        """Serialize data for size calculation."""
        try:
            return json.dumps(data, sort_keys=True, default=str)
        except Exception:
            return str(data)
