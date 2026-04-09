"""Data Snapshot Action Module.

Creates point-in-time snapshots of data structures with support for
diffing, rollback, and incremental snapshots.
"""

import time
import json
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class Snapshot:
    snapshot_id: str
    created_at: float
    data_hash: str
    size_bytes: int
    parent_id: Optional[str]
    label: str
    item_count: int
    delta: Optional[Dict[str, Any]] = None


class DataSnapshotAction:
    """Manages point-in-time snapshots of data with diff and rollback."""

    def __init__(
        self,
        storage_dir: str = "/tmp/data_snapshots",
        max_snapshots: int = 100,
    ) -> None:
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.max_snapshots = max_snapshots
        self._snapshots: Dict[str, Snapshot] = {}
        self._load_index()

    def create_snapshot(
        self,
        data: Any,
        label: str = "default",
        parent_id: Optional[str] = None,
        compute_delta: bool = True,
    ) -> str:
        snapshot_id = f"snap_{int(time.time() * 1000)}"
        data_bytes = json.dumps(data, default=str).encode("utf-8")
        data_hash = hashlib.sha256(data_bytes).hexdigest()[:16]
        size_bytes = len(data_bytes)
        delta = None
        if compute_delta and parent_id:
            parent = self._snapshots.get(parent_id)
            if parent:
                delta = self._compute_delta(parent_id, data)
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            created_at=time.time(),
            data_hash=data_hash,
            size_bytes=size_bytes,
            parent_id=parent_id,
            label=label,
            item_count=self._count_items(data),
            delta=delta,
        )
        self._snapshots[snapshot_id] = snapshot
        snapshot_path = self._get_snapshot_path(snapshot_id)
        with open(snapshot_path, "wb") as f:
            f.write(data_bytes)
        self._save_index()
        self._prune_old()
        logger.info(f"Created snapshot {snapshot_id} ({size_bytes} bytes)")
        return snapshot_id

    def restore(self, snapshot_id: str) -> Optional[Any]:
        snapshot = self._snapshots.get(snapshot_id)
        if not snapshot:
            logger.error(f"Snapshot {snapshot_id} not found")
            return None
        snapshot_path = self._get_snapshot_path(snapshot_id)
        if not snapshot_path.exists():
            logger.error(f"Snapshot file {snapshot_path} not found")
            return None
        with open(snapshot_path, "rb") as f:
            data_bytes = f.read()
        try:
            return json.loads(data_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse snapshot {snapshot_id}: {e}")
            return None

    def diff(
        self,
        snapshot_id_a: str,
        snapshot_id_b: str,
    ) -> Optional[Dict[str, Any]]:
        data_a = self.restore(snapshot_id_a)
        data_b = self.restore(snapshot_id_b)
        if data_a is None or data_b is None:
            return None
        return self._compute_diff(data_a, data_b)

    def _compute_delta(
        self,
        parent_id: str,
        new_data: Any,
    ) -> Dict[str, Any]:
        parent_data = self.restore(parent_id)
        if parent_data is None:
            return {}
        return self._compute_diff(parent_data, new_data)

    def _compute_diff(
        self,
        data_a: Any,
        data_b: Any,
    ) -> Dict[str, Any]:
        diff: Dict[str, Any] = {"added": [], "removed": [], "changed": []}
        if isinstance(data_a, dict) and isinstance(data_b, dict):
            all_keys: Set[str] = set(data_a.keys()) | set(data_b.keys())
            for key in all_keys:
                if key not in data_a:
                    diff["added"].append(key)
                elif key not in data_b:
                    diff["removed"].append(key)
                elif data_a[key] != data_b[key]:
                    diff["changed"].append(
                        {"key": key, "old": data_a[key], "new": data_b[key]}
                    )
        return diff

    def list_snapshots(self, label: Optional[str] = None) -> List[Dict[str, Any]]:
        result = []
        for s in self._snapshots.values():
            if label and s.label != label:
                continue
            result.append(
                {
                    "snapshot_id": s.snapshot_id,
                    "created_at": s.created_at,
                    "label": s.label,
                    "size_bytes": s.size_bytes,
                    "item_count": s.item_count,
                    "parent_id": s.parent_id,
                    "has_delta": s.delta is not None,
                    "age_hours": (time.time() - s.created_at) / 3600,
                }
            )
        result.sort(key=lambda x: x["created_at"], reverse=True)
        return result

    def delete_snapshot(self, snapshot_id: str) -> bool:
        snapshot = self._snapshots.pop(snapshot_id, None)
        if not snapshot:
            return False
        snapshot_path = self._get_snapshot_path(snapshot_id)
        if snapshot_path.exists():
            snapshot_path.unlink()
        self._save_index()
        logger.info(f"Deleted snapshot {snapshot_id}")
        return True

    def get_latest(self, label: Optional[str] = None) -> Optional[str]:
        snapshots = self.list_snapshots(label)
        return snapshots[0]["snapshot_id"] if snapshots else None

    def _get_snapshot_path(self, snapshot_id: str) -> Path:
        return self.storage_dir / f"{snapshot_id}.json"

    def _count_items(self, data: Any) -> int:
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            return len(data)
        return 1

    def _load_index(self) -> None:
        index_path = self.storage_dir / "snapshot_index.json"
        if index_path.exists():
            try:
                with open(index_path) as f:
                    raw = json.load(f)
                    for item in raw.get("snapshots", []):
                        self._snapshots[item["snapshot_id"]] = Snapshot(**item)
            except Exception as e:
                logger.warning(f"Failed to load snapshot index: {e}")

    def _save_index(self) -> None:
        index_path = self.storage_dir / "snapshot_index.json"
        data = {
            "snapshots": [
                {
                    "snapshot_id": s.snapshot_id,
                    "created_at": s.created_at,
                    "data_hash": s.data_hash,
                    "size_bytes": s.size_bytes,
                    "parent_id": s.parent_id,
                    "label": s.label,
                    "item_count": s.item_count,
                    "delta": s.delta,
                }
                for s in self._snapshots.values()
            ]
        }
        with open(index_path, "w") as f:
            json.dump(data, f, indent=2)

    def _prune_old(self) -> None:
        if len(self._snapshots) <= self.max_snapshots:
            return
        sorted_snaps = sorted(self._snapshots.values(), key=lambda s: s.created_at)
        excess = len(sorted_snaps) - self.max_snapshots
        for s in sorted_snaps[:excess]:
            self.delete_snapshot(s.snapshot_id)
