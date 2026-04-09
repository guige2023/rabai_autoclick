"""Data Watermark and Lineage Tracker.

This module provides data watermark tracking:
- Watermark generation
- Lineage tracking
- Provenance logging
- Transform history

Example:
    >>> from actions.data_watermark_action import WatermarkTracker
    >>> tracker = WatermarkTracker()
    >>> tracker.add_record({"id": 1, "data": "value"}, source="ingest")
"""

from __future__ import annotations

import time
import uuid
import hashlib
import logging
import threading
from typing import Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Watermark:
    """A data watermark."""
    watermark_id: str
    record_id: str
    source: str
    created_at: float
    hash: str
    version: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEntry:
    """A lineage entry for a record."""
    record_id: str
    watermark_id: str
    operation: str
    timestamp: float
    source_system: str
    target_system: str
    transform: Optional[str] = None


class WatermarkTracker:
    """Tracks data watermarks and lineage."""

    def __init__(self) -> None:
        """Initialize the watermark tracker."""
        self._watermarks: dict[str, Watermark] = {}
        self._lineage: dict[str, list[LineageEntry]] = {}
        self._lock = threading.RLock()
        self._stats = {"watermarks_created": 0, "lineage_entries": 0}

    def add_record(
        self,
        record: dict[str, Any],
        source: str,
        record_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Watermark:
        """Add a record with watermark.

        Args:
            record: Record dict.
            source: Data source name.
            record_id: Optional record ID.
            metadata: Additional metadata.

        Returns:
            Created Watermark.
        """
        rid = record_id or str(uuid.uuid4())
        record_hash = self._hash_record(record)

        existing = self._watermarks.get(rid)
        version = (existing.version + 1) if existing else 1

        watermark = Watermark(
            watermark_id=str(uuid.uuid4()),
            record_id=rid,
            source=source,
            created_at=time.time(),
            hash=record_hash,
            version=version,
            metadata=metadata or {},
        )

        with self._lock:
            self._watermarks[rid] = watermark
            self._stats["watermarks_created"] += 1

        logger.info("Created watermark for record %s (v%d)", rid, version)
        return watermark

    def track_transform(
        self,
        record_id: str,
        operation: str,
        source_system: str,
        target_system: str,
        transform: Optional[str] = None,
    ) -> LineageEntry:
        """Track a data transform operation.

        Args:
            record_id: Record identifier.
            operation: Operation performed.
            source_system: Source system.
            target_system: Target system.
            transform: Transform description.

        Returns:
            Created LineageEntry.
        """
        watermark = self._watermarks.get(record_id)
        watermark_id = watermark.watermark_id if watermark else ""

        entry = LineageEntry(
            record_id=record_id,
            watermark_id=watermark_id,
            operation=operation,
            timestamp=time.time(),
            source_system=source_system,
            target_system=target_system,
            transform=transform,
        )

        with self._lock:
            if record_id not in self._lineage:
                self._lineage[record_id] = []
            self._lineage[record_id].append(entry)
            self._stats["lineage_entries"] += 1

        logger.info("Tracked transform for record %s: %s", record_id, operation)
        return entry

    def get_watermark(self, record_id: str) -> Optional[Watermark]:
        """Get watermark for a record.

        Args:
            record_id: Record identifier.

        Returns:
            Watermark or None.
        """
        with self._lock:
            return self._watermarks.get(record_id)

    def get_lineage(self, record_id: str) -> list[LineageEntry]:
        """Get lineage for a record.

        Args:
            record_id: Record identifier.

        Returns:
            List of lineage entries.
        """
        with self._lock:
            return list(self._lineage.get(record_id, []))

    def verify_record(
        self,
        record: dict[str, Any],
        record_id: str,
    ) -> bool:
        """Verify record integrity against watermark.

        Args:
            record: Record to verify.
            record_id: Record identifier.

        Returns:
            True if record matches watermark hash.
        """
        watermark = self.get_watermark(record_id)
        if watermark is None:
            return False

        current_hash = self._hash_record(record)
        return current_hash == watermark.hash

    def get_record_versions(self, record_id: str) -> list[Watermark]:
        """Get all versions of a record.

        Args:
            record_id: Record identifier.

        Returns:
            List of watermarks for all versions.
        """
        with self._lock:
            current = self._watermarks.get(record_id)
            if current is None:
                return []
            return [current]

    def _hash_record(self, record: dict[str, Any]) -> str:
        """Create a hash of a record."""
        normalized = self._normalize_record(record)
        data = str(normalized).encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    def _normalize_record(self, record: dict[str, Any]) -> str:
        """Normalize record for hashing."""
        items = sorted((k, v) for k, v in record.items())
        return str(items)

    def get_stats(self) -> dict[str, Any]:
        """Get tracker statistics."""
        with self._lock:
            return {
                **self._stats,
                "tracked_records": len(self._watermarks),
                "records_with_lineage": len(self._lineage),
            }
