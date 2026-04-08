"""
Data synchronization module for keeping data stores in sync.

Supports full sync, incremental sync, bidirectional sync,
conflict resolution, and CDC (Change Data Capture).
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class SyncDirection(Enum):
    """Direction of data sync."""
    SOURCE_TO_TARGET = "source_to_target"
    TARGET_TO_SOURCE = "target_to_source"
    BIDIRECTIONAL = "bidirectional"


class SyncMode(Enum):
    """Sync mode."""
    FULL = "full"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class ConflictResolution(Enum):
    """Conflict resolution strategy."""
    SOURCE_WINS = "source_wins"
    TARGET_WINS = "target_wins"
    NEWEST_WINS = "newest_wins"
    MANUAL = "manual"


@dataclass
class SyncMapping:
    """Mapping between source and target fields."""
    source_field: str
    target_field: str
    transform: Optional[Callable] = None


@dataclass
class SyncRecord:
    """A record in a sync operation."""
    id: str
    operation: str
    data: dict
    timestamp: float
    hash: str = ""
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.hash:
            self.hash = self._compute_hash()

    def _compute_hash(self) -> str:
        return hashlib.md5(json.dumps(self.data, sort_keys=True).encode()).hexdigest()


@dataclass
class SyncCheckpoint:
    """Checkpoint for incremental sync."""
    sync_id: str
    last_sync_timestamp: float
    last_record_id: Optional[str] = None
    watermark: dict = field(default_factory=dict)


@dataclass
class SyncConflict:
    """A sync conflict."""
    record_id: str
    source_data: dict
    target_data: dict
    source_timestamp: float
    target_timestamp: float
    resolution: Optional[str] = None


@dataclass
class SyncJob:
    """A sync job definition."""
    id: str
    name: str
    source_name: str
    target_name: str
    direction: SyncDirection
    mode: SyncMode
    mappings: list[SyncMapping]
    conflict_resolution: ConflictResolution = ConflictResolution.SOURCE_WINS
    filter_function: Optional[Callable] = None
    enabled: bool = True


@dataclass
class SyncResult:
    """Result of a sync operation."""
    sync_id: str
    status: str
    records_synced: int
    records_failed: int
    conflicts: int
    duration_ms: float
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    errors: list[str] = field(default_factory=list)


class DataSynchronizer:
    """
    Data synchronization service.

    Supports full sync, incremental sync, bidirectional sync,
    conflict resolution, and CDC.
    """

    def __init__(self):
        self._sync_jobs: dict[str, SyncJob] = {}
        self._checkpoints: dict[str, SyncCheckpoint] = {}
        self._sync_history: list[SyncResult] = []
        self._conflicts: list[SyncConflict] = []

    def create_sync_job(
        self,
        name: str,
        source_name: str,
        target_name: str,
        direction: SyncDirection = SyncDirection.SOURCE_TO_TARGET,
        mode: SyncMode = SyncMode.FULL,
        mappings: Optional[list[SyncMapping]] = None,
        conflict_resolution: ConflictResolution = ConflictResolution.SOURCE_WINS,
    ) -> SyncJob:
        """Create a new sync job."""
        job = SyncJob(
            id=str(uuid.uuid4())[:12],
            name=name,
            source_name=source_name,
            target_name=target_name,
            direction=direction,
            mode=mode,
            mappings=mappings or [],
            conflict_resolution=conflict_resolution,
        )
        self._sync_jobs[job.id] = job
        return job

    def execute_sync(
        self,
        sync_id: str,
        source_reader: Callable,
        target_writer: Callable,
        source_filter: Optional[Callable] = None,
    ) -> SyncResult:
        """Execute a sync job."""
        job = self._sync_jobs.get(sync_id)
        if not job:
            raise ValueError(f"Sync job not found: {sync_id}")

        start_time = time.time()
        result = SyncResult(
            sync_id=sync_id,
            status="running",
            records_synced=0,
            records_failed=0,
            conflicts=0,
            duration_ms=0,
        )

        try:
            checkpoint = self._checkpoints.get(sync_id)
            last_timestamp = checkpoint.last_sync_timestamp if checkpoint else 0

            source_records = source_reader()

            if job.filter_function:
                source_records = [r for r in source_records if job.filter_function(r)]

            if last_timestamp > 0 and job.mode == SyncMode.INCREMENTAL:
                source_records = [
                    r for r in source_records
                    if r.get("timestamp", 0) > last_timestamp
                ]

            for record in source_records:
                try:
                    mapped_data = self._apply_mappings(record, job.mappings)

                    if job.direction == SyncDirection.BIDIRECTIONAL:
                        conflict = self._detect_conflict(mapped_data, record, target_writer)
                        if conflict:
                            self._conflicts.append(conflict)
                            result.conflicts += 1
                            mapped_data = self._resolve_conflict(
                                conflict, job.conflict_resolution
                            )

                    target_writer(mapped_data)
                    result.records_synced += 1

                except Exception as e:
                    result.records_failed += 1
                    result.errors.append(f"Record {record.get('id')}: {str(e)}")

            checkpoint = SyncCheckpoint(
                sync_id=sync_id,
                last_sync_timestamp=time.time(),
                last_record_id=source_records[-1].get("id") if source_records else None,
            )
            self._checkpoints[sync_id] = checkpoint

            result.status = "completed"
            result.end_time = time.time()
            result.duration_ms = (result.end_time - start_time) * 1000

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.end_time = time.time()

        self._sync_history.append(result)
        return result

    def _apply_mappings(
        self,
        record: dict,
        mappings: list[SyncMapping],
    ) -> dict:
        """Apply field mappings to a record."""
        result = {}
        for mapping in mappings:
            value = record.get(mapping.source_field)
            if mapping.transform:
                value = mapping.transform(value)
            result[mapping.target_field] = value

        unmapped = {k: v for k, v in record.items()
                   if not any(m.source_field == k for m in mappings)}
        result.update(unmapped)

        return result

    def _detect_conflict(
        self,
        source_data: dict,
        original_data: dict,
        target_reader: Callable,
    ) -> Optional[SyncConflict]:
        """Detect if there's a conflict with target data."""
        record_id = source_data.get("id")
        if not record_id:
            return None

        target_data = target_reader(record_id)
        if not target_data:
            return None

        source_hash = hashlib.md5(json.dumps(source_data, sort_keys=True).encode()).hexdigest()
        target_hash = hashlib.md5(json.dumps(target_data, sort_keys=True).encode()).hexdigest()

        if source_hash != target_hash:
            return SyncConflict(
                record_id=record_id,
                source_data=source_data,
                target_data=target_data,
                source_timestamp=source_data.get("timestamp", 0),
                target_timestamp=target_data.get("timestamp", 0),
            )

        return None

    def _resolve_conflict(
        self,
        conflict: SyncConflict,
        resolution: ConflictResolution,
    ) -> dict:
        """Resolve a sync conflict."""
        if resolution == ConflictResolution.SOURCE_WINS:
            return conflict.source_data
        elif resolution == ConflictResolution.TARGET_WINS:
            return conflict.target_data
        elif resolution == ConflictResolution.NEWEST_WINS:
            if conflict.source_timestamp > conflict.target_timestamp:
                return conflict.source_data
            return conflict.target_data

        return conflict.source_data

    def get_checkpoint(self, sync_id: str) -> Optional[SyncCheckpoint]:
        """Get the current checkpoint for a sync job."""
        return self._checkpoints.get(sync_id)

    def set_checkpoint(self, sync_id: str, checkpoint: SyncCheckpoint) -> None:
        """Set a checkpoint for a sync job."""
        self._checkpoints[sync_id] = checkpoint

    def get_sync_history(
        self,
        sync_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[SyncResult]:
        """Get sync history."""
        history = self._sync_history
        if sync_id:
            history = [h for h in history if h.sync_id == sync_id]
        return history[-limit:]

    def get_conflicts(self, sync_id: Optional[str] = None) -> list[SyncConflict]:
        """Get unresolved conflicts."""
        return self._conflicts

    def resolve_conflict(
        self,
        conflict: SyncConflict,
        resolution: str,
        resolved_data: Optional[dict] = None,
    ) -> None:
        """Manually resolve a conflict."""
        conflict.resolution = resolution
        if resolved_data:
            conflict.source_data = resolved_data

    def list_sync_jobs(self) -> list[SyncJob]:
        """List all sync jobs."""
        return list(self._sync_jobs.values())

    def get_sync_stats(self, sync_id: str) -> dict:
        """Get sync statistics."""
        history = [h for h in self._sync_history if h.sync_id == sync_id]
        if not history:
            return {}

        total_synced = sum(h.records_synced for h in history)
        total_failed = sum(h.records_failed for h in history)
        total_conflicts = sum(h.conflicts for h in history)

        return {
            "sync_id": sync_id,
            "total_runs": len(history),
            "total_records_synced": total_synced,
            "total_records_failed": total_failed,
            "total_conflicts": total_conflicts,
            "success_rate": total_synced / (total_synced + total_failed) if (total_synced + total_failed) > 0 else 0,
        }
