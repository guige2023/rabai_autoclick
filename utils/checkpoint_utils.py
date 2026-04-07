"""Checkpoint and restart utilities.

Checkpoint-based processing for long-running tasks with restart capability.
Enables resuming from failure without reprocessing completed work.

Example:
    checkpoint = CheckpointManager("task_v1", storage=JSONStateStore("./checkpoints"))
    for item in checkpoint.resume(items):
        process(item)
        checkpoint.mark_done(item["id"])
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Represents a processing checkpoint."""
    task_id: str
    version: str
    completed_ids: set[str] = field(default_factory=set)
    failed_ids: dict[str, str] = field(default_factory=dict)
    total_count: int = 0
    processed_count: int = 0
    started_at: datetime | None = None
    last_updated: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class CheckpointManager:
    """Manages checkpoints for resumable batch processing.

    Tracks completed and failed items, persists state, and provides
    iterators that skip already-processed items on resume.
    """

    def __init__(
        self,
        task_id: str,
        version: str = "v1",
        checkpoint_dir: str | Path = "./checkpoints",
        auto_save_interval: int = 10,
    ) -> None:
        """Initialize checkpoint manager.

        Args:
            task_id: Unique identifier for the task.
            version: Version string for cache busting on schema changes.
            checkpoint_dir: Directory for checkpoint files.
            auto_save_interval: Auto-save after this many mark_done calls.
        """
        self.task_id = task_id
        self.version = version
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.auto_save_interval = auto_save_interval

        self._checkpoint = Checkpoint(
            task_id=task_id,
            version=version,
            started_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
        )
        self._since_save = 0
        self._lock = threading.RLock()

        self._load()

    def _checkpoint_path(self) -> Path:
        """Get path for checkpoint file."""
        return self.checkpoint_dir / f"{self.task_id}.json"

    def _load(self) -> None:
        """Load checkpoint from disk if exists."""
        path = self._checkpoint_path()
        if not path.exists():
            return

        try:
            data = json.loads(path.read_text())
            self._checkpoint = Checkpoint(
                task_id=data["task_id"],
                version=data["version"],
                completed_ids=set(data.get("completed_ids", [])),
                failed_ids=data.get("failed_ids", {}),
                total_count=data.get("total_count", 0),
                processed_count=data.get("processed_count", 0),
                started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
                last_updated=datetime.fromisoformat(data["last_updated"]) if data.get("last_updated") else None,
                metadata=data.get("metadata", {}),
            )
            logger.info(
                "Loaded checkpoint: %d completed, %d failed",
                len(self._checkpoint.completed_ids),
                len(self._checkpoint.failed_ids),
            )
        except Exception as e:
            logger.error("Failed to load checkpoint: %s", e)

    def _save(self) -> None:
        """Persist checkpoint to disk."""
        path = self._checkpoint_path()
        data = {
            "task_id": self._checkpoint.task_id,
            "version": self._checkpoint.version,
            "completed_ids": list(self._checkpoint.completed_ids),
            "failed_ids": self._checkpoint.failed_ids,
            "total_count": self._checkpoint.total_count,
            "processed_count": self._checkpoint.processed_count,
            "started_at": self._checkpoint.started_at.isoformat() if self._checkpoint.started_at else None,
            "last_updated": datetime.utcnow().isoformat(),
            "metadata": self._checkpoint.metadata,
        }
        path.write_text(json.dumps(data, indent=2))
        self._since_save = 0

    def resume(
        self,
        items: Sequence[dict[str, Any]],
        id_field: str = "id",
    ) -> Generator[dict[str, Any], None, None]:
        """Iterate items, skipping already completed ones.

        Args:
            items: Sequence of items to process.
            id_field: Field name for item identifier.

        Yields:
            Items that haven't been processed yet.
        """
        self._checkpoint.total_count = len(items)
        self._checkpoint.processed_count = len(self._checkpoint.completed_ids)

        for item in items:
            item_id = str(item.get(id_field))
            if item_id in self._checkpoint.completed_ids:
                logger.debug("Skipping completed item: %s", item_id)
                continue
            if item_id in self._checkpoint.failed_ids:
                logger.debug("Skipping previously failed item: %s", item_id)
                continue
            yield item

    def mark_done(self, item_id: str) -> None:
        """Mark an item as successfully processed.

        Args:
            item_id: Identifier of processed item.
        """
        with self._lock:
            self._checkpoint.completed_ids.add(str(item_id))
            self._checkpoint.processed_count = len(self._checkpoint.completed_ids)
            self._checkpoint.last_updated = datetime.utcnow()
            self._since_save += 1

            if self._since_save >= self.auto_save_interval:
                self._save()

    def mark_failed(self, item_id: str, error: str) -> None:
        """Mark an item as failed.

        Args:
            item_id: Identifier of failed item.
            error: Error message or description.
        """
        with self._lock:
            self._checkpoint.failed_ids[str(item_id)] = error
            self._checkpoint.last_updated = datetime.utcnow()
            self._since_save += 1

            if self._since_save >= self.auto_save_interval:
                self._save()

    def save(self) -> None:
        """Force save checkpoint to disk."""
        with self._lock:
            self._save()

    def reset(self) -> None:
        """Reset checkpoint, clearing all progress."""
        with self._lock:
            self._checkpoint.completed_ids.clear()
            self._checkpoint.failed_ids.clear()
            self._checkpoint.processed_count = 0
            self._checkpoint.last_updated = datetime.utcnow()
            self._save()

    def get_stats(self) -> dict[str, Any]:
        """Get checkpoint statistics."""
        return {
            "task_id": self.task_id,
            "version": self.version,
            "total": self._checkpoint.total_count,
            "completed": len(self._checkpoint.completed_ids),
            "failed": len(self._checkpoint.failed_ids),
            "remaining": (
                self._checkpoint.total_count
                - len(self._checkpoint.completed_ids)
                - len(self._checkpoint.failed_ids)
            ),
            "progress_percent": (
                len(self._checkpoint.completed_ids) / max(self._checkpoint.total_count, 1)
            ) * 100,
            "last_updated": (
                self._checkpoint.last_updated.isoformat()
                if self._checkpoint.last_updated else None
            ),
        }


class CheckpointProcessor:
    """Wraps processing with automatic checkpointing.

    Example:
        processor = CheckpointProcessor(
            task_id="import_v2",
            items=records,
            process_fn=lambda r: db.insert(r),
        )
        stats = processor.run()
    """

    def __init__(
        self,
        task_id: str,
        items: Sequence[dict[str, Any]],
        process_fn: Callable[[dict[str, Any]], Any],
        checkpoint_manager: CheckpointManager | None = None,
        id_field: str = "id",
        continue_on_error: bool = True,
    ) -> None:
        self.checkpoint = checkpoint_manager or CheckpointManager(task_id)
        self.items = items
        self.process_fn = process_fn
        self.id_field = id_field
        self.continue_on_error = continue_on_error

    def run(self) -> dict[str, Any]:
        """Run processing with checkpoint support.

        Returns:
            Dict with processing statistics.
        """
        import time
        start = time.perf_counter()
        processed = 0
        failed = 0

        for item in self.checkpoint.resume(self.items, id_field=self.id_field):
            item_id = str(item.get(self.id_field, "unknown"))

            try:
                self.process_fn(item)
                self.checkpoint.mark_done(item_id)
                processed += 1

                if processed % 100 == 0:
                    logger.info("Checkpoint: %s", self.checkpoint.get_stats())

            except Exception as e:
                logger.error("Failed to process %s: %s", item_id, e)
                self.checkpoint.mark_failed(item_id, str(e))
                failed += 1

                if not self.continue_on_error:
                    break

        self.checkpoint.save()

        return {
            "task_id": self.checkpoint.task_id,
            "processed": processed,
            "failed": failed,
            "duration_seconds": time.perf_counter() - start,
            "stats": self.checkpoint.get_stats(),
        }
