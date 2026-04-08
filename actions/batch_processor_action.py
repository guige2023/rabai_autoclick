"""Batch Processor Action Module.

Provides batch processing capabilities with configurable batch sizes,
parallelism, checkpointing, and error handling.
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class BatchStatus(Enum):
    """Batch job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchJob:
    """Batch job definition."""
    id: str
    name: str
    items: List[Any]
    batch_size: int
    status: BatchStatus
    created_at: float = field(default_factory=time.time)
    started_at: float = 0.0
    completed_at: float = 0.0
    progress: float = 0.0
    processed_count: int = 0
    failed_count: int = 0


@dataclass
class BatchResult:
    """Batch processing result."""
    success: bool
    job_id: str
    total_items: int
    processed_count: int
    failed_count: int
    duration_ms: float
    errors: List[Dict[str, Any]] = field(default_factory=list)


class BatchProcessorStore:
    """In-memory batch processor store."""

    def __init__(self):
        self._jobs: Dict[str, BatchJob] = {}
        self._checkpoints: Dict[str, int] = defaultdict(int)

    def create_job(self, name: str, items: List[Any],
                   batch_size: int = 100) -> BatchJob:
        """Create batch job."""
        job_id = uuid.uuid4().hex
        job = BatchJob(
            id=job_id,
            name=name,
            items=items,
            batch_size=batch_size,
            status=BatchStatus.PENDING
        )
        self._jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> Optional[BatchJob]:
        """Get job by ID."""
        return self._jobs.get(job_id)

    def set_checkpoint(self, job_id: str, checkpoint: int) -> None:
        """Set checkpoint."""
        self._checkpoints[job_id] = checkpoint

    def get_checkpoint(self, job_id: str) -> int:
        """Get checkpoint."""
        return self._checkpoints.get(job_id, 0)


_global_store = BatchProcessorStore()


class BatchProcessorAction:
    """Batch processor action.

    Example:
        action = BatchProcessorAction()

        job_id = action.create_job("process-users", user_ids, batch_size=50)
        result = action.run(job_id, process_func)
    """

    def __init__(self, store: Optional[BatchProcessorStore] = None):
        self._store = store or _global_store

    def create_job(self, name: str, items: List[Any],
                   batch_size: int = 100) -> Dict[str, Any]:
        """Create batch job."""
        job = self._store.create_job(name, items, batch_size)

        return {
            "success": True,
            "job": {
                "id": job.id,
                "name": job.name,
                "total_items": len(items),
                "batch_size": batch_size,
                "status": job.status.value,
                "created_at": job.created_at
            },
            "message": f"Created batch job: {name}"
        }

    def get_job(self, job_id: str) -> Dict[str, Any]:
        """Get job status."""
        job = self._store.get_job(job_id)
        if job:
            return {
                "success": True,
                "job": {
                    "id": job.id,
                    "name": job.name,
                    "total_items": len(job.items),
                    "batch_size": job.batch_size,
                    "status": job.status.value,
                    "progress": job.progress,
                    "processed_count": job.processed_count,
                    "failed_count": job.failed_count,
                    "created_at": job.created_at,
                    "started_at": job.started_at,
                    "completed_at": job.completed_at
                }
            }
        return {"success": False, "message": "Job not found"}

    def run(self, job_id: str,
            process_func: Optional[Callable] = None) -> Dict[str, Any]:
        """Run batch job (simulated)."""
        job = self._store.get_job(job_id)
        if not job:
            return {"success": False, "message": "Job not found"}

        start = time.time()
        job.status = BatchStatus.RUNNING
        job.started_at = time.time()

        checkpoint = self._store.get_checkpoint(job_id)
        processed = checkpoint
        failed = 0
        errors = []

        total_batches = (len(job.items) + job.batch_size - 1) // job.batch_size
        for i in range(checkpoint, total_batches):
            batch_num = i + 1
            job.progress = (batch_num / total_batches) * 100
            job.processed_count = batch_num * job.batch_size
            processed = batch_num

            self._store.set_checkpoint(job_id, batch_num)

            time.sleep(0.01)

        job.status = BatchStatus.COMPLETED
        job.progress = 100.0
        job.completed_at = time.time()
        job.processed_count = len(job.items)

        return {
            "success": True,
            "job_id": job_id,
            "total_items": len(job.items),
            "processed_count": processed * job.batch_size,
            "failed_count": failed,
            "duration_ms": (time.time() - start) * 1000,
            "errors": errors,
            "message": f"Completed batch job: {job.name}"
        }

    def cancel(self, job_id: str) -> Dict[str, Any]:
        """Cancel batch job."""
        job = self._store.get_job(job_id)
        if not job:
            return {"success": False, "message": "Job not found"}

        job.status = BatchStatus.CANCELLED
        job.completed_at = time.time()

        return {
            "success": True,
            "job_id": job_id,
            "message": f"Cancelled batch job: {job.name}"
        }

    def get_checkpoint(self, job_id: str) -> Dict[str, Any]:
        """Get job checkpoint."""
        checkpoint = self._store.get_checkpoint(job_id)
        return {
            "success": True,
            "job_id": job_id,
            "checkpoint": checkpoint,
            "message": f"Checkpoint: {checkpoint}"
        }

    def reset_checkpoint(self, job_id: str) -> Dict[str, Any]:
        """Reset checkpoint to beginning."""
        self._store.set_checkpoint(job_id, 0)
        return {
            "success": True,
            "job_id": job_id,
            "message": "Checkpoint reset"
        }

    def list_jobs(self, status: Optional[str] = None) -> Dict[str, Any]:
        """List all jobs."""
        jobs = list(self._store._jobs.values())
        if status:
            try:
                batch_status = BatchStatus(status)
                jobs = [j for j in jobs if j.status == batch_status]
            except ValueError:
                pass

        return {
            "success": True,
            "jobs": [
                {
                    "id": j.id,
                    "name": j.name,
                    "total_items": len(j.items),
                    "status": j.status.value,
                    "progress": j.progress
                }
                for j in jobs
            ],
            "count": len(jobs)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute batch processor action."""
    operation = params.get("operation", "")
    action = BatchProcessorAction()

    try:
        if operation == "create":
            name = params.get("name", "")
            items = params.get("items", [])
            batch_size = params.get("batch_size", 100)
            if not name or not items:
                return {"success": False, "message": "name and items required"}
            return action.create_job(name, items, batch_size)

        elif operation == "get":
            job_id = params.get("job_id", "")
            if not job_id:
                return {"success": False, "message": "job_id required"}
            return action.get_job(job_id)

        elif operation == "run":
            job_id = params.get("job_id", "")
            if not job_id:
                return {"success": False, "message": "job_id required"}
            return action.run(job_id)

        elif operation == "cancel":
            job_id = params.get("job_id", "")
            if not job_id:
                return {"success": False, "message": "job_id required"}
            return action.cancel(job_id)

        elif operation == "get_checkpoint":
            job_id = params.get("job_id", "")
            if not job_id:
                return {"success": False, "message": "job_id required"}
            return action.get_checkpoint(job_id)

        elif operation == "reset_checkpoint":
            job_id = params.get("job_id", "")
            if not job_id:
                return {"success": False, "message": "job_id required"}
            return action.reset_checkpoint(job_id)

        elif operation == "list":
            return action.list_jobs(params.get("status"))

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Batch processor error: {str(e)}"}
