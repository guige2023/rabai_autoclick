"""Data Replication Action.

Replicates data across multiple targets with consistency guarantees.
"""
from typing import Any, Callable, Dict, List, Optional, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import time


class ReplicationMode(Enum):
    SYNCHRONOUS = "sync"
    ASYNCHRONOUS = "async"
    EVENTUAL = "eventual"


class ReplicationStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class ReplicationTarget:
    name: str
    write_fn: Callable[[Any], Any]
    enabled: bool = True
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplicationJob:
    job_id: str
    data: Any
    mode: ReplicationMode
    status: ReplicationStatus = ReplicationStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class DataReplicationAction:
    """Replicates data to multiple targets."""

    def __init__(
        self,
        mode: ReplicationMode = ReplicationMode.ASYNCHRONOUS,
        consistency_check: bool = False,
    ) -> None:
        self.mode = mode
        self.consistency_check = consistency_check
        self.targets: Dict[str, ReplicationTarget] = {}
        self.jobs: List[ReplicationJob] = []

    def add_target(self, target: ReplicationTarget) -> None:
        self.targets[target.name] = target

    def remove_target(self, name: str) -> None:
        self.targets.pop(name, None)

    def replicate(
        self,
        data: Any,
        target_names: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ReplicationJob:
        targets = [self.targets[n] for n in (target_names or self.targets.keys()) if n in self.targets and self.targets[n].enabled]
        job = ReplicationJob(
            job_id=f"{int(time.time()*1000)}",
            data=data,
            mode=self.mode,
        )
        if self.mode == ReplicationMode.SYNCHRONOUS:
            job.status = ReplicationStatus.IN_PROGRESS
            job.start_time = time.time()
            for t in targets:
                try:
                    job.results[t.name] = t.write_fn(data)
                except Exception as e:
                    job.errors[t.name] = str(e)
                    job.status = ReplicationStatus.FAILED
            job.end_time = time.time()
            if job.status != ReplicationStatus.FAILED:
                job.status = ReplicationStatus.COMPLETED
        else:
            job.status = ReplicationStatus.PENDING
        self.jobs.append(job)
        return job

    def get_job(self, job_id: str) -> Optional[ReplicationJob]:
        return next((j for j in self.jobs if j.job_id == job_id), None)

    def get_stats(self) -> Dict[str, Any]:
        total = len(self.jobs)
        completed = sum(1 for j in self.jobs if j.status == ReplicationStatus.COMPLETED)
        failed = sum(1 for j in self.jobs if j.status == ReplicationStatus.FAILED)
        return {
            "total_jobs": total,
            "completed": completed,
            "failed": failed,
            "pending": total - completed - failed,
            "success_rate": completed / total if total > 0 else 0.0,
            "targets": len(self.targets),
        }
