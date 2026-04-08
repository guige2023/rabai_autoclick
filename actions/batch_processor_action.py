"""
Batch processing and ETL module for large-scale data operations.

Supports chunking, parallel processing, checkpointing,
error handling, and data transformation pipelines.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Iterator, Optional


class ProcessingMode(Enum):
    """Batch processing mode."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    DISTRIBUTED = "distributed"


class ProcessingStatus(Enum):
    """Processing job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 1000
    max_parallelism: int = 4
    mode: ProcessingMode = ProcessingMode.SEQUENTIAL
    retry_count: int = 3
    retry_delay_seconds: float = 1.0
    checkpoint_enabled: bool = True
    checkpoint_interval: int = 10
    timeout_seconds: int = 3600


@dataclass
class Chunk:
    """A chunk of data for processing."""
    id: str
    index: int
    data: list
    start_offset: int
    end_offset: int
    status: ProcessingStatus = ProcessingStatus.PENDING
    attempts: int = 0
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    def duration_ms(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time) * 1000
        return 0.0


@dataclass
class Transform:
    """A data transformation step."""
    name: str
    func: Callable
    input_field: Optional[str] = None
    output_field: Optional[str] = None
    condition: Optional[Callable] = None


@dataclass
class BatchJob:
    """A batch processing job."""
    id: str
    name: str
    config: BatchConfig
    total_records: int
    chunks: list[Chunk] = field(default_factory=list)
    status: ProcessingStatus = ProcessingStatus.PENDING
    progress: float = 0.0
    processed_count: int = 0
    failed_count: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    checkpoint_data: dict = field(default_factory=dict)


@dataclass
class BatchResult:
    """Result of batch processing."""
    job_id: str
    status: ProcessingStatus
    total_records: int
    processed_records: int
    failed_records: int
    duration_ms: float
    chunks_processed: int
    chunks_failed: int
    errors: list[str] = field(default_factory=list)


class BatchProcessor:
    """
    Batch processing and ETL service.

    Provides chunking, parallel processing, checkpointing,
    and transformation pipelines for large-scale data operations.
    """

    def __init__(self):
        self._jobs: dict[str, BatchJob] = {}
        self._checkpoints: dict[str, dict] = {}

    def create_job(
        self,
        name: str,
        data: list,
        config: Optional[BatchConfig] = None,
    ) -> BatchJob:
        """Create a new batch processing job."""
        config = config or BatchConfig()
        job_id = str(uuid.uuid4())[:12]

        chunks = self._create_chunks(data, config.chunk_size)

        job = BatchJob(
            id=job_id,
            name=name,
            config=config,
            total_records=len(data),
            chunks=chunks,
        )

        self._jobs[job_id] = job
        return job

    def _create_chunks(self, data: list, chunk_size: int) -> list[Chunk]:
        """Split data into chunks."""
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunk = Chunk(
                id=str(uuid.uuid4())[:8],
                index=i // chunk_size,
                data=data[i:i + chunk_size],
                start_offset=i,
                end_offset=min(i + chunk_size, len(data)),
            )
            chunks.append(chunk)
        return chunks

    def execute_job(
        self,
        job_id: str,
        processor: Callable,
        transforms: Optional[list[Transform]] = None,
    ) -> BatchResult:
        """Execute a batch processing job."""
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        job.status = ProcessingStatus.RUNNING

        try:
            for i, chunk in enumerate(job.chunks):
                if job.status == ProcessingStatus.CANCELLED:
                    break

                self._process_chunk(chunk, processor, transforms)

                job.processed_count += chunk.end_offset - chunk.start_offset
                if chunk.status == ProcessingStatus.FAILED:
                    job.failed_count += len(chunk.data)

                if job.config.checkpoint_enabled and (i + 1) % job.config.checkpoint_interval == 0:
                    self._save_checkpoint(job)

                job.progress = (i + 1) / len(job.chunks) * 100

            failed_chunks = [c for c in job.chunks if c.status == ProcessingStatus.FAILED]
            job.status = ProcessingStatus.COMPLETED if not failed_chunks else ProcessingStatus.FAILED

        except Exception as e:
            job.status = ProcessingStatus.FAILED

        job.end_time = time.time()

        return BatchResult(
            job_id=job_id,
            status=job.status,
            total_records=job.total_records,
            processed_records=job.processed_count,
            failed_records=job.failed_count,
            duration_ms=(job.end_time - job.start_time) * 1000,
            chunks_processed=sum(1 for c in job.chunks if c.status == ProcessingStatus.COMPLETED),
            chunks_failed=sum(1 for c in job.chunks if c.status == ProcessingStatus.FAILED),
        )

    def _process_chunk(
        self,
        chunk: Chunk,
        processor: Callable,
        transforms: Optional[list[Transform]],
    ) -> None:
        """Process a single chunk."""
        chunk.status = ProcessingStatus.RUNNING
        chunk.start_time = time.time()
        chunk.attempts += 1

        try:
            data = chunk.data

            if transforms:
                for transform in transforms:
                    if transform.condition and not transform.condition(data):
                        continue
                    data = self._apply_transform(data, transform)

            result = processor(data)

            chunk.status = ProcessingStatus.COMPLETED
            chunk.end_time = time.time()

        except Exception as e:
            chunk.error = str(e)
            chunk.status = ProcessingStatus.FAILED
            chunk.end_time = time.time()

    def _apply_transform(self, data: list, transform: Transform) -> list:
        """Apply a transformation to data."""
        if transform.input_field and transform.output_field:
            return [
                {**item, transform.output_field: transform.func(item.get(transform.input_field))}
                for item in data
            ]
        return [transform.func(item) for item in data]

    def _save_checkpoint(self, job: BatchJob) -> None:
        """Save a processing checkpoint."""
        self._checkpoints[job.id] = {
            "processed_count": job.processed_count,
            "failed_count": job.failed_count,
            "progress": job.progress,
            "chunk_index": job.chunks.index(next(
                c for c in job.chunks if c.status == ProcessingStatus.PENDING
            )) if any(c.status == ProcessingStatus.PENDING for c in job.chunks) else len(job.chunks),
            "timestamp": time.time(),
        }

    def resume_job(
        self,
        job_id: str,
        processor: Callable,
        transforms: Optional[list[Transform]] = None,
    ) -> BatchResult:
        """Resume a failed or paused job from checkpoint."""
        job = self._jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        checkpoint = self._checkpoints.get(job_id)
        if checkpoint:
            pending_chunks = [
                c for c in job.chunks
                if c.index >= checkpoint.get("chunk_index", 0)
                and c.status in (ProcessingStatus.PENDING, ProcessingStatus.FAILED)
            ]
        else:
            pending_chunks = [c for c in job.chunks if c.status == ProcessingStatus.PENDING]

        job.chunks = pending_chunks
        job.status = ProcessingStatus.RUNNING

        return self.execute_job(job_id, processor, transforms)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        job = self._jobs.get(job_id)
        if not job:
            return False

        job.status = ProcessingStatus.CANCELLED
        return True

    def get_job_status(self, job_id: str) -> Optional[BatchJob]:
        """Get the status of a job."""
        return self._jobs.get(job_id)

    def list_jobs(self, status: Optional[ProcessingStatus] = None) -> list[BatchJob]:
        """List all jobs with optional status filter."""
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        return sorted(jobs, key=lambda j: j.start_time, reverse=True)

    def delete_job(self, job_id: str) -> bool:
        """Delete a job and its checkpoint."""
        if job_id in self._jobs:
            del self._jobs[job_id]
        if job_id in self._checkpoints:
            del self._checkpoints[job_id]
        return True
