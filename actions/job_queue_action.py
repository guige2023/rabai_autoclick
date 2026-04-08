"""Job queue action module for RabAI AutoClick.

Provides job queue management with enqueue, dequeue, priority support,
dead letter handling, and job status tracking.
"""

import json
import time
import sys
import os
import uuid
import threading
from typing import Any, Dict, List, Optional, Union, Callable
from collections import deque
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JobStatus(Enum):
    """Job status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Job:
    """Represents a queued job."""
    
    def __init__(
        self,
        job_id: str,
        payload: Any,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.job_id = job_id
        self.payload = payload
        self.priority = priority
        self.metadata = metadata or {}
        self.status = JobStatus.PENDING
        self.created_at = time.time()
        self.started_at = None
        self.completed_at = None
        self.result = None
        self.error = None
        self.attempts = 0
        self.max_attempts = metadata.get('max_attempts', 3) if metadata else 3
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary."""
        return {
            'job_id': self.job_id,
            'payload': self.payload,
            'priority': self.priority,
            'metadata': self.metadata,
            'status': self.status.value,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'result': self.result,
            'error': self.error,
            'attempts': self.attempts,
            'max_attempts': self.max_attempts
        }


class JobQueueAction(BaseAction):
    """Manage job queues with priority, retries, and dead letter handling.
    
    Supports priority-based ordering, job retries with backoff,
    dead letter queue for failed jobs, and status tracking.
    """
    action_type = "job_queue"
    display_name = "作业队列"
    description = "作业队列管理，支持优先级和重试"
    
    def __init__(self):
        super().__init__()
        self._queues: Dict[str, List[Job]] = {}
        self._dead_letter_queue: List[Job] = []
        self._processing: Dict[str, Job] = {}
        self._lock = threading.RLock()
        self._callbacks: Dict[str, Callable] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute job queue operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (enqueue, dequeue, status,
                   list_queues, move_to_dlq, retry), queue_name, job_config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'enqueue')
        queue_name = params.get('queue_name', 'default')
        
        with self._lock:
            if queue_name not in self._queues:
                self._queues[queue_name] = []
            
            if action == 'enqueue':
                return self._enqueue(queue_name, params)
            elif action == 'dequeue':
                return self._dequeue(queue_name, params)
            elif action == 'status':
                return self._get_status(queue_name, params)
            elif action == 'list_queues':
                return self._list_queues()
            elif action == 'move_to_dlq':
                return self._move_to_dlq(queue_name, params)
            elif action == 'retry':
                return self._retry_job(queue_name, params)
            elif action == 'complete':
                return self._complete_job(queue_name, params)
            elif action == 'fail':
                return self._fail_job(queue_name, params)
            elif action == 'list_jobs':
                return self._list_jobs(queue_name, params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown action: {action}"
                )
    
    def _enqueue(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Enqueue a new job."""
        payload = params.get('payload')
        if payload is None:
            return ActionResult(success=False, message="payload is required")
        
        priority = params.get('priority', 0)
        job_id = params.get('job_id', str(uuid.uuid4()))
        metadata = params.get('metadata', {})
        max_attempts = params.get('max_attempts', 3)
        
        metadata['max_attempts'] = max_attempts
        
        job = Job(job_id, payload, priority, metadata)
        
        queue = self._queues[queue_name]
        queue.append(job)
        queue.sort(key=lambda j: j.priority, reverse=True)
        
        return ActionResult(
            success=True,
            message=f"Job {job_id} enqueued with priority {priority}",
            data={'job_id': job_id, 'queue_size': len(queue)}
        )
    
    def _dequeue(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Dequeue a job for processing."""
        timeout = params.get('timeout', 0)
        max_attempts = params.get('max_attempts', 3)
        
        queue = self._queues.get(queue_name, [])
        
        start_time = time.time()
        while True:
            for idx, job in enumerate(queue):
                if job.status == JobStatus.PENDING:
                    job.status = JobStatus.PROCESSING
                    job.started_at = time.time()
                    job.attempts += 1
                    self._processing[job.job_id] = job
                    queue.pop(idx)
                    
                    return ActionResult(
                        success=True,
                        message=f"Dequeued job {job.job_id} (attempt {job.attempts})",
                        data=job.to_dict()
                    )
            
            if timeout <= 0:
                return ActionResult(
                    success=False,
                    message="Queue is empty",
                    data={'queue_size': len(queue)}
                )
            
            if time.time() - start_time >= timeout:
                return ActionResult(
                    success=False,
                    message=f"Dequeue timeout after {timeout}s",
                    data={'queue_size': len(queue)}
                )
            
            time.sleep(0.1)
    
    def _complete_job(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Mark a job as completed."""
        job_id = params.get('job_id')
        result = params.get('result')
        
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        if job_id not in self._processing:
            return ActionResult(success=False, message=f"Job {job_id} not in processing")
        
        job = self._processing.pop(job_id)
        job.status = JobStatus.COMPLETED
        job.completed_at = time.time()
        job.result = result
        
        return ActionResult(
            success=True,
            message=f"Job {job_id} completed",
            data=job.to_dict()
        )
    
    def _fail_job(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Mark a job as failed."""
        job_id = params.get('job_id')
        error = params.get('error', 'Unknown error')
        
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        if job_id not in self._processing:
            return ActionResult(success=False, message=f"Job {job_id} not in processing")
        
        job = self._processing.pop(job_id)
        job.error = error
        
        if job.attempts < job.max_attempts:
            job.status = JobStatus.PENDING
            job.started_at = None
            if queue_name not in self._queues:
                self._queues[queue_name] = []
            self._queues[queue_name].append(job)
            self._queues[queue_name].sort(key=lambda j: j.priority, reverse=True)
            
            return ActionResult(
                success=True,
                message=f"Job {job_id} failed, requeued (attempt {job.attempts}/{job.max_attempts})",
                data=job.to_dict()
            )
        else:
            job.status = JobStatus.FAILED
            job.completed_at = time.time()
            self._dead_letter_queue.append(job)
            
            return ActionResult(
                success=False,
                message=f"Job {job_id} moved to DLQ after {job.attempts} attempts",
                data=job.to_dict()
            )
    
    def _get_status(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get status of a specific job."""
        job_id = params.get('job_id')
        
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        if job_id in self._processing:
            return ActionResult(
                success=True,
                message=f"Job {job_id} is processing",
                data=self._processing[job_id].to_dict()
            )
        
        for queue in self._queues.values():
            for job in queue:
                if job.job_id == job_id:
                    return ActionResult(
                        success=True,
                        message=f"Job {job_id} is in queue",
                        data=job.to_dict()
                    )
        
        for job in self._dead_letter_queue:
            if job.job_id == job_id:
                return ActionResult(
                    success=True,
                    message=f"Job {job_id} is in DLQ",
                    data=job.to_dict()
                )
        
        return ActionResult(
            success=False,
            message=f"Job {job_id} not found"
        )
    
    def _list_queues(self) -> ActionResult:
        """List all queues and their sizes."""
        result = {}
        total_pending = 0
        total_processing = len(self._processing)
        
        for name, queue in self._queues.items():
            pending = sum(1 for j in queue if j.status == JobStatus.PENDING)
            result[name] = {
                'pending': pending,
                'total': len(queue)
            }
            total_pending += pending
        
        return ActionResult(
            success=True,
            message=f"Found {len(self._queues)} queues",
            data={
                'queues': result,
                'total_pending': total_pending,
                'total_processing': total_processing,
                'dead_letter_size': len(self._dead_letter_queue)
            }
        )
    
    def _move_to_dlq(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manually move a job to dead letter queue."""
        job_id = params.get('job_id')
        
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        if job_id in self._processing:
            job = self._processing.pop(job_id)
            job.status = JobStatus.FAILED
            job.completed_at = time.time()
            self._dead_letter_queue.append(job)
            
            return ActionResult(
                success=True,
                message=f"Job {job_id} moved to DLQ",
                data=job.to_dict()
            )
        
        return ActionResult(
            success=False,
            message=f"Job {job_id} not found in processing"
        )
    
    def _retry_job(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Retry a failed job from DLQ."""
        job_id = params.get('job_id')
        
        if not job_id:
            return ActionResult(success=False, message="job_id is required")
        
        for idx, job in enumerate(self._dead_letter_queue):
            if job.job_id == job_id:
                job.status = JobStatus.PENDING
                job.attempts = 0
                job.error = None
                job.started_at = None
                job.completed_at = None
                self._dead_letter_queue.pop(idx)
                
                if queue_name not in self._queues:
                    self._queues[queue_name] = []
                self._queues[queue_name].append(job)
                
                return ActionResult(
                    success=True,
                    message=f"Job {job_id} retry scheduled",
                    data=job.to_dict()
                )
        
        return ActionResult(
            success=False,
            message=f"Job {job_id} not found in DLQ"
        )
    
    def _list_jobs(
        self,
        queue_name: str,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List jobs in a queue."""
        status_filter = params.get('status')
        
        queue = self._queues.get(queue_name, [])
        jobs = [job.to_dict() for job in queue]
        
        if status_filter:
            try:
                status_enum = JobStatus(status_filter)
                jobs = [j for j in jobs if j['status'] == status_enum.value]
            except ValueError:
                pass
        
        return ActionResult(
            success=True,
            message=f"Found {len(jobs)} jobs",
            data={'jobs': jobs, 'count': len(jobs)}
        )
