"""Queue worker action module for RabAI AutoClick.

Provides queue-based task processing with priorities,
dead letter handling, and concurrent workers.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from threading import Thread, Lock, Event
from collections import deque
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class QueuePriority(Enum):
    """Queue priority levels."""
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class QueueTask:
    """A task in the queue."""
    id: str
    payload: Any
    priority: QueuePriority = QueuePriority.NORMAL
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    max_attempts: int = 3
    timeout: float = 60.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeadLetterEntry:
    """A failed task that exceeded retry limits."""
    task: QueueTask
    error: str
    failed_at: float


class QueueWorkerAction(BaseAction):
    """Process tasks from a queue with multiple workers.
    
    Supports priority-based ordering, dead letter handling,
    concurrent processing, and task acknowledgment.
    """
    action_type = "queue_worker"
    display_name = "队列处理器"
    description = "队列任务处理和工作线程管理"
    
    def __init__(self):
        super().__init__()
        self._queues: Dict[QueuePriority, deque] = {
            QueuePriority.HIGH: deque(),
            QueuePriority.NORMAL: deque(),
            QueuePriority.LOW: deque()
        }
        self._dead_letters: List[DeadLetterEntry] = []
        self._processing: Dict[str, QueueTask] = {}
        self._workers: List[Thread] = []
        self._stop_event = Event()
        self._lock = Lock()
        self._handlers: Dict[str, Callable] = {}
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute queue operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'enqueue', 'dequeue', 'start', 'stop', 'status'
                - task: Task to enqueue
                - handler: Task handler name
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'enqueue').lower()
        
        if operation == 'enqueue':
            return self._enqueue(params)
        elif operation == 'dequeue':
            return self._dequeue(params)
        elif operation == 'start':
            return self._start_workers(params)
        elif operation == 'stop':
            return self._stop_workers(params)
        elif operation == 'status':
            return self._get_status(params)
        elif operation == 'dead_letters':
            return self._get_dead_letters(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _enqueue(self, params: Dict[str, Any]) -> ActionResult:
        """Add a task to the queue."""
        task_id = params.get('task_id') or f"task_{int(time.time() * 1000)}"
        payload = params.get('payload')
        priority = params.get('priority', 'normal').upper()
        
        if payload is None:
            return ActionResult(success=False, message="payload is required")
        
        try:
            prio = QueuePriority[priority]
        except KeyError:
            prio = QueuePriority.NORMAL
        
        task = QueueTask(
            id=task_id,
            payload=payload,
            priority=prio,
            max_attempts=params.get('max_attempts', 3),
            timeout=params.get('timeout', 60.0),
            metadata=params.get('metadata', {})
        )
        
        with self._lock:
            self._queues[prio].append(task)
        
        return ActionResult(
            success=True,
            message=f"Enqueued task {task_id} with priority {priority}",
            data={
                'task_id': task_id,
                'priority': priority,
                'queue_size': self._get_total_size()
            }
        )
    
    def _dequeue(self, params: Dict[str, Any]) -> ActionResult:
        """Get next task from queue."""
        handler_name = params.get('handler', 'default')
        
        with self._lock:
            # Get highest priority task
            task = None
            for prio in [QueuePriority.HIGH, QueuePriority.NORMAL, QueuePriority.LOW]:
                if self._queues[prio]:
                    task = self._queues[prio].popleft()
                    break
            
            if task:
                self._processing[task.id] = task
        
        if not task:
            return ActionResult(
                success=True,
                message="Queue is empty",
                data={'task': None}
            )
        
        # Execute handler
        handler = self._handlers.get(handler_name)
        
        if handler:
            try:
                result = handler(task.payload)
                # Success - task done
                with self._lock:
                    if task.id in self._processing:
                        del self._processing[task.id]
                return ActionResult(
                    success=True,
                    message=f"Task {task.id} processed successfully",
                    data={'task_id': task.id, 'result': result}
                )
            except Exception as e:
                # Handle failure
                return self._handle_task_failure(task, str(e))
        else:
            return ActionResult(
                success=True,
                message=f"Dequeued task {task.id}",
                data={'task_id': task.id, 'payload': task.payload}
            )
    
    def _handle_task_failure(
        self,
        task: QueueTask,
        error: str
    ) -> ActionResult:
        """Handle task processing failure."""
        task.attempts += 1
        
        with self._lock:
            if task.id in self._processing:
                del self._processing[task.id]
        
        if task.attempts >= task.max_attempts:
            # Move to dead letter
            with self._lock:
                self._dead_letters.append(DeadLetterEntry(
                    task=task,
                    error=error,
                    failed_at=time.time()
                ))
            
            return ActionResult(
                success=False,
                message=f"Task {task.id} moved to dead letter after {task.attempts} attempts",
                data={'task_id': task.id, 'error': error}
            )
        else:
            # Re-enqueue with same priority
            with self._lock:
                self._queues[task.priority].append(task)
            
            return ActionResult(
                success=False,
                message=f"Task {task.id} failed, will retry ({task.attempts}/{task.max_attempts})",
                data={'task_id': task.id, 'attempts': task.attempts}
            )
    
    def _start_workers(self, params: Dict[str, Any]) -> ActionResult:
        """Start worker threads."""
        num_workers = params.get('num_workers', 2)
        handler_name = params.get('handler', 'default')
        
        self._stop_event.clear()
        
        for i in range(num_workers):
            worker = Thread(
                target=self._worker_loop,
                args=(handler_name,),
                daemon=True
            )
            worker.start()
            self._workers.append(worker)
        
        return ActionResult(
            success=True,
            message=f"Started {num_workers} workers",
            data={'workers': num_workers}
        )
    
    def _worker_loop(self, handler_name: str) -> None:
        """Worker thread main loop."""
        while not self._stop_event.is_set():
            # Try to dequeue
            result = self._dequeue({'handler': handler_name})
            
            if result.data.get('task') is None:
                # Empty queue, wait
                time.sleep(0.1)
    
    def _stop_workers(self, params: Dict[str, Any]) -> ActionResult:
        """Stop all worker threads."""
        self._stop_event.set()
        
        for worker in self._workers:
            worker.join(timeout=1.0)
        
        self._workers.clear()
        
        return ActionResult(
            success=True,
            message="All workers stopped"
        )
    
    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get queue status."""
        with self._lock:
            return ActionResult(
                success=True,
                message="Queue status",
                data={
                    'queues': {
                        'high': len(self._queues[QueuePriority.HIGH]),
                        'normal': len(self._queues[QueuePriority.NORMAL]),
                        'low': len(self._queues[QueuePriority.LOW])
                    },
                    'processing': len(self._processing),
                    'dead_letters': len(self._dead_letters),
                    'workers': len(self._workers)
                }
            )
    
    def _get_dead_letters(self, params: Dict[str, Any]) -> ActionResult:
        """Get dead letter entries."""
        limit = params.get('limit', 100)
        
        with self._lock:
            entries = [
                {
                    'task_id': e.task.id,
                    'payload': e.task.payload,
                    'error': e.error,
                    'failed_at': e.failed_at,
                    'attempts': e.task.attempts
                }
                for e in self._dead_letters[-limit:]
            ]
        
        return ActionResult(
            success=True,
            message=f"{len(entries)} dead letter entries",
            data={'entries': entries, 'count': len(entries)}
        )
    
    def _get_total_size(self) -> int:
        """Get total queue size."""
        return sum(len(q) for q in self._queues.values())
    
    def register_handler(self, name: str, handler: Callable) -> None:
        """Register a task handler."""
        self._handlers[name] = handler
