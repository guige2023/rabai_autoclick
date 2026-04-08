"""
Workflow Scheduler Action Module.

Provides advanced workflow scheduling capabilities including cron expressions,
dependent workflows, priority queues, and workflow state persistence.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class ScheduleType(Enum):
    """Types of scheduling mechanisms."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    DEPENDENT = "dependent"
    EVENT_DRIVEN = "event_driven"


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class Priority(Enum):
    """Workflow priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


@dataclass
class CronSchedule:
    """Cron expression schedule."""
    expression: str
    timezone: str = "UTC"
    next_fire_times: List[datetime] = field(default_factory=list)

    def get_next_fire(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """Calculate next fire time from cron expression."""
        # Simplified cron parser - in production use croniter
        return None  # Placeholder


@dataclass
class IntervalSchedule:
    """Fixed interval schedule."""
    interval_seconds: float
    interval_type: str = "seconds"


@dataclass
class WorkflowDefinition:
    """Definition of a schedulable workflow."""
    id: str
    name: str
    func: Callable
    schedule: Any
    schedule_type: ScheduleType
    enabled: bool = True
    timeout_seconds: Optional[float] = None
    retry_config: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowInstance:
    """Instance of a scheduled workflow execution."""
    id: str
    workflow_id: str
    status: WorkflowStatus
    priority: Priority = Priority.NORMAL
    scheduled_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Any = None
    error: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "priority": self.priority.value,
            "scheduled_at": self.scheduled_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error": self.error,
        }


@dataclass
class ScheduledJob:
    """A scheduled job in the queue."""
    id: str
    workflow_def: WorkflowDefinition
    next_run: datetime
    last_run: Optional[datetime] = None
    run_count: int = 0
    enabled: bool = True
    tags: Set[str] = field(default_factory=set)


class WorkflowScheduler:
    """
    Advanced workflow scheduler with cron, interval, and dependency support.

    Manages workflow scheduling, execution queuing, and state tracking.
    Supports dependent workflows, priority scheduling, and workflow persistence.

    Example:
        >>> scheduler = WorkflowScheduler()
        >>> job_id = scheduler.schedule_cron("daily_report", func, "0 8 * * *")
        >>> scheduler.schedule_interval("health_check", func, interval_seconds=60)
        >>> scheduler.start()  # Start the scheduler loop
    """

    def __init__(self, storage: Optional[Callable] = None):
        self.storage = storage
        self.workflows: Dict[str, WorkflowDefinition] = {}
        self.jobs: Dict[str, ScheduledJob] = {}
        self.instances: Dict[str, WorkflowInstance] = {}
        self.pending_queue: List[WorkflowInstance] = []
        self.running: Set[str] = set()
        self.completed: Dict[str, List[str]] = defaultdict(list)
        self.failed: Dict[str, List[str]] = defaultdict(list)
        self._execution_fn: Optional[Callable] = None

    def register_workflow(
        self,
        name: str,
        func: Callable,
        schedule: Any,
        schedule_type: ScheduleType,
        timeout: Optional[float] = None,
        max_retries: int = 3,
    ) -> str:
        """Register a new workflow definition."""
        workflow_id = str(uuid.uuid4())
        workflow = WorkflowDefinition(
            id=workflow_id,
            name=name,
            func=func,
            schedule=schedule,
            schedule_type=schedule_type,
            timeout_seconds=timeout,
            retry_config={"max_retries": max_retries},
        )
        self.workflows[workflow_id] = workflow
        return workflow_id

    def schedule_cron(
        self,
        name: str,
        func: Callable,
        cron_expression: str,
        timezone: str = "UTC",
        enabled: bool = True,
    ) -> str:
        """Schedule a workflow with a cron expression."""
        schedule = CronSchedule(expression=cron_expression, timezone=timezone)
        workflow_id = self.register_workflow(
            name=name,
            func=func,
            schedule=schedule,
            schedule_type=ScheduleType.CRON,
        )
        job = ScheduledJob(
            id=str(uuid.uuid4()),
            workflow_def=self.workflows[workflow_id],
            next_run=datetime.now(),
            enabled=enabled,
        )
        self.jobs[job.id] = job
        return job.id

    def schedule_interval(
        self,
        name: str,
        func: Callable,
        interval_seconds: float,
        enabled: bool = True,
    ) -> str:
        """Schedule a workflow to run at fixed intervals."""
        schedule = IntervalSchedule(interval_seconds=interval_seconds)
        workflow_id = self.register_workflow(
            name=name,
            func=func,
            schedule=schedule,
            schedule_type=ScheduleType.INTERVAL,
        )
        job = ScheduledJob(
            id=str(uuid.uuid4()),
            workflow_def=self.workflows[workflow_id],
            next_run=datetime.now(),
            enabled=enabled,
        )
        self.jobs[job.id] = job
        return job.id

    def schedule_dependent(
        self,
        name: str,
        func: Callable,
        dependencies: List[str],
    ) -> str:
        """Schedule a workflow that depends on other workflows completing."""
        workflow_id = self.register_workflow(
            name=name,
            func=func,
            schedule=None,
            schedule_type=ScheduleType.DEPENDENT,
        )
        instance = WorkflowInstance(
            id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            status=WorkflowStatus.PENDING,
            dependencies=dependencies,
        )
        self.instances[instance.id] = instance
        self._check_dependencies_and_queue(instance)
        return instance.id

    def trigger_now(self, workflow_id: str, priority: Priority = Priority.NORMAL) -> str:
        """Trigger a workflow to run immediately."""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow {workflow_id} not found")

        instance = WorkflowInstance(
            id=str(uuid.uuid4()),
            workflow_id=workflow_id,
            status=WorkflowStatus.PENDING,
            priority=priority,
            scheduled_at=datetime.now(),
        )
        self.instances[instance.id] = instance
        self._queue_instance(instance)
        return instance.id

    def cancel_instance(self, instance_id: str) -> bool:
        """Cancel a running or pending workflow instance."""
        if instance_id not in self.instances:
            return False
        instance = self.instances[instance_id]
        if instance.status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED):
            return False
        instance.status = WorkflowStatus.CANCELLED
        if instance_id in self.running:
            self.running.remove(instance_id)
        return True

    def get_instance_status(self, instance_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a workflow instance."""
        if instance_id not in self.instances:
            return None
        return self.instances[instance_id].to_dict()

    def get_pending_workflows(self) -> List[Dict[str, Any]]:
        """Get list of pending workflow instances."""
        return [
            inst.to_dict()
            for inst in self.instances.values()
            if inst.status == WorkflowStatus.PENDING
        ]

    def get_running_workflows(self) -> List[Dict[str, Any]]:
        """Get list of currently running workflow instances."""
        return [
            inst.to_dict()
            for inst in self.instances.values()
            if inst.status == WorkflowStatus.RUNNING
        ]

    def get_workflow_history(self, workflow_id: str, limit: int = 50) -> Dict[str, List]:
        """Get execution history for a workflow."""
        return {
            "completed": self.completed.get(workflow_id, [])[-limit:],
            "failed": self.failed.get(workflow_id, [])[-limit:],
        }

    def _queue_instance(self, instance: WorkflowInstance) -> None:
        """Add instance to priority queue."""
        instance.status = WorkflowStatus.PENDING
        self.pending_queue.append(instance)
        self.pending_queue.sort(key=lambda x: x.priority.value)

    def _check_dependencies_and_queue(self, instance: WorkflowInstance) -> None:
        """Check if dependencies are met and queue if so."""
        all_deps_met = all(
            self.instances.get(dep_id, WorkflowInstance(id="", workflow_id="", status=WorkflowStatus.FAILED)).status
            == WorkflowStatus.COMPLETED
            for dep_id in instance.dependencies
        )
        if all_deps_met:
            self._queue_instance(instance)

    def _process_pending(self) -> None:
        """Process pending workflow instances."""
        for instance in list(self.pending_queue):
            if instance.status != WorkflowStatus.PENDING:
                continue
            if all(
                self.instances.get(dep_id, WorkflowInstance(id="", workflow_id="", status=WorkflowStatus.FAILED)).status
                == WorkflowStatus.COMPLETED
                for dep_id in instance.dependencies
            ):
                self.pending_queue.remove(instance)
                self._execute_instance(instance)

    def _execute_instance(self, instance: WorkflowInstance) -> None:
        """Execute a workflow instance."""
        workflow = self.workflows.get(instance.workflow_id)
        if not workflow:
            instance.status = WorkflowStatus.FAILED
            instance.error = "Workflow not found"
            return

        instance.status = WorkflowStatus.RUNNING
        instance.started_at = datetime.now()
        self.running.add(instance.id)

        try:
            if self._execution_fn:
                result = self._execution_fn(workflow.func, instance)
            else:
                result = workflow.func()

            instance.result = result
            instance.status = WorkflowStatus.COMPLETED
            instance.completed_at = datetime.now()
            self.running.remove(instance.id)
            self.completed[instance.workflow_id].append(instance.id)

            # Check dependent workflows
            self._notify_dependents(instance.id)

        except Exception as e:
            instance.error = str(e)
            instance.status = WorkflowStatus.FAILED
            instance.completed_at = datetime.now()
            self.running.remove(instance.id)
            self.failed[instance.workflow_id].append(instance.id)

    def _notify_dependents(self, completed_instance_id: str) -> None:
        """Notify workflows waiting on a completed instance."""
        for instance in self.instances.values():
            if completed_instance_id in instance.dependencies:
                self._check_dependencies_and_queue(instance)

    def set_execution_fn(self, fn: Callable) -> None:
        """Set custom execution function."""
        self._execution_fn = fn


def create_scheduler(storage: Optional[Callable] = None) -> WorkflowScheduler:
    """Factory to create a workflow scheduler."""
    return WorkflowScheduler(storage=storage)
