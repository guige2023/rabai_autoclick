# Copyright (c) 2024. coded by claude
"""Automation Monitor Action Module.

Monitors automation workflow execution with support for
real-time metrics, health checks, and alerting.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class WorkflowMetrics:
    workflow_id: str
    status: WorkflowStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_ms: Optional[float]
    steps_completed: int
    steps_failed: int
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0


@dataclass
class HealthStatus:
    healthy: bool
    message: str
    uptime_seconds: float
    active_workflows: int
    completed_workflows: int
    failed_workflows: int


class AutomationMonitor:
    def __init__(self):
        self._workflows: Dict[str, WorkflowMetrics] = {}
        self._alert_callbacks: List[Callable] = []
        self._start_time = datetime.now()
        self._lock = asyncio.Lock()

    async def start_workflow(self, workflow_id: str) -> None:
        async with self._lock:
            self._workflows[workflow_id] = WorkflowMetrics(
                workflow_id=workflow_id,
                status=WorkflowStatus.RUNNING,
                started_at=datetime.now(),
                completed_at=None,
                duration_ms=None,
                steps_completed=0,
                steps_failed=0,
            )

    async def update_workflow(self, workflow_id: str, status: WorkflowStatus, steps_completed: Optional[int] = None, steps_failed: Optional[int] = None) -> None:
        async with self._lock:
            if workflow_id in self._workflows:
                metrics = self._workflows[workflow_id]
                metrics.status = status
                if steps_completed is not None:
                    metrics.steps_completed = steps_completed
                if steps_failed is not None:
                    metrics.steps_failed = steps_failed
                if status in (WorkflowStatus.COMPLETED, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED):
                    metrics.completed_at = datetime.now()
                    metrics.duration_ms = (metrics.completed_at - metrics.started_at).total_seconds() * 1000 if metrics.started_at else None

    async def complete_workflow(self, workflow_id: str, success: bool) -> None:
        status = WorkflowStatus.COMPLETED if success else WorkflowStatus.FAILED
        await self.update_workflow(workflow_id, status)

    async def get_health_status(self) -> HealthStatus:
        async with self._lock:
            total = len(self._workflows)
            active = sum(1 for w in self._workflows.values() if w.status == WorkflowStatus.RUNNING)
            completed = sum(1 for w in self._workflows.values() if w.status == WorkflowStatus.COMPLETED)
            failed = sum(1 for w in self._workflows.values() if w.status == WorkflowStatus.FAILED)
            uptime = (datetime.now() - self._start_time).total_seconds()
            healthy = failed == 0 or (failed / total < 0.1) if total > 0 else True
            return HealthStatus(
                healthy=healthy,
                message="System operational" if healthy else f"High failure rate: {failed}/{total}",
                uptime_seconds=uptime,
                active_workflows=active,
                completed_workflows=completed,
                failed_workflows=failed,
            )

    def register_alert_callback(self, callback: Callable) -> None:
        self._alert_callbacks.append(callback)

    async def trigger_alert(self, workflow_id: str, message: str) -> None:
        for callback in self._alert_callbacks:
            try:
                await callback(workflow_id, message)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")

    def get_workflow_metrics(self, workflow_id: str) -> Optional[WorkflowMetrics]:
        return self._workflows.get(workflow_id)

    def get_all_metrics(self) -> List[WorkflowMetrics]:
        return list(self._workflows.values())
