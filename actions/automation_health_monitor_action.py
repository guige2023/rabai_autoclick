"""Automation Health Monitor Action.

Monitors automation workflow health with heartbeat detection,
failure isolation, and automatic circuit breaking.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class Heartbeat:
    """A heartbeat record."""
    workflow_id: str
    timestamp: float
    sequence: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    workflow_id: str
    status: HealthStatus
    last_heartbeat: Optional[float] = None
    missed_heartbeats: int = 0
    consecutive_failures: int = 0
    message: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class WorkflowHealth:
    """Health state for a workflow."""
    workflow_id: str
    status: HealthStatus
    registered_at: float
    last_heartbeat: Optional[float] = None
    last_check: Optional[float] = None
    consecutive_failures: int = 0
    circuit_open: bool = False
    circuit_opened_at: Optional[float] = None
    total_runs: int = 0
    total_failures: int = 0
    avg_execution_time: float = 0.0


class AutomationHealthMonitorAction:
    """Monitors automation workflow health with circuit breaker support."""

    def __init__(
        self,
        heartbeat_timeout_sec: float = 60.0,
        max_missed_heartbeats: int = 3,
        circuit_break_threshold: int = 5,
        circuit_reset_timeout_sec: float = 300.0,
    ) -> None:
        self.heartbeat_timeout = heartbeat_timeout_sec
        self.max_missed = max_missed_heartbeats
        self.circuit_threshold = circuit_break_threshold
        self.circuit_reset_timeout = circuit_reset_timeout_sec

        self._workflows: Dict[str, WorkflowHealth] = {}
        self._heartbeats: Dict[str, deque] = {}
        self._health_checks: Dict[str, Callable] = {}
        self._max_heartbeats = 100

    def register_workflow(
        self,
        workflow_id: str,
        health_check_fn: Optional[Callable[[], bool]] = None,
    ) -> WorkflowHealth:
        """Register a workflow for health monitoring."""
        health = WorkflowHealth(
            workflow_id=workflow_id,
            status=HealthStatus.UNKNOWN,
            registered_at=time.time(),
        )
        self._workflows[workflow_id] = health
        self._heartbeats[workflow_id] = deque(maxlen=self._max_heartbeats)

        if health_check_fn:
            self._health_checks[workflow_id] = health_check_fn

        return health

    def unregister_workflow(self, workflow_id: str) -> bool:
        """Unregister a workflow from monitoring."""
        if workflow_id in self._workflows:
            del self._workflows[workflow_id]
            if workflow_id in self._heartbeats:
                del self._heartbeats[workflow_id]
            if workflow_id in self._health_checks:
                del self._health_checks[workflow_id]
            return True
        return False

    def record_heartbeat(
        self,
        workflow_id: str,
        sequence: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Record a heartbeat for a workflow."""
        if workflow_id not in self._workflows:
            return False

        heartbeats = self._heartbeats[workflow_id]
        seq = sequence if sequence is not None else len(heartbeats)

        heartbeat = Heartbeat(
            workflow_id=workflow_id,
            timestamp=time.time(),
            sequence=seq,
            metadata=metadata or {},
        )
        heartbeats.append(heartbeat)

        health = self._workflows[workflow_id]
        health.last_heartbeat = heartbeat.timestamp
        health.status = HealthStatus.HEALTHY
        health.consecutive_failures = 0

        if health.circuit_open:
            if health.circuit_opened_at and \
               (time.time() - health.circuit_opened_at) > self.circuit_reset_timeout:
                health.circuit_open = False
                health.circuit_opened_at = None

        return True

    def record_execution(
        self,
        workflow_id: str,
        success: bool,
        execution_time_sec: float,
    ) -> None:
        """Record workflow execution outcome."""
        if workflow_id not in self._workflows:
            return

        health = self._workflows[workflow_id]
        health.total_runs += 1

        if not success:
            health.total_failures += 1
            health.consecutive_failures += 1

            if health.consecutive_failures >= self.circuit_threshold:
                health.circuit_open = True
                health.circuit_opened_at = time.time()
                health.status = HealthStatus.UNHEALTHY

        n = health.total_runs
        health.avg_execution_time = (
            (health.avg_execution_time * (n - 1) + execution_time_sec) / n
        )

    def check_health(self, workflow_id: str) -> HealthCheckResult:
        """Perform a health check on a workflow."""
        if workflow_id not in self._workflows:
            return HealthCheckResult(
                workflow_id=workflow_id,
                status=HealthStatus.UNKNOWN,
                message="Workflow not registered",
            )

        health = self._workflows[workflow_id]
        now = time.time()

        missed = 0
        if health.last_heartbeat:
            time_since = now - health.last_heartbeat
            missed = int(time_since // self.heartbeat_timeout)

        result = HealthCheckResult(
            workflow_id=workflow_id,
            status=health.status,
            last_heartbeat=health.last_heartbeat,
            missed_heartbeats=missed,
            consecutive_failures=health.consecutive_failures,
            timestamp=now,
        )

        if health.circuit_open:
            result.status = HealthStatus.UNHEALTHY
            result.message = "Circuit breaker open"
        elif missed >= self.max_missed:
            result.status = HealthStatus.UNHEALTHY
            result.message = f"Missed {missed} heartbeats"
        elif missed > 0:
            result.status = HealthStatus.DEGRADED
            result.message = f"Missed {missed} heartbeat(s)"
        elif health.consecutive_failures > 0:
            result.status = HealthStatus.DEGRADED
            result.message = f"{health.consecutive_failures} consecutive failures"
        else:
            result.status = HealthStatus.HEALTHY
            result.message = "Healthy"

        if workflow_id in self._health_checks:
            try:
                check_passed = self._health_checks[workflow_id]()
                if not check_passed and result.status != HealthStatus.UNHEALTHY:
                    result.status = HealthStatus.UNHEALTHY
                    result.message = "Custom health check failed"
            except Exception:
                pass

        health.last_check = now
        return result

    def check_all(self) -> Dict[str, HealthCheckResult]:
        """Run health checks on all registered workflows."""
        return {
            workflow_id: self.check_health(workflow_id)
            for workflow_id in self._workflows
        }

    def is_circuit_open(self, workflow_id: str) -> bool:
        """Check if circuit breaker is open for a workflow."""
        if workflow_id in self._workflows:
            return self._workflows[workflow_id].circuit_open
        return False

    def reset_circuit(self, workflow_id: str) -> bool:
        """Manually reset a circuit breaker."""
        if workflow_id in self._workflows:
            health = self._workflows[workflow_id]
            health.circuit_open = False
            health.circuit_opened_at = None
            health.consecutive_failures = 0
            health.status = HealthStatus.HEALTHY
            return True
        return False

    def get_workflow_health(self, workflow_id: str) -> Optional[WorkflowHealth]:
        """Get detailed health info for a workflow."""
        return self._workflows.get(workflow_id)

    def get_heartbeats(
        self,
        workflow_id: str,
        limit: int = 10,
    ) -> List[Heartbeat]:
        """Get recent heartbeats for a workflow."""
        if workflow_id not in self._heartbeats:
            return []
        heartbeats = list(self._heartbeats[workflow_id])
        return heartbeats[-limit:]

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all workflow health statuses."""
        if not self._workflows:
            return {"total": 0, "healthy": 0, "degraded": 0, "unhealthy": 0}

        results = self.check_all()
        return {
            "total": len(self._workflows),
            "healthy": sum(1 for r in results.values() if r.status == HealthStatus.HEALTHY),
            "degraded": sum(1 for r in results.values() if r.status == HealthStatus.DEGRADED),
            "unhealthy": sum(1 for r in results.values() if r.status == HealthStatus.UNHEALTHY),
            "circuits_open": sum(1 for w in self._workflows.values() if w.circuit_open),
        }
