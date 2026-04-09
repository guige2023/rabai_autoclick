"""Automation recovery mechanisms for fault tolerance.

Provides automatic recovery, retry with backoff, and state restoration
for failed automation workflows.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import copy
import random


class RecoveryStrategy(Enum):
    """Strategies for recovering from failures."""
    RETRY = "retry"
    FALLBACK = "fallback"
    ROLLBACK = "rollback"
    SKIP = "skip"
    ESCALATE = "escalate"
    MANUAL = "manual"


class FailureType(Enum):
    """Types of failures that can occur."""
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    TIMEOUT = "timeout"
    RESOURCE = "resource"
    CONFIGURATION = "configuration"
    UNKNOWN = "unknown"


@dataclass
class RecoveryPolicy:
    """Policy for handling failures."""
    policy_id: str
    name: str
    failure_type: FailureType
    strategy: RecoveryStrategy
    max_retries: int
    base_delay_seconds: float
    max_delay_seconds: float
    backoff_multiplier: float
    jitter: bool
    enabled: bool = True
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FailureRecord:
    """Record of a failure event."""
    failure_id: str
    workflow_id: str
    step_id: str
    failure_type: FailureType
    error_message: str
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    recovery_attempted: bool = False
    recovery_succeeded: Optional[bool] = None
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryAction:
    """A recovery action to take."""
    action_id: str
    strategy: RecoveryStrategy
    delay_seconds: float
    fallback_fn: Optional[str] = None
    rollback_snapshot: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class BackoffCalculator:
    """Calculates backoff delays for retries."""

    @staticmethod
    def calculate(
        attempt: int,
        base_delay: float,
        max_delay: float,
        multiplier: float = 2.0,
        jitter: bool = True,
    ) -> float:
        """Calculate delay with exponential backoff."""
        delay = min(base_delay * (multiplier ** attempt), max_delay)

        if jitter:
            delay = delay * (0.5 + random.random() * 0.5)

        return delay

    @staticmethod
    def linear(
        attempt: int,
        base_delay: float,
        max_delay: float,
    ) -> float:
        """Calculate linear backoff delay."""
        return min(base_delay * (attempt + 1), max_delay)

    @staticmethod
    def fibonacci(
        attempt: int,
        base_delay: float,
        max_delay: float,
    ) -> float:
        """Calculate Fibonacci backoff delay."""
        fib = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        idx = min(attempt, len(fib) - 1)
        delay = base_delay * fib[idx]
        return min(delay, max_delay)


class RecoveryEngine:
    """Core recovery and retry engine."""

    def __init__(self):
        self._policies: Dict[str, RecoveryPolicy] = {}
        self._failure_history: deque = deque(maxlen=1000)
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._retry_counters: Dict[str, int] = {}
        self._setup_default_policies()

    def _setup_default_policies(self) -> None:
        """Set up default recovery policies."""
        default_policies = [
            RecoveryPolicy(
                policy_id="transient_retry",
                name="Transient Error Retry",
                failure_type=FailureType.TRANSIENT,
                strategy=RecoveryStrategy.RETRY,
                max_retries=3,
                base_delay_seconds=1.0,
                max_delay_seconds=30.0,
                backoff_multiplier=2.0,
                jitter=True,
            ),
            RecoveryPolicy(
                policy_id="timeout_retry",
                name="Timeout Retry",
                failure_type=FailureType.TIMEOUT,
                strategy=RecoveryStrategy.RETRY,
                max_retries=2,
                base_delay_seconds=5.0,
                max_delay_seconds=60.0,
                backoff_multiplier=2.0,
                jitter=True,
            ),
            RecoveryPolicy(
                policy_id="resource_fallback",
                name="Resource Failure Fallback",
                failure_type=FailureType.RESOURCE,
                strategy=RecoveryStrategy.FALLBACK,
                max_retries=1,
                base_delay_seconds=10.0,
                max_delay_seconds=30.0,
                backoff_multiplier=1.5,
                jitter=False,
            ),
            RecoveryPolicy(
                policy_id="permanent_escalate",
                name="Permanent Failure Escalation",
                failure_type=FailureType.PERMANENT,
                strategy=RecoveryStrategy.ESCALATE,
                max_retries=0,
                base_delay_seconds=0.0,
                max_delay_seconds=0.0,
                backoff_multiplier=1.0,
                jitter=False,
            ),
        ]

        for policy in default_policies:
            self._policies[policy.policy_id] = policy

    def add_policy(
        self,
        name: str,
        failure_type: FailureType,
        strategy: RecoveryStrategy,
        max_retries: int = 3,
        base_delay_seconds: float = 1.0,
        max_delay_seconds: float = 30.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add a new recovery policy."""
        policy_id = str(uuid.uuid4())[:12]

        policy = RecoveryPolicy(
            policy_id=policy_id,
            name=name,
            failure_type=failure_type,
            strategy=strategy,
            max_retries=max_retries,
            base_delay_seconds=base_delay_seconds,
            max_delay_seconds=max_delay_seconds,
            backoff_multiplier=backoff_multiplier,
            jitter=jitter,
            metadata=metadata or {},
        )

        with self._lock:
            self._policies[policy_id] = policy

        return policy_id

    def get_policy(self, policy_id: str) -> Optional[RecoveryPolicy]:
        """Get a recovery policy by ID."""
        with self._lock:
            return copy.deepcopy(self._policies.get(policy_id))

    def determine_failure_type(self, error: Exception) -> FailureType:
        """Determine the type of failure from an exception."""
        error_msg = str(error).lower()

        if "timeout" in error_msg or "timed out" in error_msg:
            return FailureType.TIMEOUT

        if "memory" in error_msg or "disk" in error_msg or "cpu" in error_msg:
            return FailureType.RESOURCE

        if "configuration" in error_msg or "config" in error_msg or "invalid" in error_msg:
            return FailureType.CONFIGURATION

        if any(word in error_msg for word in ["connection", "network", "temporary", "refused"]):
            return FailureType.TRANSIENT

        return FailureType.UNKNOWN

    def get_recovery_action(
        self,
        failure: FailureRecord,
    ) -> Optional[RecoveryAction]:
        """Determine recovery action for a failure."""
        with self._lock:
            for policy in self._policies.values():
                if not policy.enabled:
                    continue
                if policy.failure_type == failure.failure_type:
                    break
            else:
                return None

        if failure.retry_count >= policy.max_retries:
            if policy.strategy == RecoveryStrategy.ESCALATE:
                return RecoveryAction(
                    action_id=str(uuid.uuid4())[:12],
                    strategy=RecoveryStrategy.ESCALATE,
                    delay_seconds=0,
                    metadata={"reason": "max_retries_exceeded"},
                )
            return None

        delay = BackoffCalculator.calculate(
            attempt=failure.retry_count,
            base_delay=policy.base_delay_seconds,
            max_delay=policy.max_delay_seconds,
            multiplier=policy.backoff_multiplier,
            jitter=policy.jitter,
        )

        return RecoveryAction(
            action_id=str(uuid.uuid4())[:12],
            strategy=policy.strategy,
            delay_seconds=delay,
            metadata={
                "policy_id": policy.policy_id,
                "retry_count": failure.retry_count,
                "max_retries": policy.max_retries,
            },
        )

    def record_failure(
        self,
        workflow_id: str,
        step_id: str,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
    ) -> FailureRecord:
        """Record a failure event."""
        failure_type = self.determine_failure_type(error)

        failure = FailureRecord(
            failure_id=str(uuid.uuid4())[:12],
            workflow_id=workflow_id,
            step_id=step_id,
            failure_type=failure_type,
            error_message=str(error),
            context=context or {},
        )

        with self._lock:
            self._failure_history.append(failure)

        return failure

    def save_snapshot(
        self,
        workflow_id: str,
        state: Dict[str, Any],
    ) -> str:
        """Save a workflow state snapshot for rollback."""
        snapshot_id = str(uuid.uuid4())[:12]

        with self._lock:
            self._snapshots[snapshot_id] = {
                "workflow_id": workflow_id,
                "state": copy.deepcopy(state),
                "created_at": time.time(),
            }

        return snapshot_id

    def restore_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Restore a workflow state from snapshot."""
        with self._lock:
            snapshot = self._snapshots.get(snapshot_id)
            if snapshot:
                return copy.deepcopy(snapshot.get("state"))
        return None

    def get_failure_history(
        self,
        workflow_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get failure history."""
        with self._lock:
            history = list(self._failure_history)

        if workflow_id:
            history = [f for f in history if f.workflow_id == workflow_id]

        history = history[-limit:]

        return [
            {
                "failure_id": f.failure_id,
                "workflow_id": f.workflow_id,
                "step_id": f.step_id,
                "failure_type": f.failure_type.value,
                "error_message": f.error_message,
                "timestamp": datetime.fromtimestamp(f.timestamp).isoformat(),
                "retry_count": f.retry_count,
                "recovery_attempted": f.recovery_attempted,
                "recovery_succeeded": f.recovery_succeeded,
            }
            for f in reversed(history)
        ]


class AutomationRecoveryAction:
    """Action providing recovery mechanisms for automation workflows."""

    def __init__(self, engine: Optional[RecoveryEngine] = None):
        self._engine = engine or RecoveryEngine()

    def add_policy(
        self,
        name: str,
        failure_type: str,
        strategy: str,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
    ) -> Dict[str, Any]:
        """Add a recovery policy."""
        try:
            ft_enum = FailureType(failure_type.lower())
        except ValueError:
            ft_enum = FailureType.UNKNOWN

        try:
            strat_enum = RecoveryStrategy(strategy.lower())
        except ValueError:
            strat_enum = RecoveryStrategy.RETRY

        policy_id = self._engine.add_policy(
            name=name,
            failure_type=ft_enum,
            strategy=strat_enum,
            max_retries=max_retries,
            base_delay_seconds=base_delay,
            max_delay_seconds=max_delay,
        )

        return {"policy_id": policy_id, "name": name}

    def execute_with_recovery(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an operation with automatic recovery handling.

        Required params:
            workflow_id: str - ID of the workflow
            step_id: str - ID of the step
            operation: callable - The operation to execute

        Optional params:
            save_snapshot: bool - Whether to save state snapshot
            max_attempts: int - Maximum total attempts
        """
        workflow_id = params.get("workflow_id")
        step_id = params.get("step_id")
        operation = params.get("operation")

        if not workflow_id or not step_id:
            raise ValueError("workflow_id and step_id are required")
        if not callable(operation):
            raise ValueError("operation must be a callable")

        save_snapshot = params.get("save_snapshot", False)
        initial_state = params.get("initial_state", {})

        if save_snapshot:
            snapshot_id = self._engine.save_snapshot(workflow_id, initial_state)
        else:
            snapshot_id = None

        attempt = 0
        max_attempts = params.get("max_attempts", 10)
        last_error = None

        while attempt < max_attempts:
            try:
                result = operation(context=context, params=params)
                return {
                    "success": True,
                    "result": result,
                    "attempts": attempt + 1,
                    "workflow_id": workflow_id,
                    "step_id": step_id,
                }

            except Exception as e:
                last_error = e
                attempt += 1

                failure = self._engine.record_failure(
                    workflow_id=workflow_id,
                    step_id=step_id,
                    error=e,
                    context={"attempt": attempt},
                )

                recovery_action = self._engine.get_recovery_action(failure)

                if not recovery_action:
                    return {
                        "success": False,
                        "error": str(e),
                        "failure_type": failure.failure_type.value,
                        "attempts": attempt,
                        "workflow_id": workflow_id,
                        "step_id": step_id,
                        "message": "No recovery action available",
                    }

                if recovery_action.delay_seconds > 0:
                    time.sleep(recovery_action.delay_seconds)

                if recovery_action.strategy == RecoveryStrategy.ROLLBACK:
                    restored = self._engine.restore_snapshot(
                        recovery_action.rollback_snapshot or snapshot_id or ""
                    )
                    if restored:
                        params["restored_state"] = restored

                if recovery_action.strategy == RecoveryStrategy.SKIP:
                    return {
                        "success": True,
                        "result": None,
                        "attempts": attempt,
                        "skipped": True,
                        "workflow_id": workflow_id,
                        "step_id": step_id,
                    }

        return {
            "success": False,
            "error": str(last_error) if last_error else "Max attempts exceeded",
            "attempts": attempt,
            "workflow_id": workflow_id,
            "step_id": step_id,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a recovery operation."""
        operation = params.get("operation")

        if operation == "add_policy":
            return self.add_policy(
                name=params.get("name"),
                failure_type=params.get("failure_type", "unknown"),
                strategy=params.get("strategy", "retry"),
                max_retries=params.get("max_retries", 3),
                base_delay=params.get("base_delay", 1.0),
                max_delay=params.get("max_delay", 30.0),
            )

        elif operation == "get_failure_history":
            return {
                "failures": self._engine.get_failure_history(
                    workflow_id=params.get("workflow_id"),
                    limit=params.get("limit", 100),
                )
            }

        elif operation == "save_snapshot":
            snapshot_id = self._engine.save_snapshot(
                params.get("workflow_id", "unknown"),
                params.get("state", {}),
            )
            return {"snapshot_id": snapshot_id}

        elif operation == "restore_snapshot":
            state = self._engine.restore_snapshot(params.get("snapshot_id"))
            return {"state": state, "found": state is not None}

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_policies(self) -> List[Dict[str, Any]]:
        """Get all recovery policies."""
        return [
            {
                "policy_id": p.policy_id,
                "name": p.name,
                "failure_type": p.failure_type.value,
                "strategy": p.strategy.value,
                "max_retries": p.max_retries,
                "enabled": p.enabled,
            }
            for p in self._engine._policies.values()
        ]
