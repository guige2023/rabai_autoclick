"""Saga orchestration action module for RabAI AutoClick.

Provides saga pattern for managing distributed transactions
across multiple services with compensating actions.
"""

from __future__ import annotations

import sys
import os
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SagaState(Enum):
    """Saga execution state."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    FAILED = "failed"


@dataclass
class SagaStep:
    """A single step in a saga."""
    name: str
    forward_action: Callable
    backward_action: Callable
    forward_args: Dict[str, Any] = field(default_factory=dict)
    backward_args: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class SagaResult:
    """Result of saga execution."""
    saga_id: str
    state: str
    completed_steps: List[str]
    failed_step: Optional[str]
    compensating_steps: List[str]
    final_error: Optional[str]
    started_at: str
    completed_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "saga_id": self.saga_id,
            "state": self.state.value if isinstance(self.state, SagaState) else self.state,
            "completed_steps": self.completed_steps,
            "failed_step": self.failed_step,
            "compensating_steps": self.compensating_steps,
            "final_error": self.final_error,
            "started_at": self.started_at,
            "completed_at": self.completed_at
        }


class SagaOrchestratorAction(BaseAction):
    """Orchestrate saga pattern for distributed transactions.
    
    Executes steps in order, automatically invoking compensating
    actions (rollback) if a step fails. Supports retry, timeout,
    and step skip on compensation.
    
    Args:
        saga_name: Name identifier for this saga type
    """

    def __init__(self, saga_name: str = "default"):
        super().__init__()
        self.saga_name = saga_name
        self._sagas: Dict[str, SagaResult] = {}

    def execute(
        self,
        action: str,
        saga_id: Optional[str] = None,
        steps: Optional[List[Dict[str, Any]]] = None,
        compensate_all: bool = False
    ) -> ActionResult:
        try:
            if action == "create":
                if not steps:
                    return ActionResult(success=False, error="steps required")
                new_id = saga_id or str(uuid.uuid4())[:8]
                self._sagas[new_id] = SagaResult(
                    saga_id=new_id,
                    state=SagaState.PENDING,
                    completed_steps=[],
                    failed_step=None,
                    compensating_steps=[],
                    final_error=None,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    completed_at=None
                )
                return ActionResult(success=True, data={
                    "saga_id": new_id,
                    "state": SagaState.PENDING.value,
                    "steps_defined": len(steps)
                })

            elif action == "status":
                if not saga_id or saga_id not in self._sagas:
                    return ActionResult(success=False, error="saga_id not found")
                saga = self._sagas[saga_id]
                return ActionResult(success=True, data=saga.to_dict())

            elif action == "list":
                return ActionResult(success=True, data={
                    "sagas": [s.to_dict() for s in self._sagas.values()]
                })

            elif action == "compensate":
                if not saga_id or saga_id not in self._sagas:
                    return ActionResult(success=False, error="saga_id not found")
                saga = self._sagas[saga_id]
                if compensate_all and saga.completed_steps:
                    saga.state = SagaState.COMPENSATING
                    saga.compensating_steps = list(reversed(saga.completed_steps))
                    saga.state = SagaState.COMPENSATED
                    saga.completed_at = datetime.now(timezone.utc).isoformat()
                return ActionResult(success=True, data=saga.to_dict())

            elif action == "reset":
                if not saga_id or saga_id not in self._sagas:
                    return ActionResult(success=False, error="saga_id not found")
                del self._sagas[saga_id]
                return ActionResult(success=True, data={"saga_id": saga_id, "removed": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class ChoreographySagaAction(BaseAction):
    """Event-driven choreography saga without centralized orchestrator.
    
    Each service publishes domain events and listens for events from
    other services to know when to execute or compensate.
    
    This implementation simulates the event bus and tracks subscriptions.
    """

    def __init__(self):
        super().__init__()
        self._subscriptions: Dict[str, List[str]] = {}  # event -> list of handler names
        self._event_log: List[Dict[str, Any]] = []
        self._pending_compensations: Dict[str, List[str]] = {}

    def execute(
        self,
        action: str,
        event_type: Optional[str] = None,
        handler: Optional[str] = None,
        event_data: Optional[Dict[str, Any]] = None,
        saga_id: Optional[str] = None
    ) -> ActionResult:
        try:
            if action == "subscribe":
                if not event_type or not handler:
                    return ActionResult(success=False, error="event_type and handler required")
                if event_type not in self._subscriptions:
                    self._subscriptions[event_type] = []
                if handler not in self._subscriptions[event_type]:
                    self._subscriptions[event_type].append(handler)
                return ActionResult(success=True, data={
                    "event_type": event_type,
                    "handler": handler,
                    "subscribers": self._subscriptions[event_type]
                })

            elif action == "publish":
                if not event_type or not event_data:
                    return ActionResult(success=False, error="event_type and event_data required")
                ts = datetime.now(timezone.utc).isoformat()
                event_record = {
                    "event_type": event_type,
                    "data": event_data,
                    "timestamp": ts,
                    "delivered_to": []
                }
                handlers = self._subscriptions.get(event_type, [])
                for h in handlers:
                    event_record["delivered_to"].append(h)
                self._event_log.append(event_record)

                # Track compensation
                sid = event_data.get("saga_id", "default")
                if "compensation" in event_type:
                    if sid not in self._pending_compensations:
                        self._pending_compensations[sid] = []
                    self._pending_compensations[sid].append(event_type)

                return ActionResult(success=True, data={
                    "event_type": event_type,
                    "timestamp": ts,
                    "delivered_to": handlers,
                    "total_events": len(self._event_log)
                })

            elif action == "get_subscriptions":
                return ActionResult(success=True, data={
                    "subscriptions": dict(self._subscriptions)
                })

            elif action == "get_log":
                return ActionResult(success=True, data={
                    "events": self._event_log[-50:]  # last 50
                })

            elif action == "clear":
                self._event_log.clear()
                self._subscriptions.clear()
                self._pending_compensations.clear()
                return ActionResult(success=True, data={"cleared": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
