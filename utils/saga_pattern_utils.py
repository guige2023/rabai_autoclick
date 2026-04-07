"""Saga pattern utilities: distributed transaction coordination with compensating actions."""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

__all__ = [
    "SagaStep",
    "SagaState",
    "Saga",
    "SagaCoordinator",
    "execute_saga",
]


class SagaState(Enum):
    """Saga execution states."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    COMPENSATING = "compensating"
    FAILED = "failed"


@dataclass
class SagaStep:
    """A single step in a saga."""

    name: str
    execute: Callable[[], Any]
    compensate: Callable[[], None] | None = None
    retryable: bool = False
    timeout_seconds: float = 30.0

    def run(self) -> Any:
        return self.execute()

    def rollback(self) -> None:
        if self.compensate:
            self.compensate()


@dataclass
class Saga:
    """A saga - distributed transaction with compensating actions."""

    id: str
    name: str
    steps: list[SagaStep] = field(default_factory=list)
    state: SagaState = SagaState.PENDING
    completed_steps: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def add_step(self, step: SagaStep) -> "Saga":
        self.steps.append(step)
        return self

    def execute(self) -> bool:
        """Execute all saga steps sequentially."""
        self.state = SagaState.RUNNING
        for step in self.steps:
            try:
                step.run()
                self.completed_steps.append(step.name)
            except Exception as e:
                self.errors.append(f"{step.name}: {e}")
                self.state = SagaState.COMPENSATING
                self._compensate()
                self.state = SagaState.FAILED
                return False
        self.state = SagaState.COMPLETED
        return True

    def _compensate(self) -> None:
        """Roll back completed steps in reverse order."""
        for step in reversed(self.completed_steps):
            try:
                s = next(s for s in self.steps if s.name == step)
                s.rollback()
            except Exception as e:
                self.errors.append(f"Rollback failed for {step}: {e}")


class SagaCoordinator:
    """Coordinates saga execution across multiple services."""

    def __init__(self) -> None:
        self._sagas: dict[str, Saga] = {}
        self._lock = threading.Lock()

    def start(self, saga: Saga) -> str:
        """Start a new saga."""
        with self._lock:
            self._sagas[saga.id] = saga
        return saga.id

    def get(self, saga_id: str) -> Saga | None:
        return self._sagas.get(saga_id)

    def wait_for(self, saga_id: str, timeout: float = 60.0) -> SagaState:
        """Wait for a saga to complete."""
        import time
        start = time.time()
        while time.time() - start < timeout:
            saga = self.get(saga_id)
            if saga and saga.state in (SagaState.COMPLETED, SagaState.FAILED):
                return saga.state
            time.sleep(0.1)
        return SagaState.FAILED


def execute_saga(name: str, steps: list[tuple[str, Callable, Callable | None]]) -> bool:
    """Convenience function to execute a simple saga."""
    saga_id = uuid.uuid4().hex
    saga = Saga(id=saga_id, name=name)
    for step_name, exec_fn, comp_fn in steps:
        saga.add_step(SagaStep(name=step_name, execute=exec_fn, compensate=comp_fn))
    coordinator = SagaCoordinator()
    coordinator.start(saga)
    return saga.execute()
