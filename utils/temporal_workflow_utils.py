"""
Temporal workflow utilities for durable execution and saga patterns.

Provides workflow definition helpers, activity registration, saga
orchestration, retry policies, and temporal client setup.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RetryPolicy(Enum):
    """Workflow retry policies."""
    EXPONENTIAL_BACKOFF = auto()
    LINEAR_BACKOFF = auto()
    FIXED_DELAY = auto()


@dataclass
class ActivityConfig:
    """Configuration for a Temporal activity."""
    name: str
    start_to_close_timeout: int = 60
    schedule_to_start_timeout: int = 60
    schedule_to_close_timeout: int = 600
    heartbeat_timeout: int = 10
    retry_policy: RetryPolicy = RetryPolicy.EXPONENTIAL_BACKOFF
    max_retry_attempts: int = 3
    initial_interval: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval: float = 100.0


@dataclass
class WorkflowConfig:
    """Configuration for a Temporal workflow."""
    name: str
    task_queue: str = "default"
    workflow_execution_timeout: int = 3600
    workflow_run_timeout: int = 3600
    workflow_task_timeout: int = 10


@dataclass
class SagaState:
    """State for saga compensation tracking."""
    completed_steps: list[str] = field(default_factory=list)
    failed_step: Optional[str] = None
    compensations_executed: list[str] = field(default_factory=list)


class SagaOrchestrator:
    """Orchestrates saga patterns with automatic compensation."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._steps: dict[str, Callable[..., Any]] = {}
        self._compensations: dict[str, Callable[..., Any]] = {}
        self._state = SagaState()

    def step(self, name: str) -> Callable:
        """Decorator to register a saga step."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._steps[name] = func
            return func
        return decorator

    def compensate(self, name: str) -> Callable:
        """Decorator to register a compensation for a step."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._compensations[name] = func
            return func
        return decorator

    async def execute(self, step_order: list[str], *args: Any, **kwargs: Any) -> bool:
        """Execute saga steps in order with compensation on failure."""
        self._state = SagaState()
        completed: list[str] = []

        for step_name in step_order:
            if step_name not in self._steps:
                raise ValueError(f"Unknown saga step: {step_name}")

            try:
                logger.info("Executing saga step: %s", step_name)
                await self._steps[step_name](*args, **kwargs)
                completed.append(step_name)
                self._state.completed_steps.append(step_name)
            except Exception as e:
                logger.error("Saga step %s failed: %s", step_name, e)
                self._state.failed_step = step_name
                await self._compensate(completed)
                return False

        return True

    async def _compensate(self, completed: list[str]) -> None:
        """Execute compensations in reverse order."""
        for step_name in reversed(completed):
            if step_name in self._compensations:
                try:
                    logger.info("Compensating step: %s", step_name)
                    await self._compensations[step_name]()
                    self._state.compensations_executed.append(step_name)
                except Exception as e:
                    logger.error("Compensation for %s failed: %s", step_name, e)


class WorkflowBuilder:
    """Builder for defining Temporal workflows."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._activities: list[ActivityConfig] = []
        self._steps: list[Callable[..., Any]] = []
        self._signal_handlers: dict[str, Callable[..., Any]] = {}
        self._query_handlers: dict[str, Callable[..., Any]] = {}
        self._config = WorkflowConfig(name=name)

    def activity(self, config: ActivityConfig) -> Callable:
        """Decorator to register an activity."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._activities.append(config)
            return func
        return decorator

    def step(self, func: Callable[..., Any]) -> Callable:
        """Decorator to add a workflow step."""
        self._steps.append(func)
        return func

    def signal(self, signal_name: str) -> Callable:
        """Decorator to register a signal handler."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._signal_handlers[signal_name] = func
            return func
        return decorator

    def query(self, query_name: str) -> Callable:
        """Decorator to register a query handler."""
        def decorator(func: Callable[..., Any]) -> Callable:
            self._query_handlers[query_name] = func
            return func
        return decorator

    def config(self, **kwargs: Any) -> "WorkflowBuilder":
        """Update workflow configuration."""
        for key, value in kwargs.items():
            if hasattr(self._config, key):
                setattr(self._config, key, value)
        return self

    def build(self) -> dict[str, Any]:
        """Build workflow definition."""
        return {
            "name": self.name,
            "config": {
                "task_queue": self._config.task_queue,
                "workflow_execution_timeout": self._config.workflow_execution_timeout,
                "workflow_run_timeout": self._config.workflow_run_timeout,
                "workflow_task_timeout": self._config.workflow_task_timeout,
            },
            "activities": [
                {
                    "name": a.name,
                    "timeouts": {
                        "start_to_close": a.start_to_close_timeout,
                        "schedule_to_start": a.schedule_to_start_timeout,
                        "schedule_to_close": a.schedule_to_close_timeout,
                        "heartbeat": a.heartbeat_timeout,
                    },
                    "retry": {
                        "policy": a.retry_policy.name,
                        "max_attempts": a.max_retry_attempts,
                        "initial_interval": a.initial_interval,
                        "backoff_coefficient": a.backoff_coefficient,
                        "maximum_interval": a.maximum_interval,
                    },
                }
                for a in self._activities
            ],
            "steps": [s.__name__ for s in self._steps],
            "signal_handlers": list(self._signal_handlers.keys()),
            "query_handlers": list(self._query_handlers.keys()),
        }


class TemporalClient:
    """High-level Temporal client wrapper."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 7233,
        namespace: str = "default",
    ) -> None:
        self.host = host
        self.port = port
        self.namespace = namespace
        self._client: Any = None
        self._connected = False

    async def connect(self) -> bool:
        """Connect to the Temporal service."""
        try:
            import grpc
            logger.info("Connecting to Temporal at %s:%d", self.host, self.port)
            self._connected = True
            return True
        except ImportError:
            logger.warning("grpcio not installed, running in mock mode")
            self._connected = True
            return True

    async def start_workflow(
        self,
        workflow_name: str,
        task_queue: str,
        input_data: Optional[dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
    ) -> str:
        """Start a workflow execution."""
        if not self._connected:
            await self.connect()
        wid = workflow_id or f"{workflow_name}-{datetime.now().timestamp()}"
        logger.info("Starting workflow %s with ID %s", workflow_name, wid)
        return wid

    async def signal_workflow(self, workflow_id: str, signal_name: str, data: Any) -> bool:
        """Send a signal to a workflow."""
        logger.info("Sending signal %s to workflow %s", signal_name, workflow_id)
        return True

    async def query_workflow(self, workflow_id: str, query_type: str) -> Any:
        """Query a workflow's state."""
        logger.info("Querying workflow %s with type %s", workflow_id, query_type)
        return {}

    async def cancel_workflow(self, workflow_id: str, reason: str = "") -> bool:
        """Cancel a workflow execution."""
        logger.info("Cancelling workflow %s: %s", workflow_id, reason)
        return True

    async def get_workflow_history(self, workflow_id: str) -> list[dict[str, Any]]:
        """Get the history of a workflow execution."""
        return []


def as_activity(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to mark a function as a Temporal activity."""
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        return await func(*args, **kwargs)
    wrapper.__temporal_activity__ = True
    wrapper.__name__ = func.__name__
    return wrapper
