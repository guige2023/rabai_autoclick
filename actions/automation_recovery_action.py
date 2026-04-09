"""
Automation Recovery Action Module

Provides recovery mechanisms for failed automation tasks.
Supports automatic retry, checkpoint recovery, and state restoration.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import copy
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
import threading

T = TypeVar('T')


class RecoveryStrategy(Enum):
    """Recovery strategy."""
    RETRY = "retry"
    CHECKPOINT = "checkpoint"
    STATE_RESTORE = "state_restore"
    FALLBACK = "fallback"
    COMPENSATION = "compensation"
    MANUAL = "manual"


class RecoveryStatus(Enum):
    """Recovery operation status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass
class RecoveryConfig:
    """Configuration for recovery."""
    strategy: RecoveryStrategy = RecoveryStrategy.RETRY
    max_attempts: int = 3
    retry_delay_seconds: float = 5.0
    exponential_backoff: bool = True
    backoff_multiplier: float = 2.0
    checkpoint_interval: int = 10
    state_retention_minutes: int = 60


@dataclass
class Checkpoint:
    """A recovery checkpoint."""
    id: str
    step: int
    name: str
    state: Any
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryAction:
    """A recovery action."""
    id: str
    strategy: RecoveryStrategy
    target_step: int
    action_fn: Callable
    compensation_fn: Optional[Callable] = None
    conditions: list[str] = field(default_factory=list)


@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    status: RecoveryStatus
    attempts: int = 0
    restored_state: Any = None
    recovered_steps: int = 0
    total_steps: int = 0
    errors: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class AutomationRecoveryAction:
    """
    Recovery manager for automation workflows.
    
    Example:
        recovery = AutomationRecoveryAction()
        
        recovery.create_checkpoint(step=5, name="processed_items", state=current_state)
        
        result = await recovery.recover(
            failed_at_step=7,
            recovery_fn=resume_processing,
            state=get_current_state()
        )
    """
    
    def __init__(self, config: Optional[RecoveryConfig] = None):
        self.config = config or RecoveryConfig()
        self._checkpoints: dict[str, deque[Checkpoint]] = {}
        self._recovery_actions: dict[str, list[RecoveryAction]] = {}
        self._current_workflow_id: Optional[str] = None
        self._workflow_states: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._stats = {
            "total_recoveries": 0,
            "successful_recoveries": 0,
            "failed_recoveries": 0,
            "total_checkpoints": 0,
            "total_retries": 0
        }
    
    def start_workflow(self, workflow_id: str, initial_state: Any = None) -> None:
        """Start tracking a new workflow."""
        self._current_workflow_id = workflow_id
        self._checkpoints[workflow_id] = deque(maxlen=100)
        self._recovery_actions[workflow_id] = []
        if initial_state is not None:
            self._workflow_states[workflow_id] = copy.deepcopy(initial_state)
    
    def create_checkpoint(
        self,
        step: int,
        name: str,
        state: Any,
        metadata: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create a checkpoint for the current workflow.
        
        Args:
            step: Current step number
            name: Checkpoint name
            state: State to save
            metadata: Optional metadata
            
        Returns:
            Checkpoint ID
        """
        if not self._current_workflow_id:
            raise RuntimeError("No active workflow")
        
        checkpoint_id = f"cp_{self._current_workflow_id}_{step}_{int(datetime.now().timestamp())}"
        
        checkpoint = Checkpoint(
            id=checkpoint_id,
            step=step,
            name=name,
            state=copy.deepcopy(state),
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        
        self._checkpoints[self._current_workflow_id].append(checkpoint)
        self._workflow_states[self._current_workflow_id] = copy.deepcopy(state)
        self._stats["total_checkpoints"] += 1
        
        return checkpoint_id
    
    def register_recovery_action(
        self,
        step: int,
        strategy: RecoveryStrategy,
        action_fn: Callable,
        compensation_fn: Optional[Callable] = None
    ) -> str:
        """Register a recovery action for a step."""
        if not self._current_workflow_id:
            raise RuntimeError("No active workflow")
        
        action_id = f"action_{self._current_workflow_id}_{step}_{int(datetime.now().timestamp())}"
        
        action = RecoveryAction(
            id=action_id,
            strategy=strategy,
            target_step=step,
            action_fn=action_fn,
            compensation_fn=compensation_fn
        )
        
        self._recovery_actions[self._current_workflow_id].append(action)
        return action_id
    
    async def recover(
        self,
        failed_at_step: int,
        recovery_fn: Callable,
        state: Any,
        max_attempts: Optional[int] = None
    ) -> RecoveryResult:
        """
        Attempt to recover from a failure.
        
        Args:
            failed_at_step: Step that failed
            recovery_fn: Function to retry
            state: Current state
            max_attempts: Maximum recovery attempts
            
        Returns:
            RecoveryResult with recovery details
        """
        start_time = datetime.now()
        max_attempts = max_attempts or self.config.max_attempts
        self._stats["total_recoveries"] += 1
        
        strategy = self.config.strategy
        
        if strategy == RecoveryStrategy.RETRY:
            return await self._retry_recovery(recovery_fn, state, max_attempts, start_time)
        elif strategy == RecoveryStrategy.CHECKPOINT:
            return await self._checkpoint_recovery(failed_at_step, recovery_fn, start_time)
        elif strategy == RecoveryStrategy.STATE_RESTORE:
            return await self._state_restore_recovery(failed_at_step, recovery_fn, start_time)
        else:
            return await self._retry_recovery(recovery_fn, state, max_attempts, start_time)
    
    async def _retry_recovery(
        self,
        recovery_fn: Callable,
        state: Any,
        max_attempts: int,
        start_time: datetime
    ) -> RecoveryResult:
        """Recovery via retry with backoff."""
        errors = []
        last_error = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(recovery_fn):
                    result = await recovery_fn()
                else:
                    result = recovery_fn()
                
                self._stats["successful_recoveries"] += 1
                duration_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                return RecoveryResult(
                    status=RecoveryStatus.SUCCESS,
                    attempts=attempt,
                    restored_state=result,
                    recovered_steps=1,
                    total_steps=1,
                    duration_ms=duration_ms
                )
            
            except Exception as e:
                last_error = str(e)
                errors.append(f"Attempt {attempt}: {last_error}")
                self._stats["total_retries"] += 1
                
                if attempt < max_attempts:
                    delay = self.config.retry_delay_seconds
                    if self.config.exponential_backoff:
                        delay *= (self.config.backoff_multiplier ** (attempt - 1))
                    await asyncio.sleep(delay)
        
        self._stats["failed_recoveries"] += 1
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return RecoveryResult(
            status=RecoveryStatus.FAILED,
            attempts=max_attempts,
            errors=errors,
            duration_ms=duration_ms
        )
    
    async def _checkpoint_recovery(
        self,
        failed_at_step: int,
        recovery_fn: Callable,
        start_time: datetime
    ) -> RecoveryResult:
        """Recovery from checkpoint."""
        if not self._current_workflow_id:
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=["No active workflow"]
            )
        
        checkpoints = self._checkpoints.get(self._current_workflow_id, [])
        target_checkpoint = None
        
        for cp in reversed(checkpoints):
            if cp.step < failed_at_step:
                target_checkpoint = cp
                break
        
        if not target_checkpoint:
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=["No valid checkpoint found"]
            )
        
        try:
            result = await recovery_fn()
            self._stats["successful_recoveries"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return RecoveryResult(
                status=RecoveryStatus.SUCCESS,
                restored_state=target_checkpoint.state,
                recovered_steps=failed_at_step - target_checkpoint.step,
                total_steps=failed_at_step,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["failed_recoveries"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=[str(e)],
                restored_state=target_checkpoint.state,
                duration_ms=duration_ms
            )
    
    async def _state_restore_recovery(
        self,
        failed_at_step: int,
        recovery_fn: Callable,
        start_time: datetime
    ) -> RecoveryResult:
        """Recovery via state restoration."""
        if not self._current_workflow_id:
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=["No active workflow"]
            )
        
        saved_state = self._workflow_states.get(self._current_workflow_id)
        
        if saved_state is None:
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=["No saved state found"]
            )
        
        try:
            result = await recovery_fn()
            self._stats["successful_recoveries"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return RecoveryResult(
                status=RecoveryStatus.SUCCESS,
                restored_state=saved_state,
                recovered_steps=failed_at_step,
                total_steps=failed_at_step,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["failed_recoveries"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return RecoveryResult(
                status=RecoveryStatus.FAILED,
                errors=[str(e)],
                restored_state=saved_state,
                duration_ms=duration_ms
            )
    
    def get_latest_checkpoint(self, workflow_id: Optional[str] = None) -> Optional[Checkpoint]:
        """Get the latest checkpoint for a workflow."""
        wid = workflow_id or self._current_workflow_id
        if not wid:
            return None
        checkpoints = self._checkpoints.get(wid, [])
        return checkpoints[-1] if checkpoints else None
    
    def get_recovery_actions(self, workflow_id: Optional[str] = None) -> list[RecoveryAction]:
        """Get recovery actions for a workflow."""
        wid = workflow_id or self._current_workflow_id
        return self._recovery_actions.get(wid, []) if wid else []
    
    def get_stats(self) -> dict[str, Any]:
        """Get recovery statistics."""
        return {
            **self._stats,
            "active_workflows": len(self._checkpoints),
            "recovery_rate": (
                self._stats["successful_recoveries"] / self._stats["total_recoveries"]
                if self._stats["total_recoveries"] > 0 else 0
            )
        }
    
    def end_workflow(self, workflow_id: Optional[str] = None) -> None:
        """End tracking a workflow."""
        wid = workflow_id or self._current_workflow_id
        if wid:
            self._checkpoints.pop(wid, None)
            self._recovery_actions.pop(wid, None)
            self._workflow_states.pop(wid, None)
            if self._current_workflow_id == wid:
                self._current_workflow_id = None
