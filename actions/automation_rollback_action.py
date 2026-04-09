"""
Automation Rollback Action Module

Provides rollback and recovery capabilities for automation workflows.
Supports checkpoints, undo/redo, and transaction-style operations.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import copy
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
import threading


class RollbackStrategy(Enum):
    """Rollback strategy."""
    SNAPSHOT = "snapshot"
    INCREMENTAL = "incremental"
    COMPENSATION = "compensation"
    CHECKPOINT = "checkpoint"


class OperationType(Enum):
    """Type of operation."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    CUSTOM = "custom"


@dataclass
class Checkpoint:
    """A checkpoint for rollback."""
    id: str
    name: str
    timestamp: datetime
    state: Any
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Operation:
    """A recorded operation."""
    id: str
    operation_type: OperationType
    timestamp: datetime
    target: Any
    previous_state: Any
    new_state: Any
    action: Optional[Callable] = None
    compensation: Optional[Callable] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RollbackResult:
    """Result of a rollback operation."""
    success: bool
    operations_rolled_back: int
    checkpoints_restored: int
    errors: list[str] = field(default_factory=list)
    duration_ms: float = 0.0


class AutomationRollbackAction:
    """
    Rollback and recovery manager for automation.
    
    Example:
        rollback_mgr = AutomationRollbackAction()
        
        # Record an operation
        rollback_mgr.record_operation(
            operation_type=OperationType.UPDATE,
            target=record,
            previous_state=old_record,
            new_state=new_record,
            compensation=lambda: restore_record(old_record)
        )
        
        # Rollback if needed
        result = rollback_mgr.rollback_last()
    """
    
    def __init__(
        self,
        strategy: RollbackStrategy = RollbackStrategy.SNAPSHOT,
        max_checkpoints: int = 100,
        max_operations: int = 1000
    ):
        self.strategy = strategy
        self._max_checkpoints = max_checkpoints
        self._max_operations = max_operations
        self._checkpoints: deque[Checkpoint] = deque(maxlen=max_checkpoints)
        self._operations: deque[Operation] = deque(maxlen=max_operations)
        self._undone_operations: deque[Operation] = deque(maxlen=max_operations)
        self._lock = threading.Lock()
        self._stats = {
            "total_operations": 0,
            "total_rollbacks": 0,
            "total_checkpoints": 0,
            "operations_since_checkpoint": 0
        }
    
    def create_checkpoint(
        self,
        state: Any,
        name: str,
        metadata: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Create a checkpoint.
        
        Args:
            state: State to save
            name: Checkpoint name
            metadata: Optional metadata
            
        Returns:
            Checkpoint ID
        """
        checkpoint_id = f"checkpoint_{len(self._checkpoints)}_{int(datetime.now().timestamp())}"
        
        state_copy = copy.deepcopy(state)
        
        checkpoint = Checkpoint(
            id=checkpoint_id,
            name=name,
            timestamp=datetime.now(),
            state=state_copy,
            metadata=metadata or {}
        )
        
        self._checkpoints.append(checkpoint)
        self._stats["total_checkpoints"] += 1
        self._stats["operations_since_checkpoint"] = 0
        
        return checkpoint_id
    
    def record_operation(
        self,
        operation_type: OperationType,
        target: Any,
        previous_state: Any,
        new_state: Any,
        action: Optional[Callable] = None,
        compensation: Optional[Callable] = None,
        metadata: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Record an operation for potential rollback.
        
        Args:
            operation_type: Type of operation
            target: Target object
            previous_state: State before the operation
            new_state: State after the operation
            action: Optional action to execute on redo
            compensation: Optional compensation function for rollback
            metadata: Optional metadata
            
        Returns:
            Operation ID
        """
        operation_id = f"op_{len(self._operations)}_{int(datetime.now().timestamp())}"
        
        prev_copy = copy.deepcopy(previous_state)
        new_copy = copy.deepcopy(new_state)
        
        operation = Operation(
            id=operation_id,
            operation_type=operation_type,
            timestamp=datetime.now(),
            target=target,
            previous_state=prev_copy,
            new_state=new_copy,
            action=action,
            compensation=compensation,
            metadata=metadata or {}
        )
        
        self._operations.append(operation)
        self._undone_operations.clear()
        self._stats["total_operations"] += 1
        self._stats["operations_since_checkpoint"] += 1
        
        return operation_id
    
    def rollback_last(self, count: int = 1) -> RollbackResult:
        """
        Rollback the last N operations.
        
        Args:
            count: Number of operations to rollback
            
        Returns:
            RollbackResult with details
        """
        start_time = datetime.now()
        errors = []
        rolled_back = 0
        checkpoints_restored = 0
        
        with self._lock:
            for _ in range(min(count, len(self._operations))):
                if not self._operations:
                    break
                
                operation = self._operations.pop()
                
                try:
                    if operation.compensation:
                        operation.compensation()
                        rolled_back += 1
                    else:
                        self._restore_state(operation.target, operation.previous_state)
                        rolled_back += 1
                    
                    self._undone_operations.append(operation)
                
                except Exception as e:
                    errors.append(f"Failed to rollback {operation.id}: {str(e)}")
            
            if rolled_back > 0:
                self._stats["total_rollbacks"] += 1
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return RollbackResult(
            success=len(errors) == 0,
            operations_rolled_back=rolled_back,
            checkpoints_restored=checkpoints_restored,
            errors=errors,
            duration_ms=duration_ms
        )
    
    def rollback_to_checkpoint(self, checkpoint_id: str) -> RollbackResult:
        """
        Rollback to a specific checkpoint.
        
        Args:
            checkpoint_id: Checkpoint ID to rollback to
            
        Returns:
            RollbackResult with details
        """
        start_time = datetime.now()
        errors = []
        
        checkpoint = None
        for cp in self._checkpoints:
            if cp.id == checkpoint_id:
                checkpoint = cp
                break
        
        if not checkpoint:
            return RollbackResult(
                success=False,
                operations_rolled_back=0,
                checkpoints_restored=0,
                errors=[f"Checkpoint not found: {checkpoint_id}"],
                duration_ms=0.0
            )
        
        with self._lock:
            rollback_result = self.rollback_last(len(self._operations))
            errors.extend(rollback_result.errors)
            
            if checkpoint.state is not None:
                self._restore_state(None, checkpoint.state)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return RollbackResult(
            success=len(errors) == 0,
            operations_rolled_back=rollback_result.operations_rolled_back,
            checkpoints_restored=1,
            errors=errors,
            duration_ms=duration_ms
        )
    
    def redo(self, count: int = 1) -> RollbackResult:
        """
        Redo the last N undone operations.
        
        Args:
            count: Number of operations to redo
            
        Returns:
            RollbackResult with details
        """
        start_time = datetime.now()
        errors = []
        redone = 0
        
        with self._lock:
            for _ in range(min(count, len(self._undone_operations))):
                if not self._undone_operations:
                    break
                
                operation = self._undone_operations.pop()
                
                try:
                    if operation.action:
                        operation.action()
                        redone += 1
                    else:
                        self._restore_state(operation.target, operation.new_state)
                        redone += 1
                    
                    self._operations.append(operation)
                
                except Exception as e:
                    errors.append(f"Failed to redo {operation.id}: {str(e)}")
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        return RollbackResult(
            success=len(errors) == 0,
            operations_rolled_back=redone,
            checkpoints_restored=0,
            errors=errors,
            duration_ms=duration_ms
        )
    
    def _restore_state(self, target: Any, state: Any) -> None:
        """Restore state to a target."""
        if target is None:
            return
        
        if isinstance(target, dict) and isinstance(state, dict):
            target.clear()
            target.update(state)
        elif hasattr(target, '__dict__'):
            target.__dict__.update(state.__dict__ if hasattr(state, '__dict__') else state)
    
    def get_checkpoint(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get a checkpoint by ID."""
        for cp in self._checkpoints:
            if cp.id == checkpoint_id:
                return cp
        return None
    
    def get_latest_checkpoint(self) -> Optional[Checkpoint]:
        """Get the most recent checkpoint."""
        return self._checkpoints[-1] if self._checkpoints else None
    
    def get_operations(self, limit: int = 100) -> list[Operation]:
        """Get recent operations."""
        return list(self._operations)[-limit:]
    
    def get_stats(self) -> dict[str, Any]:
        """Get rollback statistics."""
        return {
            **self._stats,
            "available_undone": len(self._undone_operations),
            "checkpoints_available": len(self._checkpoints)
        }
    
    def clear(self) -> None:
        """Clear all checkpoints and operations."""
        self._checkpoints.clear()
        self._operations.clear()
        self._undone_operations.clear()
