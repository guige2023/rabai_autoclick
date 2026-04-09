"""
Automation Recovery Module.

Provides automatic recovery strategies, state rollback,
checkpoint management, and fault tolerance for workflows.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from collections import deque
import logging
import hashlib

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RecoveryStrategy(Enum):
    """Recovery strategy types."""
    RETRY = "retry"
    ROLLBACK = "rollback"
    CHECKPOINT = "checkpoint"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    STATEFUL_RETRY = "stateful_retry"


class CheckpointStatus(Enum):
    """Checkpoint status."""
    PENDING = "pending"
    COMMITTED = "committed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


@dataclass
class Checkpoint:
    """Workflow checkpoint for recovery."""
    checkpoint_id: str
    workflow_id: str
    step: int
    state: Dict[str, Any]
    created_at: float
    status: CheckpointStatus
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    
@dataclass
class RecoveryConfig:
    """Configuration for recovery mechanisms."""
    max_retries: int = 3
    retry_delay: float = 1.0
    exponential_backoff: bool = True
    max_backoff: float = 60.0
    enable_checkpoints: bool = True
    checkpoint_interval: int = 10
    enable_rollback: bool = True
    fallback_enabled: bool = True
    checkpoint_storage: str = "memory"  # memory, file, redis
    

@dataclass
class RecoveryResult:
    """Result of a recovery operation."""
    success: bool
    recovered: bool
    strategy_used: RecoveryStrategy
    attempts: int
    final_error: Optional[str] = None
    restored_state: Optional[Dict[str, Any]] = None


class CheckpointManager:
    """
    Manages workflow checkpoints for recovery.
    
    Example:
        manager = CheckpointManager()
        
        # Create checkpoint
        await manager.create(
            workflow_id="wf_123",
            step=5,
            state={"counter": 10, "data": [...]}
        )
        
        # Restore from checkpoint
        state = await manager.restore("wf_123", step=5)
    """
    
    def __init__(
        self,
        storage: str = "memory",
        storage_path: Optional[str] = None,
    ) -> None:
        """
        Initialize checkpoint manager.
        
        Args:
            storage: Storage backend (memory, file, redis).
            storage_path: Path for file storage.
        """
        self.storage = storage
        self.storage_path = storage_path
        self._checkpoints: Dict[str, List[Checkpoint]] = {}
        self._lock = asyncio.Lock()
        
    async def create(
        self,
        workflow_id: str,
        step: int,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Checkpoint:
        """
        Create a checkpoint.
        
        Args:
            workflow_id: Workflow identifier.
            step: Current step number.
            state: Current workflow state.
            metadata: Optional metadata.
            
        Returns:
            Created checkpoint.
        """
        checkpoint_id = self._generate_id(workflow_id, step)
        
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            workflow_id=workflow_id,
            step=step,
            state=state,
            created_at=time.time(),
            status=CheckpointStatus.COMMITTED,
            metadata=metadata or {},
        )
        
        async with self._lock:
            if workflow_id not in self._checkpoints:
                self._checkpoints[workflow_id] = []
            self._checkpoints[workflow_id].append(checkpoint)
            
        # Persist if using file storage
        if self.storage == "file":
            await self._persist_checkpoint(checkpoint)
            
        logger.info(f"Created checkpoint: {checkpoint_id}")
        return checkpoint
        
    async def restore(
        self,
        workflow_id: str,
        step: Optional[int] = None,
        checkpoint_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Restore state from a checkpoint.
        
        Args:
            workflow_id: Workflow identifier.
            step: Step number (restores latest if None).
            checkpoint_id: Specific checkpoint ID.
            
        Returns:
            Restored state or None.
        """
        async with self._lock:
            checkpoints = self._checkpoints.get(workflow_id, [])
            
        if not checkpoints:
            return None
            
        if checkpoint_id:
            target = next((cp for cp in checkpoints if cp.checkpoint_id == checkpoint_id), None)
        elif step is not None:
            # Find latest checkpoint at or before step
            valid = [cp for cp in checkpoints if cp.step <= step]
            target = max(valid, key=lambda cp: cp.step) if valid else None
        else:
            # Get latest
            target = max(checkpoints, key=lambda cp: cp.step)
            
        if target:
            logger.info(f"Restored checkpoint: {target.checkpoint_id}")
            return target.state
            
        return None
        
    async def get_latest(self, workflow_id: str) -> Optional[Checkpoint]:
        """Get latest checkpoint for workflow."""
        async with self._lock:
            checkpoints = self._checkpoints.get(workflow_id, [])
            
        if checkpoints:
            return max(checkpoints, key=lambda cp: cp.step)
        return None
        
    async def delete(self, workflow_id: str, step: Optional[int] = None) -> bool:
        """Delete checkpoints."""
        async with self._lock:
            if step is not None:
                self._checkpoints[workflow_id] = [
                    cp for cp in self._checkpoints.get(workflow_id, [])
                    if cp.step != step
                ]
            else:
                if workflow_id in self._checkpoints:
                    del self._checkpoints[workflow_id]
                    return True
        return False
        
    def _generate_id(self, workflow_id: str, step: int) -> str:
        """Generate unique checkpoint ID."""
        data = f"{workflow_id}:{step}:{time.time()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
        
    async def _persist_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Persist checkpoint to file."""
        import aiofiles
        
        if not self.storage_path:
            return
            
        path = f"{self.storage_path}/{checkpoint.workflow_id}.json"
        
        async with aiofiles.open(path, "w") as f:
            await f.write(json.dumps({
                "checkpoint_id": checkpoint.checkpoint_id,
                "step": checkpoint.step,
                "state": checkpoint.state,
                "created_at": checkpoint.created_at,
            }))


class RecoveryManager:
    """
    Manages recovery strategies for failed workflows.
    
    Example:
        manager = RecoveryManager(RecoveryConfig(
            max_retries=3,
            enable_checkpoints=True
        ))
        
        result = await manager.execute_with_recovery(
            workflow_id="wf_123",
            func=run_workflow_step,
            rollback_func=rollback_workflow_step,
            state={"step": 5}
        )
    """
    
    def __init__(self, config: Optional[RecoveryConfig] = None) -> None:
        """
        Initialize recovery manager.
        
        Args:
            config: Recovery configuration.
        """
        self.config = config or RecoveryConfig()
        self.checkpoint_manager = CheckpointManager()
        self._recovery_history: deque = deque(maxlen=1000)
        
    async def execute_with_recovery(
        self,
        workflow_id: str,
        func: Callable[..., Any],
        rollback_func: Optional[Callable[..., Any]] = None,
        state: Optional[Dict[str, Any]] = None,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> RecoveryResult:
        """
        Execute function with recovery support.
        
        Args:
            workflow_id: Workflow identifier.
            func: Function to execute.
            rollback_func: Optional rollback function.
            state: Current workflow state.
            args: Function arguments.
            kwargs: Function keyword arguments.
            
        Returns:
            RecoveryResult with outcome.
        """
        kwargs = kwargs or {}
        last_error = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Create checkpoint before execution
                if self.config.enable_checkpoints and state:
                    step = state.get("step", 0)
                    await self.checkpoint_manager.create(
                        workflow_id=workflow_id,
                        step=step,
                        state=state,
                    )
                    
                # Execute
                result = await func(*args, **kwargs)
                
                # Record success
                self._record_recovery(workflow_id, RecoveryStrategy.RETRY, True, attempt)
                
                return RecoveryResult(
                    success=True,
                    recovered=attempt > 0,
                    strategy_used=RecoveryStrategy.RETRY if attempt > 0 else RecoveryStrategy.RETRY,
                    attempts=attempt + 1,
                    restored_state=state,
                )
                
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Recovery attempt {attempt + 1} failed for {workflow_id}: {e}"
                )
                
                # Rollback if enabled
                if self.config.enable_rollback and rollback_func:
                    try:
                        await rollback_func(state)
                    except Exception as rb_error:
                        logger.error(f"Rollback failed: {rb_error}")
                        
                # Exponential backoff
                if self.config.exponential_backoff:
                    delay = min(
                        self.config.retry_delay * (2 ** attempt),
                        self.config.max_backoff
                    )
                    await asyncio.sleep(delay)
                    
        # All retries failed
        return RecoveryResult(
            success=False,
            recovered=False,
            strategy_used=RecoveryStrategy.RETRY,
            attempts=self.config.max_retries + 1,
            final_error=last_error,
            restored_state=state,
        )
        
    async def execute_with_checkpoint(
        self,
        workflow_id: str,
        steps: List[Callable[[Dict[str, Any]], Any]],
        initial_state: Dict[str, Any],
    ) -> RecoveryResult:
        """
        Execute multi-step workflow with checkpoint recovery.
        
        Args:
            workflow_id: Workflow identifier.
            steps: List of step functions.
            initial_state: Initial workflow state.
            
        Returns:
            RecoveryResult with outcome.
        """
        state = initial_state.copy()
        last_successful_step = 0
        
        # Try to restore from checkpoint
        restored_state = await self.checkpoint_manager.restore(workflow_id)
        if restored_state:
            state = restored_state
            last_successful_step = state.get("step", 0)
            logger.info(f"Restored workflow {workflow_id} from step {last_successful_step}")
            
        # Execute remaining steps
        for i, step_func in enumerate(steps):
            if i < last_successful_step:
                continue
                
            step_num = i + 1
            state["step"] = step_num
            
            try:
                # Create checkpoint before step
                if self.config.enable_checkpoints and i % self.config.checkpoint_interval == 0:
                    await self.checkpoint_manager.create(
                        workflow_id=workflow_id,
                        step=step_num,
                        state=state,
                    )
                    
                # Execute step
                result = await step_func(state)
                state["last_result"] = result
                
            except Exception as e:
                logger.error(f"Step {step_num} failed: {e}")
                
                # Try to restore to last checkpoint
                checkpoint = await self.checkpoint_manager.get_latest(workflow_id)
                if checkpoint:
                    state = checkpoint.state
                    return RecoveryResult(
                        success=False,
                        recovered=True,
                        strategy_used=RecoveryStrategy.CHECKPOINT,
                        attempts=1,
                        final_error=str(e),
                        restored_state=state,
                    )
                    
                return RecoveryResult(
                    success=False,
                    recovered=False,
                    strategy_used=RecoveryStrategy.CHECKPOINT,
                    attempts=1,
                    final_error=str(e),
                    restored_state=state,
                )
                
        # Cleanup checkpoints on success
        await self.checkpoint_manager.delete(workflow_id)
        
        return RecoveryResult(
            success=True,
            recovered=False,
            strategy_used=RecoveryStrategy.CHECKPOINT,
            attempts=1,
            restored_state=state,
        )
        
    def _record_recovery(
        self,
        workflow_id: str,
        strategy: RecoveryStrategy,
        success: bool,
        attempts: int,
    ) -> None:
        """Record recovery attempt in history."""
        self._recovery_history.append({
            "workflow_id": workflow_id,
            "strategy": strategy.value,
            "success": success,
            "attempts": attempts,
            "timestamp": time.time(),
        })
        
    def get_history(
        self,
        workflow_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get recovery history."""
        history = list(self._recovery_history)
        
        if workflow_id:
            history = [h for h in history if h["workflow_id"] == workflow_id]
            
        return history[-limit:]


class StatefulRetry:
    """
    Stateful retry with state preservation.
    
    Example:
        retry = StatefulRetry(max_attempts=5)
        
        async for attempt in retry.execute(
            func=unreliable_operation,
            state={"counter": 0}
        ):
            print(f"Attempt {attempt.number}, state: {attempt.state}")
    """
    
    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        exponential_backoff: bool = True,
    ) -> None:
        """
        Initialize stateful retry.
        
        Args:
            max_attempts: Maximum retry attempts.
            delay: Base delay between retries.
            exponential_backoff: Use exponential backoff.
        """
        self.max_attempts = max_attempts
        self.delay = delay
        self.exponential_backoff = exponential_backoff
        
    async def execute(
        self,
        func: Callable[..., Any],
        state: Dict[str, Any],
    ) -> Any:
        """
        Execute with stateful retries.
        
        Args:
            func: Async function to retry.
            state: Initial state (modified in place).
            
        Returns:
            Final result after retries.
        """
        last_error = None
        
        for attempt in range(self.max_attempts):
            try:
                state["attempt"] = attempt
                result = await func(state)
                return result
                
            except Exception as e:
                last_error = e
                state["last_error"] = str(e)
                
                if attempt < self.max_attempts - 1:
                    delay = self.delay
                    if self.exponential_backoff:
                        delay = self.delay * (2 ** attempt)
                    await asyncio.sleep(delay)
                    
        raise last_error
