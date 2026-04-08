"""
Automation Recovery Action Module

Provides recovery mechanisms, checkpoints, and state restoration for automation.
"""
from typing import Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import deque
import asyncio
import json


class RecoveryStrategy(Enum):
    """Recovery strategies."""
    RETRY = "retry"
    CHECKPOINT = "checkpoint"
    FALLBACK = "fallback"
    ROLLBACK = "rollback"
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class Checkpoint:
    """A recovery checkpoint."""
    checkpoint_id: str
    operation_name: str
    state: dict[str, Any]
    timestamp: datetime
    ttl_seconds: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryAction:
    """A recovery action to execute."""
    action_id: str
    action_type: RecoveryStrategy
    target_operation: str
    executor: Callable[[], Awaitable[Any]]
    max_attempts: int = 3
    backoff_seconds: float = 1.0
    fallback_value: Any = None


@dataclass
class RecoveryResult:
    """Result of recovery operation."""
    success: bool
    recovered: bool
    attempts: int
    final_value: Any = None
    error: Optional[str] = None
    recovery_steps: list[str] = field(default_factory=list)
    duration_ms: float = 0


@dataclass
class OperationContext:
    """Context for an operation that can be checkpointed."""
    operation_id: str
    operation_name: str
    state: dict[str, Any]
    checkpoints: list[Checkpoint] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)


class CheckpointManager:
    """Manages checkpoints for recovery."""
    
    def __init__(self, max_checkpoints: int = 100):
        self.max_checkpoints = max_checkpoints
        self._checkpoints: dict[str, deque[Checkpoint]] = {}
        self._lock = asyncio.Lock()
    
    async def save_checkpoint(
        self,
        context: OperationContext,
        ttl_seconds: Optional[float] = None
    ) -> Checkpoint:
        """Save a checkpoint for the operation."""
        async with self._lock:
            checkpoint = Checkpoint(
                checkpoint_id=f"{context.operation_id}:{len(context.checkpoints)}",
                operation_name=context.operation_name,
                state=context.state.copy(),
                timestamp=datetime.now(),
                ttl_seconds=ttl_seconds
            )
            
            if context.operation_id not in self._checkpoints:
                self._checkpoints[context.operation_id] = deque(maxlen=self.max_checkpoints)
            
            self._checkpoints[context.operation_id].append(checkpoint)
            
            return checkpoint
    
    async def get_latest_checkpoint(
        self,
        operation_id: str
    ) -> Optional[Checkpoint]:
        """Get the latest checkpoint for an operation."""
        if operation_id not in self._checkpoints:
            return None
        
        checkpoints = self._checkpoints[operation_id]
        if not checkpoints:
            return None
        
        latest = checkpoints[-1]
        
        # Check TTL
        if latest.ttl_seconds:
            age = (datetime.now() - latest.timestamp).total_seconds()
            if age > latest.ttl_seconds:
                return None
        
        return latest
    
    async def get_checkpoint_by_id(
        self,
        checkpoint_id: str
    ) -> Optional[Checkpoint]:
        """Get a specific checkpoint by ID."""
        op_id = checkpoint_id.rsplit(":", 1)[0]
        
        if op_id not in self._checkpoints:
            return None
        
        for checkpoint in self._checkpoints[op_id]:
            if checkpoint.checkpoint_id == checkpoint_id:
                return checkpoint
        
        return None
    
    async def list_checkpoints(
        self,
        operation_id: str
    ) -> list[Checkpoint]:
        """List all checkpoints for an operation."""
        if operation_id not in self._checkpoints:
            return []
        
        return list(self._checkpoints[operation_id])
    
    async def clear_checkpoints(self, operation_id: str):
        """Clear all checkpoints for an operation."""
        if operation_id in self._checkpoints:
            del self._checkpoints[operation_id]


class AutomationRecoveryAction:
    """Main recovery automation action handler."""
    
    def __init__(self):
        self._checkpoint_manager = CheckpointManager()
        self._recovery_handlers: dict[str, Callable] = {}
        self._fallback_handlers: dict[str, Callable] = {}
        self._circuit_breakers: dict[str, dict] = {}
        self._stats: dict[str, Any] = defaultdict(int)
    
    def register_recovery_handler(
        self,
        operation_name: str,
        handler: Callable[[Exception, OperationContext], Awaitable[Any]]
    ) -> "AutomationRecoveryAction":
        """Register a recovery handler for an operation."""
        self._recovery_handlers[operation_name] = handler
        return self
    
    def register_fallback(
        self,
        operation_name: str,
        fallback: Callable[[], Awaitable[Any]]
    ) -> "AutomationRecoveryAction":
        """Register a fallback handler for an operation."""
        self._fallback_handlers[operation_name] = fallback
        return self
    
    async def execute_with_recovery(
        self,
        context: OperationContext,
        operation: Callable[[OperationContext], Awaitable[Any]],
        recovery_config: Optional[dict[str, Any]] = None
    ) -> RecoveryResult:
        """
        Execute an operation with recovery support.
        
        Args:
            context: Operation context
            operation: Operation to execute
            recovery_config: Recovery configuration
            
        Returns:
            RecoveryResult with outcome
        """
        start_time = datetime.now()
        attempts = 0
        recovery_steps = []
        max_attempts = recovery_config.get("max_attempts", 3) if recovery_config else 3
        backoff = recovery_config.get("backoff_seconds", 1.0) if recovery_config else 1.0
        
        config = recovery_config or {}
        
        while attempts < max_attempts:
            attempts += 1
            
            try:
                # Save checkpoint before execution
                await self._checkpoint_manager.save_checkpoint(context)
                
                # Execute operation
                result = await operation(context)
                
                self._stats["successful_operations"] += 1
                
                return RecoveryResult(
                    success=True,
                    recovered=False,
                    attempts=attempts,
                    final_value=result,
                    recovery_steps=recovery_steps,
                    duration_ms=(datetime.now() - start_time).total_seconds() * 1000
                )
                
            except Exception as e:
                self._stats["operation_errors"] += 1
                last_error = e
                
                # Save checkpoint on failure
                await self._checkpoint_manager.save_checkpoint(context)
                
                # Try to recover
                if context.operation_name in self._recovery_handlers:
                    try:
                        recovered_value = await self._recovery_handlers[context.operation_name](
                            e, context
                        )
                        recovery_steps.append(f"Recovery handler succeeded: {context.operation_name}")
                        
                        return RecoveryResult(
                            success=True,
                            recovered=True,
                            attempts=attempts,
                            final_value=recovered_value,
                            recovery_steps=recovery_steps,
                            duration_ms=(datetime.now() - start_time).total_seconds() * 1000
                        )
                    except Exception as recovery_error:
                        recovery_steps.append(f"Recovery failed: {recovery_error}")
                
                # Apply backoff before retry
                if attempts < max_attempts:
                    await asyncio.sleep(backoff * (2 ** (attempts - 1)))
        
        # All retries exhausted - try fallback
        if context.operation_name in self._fallback_handlers:
            try:
                fallback_value = await self._fallback_handlers[context.operation_name]()
                recovery_steps.append("Fallback executed")
                
                self._stats["fallback_success"] += 1
                
                return RecoveryResult(
                    success=True,
                    recovered=True,
                    attempts=attempts,
                    final_value=fallback_value,
                    recovery_steps=recovery_steps,
                    duration_ms=(datetime.now() - start_time).total_seconds() * 1000
                )
            except Exception as fallback_error:
                recovery_steps.append(f"Fallback failed: {fallback_error}")
                self._stats["fallback_errors"] += 1
        
        self._stats["total_failures"] += 1
        
        return RecoveryResult(
            success=False,
            recovered=False,
            attempts=attempts,
            error=str(last_error),
            recovery_steps=recovery_steps,
            duration_ms=(datetime.now() - start_time).total_seconds() * 1000
        )
    
    async def rollback_to_checkpoint(
        self,
        operation_id: str,
        checkpoint_id: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        """
        Rollback operation to a checkpoint.
        
        Args:
            operation_id: ID of the operation
            checkpoint_id: Specific checkpoint ID, or None for latest
            
        Returns:
            Restored state if found
        """
        if checkpoint_id:
            checkpoint = await self._checkpoint_manager.get_checkpoint_by_id(checkpoint_id)
        else:
            checkpoint = await self._checkpoint_manager.get_latest_checkpoint(operation_id)
        
        if checkpoint:
            self._stats["checkpoints_restored"] += 1
            return checkpoint.state
        
        return None
    
    async def execute_with_circuit_breaker(
        self,
        operation_name: str,
        operation: Callable[[], Awaitable[Any]],
        threshold: int = 5,
        timeout_seconds: float = 60.0
    ) -> RecoveryResult:
        """
        Execute operation with circuit breaker pattern.
        
        Circuit opens after threshold consecutive failures.
        """
        start_time = datetime.now()
        
        # Get or create circuit breaker state
        if operation_name not in self._circuit_breakers:
            self._circuit_breakers[operation_name] = {
                "failures": 0,
                "successes": 0,
                "state": "closed",  # closed, open, half_open
                "last_failure": None,
                "opened_at": None
            }
        
        cb = self._circuit_breakers[operation_name]
        
        # Check if circuit is open
        if cb["state"] == "open":
            elapsed = (datetime.now() - cb["opened_at"]).total_seconds() if cb["opened_at"] else 0
            
            if elapsed > timeout_seconds:
                cb["state"] = "half_open"
            else:
                self._stats["circuit_breaker_rejected"] += 1
                return RecoveryResult(
                    success=False,
                    recovered=False,
                    attempts=1,
                    error="Circuit breaker is open",
                    recovery_steps=["Circuit breaker rejection"]
                )
        
        try:
            result = await operation()
            
            # Record success
            cb["successes"] += 1
            cb["failures"] = 0
            
            if cb["state"] == "half_open":
                cb["state"] = "closed"
            
            self._stats["circuit_breaker_success"] += 1
            
            return RecoveryResult(
                success=True,
                recovered=False,
                attempts=1,
                final_value=result,
                duration_ms=(datetime.now() - start_time).total_seconds() * 1000
            )
            
        except Exception as e:
            cb["failures"] += 1
            cb["last_failure"] = datetime.now()
            
            if cb["failures"] >= threshold:
                cb["state"] = "open"
                cb["opened_at"] = datetime.now()
            
            self._stats["circuit_breaker_errors"] += 1
            
            return RecoveryResult(
                success=False,
                recovered=False,
                attempts=1,
                error=str(e),
                duration_ms=(datetime.now() - start_time).total_seconds() * 1000
            )
    
    def get_stats(self) -> dict[str, Any]:
        """Get recovery statistics."""
        return dict(self._stats)
    
    async def get_circuit_breaker_status(self, operation_name: str) -> Optional[dict[str, Any]]:
        """Get circuit breaker status for an operation."""
        if operation_name not in self._circuit_breakers:
            return None
        
        cb = self._circuit_breakers[operation_name]
        return {
            "operation": operation_name,
            "state": cb["state"],
            "failures": cb["failures"],
            "successes": cb["successes"],
            "last_failure": cb["last_failure"].isoformat() if cb["last_failure"] else None
        }
