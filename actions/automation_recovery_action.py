"""Automation Recovery Action Module.

Provides recovery mechanisms for automation workflows including:
- Checkpoint management
- State recovery
- Transaction rollback
- Retry with state restoration

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class RecoveryPointType(Enum):
    """Types of recovery points."""
    CHECKPOINT = auto()
    SNAPSHOT = auto()
    TRANSACTION = auto()


@dataclass
class RecoveryPoint:
    """Represents a recovery point in the workflow."""
    id: str
    name: str
    type: RecoveryPointType
    created_at: str
    state: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    file_path: Optional[str] = None


@dataclass
class TransactionContext:
    """Transaction context for atomic operations."""
    id: str
    started_at: str
    operations: List[Dict[str, Any]] = field(default_factory=list)
    checkpoints: List[str] = field(default_factory=list)
    committed: bool = False
    rolled_back: bool = False


class CheckpointManager:
    """Manages workflow checkpoints for recovery.
    
    Provides persistent storage and retrieval of workflow state,
    enabling recovery from failures without losing progress.
    """
    
    def __init__(self, storage_dir: Optional[str] = None, max_checkpoints: int = 50):
        if storage_dir:
            self.storage_dir = Path(storage_dir)
        else:
            self.storage_dir = Path(tempfile.mkdtemp(prefix="autoclick_checkpoints_"))
        
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.max_checkpoints = max_checkpoints
        self._checkpoints: Dict[str, RecoveryPoint] = {}
        self._lock = asyncio.Lock()
    
    async def create_checkpoint(
        self,
        workflow_id: str,
        name: str,
        state: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        checkpoint_type: RecoveryPointType = RecoveryPointType.CHECKPOINT
    ) -> RecoveryPoint:
        """Create a new checkpoint.
        
        Args:
            workflow_id: Workflow identifier
            name: Checkpoint name
            state: Current workflow state
            metadata: Optional metadata
            checkpoint_type: Type of recovery point
            
        Returns:
            Created recovery point
        """
        async with self._lock:
            checkpoint_id = f"{workflow_id}_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            checkpoint = RecoveryPoint(
                id=checkpoint_id,
                name=name,
                type=checkpoint_type,
                created_at=datetime.now().isoformat(),
                state=state,
                metadata=metadata or {}
            )
            
            file_path = self.storage_dir / f"{checkpoint_id}.json"
            async with asyncio.Lock():
                with open(file_path, 'w') as f:
                    json.dump({
                        "id": checkpoint.id,
                        "name": checkpoint.name,
                        "type": checkpoint.type.name,
                        "created_at": checkpoint.created_at,
                        "state": checkpoint.state,
                        "metadata": checkpoint.metadata
                    }, f, indent=2, default=str)
            
            checkpoint.file_path = str(file_path)
            self._checkpoints[checkpoint_id] = checkpoint
            
            await self._prune_old_checkpoints(workflow_id)
            
            logger.info(f"Created checkpoint {checkpoint_id} for workflow {workflow_id}")
            return checkpoint
    
    async def get_checkpoint(self, checkpoint_id: str) -> Optional[RecoveryPoint]:
        """Retrieve a checkpoint by ID.
        
        Args:
            checkpoint_id: Checkpoint identifier
            
        Returns:
            Recovery point if found, None otherwise
        """
        if checkpoint_id in self._checkpoints:
            return self._checkpoints[checkpoint_id]
        
        file_path = self.storage_dir / f"{checkpoint_id}.json"
        if file_path.exists():
            with open(file_path) as f:
                data = json.load(f)
            return RecoveryPoint(
                id=data["id"],
                name=data["name"],
                type=RecoveryPointType[data["type"]],
                created_at=data["created_at"],
                state=data["state"],
                metadata=data.get("metadata", {}),
                file_path=str(file_path)
            )
        
        return None
    
    async def get_latest_checkpoint(self, workflow_id: str) -> Optional[RecoveryPoint]:
        """Get the most recent checkpoint for a workflow.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Most recent recovery point
        """
        checkpoints = [
            cp for cp in self._checkpoints.values()
            if cp.id.startswith(workflow_id)
        ]
        
        if not checkpoints:
            matching = [
                p for p in self.storage_dir.glob(f"{workflow_id}_*.json")
            ]
            if matching:
                latest = max(matching, key=lambda p: p.stat().st_mtime)
                return await self.get_checkpoint(latest.stem)
        
        return max(checkpoints, key=lambda cp: cp.created_at) if checkpoints else None
    
    async def delete_checkpoint(self, checkpoint_id: str) -> bool:
        """Delete a checkpoint.
        
        Args:
            checkpoint_id: Checkpoint identifier
            
        Returns:
            True if deleted, False if not found
        """
        async with self._lock:
            if checkpoint_id in self._checkpoints:
                cp = self._checkpoints.pop(checkpoint_id)
                if cp.file_path and os.path.exists(cp.file_path):
                    os.remove(cp.file_path)
                return True
            return False
    
    async def _prune_old_checkpoints(self, workflow_id: str) -> None:
        """Remove old checkpoints beyond max_checkpoints limit."""
        checkpoints = [
            cp for cp in self._checkpoints.values()
            if cp.id.startswith(workflow_id)
        ]
        
        if len(checkpoints) > self.max_checkpoints:
            checkpoints.sort(key=lambda cp: cp.created_at)
            to_delete = checkpoints[:-self.max_checkpoints]
            
            for cp in to_delete:
                await self.delete_checkpoint(cp.id)
    
    async def list_checkpoints(self, workflow_id: str) -> List[RecoveryPoint]:
        """List all checkpoints for a workflow.
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            List of recovery points
        """
        return [
            cp for cp in self._checkpoints.values()
            if cp.id.startswith(workflow_id)
        ]


class TransactionManager:
    """Manages transactional operations with rollback support.
    
    Provides atomic operation grouping with automatic rollback
    on failure, enabling reliable state transitions.
    """
    
    def __init__(self, checkpoint_manager: Optional[CheckpointManager] = None):
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self._transactions: Dict[str, TransactionContext] = {}
        self._lock = asyncio.Lock()
        self._rollback_handlers: Dict[str, List[Callable]] = {}
    
    async def begin_transaction(self, transaction_id: Optional[str] = None) -> TransactionContext:
        """Begin a new transaction.
        
        Args:
            transaction_id: Optional transaction ID
            
        Returns:
            Transaction context
        """
        async with self._lock:
            tid = transaction_id or f"txn_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
            
            ctx = TransactionContext(
                id=tid,
                started_at=datetime.now().isoformat()
            )
            
            self._transactions[tid] = ctx
            self._rollback_handlers[tid] = []
            
            logger.info(f"Began transaction {tid}")
            return ctx
    
    async def add_operation(self, transaction_id: str, operation: Dict[str, Any]) -> None:
        """Add an operation to the transaction.
        
        Args:
            transaction_id: Transaction identifier
            operation: Operation details
        """
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        self._transactions[transaction_id].operations.append(operation)
    
    async def add_checkpoint(self, transaction_id: str, checkpoint_id: str) -> None:
        """Add a checkpoint to the transaction.
        
        Args:
            transaction_id: Transaction identifier
            checkpoint_id: Checkpoint identifier
        """
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        self._transactions[transaction_id].checkpoints.append(checkpoint_id)
    
    def register_rollback_handler(self, transaction_id: str, handler: Callable) -> None:
        """Register a rollback handler for the transaction.
        
        Args:
            transaction_id: Transaction identifier
            handler: Async function to call on rollback
        """
        if transaction_id not in self._rollback_handlers:
            self._rollback_handlers[transaction_id] = []
        self._rollback_handlers[transaction_id].append(handler)
    
    async def commit_transaction(self, transaction_id: str) -> bool:
        """Commit a transaction.
        
        Args:
            transaction_id: Transaction identifier
            
        Returns:
            True if committed successfully
        """
        async with self._lock:
            if transaction_id not in self._transactions:
                raise ValueError(f"Transaction {transaction_id} not found")
            
            ctx = self._transactions[transaction_id]
            ctx.committed = True
            
            logger.info(f"Committed transaction {transaction_id} with {len(ctx.operations)} operations")
            
            del self._transactions[transaction_id]
            if transaction_id in self._rollback_handlers:
                del self._rollback_handlers[transaction_id]
            
            return True
    
    async def rollback_transaction(self, transaction_id: str, reason: Optional[str] = None) -> None:
        """Rollback a transaction.
        
        Args:
            transaction_id: Transaction identifier
            reason: Optional rollback reason
        """
        async with self._lock:
            if transaction_id not in self._transactions:
                logger.warning(f"Transaction {transaction_id} not found for rollback")
                return
            
            ctx = self._transactions[transaction_id]
            ctx.rolled_back = True
            
            logger.warning(f"Rolling back transaction {transaction_id}: {reason}")
            
            if transaction_id in self._rollback_handlers:
                for handler in reversed(self._rollback_handlers[transaction_id]):
                    try:
                        await handler()
                    except Exception as e:
                        logger.error(f"Rollback handler error: {e}")
            
            for checkpoint_id in reversed(ctx.checkpoints):
                cp = await self.checkpoint_manager.get_checkpoint(checkpoint_id)
                if cp:
                    logger.info(f"Restored checkpoint {checkpoint_id} during rollback")
            
            del self._transactions[transaction_id]
            if transaction_id in self._rollback_handlers:
                del self._rollback_handlers[transaction_id]


class WorkflowRecovery:
    """High-level workflow recovery orchestration.
    
    Combines checkpoint and transaction management for
    comprehensive workflow reliability.
    """
    
    def __init__(
        self,
        workflow_id: str,
        storage_dir: Optional[str] = None
    ):
        self.workflow_id = workflow_id
        self.checkpoint_manager = CheckpointManager(storage_dir)
        self.transaction_manager = TransactionManager(self.checkpoint_manager)
        self._current_state: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def save_checkpoint(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> RecoveryPoint:
        """Save a checkpoint of current workflow state.
        
        Args:
            name: Checkpoint name
            metadata: Optional metadata
            
        Returns:
            Created recovery point
        """
        async with self._lock:
            return await self.checkpoint_manager.create_checkpoint(
                workflow_id=self.workflow_id,
                name=name,
                state=self._current_state.copy(),
                metadata=metadata
            )
    
    async def restore_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Restore workflow state from a checkpoint.
        
        Args:
            checkpoint_id: Checkpoint to restore
            
        Returns:
            Restored state dictionary
        """
        checkpoint = await self.checkpoint_manager.get_checkpoint(checkpoint_id)
        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} not found")
        
        async with self._lock:
            self._current_state = checkpoint.state.copy()
            return self._current_state.copy()
    
    async def execute_with_recovery(
        self,
        operation: Callable,
        checkpoint_name: str,
        *args,
        **kwargs
    ) -> Any:
        """Execute operation with automatic checkpointing and recovery.
        
        Args:
            operation: Async operation to execute
            checkpoint_name: Name for auto-created checkpoint
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Operation result
        """
        checkpoint = await self.save_checkpoint(checkpoint_name)
        
        try:
            result = await operation(*args, **kwargs)
            return result
        except Exception as e:
            logger.error(f"Operation failed, restoring checkpoint {checkpoint.id}: {e}")
            await self.restore_checkpoint(checkpoint.id)
            raise
    
    async def execute_transactional(
        self,
        operations: List[Callable],
        transaction_id: Optional[str] = None
    ) -> List[Any]:
        """Execute operations as an atomic transaction.
        
        Args:
            operations: List of async operations
            transaction_id: Optional transaction ID
            
        Returns:
            List of operation results
        """
        txn = await self.transaction_manager.begin_transaction(transaction_id)
        
        results = []
        try:
            for op in operations:
                result = await op()
                results.append(result)
            
            await self.transaction_manager.commit_transaction(txn.id)
            return results
            
        except Exception as e:
            logger.error(f"Transaction failed, rolling back: {e}")
            await self.transaction_manager.rollback_transaction(txn.id, str(e))
            raise
