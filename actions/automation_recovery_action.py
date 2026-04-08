"""Automation recovery action module for RabAI AutoClick.

Provides recovery mechanisms for automation workflows:
- AutomationRecoveryManager: Manage workflow failure recovery
- CheckpointRecovery: Recover from checkpoints on failure
- StateRecovery: Restore automation state after crashes
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RecoveryStrategy(Enum):
    """Recovery strategies."""
    CHECKPOINT = "checkpoint"
    STATE = "state"
    COMPENSATION = "compensation"
    IDEMPOTENT_RETRY = "idempotent_retry"
    SAGA_ROLLBACK = "saga_rollback"


@dataclass
class Checkpoint:
    """Workflow checkpoint."""
    checkpoint_id: str
    step_id: str
    state: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryConfig:
    """Configuration for recovery."""
    strategy: RecoveryStrategy = RecoveryStrategy.CHECKPOINT
    checkpoint_interval: int = 5
    max_checkpoints: int = 10
    persistence_path: Optional[str] = None
    auto_recover: bool = True
    recovery_timeout: float = 300.0


class CheckpointManager:
    """Manage workflow checkpoints."""
    
    def __init__(self, config: RecoveryConfig):
        self.config = config
        self._checkpoints: Dict[str, deque] = defaultdict(lambda: deque(maxlen=config.max_checkpoints))
        self._current_checkpoint: Dict[str, Checkpoint] = {}
        self._lock = threading.RLock()
    
    def create_checkpoint(self, workflow_id: str, step_id: str, state: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> Checkpoint:
        """Create a checkpoint."""
        checkpoint_id = f"{workflow_id}_{step_id}_{int(time.time() * 1000)}"
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            step_id=step_id,
            state=dict(state),
            metadata=metadata or {}
        )
        
        with self._lock:
            self._checkpoints[workflow_id].append(checkpoint)
            self._current_checkpoint[f"{workflow_id}:{step_id}"] = checkpoint
        
        self._persist_checkpoint(workflow_id, checkpoint)
        return checkpoint
    
    def get_last_checkpoint(self, workflow_id: str) -> Optional[Checkpoint]:
        """Get most recent checkpoint for workflow."""
        with self._lock:
            checkpoints = self._checkpoints.get(workflow_id)
            if checkpoints:
                return checkpoints[-1]
        return None
    
    def get_checkpoint_by_id(self, workflow_id: str, checkpoint_id: str) -> Optional[Checkpoint]:
        """Get specific checkpoint by ID."""
        with self._lock:
            for cp in self._checkpoints.get(workflow_id, []):
                if cp.checkpoint_id == checkpoint_id:
                    return cp
        return None
    
    def list_checkpoints(self, workflow_id: str) -> List[Checkpoint]:
        """List all checkpoints for workflow."""
        with self._lock:
            return list(self._checkpoints.get(workflow_id, []))
    
    def clear_checkpoints(self, workflow_id: str):
        """Clear all checkpoints for workflow."""
        with self._lock:
            self._checkpoints.pop(workflow_id, None)
    
    def _persist_checkpoint(self, workflow_id: str, checkpoint: Checkpoint):
        """Persist checkpoint to disk."""
        if not self.config.persistence_path:
            return
        try:
            path = os.path.join(self.config.persistence_path, f"{workflow_id}_checkpoints.json")
            checkpoints = self.list_checkpoints(workflow_id)
            data = [{"checkpoint_id": cp.checkpoint_id, "step_id": cp.step_id, "state": cp.state, "timestamp": cp.timestamp} for cp in checkpoints]
            with open(path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Failed to persist checkpoint: {e}")


class AutomationRecoveryManager:
    """Manage automation workflow recovery."""
    
    def __init__(self, config: Optional[RecoveryConfig] = None):
        self.config = config or RecoveryConfig()
        self._checkpoint_manager = CheckpointManager(self.config)
        self._recovery_handlers: Dict[str, Callable] = {}
        self._compensations: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()
        self._stats = {"total_recoveries": 0, "successful_recoveries": 0, "failed_recoveries": 0}
    
    def register_compensation(self, step_id: str, compensation: Callable):
        """Register compensation action for step."""
        with self._lock:
            self._compensations[step_id].append(compensation)
    
    def register_recovery_handler(self, error_type: str, handler: Callable):
        """Register recovery handler for error type."""
        with self._lock:
            self._recovery_handlers[error_type] = handler
    
    def checkpoint(self, workflow_id: str, step_id: str, state: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> Checkpoint:
        """Create checkpoint during workflow execution."""
        return self._checkpoint_manager.create_checkpoint(workflow_id, step_id, state, metadata)
    
    def recover(self, workflow_id: str, error: Exception, context: Any) -> Tuple[bool, Any]:
        """Attempt to recover from workflow failure."""
        with self._lock:
            self._stats["total_recoveries"] += 1
        
        error_type = type(error).__name__
        
        handler = self._recovery_handlers.get(error_type)
        if handler:
            try:
                result = handler(error, context)
                with self._lock:
                    self._stats["successful_recoveries"] += 1
                return True, result
            except Exception as e:
                logging.error(f"Recovery handler failed: {e}")
                with self._lock:
                    self._stats["failed_recoveries"] += 1
                return False, e
        
        if self.config.strategy == RecoveryStrategy.CHECKPOINT:
            return self._recover_from_checkpoint(workflow_id, context)
        
        if self.config.strategy == RecoveryStrategy.COMPENSATION:
            return self._compensate(workflow_id, context)
        
        return False, error
    
    def _recover_from_checkpoint(self, workflow_id: str, context: Any) -> Tuple[bool, Any]:
        """Recover from last checkpoint."""
        checkpoint = self._checkpoint_manager.get_last_checkpoint(workflow_id)
        if not checkpoint:
            return False, "No checkpoint found"
        return True, {"state": checkpoint.state, "resume_step": checkpoint.step_id}
    
    def _compensate(self, workflow_id: str, context: Any) -> Tuple[bool, Any]:
        """Run compensation actions in reverse order."""
        compensations_run = []
        errors = []
        
        with self._lock:
            step_ids = list(self._compensations.keys())
        
        for step_id in reversed(step_ids):
            with self._lock:
                comps = list(self._compensations.get(step_id, []))
            
            for comp in comps:
                try:
                    comp(context)
                    compensations_run.append(step_id)
                except Exception as e:
                    errors.append(str(e))
        
        if errors:
            return False, {"compensations": compensations_run, "errors": errors}
        return True, {"compensations": compensations_run}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get recovery statistics."""
        with self._lock:
            return dict(self._stats)


class AutomationRecoveryAction(BaseAction):
    """Automation recovery action."""
    action_type = "automation_recovery"
    display_name = "自动化恢复"
    description = "自动化工作流失败恢复"
    
    def __init__(self):
        super().__init__()
        self._manager: Optional[AutomationRecoveryManager] = None
        self._lock = threading.Lock()
    
    def _get_manager(self, params: Dict[str, Any]) -> AutomationRecoveryManager:
        """Get or create recovery manager."""
        with self._lock:
            if self._manager is None:
                config = RecoveryConfig(
                    strategy=RecoveryStrategy[params.get("strategy", "checkpoint").upper()],
                    checkpoint_interval=params.get("checkpoint_interval", 5),
                    max_checkpoints=params.get("max_checkpoints", 10),
                    auto_recover=params.get("auto_recover", True),
                )
                self._manager = AutomationRecoveryManager(config)
            return self._manager
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute recovery operation."""
        try:
            manager = self._get_manager(params)
            command = params.get("command", "checkpoint")
            workflow_id = params.get("workflow_id", "default")
            
            if command == "checkpoint":
                step_id = params.get("step_id", "step_0")
                state = params.get("state", {})
                checkpoint = manager.checkpoint(workflow_id, step_id, state)
                return ActionResult(success=True, data={"checkpoint_id": checkpoint.checkpoint_id})
            
            elif command == "recover":
                error = params.get("error")
                result, data = manager.recover(workflow_id, error or Exception("Unknown"), context)
                return ActionResult(success=result, data={"recovery": data})
            
            elif command == "register_compensation":
                step_id = params.get("step_id")
                handler = params.get("handler")
                if step_id and handler:
                    manager.register_compensation(step_id, handler)
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = manager.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            elif command == "list_checkpoints":
                checkpoints = manager._checkpoint_manager.list_checkpoints(workflow_id)
                return ActionResult(success=True, data={"checkpoints": [{"id": cp.checkpoint_id, "step": cp.step_id} for cp in checkpoints]})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationRecoveryAction error: {str(e)}")
