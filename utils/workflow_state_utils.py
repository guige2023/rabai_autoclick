"""
Workflow state utilities for managing automation workflow state.

Provides state management, persistence, and recovery
for complex automation workflows.
"""

from __future__ import annotations

import json
import time
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum


class WorkflowState(Enum):
    """Workflow execution states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class StateSnapshot:
    """Workflow state snapshot."""
    state: WorkflowState
    step_index: int
    data: Dict[str, Any]
    timestamp: float
    error: Optional[str] = None


@dataclass
class WorkflowContext:
    """Workflow execution context."""
    workflow_id: str
    name: str
    current_state: WorkflowState
    step_index: int
    state_data: Dict[str, Any]
    history: List[StateSnapshot] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None


class WorkflowStateManager:
    """Manages workflow state and transitions."""
    
    def __init__(self, workflow_id: str, name: str):
        """
        Initialize workflow state manager.
        
        Args:
            workflow_id: Workflow identifier.
            name: Workflow name.
        """
        self.context = WorkflowContext(
            workflow_id=workflow_id,
            name=name,
            current_state=WorkflowState.IDLE,
            step_index=0,
            state_data={}
        )
        self._listeners: List[Callable] = []
    
    def transition_to(self, state: WorkflowState,
                     step_index: Optional[int] = None,
                     data: Optional[Dict] = None,
                     error: Optional[str] = None) -> None:
        """
        Transition to new state.
        
        Args:
            state: New state.
            step_index: Optional step index.
            data: Optional state data update.
            error: Optional error message.
        """
        old_state = self.context.current_state
        self.context.current_state = state
        
        if step_index is not None:
            self.context.step_index = step_index
        
        if data:
            self.context.state_data.update(data)
        
        snapshot = StateSnapshot(
            state=state,
            step_index=self.context.step_index,
            data=self.context.state_data.copy(),
            timestamp=time.time(),
            error=error
        )
        self.context.history.append(snapshot)
        
        for listener in self._listeners:
            try:
                listener(old_state, state, self.context)
            except Exception:
                pass
    
    def start(self) -> None:
        """Start workflow."""
        self.transition_to(WorkflowState.RUNNING, step_index=0)
    
    def pause(self) -> None:
        """Pause workflow."""
        if self.context.current_state == WorkflowState.RUNNING:
            self.transition_to(WorkflowState.PAUSED)
    
    def resume(self) -> None:
        """Resume workflow."""
        if self.context.current_state == WorkflowState.PAUSED:
            self.transition_to(WorkflowState.RUNNING)
    
    def complete(self) -> None:
        """Complete workflow."""
        self.transition_to(WorkflowState.COMPLETED)
        self.context.end_time = time.time()
    
    def fail(self, error: str) -> None:
        """
        Fail workflow.
        
        Args:
            error: Error message.
        """
        self.transition_to(WorkflowState.FAILED, error=error)
        self.context.end_time = time.time()
    
    def cancel(self) -> None:
        """Cancel workflow."""
        self.transition_to(WorkflowState.CANCELLED)
        self.context.end_time = time.time()
    
    def set_data(self, key: str, value: Any) -> None:
        """Set state data value."""
        self.context.state_data[key] = value
    
    def get_data(self, key: str, default: Any = None) -> Any:
        """Get state data value."""
        return self.context.state_data.get(key, default)
    
    def add_listener(self, listener: Callable) -> None:
        """Add state change listener."""
        self._listeners.append(listener)
    
    def get_context(self) -> WorkflowContext:
        """Get current workflow context."""
        return self.context
    
    def save_state(self, path: str) -> bool:
        """
        Save workflow state to file.
        
        Args:
            path: Output file path.
            
        Returns:
            True if successful.
        """
        try:
            data = {
                'workflow_id': self.context.workflow_id,
                'name': self.context.name,
                'current_state': self.context.current_state.value,
                'step_index': self.context.step_index,
                'state_data': self.context.state_data,
                'start_time': self.context.start_time,
                'end_time': self.context.end_time,
                'history': [
                    {
                        'state': s.state.value,
                        'step_index': s.step_index,
                        'data': s.data,
                        'timestamp': s.timestamp,
                        'error': s.error,
                    }
                    for s in self.context.history
                ]
            }
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False
    
    def load_state(self, path: str) -> bool:
        """
        Load workflow state from file.
        
        Args:
            path: Input file path.
            
        Returns:
            True if successful.
        """
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            self.context.current_state = WorkflowState(data['current_state'])
            self.context.step_index = data['step_index']
            self.context.state_data = data['state_data']
            self.context.start_time = data['start_time']
            self.context.end_time = data.get('end_time')
            self.context.history = [
                StateSnapshot(
                    state=WorkflowState(h['state']),
                    step_index=h['step_index'],
                    data=h['data'],
                    timestamp=h['timestamp'],
                    error=h.get('error'),
                )
                for h in data.get('history', [])
            ]
            
            return True
        except Exception:
            return False
