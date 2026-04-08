"""State action module for RabAI AutoClick.

Provides state management actions for workflow execution
including state get/set, transitions, and history tracking.
"""

import time
import threading
import sys
import os
import json
import hashlib
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StateTransitionError(Exception):
    """Raised when a state transition is invalid."""
    pass


@dataclass
class StateTransition:
    """Represents a state transition.
    
    Attributes:
        from_state: Previous state.
        to_state: New state.
        timestamp: When transition occurred.
        data: Optional transition data.
    """
    from_state: str
    to_state: str
    timestamp: float
    data: Any = None


@dataclass
class StateSnapshot:
    """A snapshot of state at a point in time.
    
    Attributes:
        state: Current state value.
        data: State data.
        timestamp: Snapshot timestamp.
    """
    state: Any
    data: Dict[str, Any]
    timestamp: float


class StateMachine:
    """Thread-safe state machine with transition history."""
    
    def __init__(self, name: str):
        """Initialize state machine.
        
        Args:
            name: Name of the state machine.
        """
        self.name = name
        self._current_state: Any = None
        self._state_data: Dict[str, Any] = {}
        self._transitions: List[StateTransition] = []
        self._lock = threading.RLock()
        self._valid_transitions: Dict[Any, List[Any]] = defaultdict(list)
        self._transition_callbacks: Dict[str, List[callable]] = defaultdict(list)
        self._max_history = 1000
    
    def define_transitions(self, transitions: Dict[Any, List[Any]]) -> None:
        """Define valid state transitions.
        
        Args:
            transitions: Dict mapping state to list of valid next states.
        """
        with self._lock:
            self._valid_transitions.clear()
            for from_state, to_states in transitions.items():
                self._valid_transitions[from_state] = to_states
    
    def add_transition_callback(self, callback: callable) -> None:
        """Add a callback for state transitions.
        
        Args:
            callback: Callable that receives (from_state, to_state, data).
        """
        with self._lock:
            self._transition_callbacks[self.name].append(callback)
    
    def set_state(self, new_state: Any, data: Any = None, force: bool = False) -> StateTransition:
        """Transition to a new state.
        
        Args:
            new_state: Target state.
            data: Optional transition data.
            force: Skip validation if True.
        
        Returns:
            StateTransition object.
        
        Raises:
            StateTransitionError: If transition is invalid.
        """
        with self._lock:
            if not force and self._current_state is not None:
                valid_next = self._valid_transitions.get(self._current_state, [])
                if valid_next and new_state not in valid_next:
                    raise StateTransitionError(
                        f"Invalid transition from {self._current_state} to {new_state}"
                    )
            
            old_state = self._current_state
            transition = StateTransition(
                from_state=old_state,
                to_state=new_state,
                timestamp=time.time(),
                data=data
            )
            
            self._transitions.append(transition)
            if len(self._transitions) > self._max_history:
                self._transitions.pop(0)
            
            self._current_state = new_state
            
            if data is not None:
                self._state_data.update(data if isinstance(data, dict) else {'data': data})
            
            for callback in self._transition_callbacks.get(self.name, []):
                try:
                    callback(old_state, new_state, data)
                except Exception:
                    pass
            
            return transition
    
    def get_state(self) -> Any:
        """Get current state."""
        with self._lock:
            return self._current_state
    
    def get_data(self, key: str = None) -> Any:
        """Get state data.
        
        Args:
            key: Specific key or None for all data.
        
        Returns:
            Value or full data dict.
        """
        with self._lock:
            if key is None:
                return dict(self._state_data)
            return self._state_data.get(key)
    
    def set_data(self, key: str, value: Any) -> None:
        """Set state data.
        
        Args:
            key: Data key.
            value: Value to set.
        """
        with self._lock:
            self._state_data[key] = value
    
    def get_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get transition history.
        
        Args:
            limit: Max number of transitions to return.
        
        Returns:
            List of transition dicts.
        """
        with self._lock:
            transitions = self._transitions[-limit:]
            return [
                {
                    "from": t.from_state,
                    "to": t.to_state,
                    "timestamp": t.timestamp,
                    "data": t.data
                }
                for t in transitions
            ]
    
    def can_transition(self, to_state: Any) -> bool:
        """Check if transition to state is valid.
        
        Args:
            to_state: Target state.
        
        Returns:
            True if transition is allowed.
        """
        with self._lock:
            if self._current_state is None:
                return True
            valid_next = self._valid_transitions.get(self._current_state, [])
            return not valid_next or to_state in valid_next


class StateStore:
    """Thread-safe key-value state store."""
    
    def __init__(self, name: str):
        """Initialize state store.
        
        Args:
            name: Store name.
        """
        self.name = name
        self._store: Dict[str, Any] = {}
        self._history: List[Dict[str, Any]] = []
        self._lock = threading.RLock()
        self._max_history = 500
    
    def set(self, key: str, value: Any) -> None:
        """Set a value.
        
        Args:
            key: Storage key.
            value: Value to store.
        """
        with self._lock:
            old_value = self._store.get(key)
            self._store[key] = value
            self._history.append({
                "action": "set",
                "key": key,
                "old_value": old_value,
                "new_value": value,
                "timestamp": time.time()
            })
            if len(self._history) > self._max_history:
                self._history.pop(0)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value.
        
        Args:
            key: Storage key.
            default: Default if key not found.
        
        Returns:
            Stored value or default.
        """
        with self._lock:
            return self._store.get(key, default)
    
    def delete(self, key: str) -> bool:
        """Delete a key.
        
        Args:
            key: Storage key.
        
        Returns:
            True if deleted.
        """
        with self._lock:
            if key in self._store:
                del self._store[key]
                self._history.append({
                    "action": "delete",
                    "key": key,
                    "timestamp": time.time()
                })
                return True
            return False
    
    def keys(self) -> List[str]:
        """Get all keys."""
        with self._lock:
            return list(self._store.keys())
    
    def get_all(self) -> Dict[str, Any]:
        """Get all key-value pairs."""
        with self._lock:
            return dict(self._store)
    
    def clear(self) -> int:
        """Clear all data.
        
        Returns:
            Number of keys cleared.
        """
        with self._lock:
            count = len(self._store)
            self._store.clear()
            return count


# Global state management
_state_machines: Dict[str, StateMachine] = {}
_state_stores: Dict[str, StateStore] = {}
_state_lock = threading.Lock()


class StateMachineTransitionAction(BaseAction):
    """Transition a state machine to a new state."""
    action_type = "state_machine_transition"
    display_name = "状态机转换"
    description = "状态机状态转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Transition state machine.
        
        Args:
            context: Execution context.
            params: Dict with keys: machine_name, new_state, data, force.
        
        Returns:
            ActionResult with transition result.
        """
        machine_name = params.get('machine_name', 'default')
        new_state = params.get('new_state', '')
        data = params.get('data', None)
        force = params.get('force', False)
        
        if not new_state and new_state != 0:
            return ActionResult(success=False, message="new_state is required")
        
        with _state_lock:
            if machine_name not in _state_machines:
                _state_machines[machine_name] = StateMachine(machine_name)
            machine = _state_machines[machine_name]
        
        try:
            transition = machine.set_state(new_state, data=data, force=force)
            
            return ActionResult(
                success=True,
                message=f"Transitioned to {new_state}",
                data={
                    "from": transition.from_state,
                    "to": transition.to_state,
                    "timestamp": transition.timestamp
                }
            )
        except StateTransitionError as e:
            return ActionResult(success=False, message=str(e))


class StateMachineGetAction(BaseAction):
    """Get current state from a state machine."""
    action_type = "state_machine_get"
    display_name = "状态机获取"
    description = "获取状态机当前状态"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get state machine state.
        
        Args:
            context: Execution context.
            params: Dict with keys: machine_name.
        
        Returns:
            ActionResult with current state.
        """
        machine_name = params.get('machine_name', 'default')
        
        with _state_lock:
            if machine_name not in _state_machines:
                return ActionResult(success=True, message="State machine not found", data={"state": None})
            machine = _state_machines[machine_name]
        
        state = machine.get_state()
        data = machine.get_data()
        history = machine.get_history()
        
        return ActionResult(
            success=True,
            message=f"Current state: {state}",
            data={
                "state": state,
                "data": data,
                "history": history
            }
        )


class StateMachineDefineAction(BaseAction):
    """Define valid transitions for a state machine."""
    action_type = "state_machine_define"
    display_name = "状态机定义"
    description = "定义状态转换规则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Define state transitions.
        
        Args:
            context: Execution context.
            params: Dict with keys: machine_name, transitions.
        
        Returns:
            ActionResult with definition status.
        """
        machine_name = params.get('machine_name', 'default')
        transitions = params.get('transitions', {})
        
        if not transitions:
            return ActionResult(success=False, message="transitions is required")
        
        with _state_lock:
            if machine_name not in _state_machines:
                _state_machines[machine_name] = StateMachine(machine_name)
            machine = _state_machines[machine_name]
        
        machine.define_transitions(transitions)
        
        return ActionResult(
            success=True,
            message=f"Defined {len(transitions)} state transitions for {machine_name}",
            data={"transitions": transitions}
        )


class StateStoreSetAction(BaseAction):
    """Set a value in the state store."""
    action_type = "state_store_set"
    display_name = "状态存储设置"
    description = "设置状态存储值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Set store value.
        
        Args:
            context: Execution context.
            params: Dict with keys: store_name, key, value.
        
        Returns:
            ActionResult with set status.
        """
        store_name = params.get('store_name', 'default')
        key = params.get('key', '')
        value = params.get('value', None)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with _state_lock:
            if store_name not in _state_stores:
                _state_stores[store_name] = StateStore(store_name)
            store = _state_stores[store_name]
        
        store.set(key, value)
        
        return ActionResult(success=True, message=f"Set {key} in {store_name}", data={"key": key})


class StateStoreGetAction(BaseAction):
    """Get a value from the state store."""
    action_type = "state_store_get"
    display_name = "状态存储获取"
    description = "获取状态存储值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get store value.
        
        Args:
            context: Execution context.
            params: Dict with keys: store_name, key, default.
        
        Returns:
            ActionResult with value.
        """
        store_name = params.get('store_name', 'default')
        key = params.get('key', '')
        default = params.get('default', None)
        
        if not key:
            return ActionResult(success=False, message="key is required")
        
        with _state_lock:
            if store_name not in _state_stores:
                return ActionResult(success=True, message="Store not found", data={"value": default})
            store = _state_stores[store_name]
        
        value = store.get(key, default)
        
        return ActionResult(success=True, message=f"Got {key} from {store_name}", data={"key": key, "value": value})


class StateStoreGetAllAction(BaseAction):
    """Get all values from a state store."""
    action_type = "state_store_get_all"
    display_name = "状态存储获取全部"
    description = "获取所有状态存储值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get all store values.
        
        Args:
            context: Execution context.
            params: Dict with keys: store_name.
        
        Returns:
            ActionResult with all values.
        """
        store_name = params.get('store_name', 'default')
        
        with _state_lock:
            if store_name not in _state_stores:
                return ActionResult(success=True, message="Store not found", data={"store": {}, "keys": []})
            store = _state_stores[store_name]
        
        all_data = store.get_all()
        
        return ActionResult(
            success=True,
            message=f"Got {len(all_data)} values from {store_name}",
            data={"store": all_data, "keys": list(all_data.keys())}
        )


class StateStoreClearAction(BaseAction):
    """Clear a state store."""
    action_type = "state_store_clear"
    display_name = "状态存储清空"
    description = "清空状态存储"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Clear store.
        
        Args:
            context: Execution context.
            params: Dict with keys: store_name.
        
        Returns:
            ActionResult with cleared count.
        """
        store_name = params.get('store_name', 'default')
        
        with _state_lock:
            if store_name not in _state_stores:
                return ActionResult(success=True, message="Store not found", data={"cleared": 0})
            store = _state_stores[store_name]
        
        cleared = store.clear()
        
        return ActionResult(success=True, message=f"Cleared {cleared} entries from {store_name}", data={"cleared": cleared})
