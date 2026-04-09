"""
Automation State Machine Action Module.

Implements a flexible state machine for automation workflows with
support for states, transitions, guards, and actions.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
from datetime import datetime


class TransitionType(Enum):
    """Types of state transitions."""
    INTERNAL = "internal"
    EXTERNAL = "external"


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[[], bool]] = None
    action: Optional[Callable[[], None]] = None
    description: str = ""


@dataclass
class State:
    """Represents a state machine state."""
    name: str
    entry_action: Optional[Callable[[], None]] = None
    exit_action: Optional[Callable[[], None]] = None
    do_action: Optional[Callable[[], None]] = None
    is_initial: bool = False
    is_final: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateMachineConfig:
    """Configuration for state machine."""
    name: str
    initial_state: str
    states: List[State] = field(default_factory=list)
    transitions: List[Transition] = field(default_factory=list)
    final_states: Set[str] = field(default_factory=set)
    context: Dict[str, Any] = field(default_factory=dict)


class StateMachineError(Exception):
    """Raised for state machine errors."""
    pass


class InvalidTransitionError(StateMachineError):
    """Raised when transition is invalid."""
    
    def __init__(self, from_state: str, event: str):
        self.from_state = from_state
        self.event = event
        super().__init__(f"Invalid transition: cannot handle event '{event}' in state '{from_state}'")


class GuardFailedError(StateMachineError):
    """Raised when transition guard evaluates to False."""
    
    def __init__(self, from_state: str, event: str):
        self.from_state = from_state
        self.event = event
        super().__init__(f"Guard failed for transition: '{event}' in state '{from_state}'")


class StateMachine:
    """
    Configurable state machine for workflow automation.
    
    Example:
        sm = StateMachine(name="order")
        sm.add_state("pending", is_initial=True)
        sm.add_state("processing")
        sm.add_state("completed", is_final=True)
        sm.add_transition("pending", "processing", "process")
        sm.add_transition("processing", "completed", "complete")
        
        sm.initialize()
        sm.send_event("process")
    """
    
    def __init__(self, name: str = "state_machine"):
        self.name = name
        self.states: Dict[str, State] = {}
        self.transitions: Dict[str, Dict[str, Transition]] = {}  # state -> event -> transition
        self.current_state: Optional[str] = None
        self.history: List[Dict[str, Any]] = []
        self.context: Dict[str, Any] = {}
        self._lock = threading.RLock()
    
    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable] = None,
        exit_action: Optional[Callable] = None,
        do_action: Optional[Callable] = None,
        is_initial: bool = False,
        is_final: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "StateMachine":
        """Add a state to the state machine."""
        state = State(
            name=name,
            entry_action=entry_action,
            exit_action=exit_action,
            do_action=do_action,
            is_initial=is_initial,
            is_final=is_final,
            metadata=metadata or {}
        )
        self.states[name] = state
        self.transitions[name] = {}
        return self
    
    def add_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard: Optional[Callable] = None,
        action: Optional[Callable] = None,
        description: str = ""
    ) -> "StateMachine":
        """Add a transition between states."""
        if from_state not in self.states:
            raise StateMachineError(f"Unknown state: {from_state}")
        if to_state not in self.states:
            raise StateMachineError(f"Unknown state: {to_state}")
        
        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
            description=description
        )
        self.transitions[from_state][event] = transition
        return self
    
    def initialize(self, initial_state: Optional[str] = None) -> "StateMachine":
        """Initialize the state machine."""
        with self._lock:
            state_name = initial_state or self._get_initial_state()
            if state_name not in self.states:
                raise StateMachineError(f"Unknown initial state: {state_name}")
            
            self.current_state = state_name
            state = self.states[state_name]
            
            if state.entry_action:
                state.entry_action()
            
            self._record_transition(None, state_name, "initialize")
        
        return self
    
    def send_event(self, event: str) -> bool:
        """Send an event to the state machine."""
        with self._lock:
            if self.current_state is None:
                raise StateMachineError("State machine not initialized")
            
            from_state = self.current_state
            
            if event not in self.transitions[from_state]:
                raise InvalidTransitionError(from_state, event)
            
            transition = self.transitions[from_state][event]
            
            # Check guard
            if transition.guard and not transition.guard():
                raise GuardFailedError(from_state, event)
            
            # Execute transition
            return self._execute_transition(transition)
    
    def _execute_transition(self, transition: Transition) -> bool:
        """Execute a state transition."""
        from_state = self.current_state
        to_state = transition.to_state
        
        # Exit action for current state
        from_state_obj = self.states[from_state]
        if from_state_obj.exit_action:
            from_state_obj.exit_action()
        
        # Transition action
        if transition.action:
            transition.action()
        
        # Entry action for new state
        self.current_state = to_state
        to_state_obj = self.states[to_state]
        if to_state_obj.entry_action:
            to_state_obj.entry_action()
        
        self._record_transition(from_state, to_state, transition.event)
        
        return True
    
    def _record_transition(self, from_state: Optional[str], to_state: str, event: str):
        """Record transition in history."""
        self.history.append({
            "timestamp": datetime.now().isoformat(),
            "from_state": from_state,
            "to_state": to_state,
            "event": event
        })
    
    def _get_initial_state(self) -> str:
        """Get the initial state."""
        for name, state in self.states.items():
            if state.is_initial:
                return name
        return list(self.states.keys())[0]
    
    def get_current_state(self) -> Optional[str]:
        """Get current state name."""
        return self.current_state
    
    def is_in_final_state(self) -> bool:
        """Check if state machine is in a final state."""
        if self.current_state is None:
            return False
        return self.states[self.current_state].is_final
    
    def get_history(self) -> List[Dict[str, Any]]:
        """Get transition history."""
        return list(self.history)
    
    def get_available_events(self) -> List[str]:
        """Get events available in current state."""
        if self.current_state is None:
            return []
        return list(self.transitions[self.current_state].keys())


class HierarchicalStateMachine(StateMachine):
    """
    Hierarchical state machine with nested states.
    
    Example:
        hsm = HierarchicalStateMachine("order")
        hsm.add_region("main")
        hsm.add_substate("main", "pending", parent="root")
    """
    
    def __init__(self, name: str = "hsm"):
        super().__init__(name)
        self.regions: Dict[str, List[str]] = {}
        self.parent_states: Dict[str, str] = {}
        self.active_states: Dict[str, str] = {}  # region -> state
    
    def add_region(self, region_name: str) -> "HierarchicalStateMachine":
        """Add a region to the state machine."""
        self.regions[region_name] = []
        return self
    
    def add_substate(
        self,
        region: str,
        name: str,
        parent: Optional[str] = None,
        **kwargs
    ) -> "HierarchicalStateMachine":
        """Add a substate to a region."""
        self.add_state(name, **kwargs)
        self.regions[region].append(name)
        if parent:
            self.parent_states[name] = parent
        return self


class ParallelStateMachine(StateMachine):
    """
    State machine with parallel state execution.
    
    Example:
        psm = ParallelStateMachine("workflow")
        psm.add_orthogonal_region("A")
        psm.add_orthogonal_region("B")
        psm.start_all()
    """
    
    def __init__(self, name: str = "parallel"):
        super().__init__(name)
        self.orthogonal_regions: Dict[str, str] = {}  # region -> state
    
    def add_orthogonal_region(self, region_name: str, initial_state: str) -> "ParallelStateMachine":
        """Add an orthogonal (parallel) region."""
        self.orthogonal_regions[region_name] = initial_state
        return self
    
    def start_all(self):
        """Start all orthogonal regions."""
        for region, state in self.orthogonal_regions.items():
            self.initialize(state)


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationStateMachineAction(BaseAction):
    """
    State machine action for workflow automation.
    
    Parameters:
        operation: Operation type (create/add_state/add_transition/send_event)
        name: State machine name
        states: List of state definitions
        transitions: List of transition definitions
        event: Event to send
    
    Example:
        action = AutomationStateMachineAction()
        result = action.execute({}, {
            "operation": "create",
            "name": "order_flow",
            "states": ["pending", "processing", "completed"]
        })
    """
    
    _machines: Dict[str, StateMachine] = {}
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute state machine operation."""
        operation = params.get("operation", "create")
        name = params.get("name", "default")
        states = params.get("states", [])
        transitions = params.get("transitions", [])
        event = params.get("event")
        
        with self._lock:
            if operation == "create":
                sm = StateMachine(name=name)
                
                for state_def in states:
                    if isinstance(state_def, str):
                        sm.add_state(state_def)
                    else:
                        sm.add_state(**state_def)
                
                for trans_def in transitions:
                    sm.add_transition(**trans_def)
                
                self._machines[name] = sm
                
                return {
                    "success": True,
                    "operation": "create",
                    "name": name,
                    "state_count": len(states),
                    "created_at": datetime.now().isoformat()
                }
            
            elif operation == "initialize":
                if name not in self._machines:
                    return {"success": False, "error": f"State machine '{name}' not found"}
                
                self._machines[name].initialize()
                return {
                    "success": True,
                    "operation": "initialize",
                    "name": name,
                    "current_state": self._machines[name].get_current_state()
                }
            
            elif operation == "send_event":
                if name not in self._machines:
                    return {"success": False, "error": f"State machine '{name}' not found"}
                
                sm = self._machines[name]
                try:
                    sm.send_event(event)
                    return {
                        "success": True,
                        "operation": "send_event",
                        "name": name,
                        "current_state": sm.get_current_state(),
                        "event": event
                    }
                except StateMachineError as e:
                    return {
                        "success": False,
                        "error": str(e),
                        "current_state": sm.get_current_state()
                    }
            
            elif operation == "status":
                if name not in self._machines:
                    return {"success": False, "error": f"State machine '{name}' not found"}
                
                sm = self._machines[name]
                return {
                    "success": True,
                    "operation": "status",
                    "name": name,
                    "current_state": sm.get_current_state(),
                    "is_final": sm.is_in_final_state(),
                    "history_length": len(sm.get_history())
                }
            
            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}
