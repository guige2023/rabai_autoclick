"""
Workflow State Machine Action.

Provides state machine implementation for workflow management.
Supports:
- Configurable states and transitions
- Guard conditions for transitions
- Entry/exit actions
- State history and auditing
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import threading
import logging
import json

logger = logging.getLogger(__name__)


class TransitionType(Enum):
    """Type of state transition."""
    EXTERNAL = "external"
    INTERNAL = "internal"
    LOCAL = "local"


@dataclass
class Transition:
    """Represents a state transition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable[["StateMachineContext"], bool]] = None
    action: Optional[Callable[["StateMachineContext"], None]] = None
    transition_type: TransitionType = TransitionType.EXTERNAL
    
    def can_execute(self, context: "StateMachineContext") -> bool:
        """Check if transition can execute."""
        if self.guard is None:
            return True
        return self.guard(context)


@dataclass
class State:
    """Represents a state in the state machine."""
    name: str
    entry_action: Optional[Callable[["StateMachineContext"], None]] = None
    exit_action: Optional[Callable[["StateMachineContext"], None]] = None
    do_action: Optional[Callable[["StateMachineContext"], None]] = None
    is_initial: bool = False
    is_final: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StateMachineContext:
    """Context for state machine execution."""
    state: str
    event: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TransitionRecord:
    """Record of a state transition."""
    from_state: str
    to_state: str
    event: str
    timestamp: datetime
    guard_passed: bool
    action_executed: bool
    error: Optional[str] = None


class WorkflowStateMachineAction:
    """
    Workflow State Machine Action.
    
    Implements a state machine for workflow management with support for:
    - Configurable states with entry/exit actions
    - Event-driven transitions with guard conditions
    - State history tracking
    - Thread-safe execution
    """
    
    def __init__(self, name: str, initial_state: Optional[str] = None):
        """
        Initialize the State Machine Action.
        
        Args:
            name: Name of the state machine
            initial_state: Initial state (first registered state if not provided)
        """
        self.name = name
        self.states: Dict[str, State] = {}
        self.transitions: Dict[str, List[Transition]] = {}  # event -> transitions
        self.current_state: Optional[str] = initial_state
        self.context = StateMachineContext(state=initial_state or "")
        self.history: List[TransitionRecord] = []
        self.listeners: List[Callable[[str, str, str], None]] = []
        self._lock = threading.RLock()
        self._is_running = False
    
    def add_state(
        self,
        name: str,
        entry_action: Optional[Callable[[StateMachineContext], None]] = None,
        exit_action: Optional[Callable[[StateMachineContext], None]] = None,
        do_action: Optional[Callable[[StateMachineContext], None]] = None,
        is_initial: bool = False,
        is_final: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> "WorkflowStateMachineAction":
        """
        Add a state to the state machine.
        
        Args:
            name: State name
            entry_action: Action to execute on state entry
            exit_action: Action to execute on state exit
            do_action: Action to execute while in state
            is_initial: Whether this is an initial state
            is_final: Whether this is a final state
            metadata: Optional metadata
        
        Returns:
            Self for chaining
        """
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
        
        if is_initial and self.current_state is None:
            self.current_state = name
            self.context.state = name
        
        return self
    
    def add_transition(
        self,
        event: str,
        from_state: str,
        to_state: str,
        guard: Optional[Callable[[StateMachineContext], bool]] = None,
        action: Optional[Callable[[StateMachineContext], None]] = None,
        transition_type: TransitionType = TransitionType.EXTERNAL
    ) -> "WorkflowStateMachineAction":
        """
        Add a transition to the state machine.
        
        Args:
            event: Event that triggers the transition
            from_state: Source state
            to_state: Target state
            guard: Optional guard condition
            action: Optional action to execute during transition
            transition_type: Type of transition
        
        Returns:
            Self for chaining
        """
        if from_state not in self.states:
            raise ValueError(f"From state '{from_state}' does not exist")
        if to_state not in self.states:
            raise ValueError(f"To state '{to_state}' does not exist")
        
        transition = Transition(
            from_state=from_state,
            to_state=to_state,
            event=event,
            guard=guard,
            action=action,
            transition_type=transition_type
        )
        
        if event not in self.transitions:
            self.transitions[event] = []
        
        self.transitions[event].append(transition)
        
        return self
    
    def add_listener(
        self,
        listener: Callable[[str, str, str], None]
    ) -> "WorkflowStateMachineAction":
        """Add a state change listener."""
        self.listeners.append(listener)
        return self
    
    def trigger(
        self,
        event: str,
        data: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Trigger an event and attempt to execute a transition.
        
        Args:
            event: Event to trigger
            data: Optional data to include in context
        
        Returns:
            True if transition was executed, False otherwise
        """
        with self._lock:
            if self.current_state is None:
                logger.error("State machine has no current state")
                return False
            
            if self.current_state not in self.transitions:
                logger.debug(f"No transitions for event '{event}' in state '{self.current_state}'")
                return False
            
            self.context.event = event
            self.context.data = data or {}
            self.context.timestamp = datetime.utcnow()
            
            # Find a valid transition
            for transition in self.transitions[self.current_state]:
                if transition.event != event:
                    continue
                
                if not transition.can_execute(self.context):
                    self._record_transition(
                        transition.from_state,
                        transition.to_state,
                        event,
                        guard_passed=False
                    )
                    continue
                
                # Execute transition
                success = self._execute_transition(transition)
                return success
            
            return False
    
    def _execute_transition(self, transition: Transition) -> bool:
        """Execute a state transition."""
        from_state = transition.from_state
        to_state = transition.to_state
        
        # Execute exit action
        if from_state in self.states:
            exit_action = self.states[from_state].exit_action
            if exit_action:
                try:
                    exit_action(self.context)
                except Exception as e:
                    logger.error(f"Exit action failed: {e}")
                    self._record_transition(from_state, to_state, transition.event, True, False, str(e))
                    return False
        
        # Execute transition action
        action_executed = False
        if transition.action:
            try:
                transition.action(self.context)
                action_executed = True
            except Exception as e:
                logger.error(f"Transition action failed: {e}")
                self._record_transition(from_state, to_state, transition.event, True, False, str(e))
                return False
        
        # Update state
        self.current_state = to_state
        self.context.state = to_state
        
        # Execute entry action
        if to_state in self.states:
            entry_action = self.states[to_state].entry_action
            if entry_action:
                try:
                    entry_action(self.context)
                except Exception as e:
                    logger.error(f"Entry action failed: {e}")
        
        # Notify listeners
        for listener in self.listeners:
            try:
                listener(from_state, to_state, transition.event)
            except Exception as e:
                logger.error(f"Listener notification failed: {e}")
        
        self._record_transition(from_state, to_state, transition.event, True, action_executed)
        logger.info(f"State transition: {from_state} -> {to_state} (event: {transition.event})")
        
        return True
    
    def _record_transition(
        self,
        from_state: str,
        to_state: str,
        event: str,
        guard_passed: bool,
        action_executed: bool = False,
        error: Optional[str] = None
    ) -> None:
        """Record a transition in history."""
        record = TransitionRecord(
            from_state=from_state,
            to_state=to_state,
            event=event,
            timestamp=datetime.utcnow(),
            guard_passed=guard_passed,
            action_executed=action_executed,
            error=error
        )
        self.history.append(record)
    
    def is_final_state(self) -> bool:
        """Check if current state is a final state."""
        if self.current_state is None:
            return False
        return self.states[self.current_state].is_final
    
    def get_state(self) -> Optional[str]:
        """Get the current state."""
        return self.current_state
    
    def get_history(self) -> List[TransitionRecord]:
        """Get transition history."""
        return self.history.copy()
    
    def get_allowed_events(self) -> List[str]:
        """Get events that can be triggered from current state."""
        if self.current_state is None:
            return []
        
        allowed = []
        if self.current_state in self.transitions:
            for t in self.transitions[self.current_state]:
                if t.can_execute(self.context):
                    allowed.append(t.event)
        
        return list(set(allowed))
    
    def get_status(self) -> Dict[str, Any]:
        """Get state machine status."""
        return {
            "name": self.name,
            "current_state": self.current_state,
            "is_final": self.is_final_state(),
            "allowed_events": self.get_allowed_events(),
            "history_count": len(self.history)
        }
    
    def reset(self, initial_state: Optional[str] = None) -> None:
        """Reset the state machine to initial state."""
        with self._lock:
            if initial_state:
                self.current_state = initial_state
            elif self.current_state:
                # Find initial state
                for name, state in self.states.items():
                    if state.is_initial:
                        self.current_state = name
                        break
            
            self.context = StateMachineContext(state=self.current_state or "")
            self.history = []
            logger.info(f"State machine '{self.name}' reset to state '{self.current_state}'")


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example: Order processing state machine
    def on_order_received(ctx: StateMachineContext):
        print(f"Order received: {ctx.data}")
    
    def on_order_processing(ctx: StateMachineContext):
        print("Processing order...")
    
    def on_order_shipped(ctx: StateMachineContext):
        print("Order shipped!")
    
    def validate_payment(ctx: StateMachineContext) -> bool:
        amount = ctx.data.get("amount", 0)
        return amount > 0
    
    sm = WorkflowStateMachineAction("order-workflow")
    
    # Add states
    sm.add_state("received", entry_action=on_order_received, is_initial=True)
    sm.add_state("processing", entry_action=on_order_processing)
    sm.add_state("paid")
    sm.add_state("shipped", entry_action=on_order_shipped, is_final=True)
    sm.add_state("cancelled", is_final=True)
    
    # Add transitions
    sm.add_transition("process", "received", "processing")
    sm.add_transition("pay", "processing", "paid", guard=validate_payment)
    sm.add_transition("ship", "paid", "shipped")
    sm.add_transition("cancel", "received", "cancelled")
    sm.add_transition("cancel", "processing", "cancelled")
    
    # Add listener
    sm.add_listener(lambda f, t, e: print(f"Transition: {f} -> {t} ({e})"))
    
    print(f"Initial state: {sm.get_state()}")
    print(f"Allowed events: {sm.get_allowed_events()}")
    
    # Process order
    sm.trigger("process", {"order_id": "123", "amount": 100})
    print(f"After process: {sm.get_state()}")
    
    sm.trigger("pay", {"order_id": "123", "amount": 100})
    print(f"After pay: {sm.get_state()}")
    
    sm.trigger("ship", {"order_id": "123"})
    print(f"After ship: {sm.get_state()}")
    
    print(f"Is final: {sm.is_final_state()}")
    print(f"History: {len(sm.get_history())} transitions")
