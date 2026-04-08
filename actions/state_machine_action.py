"""State Machine Action Module.

Provides state machine implementation for workflow automation
with configurable states, transitions, and guards.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class TransitionType(Enum):
    """Transition type."""
    EXTERNAL = "external"
    INTERNAL = "internal"


@dataclass
class Transition:
    """State transition definition."""
    from_state: str
    to_state: str
    event: str
    guard: Optional[Callable] = None
    action: Optional[Callable] = None
    transition_type: TransitionType = TransitionType.EXTERNAL


@dataclass
class State:
    """State definition."""
    name: str
    entry_action: Optional[Callable] = None
    exit_action: Optional[Callable] = None
    is_initial: bool = False
    is_final: bool = False


@dataclass
class StateMachineContext:
    """State machine execution context."""
    machine_id: str
    current_state: str
    history: List[str]
    data: Dict[str, Any]
    started_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)


@dataclass
class TransitionResult:
    """Transition execution result."""
    success: bool
    from_state: str
    to_state: str
    event: str
    error: Optional[str] = None


class StateMachineStore:
    """In-memory state machine store."""

    def __init__(self):
        self._machines: Dict[str, Dict[str, Any]] = {}
        self._contexts: Dict[str, StateMachineContext] = {}

    def define(self, name: str, states: List[str],
               transitions: List[Dict[str, Any]],
               initial_state: str) -> bool:
        """Define state machine."""
        self._machines[name] = {
            "states": states,
            "transitions": transitions,
            "initial_state": initial_state,
            "current_states": {}
        }
        return True

    def get(self, name: str) -> Optional[Dict[str, Any]]:
        """Get machine definition."""
        return self._machines.get(name)

    def create_context(self, name: str, instance_id: str,
                      initial_state: str, data: Optional[Dict[str, Any]] = None) -> StateMachineContext:
        """Create machine context."""
        ctx = StateMachineContext(
            machine_id=f"{name}:{instance_id}",
            current_state=initial_state,
            history=[initial_state],
            data=data or {}
        )
        self._contexts[ctx.machine_id] = ctx
        return ctx

    def get_context(self, machine_id: str) -> Optional[StateMachineContext]:
        """Get machine context."""
        return self._contexts.get(machine_id)


_global_store = StateMachineStore()


class StateMachineAction:
    """State machine action.

    Example:
        action = StateMachineAction()

        action.define("order", ["pending", "paid", "shipped", "delivered"],
                     [("pending", "paid", "pay"), ("paid", "shipped", "ship")],
                     "pending")

        action.create_instance("order", "order-123")
        action.trigger("order:order-123", "pay")
    """

    def __init__(self, store: Optional[StateMachineStore] = None):
        self._store = store or _global_store

    def define(self, name: str, states: List[str],
              transitions: List[Dict[str, Any]],
              initial_state: str) -> Dict[str, Any]:
        """Define state machine."""
        trans_list = []
        for t in transitions:
            trans_list.append({
                "from_state": t.get("from_state"),
                "to_state": t.get("to_state"),
                "event": t.get("event")
            })

        if self._store.define(name, states, trans_list, initial_state):
            return {
                "success": True,
                "name": name,
                "states": states,
                "initial_state": initial_state,
                "message": f"Defined state machine: {name}"
            }
        return {"success": False, "message": "Failed to define"}

    def create_instance(self, name: str, instance_id: str,
                       initial_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create state machine instance."""
        machine = self._store.get(name)
        if not machine:
            return {"success": False, "message": "Machine not found"}

        ctx = self._store.create_context(
            name, instance_id,
            machine["initial_state"],
            initial_data
        )

        return {
            "success": True,
            "instance_id": ctx.machine_id,
            "current_state": ctx.current_state,
            "message": f"Created instance: {ctx.machine_id}"
        }

    def trigger(self, instance_id: str, event: str) -> Dict[str, Any]:
        """Trigger event on instance."""
        ctx = self._store.get_context(instance_id)
        if not ctx:
            return {"success": False, "message": "Instance not found"}

        machine_name = instance_id.split(":")[0]
        machine = self._store.get(machine_name)
        if not machine:
            return {"success": False, "message": "Machine not found"}

        for trans in machine["transitions"]:
            if (trans["from_state"] == ctx.current_state and
                trans["event"] == event):
                old_state = ctx.current_state
                ctx.current_state = trans["to_state"]
                ctx.history.append(trans["to_state"])
                ctx.last_updated = time.time()

                return {
                    "success": True,
                    "instance_id": instance_id,
                    "from_state": old_state,
                    "to_state": trans["to_state"],
                    "event": event,
                    "message": f"Transitioned {old_state} -> {trans['to_state']}"
                }

        return {
            "success": False,
            "instance_id": instance_id,
            "current_state": ctx.current_state,
            "message": f"No transition for event '{event}' from state '{ctx.current_state}'"
        }

    def get_state(self, instance_id: str) -> Dict[str, Any]:
        """Get current state of instance."""
        ctx = self._store.get_context(instance_id)
        if not ctx:
            return {"success": False, "message": "Instance not found"}

        return {
            "success": True,
            "instance_id": instance_id,
            "current_state": ctx.current_state,
            "history": ctx.history,
            "data": ctx.data,
            "started_at": ctx.started_at,
            "last_updated": ctx.last_updated
        }

    def set_data(self, instance_id: str, key: str, value: Any) -> Dict[str, Any]:
        """Set data on instance."""
        ctx = self._store.get_context(instance_id)
        if not ctx:
            return {"success": False, "message": "Instance not found"}

        ctx.data[key] = value
        ctx.last_updated = time.time()

        return {
            "success": True,
            "instance_id": instance_id,
            "key": key,
            "message": f"Set {key} on {instance_id}"
        }

    def get_data(self, instance_id: str) -> Dict[str, Any]:
        """Get all data from instance."""
        ctx = self._store.get_context(instance_id)
        if not ctx:
            return {"success": False, "message": "Instance not found"}

        return {
            "success": True,
            "instance_id": instance_id,
            "data": ctx.data
        }

    def list_instances(self, name: str) -> Dict[str, Any]:
        """List all instances of machine."""
        instances = [
            ctx for ctx in self._store._contexts.values()
            if ctx.machine_id.startswith(name + ":")
        ]

        return {
            "success": True,
            "name": name,
            "instances": [
                {
                    "instance_id": ctx.machine_id,
                    "current_state": ctx.current_state,
                    "history": ctx.history
                }
                for ctx in instances
            ],
            "count": len(instances)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute state machine action."""
    operation = params.get("operation", "")
    action = StateMachineAction()

    try:
        if operation == "define":
            name = params.get("name", "")
            states = params.get("states", [])
            transitions = params.get("transitions", [])
            initial_state = params.get("initial_state", "")
            if not name or not states or not initial_state:
                return {"success": False, "message": "name, states, and initial_state required"}
            return action.define(name, states, transitions, initial_state)

        elif operation == "create":
            name = params.get("name", "")
            instance_id = params.get("instance_id", "")
            if not name or not instance_id:
                return {"success": False, "message": "name and instance_id required"}
            return action.create_instance(
                name=name,
                instance_id=instance_id,
                initial_data=params.get("initial_data")
            )

        elif operation == "trigger":
            instance_id = params.get("instance_id", "")
            event = params.get("event", "")
            if not instance_id or not event:
                return {"success": False, "message": "instance_id and event required"}
            return action.trigger(instance_id, event)

        elif operation == "get_state":
            instance_id = params.get("instance_id", "")
            if not instance_id:
                return {"success": False, "message": "instance_id required"}
            return action.get_state(instance_id)

        elif operation == "set_data":
            instance_id = params.get("instance_id", "")
            key = params.get("key", "")
            value = params.get("value")
            if not instance_id or not key:
                return {"success": False, "message": "instance_id and key required"}
            return action.set_data(instance_id, key, value)

        elif operation == "get_data":
            instance_id = params.get("instance_id", "")
            if not instance_id:
                return {"success": False, "message": "instance_id required"}
            return action.get_data(instance_id)

        elif operation == "list":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.list_instances(name)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"State machine error: {str(e)}"}
