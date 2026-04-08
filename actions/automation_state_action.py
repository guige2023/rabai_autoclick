"""Automation state action module for RabAI AutoClick.

Provides automation state management:
- AutomationStateManagerAction: Manage automation state
- StateTransitionAction: Handle state transitions
- StateHistoryAction: Track state history
- StateValidatorAction: Validate state transitions
- StateSnapshotAction: Create state snapshots
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationState(str, Enum):
    """Automation states."""
    IDLE = "idle"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AutomationStateManagerAction(BaseAction):
    """Manage automation state."""
    action_type = "automation_state_manager"
    display_name = "自动化状态管理"
    description = "管理自动化状态"

    def __init__(self):
        super().__init__()
        self._state_store = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")
            automation_id = params.get("automation_id", "default")
            new_state = params.get("new_state", None)

            if operation == "get":
                if automation_id not in self._state_store:
                    return ActionResult(
                        success=True,
                        data={
                            "automation_id": automation_id,
                            "state": AutomationState.IDLE.value,
                            "exists": False
                        },
                        message=f"Automation '{automation_id}' not found, default state: idle"
                    )
                state_data = self._state_store[automation_id]
                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "state": state_data["current_state"],
                        "previous_state": state_data.get("previous_state"),
                        "updated_at": state_data["updated_at"],
                        "exists": True
                    },
                    message=f"Automation '{automation_id}' state: {state_data['current_state']}"
                )

            elif operation == "set":
                if new_state is None:
                    return ActionResult(success=False, message="new_state is required for set operation")

                if automation_id not in self._state_store:
                    self._state_store[automation_id] = {
                        "history": [],
                        "created_at": datetime.now().isoformat()
                    }

                current = self._state_store[automation_id].get("current_state", AutomationState.IDLE.value)
                self._state_store[automation_id]["previous_state"] = current
                self._state_store[automation_id]["current_state"] = new_state
                self._state_store[automation_id]["updated_at"] = datetime.now().isoformat()
                self._state_store[automation_id]["history"].append({
                    "from": current,
                    "to": new_state,
                    "at": datetime.now().isoformat()
                })

                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "previous_state": current,
                        "new_state": new_state,
                        "transition_count": len(self._state_store[automation_id]["history"])
                    },
                    message=f"Automation '{automation_id}' state: {current} -> {new_state}"
                )

            elif operation == "reset":
                if automation_id in self._state_store:
                    self._state_store[automation_id]["current_state"] = AutomationState.IDLE.value
                    self._state_store[automation_id]["previous_state"] = None
                return ActionResult(
                    success=True,
                    data={"automation_id": automation_id, "reset": True},
                    message=f"Automation '{automation_id}' reset to idle"
                )

            elif operation == "list":
                all_states = {
                    aid: {"state": data["current_state"], "updated_at": data["updated_at"]}
                    for aid, data in self._state_store.items()
                }
                return ActionResult(
                    success=True,
                    data={
                        "automations": all_states,
                        "count": len(all_states)
                    },
                    message=f"State manager: {len(all_states)} automations tracked"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation state manager error: {str(e)}")


class StateTransitionAction(BaseAction):
    """Handle state transitions."""
    action_type = "state_transition"
    display_name = "状态转换"
    description = "处理状态转换"

    def __init__(self):
        super().__init__()
        self._transition_rules = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "transition")
            from_state = params.get("from_state", "")
            to_state = params.get("to_state", "")
            automation_id = params.get("automation_id", "default")
            transition_data = params.get("transition_data", {})

            if operation == "transition":
                allowed = self._is_transition_allowed(from_state, to_state)

                if not allowed:
                    return ActionResult(
                        success=False,
                        data={
                            "from_state": from_state,
                            "to_state": to_state,
                            "allowed": False
                        },
                        message=f"Transition {from_state} -> {to_state} not allowed"
                    )

                return ActionResult(
                    success=True,
                    data={
                        "from_state": from_state,
                        "to_state": to_state,
                        "allowed": True,
                        "transitioned_at": datetime.now().isoformat(),
                        "transition_data": transition_data
                    },
                    message=f"Transition {from_state} -> {to_state} completed"
                )

            elif operation == "add_rule":
                self._transition_rules[(from_state, to_state)] = True
                return ActionResult(
                    success=True,
                    data={
                        "from_state": from_state,
                        "to_state": to_state,
                        "rule_added": True
                    },
                    message=f"Transition rule added: {from_state} -> {to_state}"
                )

            elif operation == "remove_rule":
                if (from_state, to_state) in self._transition_rules:
                    del self._transition_rules[(from_state, to_state)]
                return ActionResult(
                    success=True,
                    data={
                        "from_state": from_state,
                        "to_state": to_state,
                        "rule_removed": True
                    },
                    message=f"Transition rule removed: {from_state} -> {to_state}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"State transition error: {str(e)}")

    def _is_transition_allowed(self, from_state: str, to_state: str) -> bool:
        if (from_state, to_state) in self._transition_rules:
            return self._transition_rules[(from_state, to_state)]

        allowed_transitions = {
            AutomationState.IDLE.value: [AutomationState.INITIALIZING.value, AutomationState.RUNNING.value],
            AutomationState.INITIALIZING.value: [AutomationState.RUNNING.value, AutomationState.FAILED.value],
            AutomationState.RUNNING.value: [AutomationState.PAUSED.value, AutomationState.WAITING.value, AutomationState.COMPLETED.value, AutomationState.FAILED.value],
            AutomationState.PAUSED.value: [AutomationState.RUNNING.value, AutomationState.CANCELLED.value],
            AutomationState.WAITING.value: [AutomationState.RUNNING.value, AutomationState.FAILED.value],
            AutomationState.COMPLETED.value: [AutomationState.IDLE.value],
            AutomationState.FAILED.value: [AutomationState.IDLE.value, AutomationState.RUNNING.value],
            AutomationState.CANCELLED.value: [AutomationState.IDLE.value]
        }

        return to_state in allowed_transitions.get(from_state, [])


class StateHistoryAction(BaseAction):
    """Track state history."""
    action_type = "state_history"
    display_name = "状态历史"
    description = "跟踪状态历史"

    def __init__(self):
        super().__init__()
        self._history = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "record")
            automation_id = params.get("automation_id", "default")
            state = params.get("state", None)
            limit = params.get("limit", 100)

            if operation == "record":
                if state is None:
                    return ActionResult(success=False, message="state is required for record")

                if automation_id not in self._history:
                    self._history[automation_id] = []

                self._history[automation_id].append({
                    "state": state,
                    "timestamp": datetime.now().isoformat()
                })

                if len(self._history[automation_id]) > limit:
                    self._history[automation_id] = self._history[automation_id][-limit:]

                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "recorded_state": state,
                        "history_length": len(self._history[automation_id])
                    },
                    message=f"State '{state}' recorded for '{automation_id}'"
                )

            elif operation == "get":
                if automation_id not in self._history:
                    return ActionResult(
                        success=True,
                        data={"automation_id": automation_id, "history": [], "count": 0},
                        message=f"No history for '{automation_id}'"
                    )

                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "history": self._history[automation_id][-limit:],
                        "count": len(self._history[automation_id])
                    },
                    message=f"Retrieved {len(self._history[automation_id][-limit:])} history entries"
                )

            elif operation == "clear":
                if automation_id in self._history:
                    self._history[automation_id] = []
                return ActionResult(
                    success=True,
                    data={"automation_id": automation_id, "cleared": True},
                    message=f"History cleared for '{automation_id}'"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"State history error: {str(e)}")


class StateValidatorAction(BaseAction):
    """Validate state transitions."""
    action_type = "state_validator"
    display_name = "状态验证"
    description = "验证状态转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            from_state = params.get("from_state", "")
            to_state = params.get("to_state", "")
            automation_id = params.get("automation_id", "default")

            valid_states = [s.value for s in AutomationState]

            errors = []
            warnings = []

            if from_state not in valid_states:
                errors.append(f"Invalid from_state: {from_state}")
            if to_state not in valid_states:
                errors.append(f"Invalid to_state: {to_state}")

            if from_state == to_state and from_state:
                warnings.append("State transition from and to are the same")

            transition_allowed = True
            if not errors:
                transition_allowed = self._validate_transition(from_state, to_state)
                if not transition_allowed:
                    errors.append(f"Transition {from_state} -> {to_state} is not allowed")

            return ActionResult(
                success=len(errors) == 0,
                data={
                    "from_state": from_state,
                    "to_state": to_state,
                    "valid": len(errors) == 0,
                    "transition_allowed": transition_allowed,
                    "errors": errors,
                    "warnings": warnings
                },
                message=f"State validation: {'VALID' if len(errors) == 0 else 'INVALID'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"State validator error: {str(e)}")

    def _validate_transition(self, from_state: str, to_state: str) -> bool:
        return True


class StateSnapshotAction(BaseAction):
    """Create state snapshots."""
    action_type = "state_snapshot"
    display_name = "状态快照"
    description = "创建状态快照"

    def __init__(self):
        super().__init__()
        self._snapshots = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            automation_id = params.get("automation_id", "default")
            snapshot_id = params.get("snapshot_id", None)
            state_data = params.get("state_data", {})

            if operation == "create":
                snapshot_id = snapshot_id or f"snap_{int(time.time() * 1000)}"

                if automation_id not in self._snapshots:
                    self._snapshots[automation_id] = {}

                self._snapshots[automation_id][snapshot_id] = {
                    "id": snapshot_id,
                    "state_data": state_data,
                    "created_at": datetime.now().isoformat()
                }

                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "snapshot_id": snapshot_id,
                        "created_at": self._snapshots[automation_id][snapshot_id]["created_at"]
                    },
                    message=f"Snapshot '{snapshot_id}' created for '{automation_id}'"
                )

            elif operation == "restore":
                if automation_id not in self._snapshots or snapshot_id not in self._snapshots[automation_id]:
                    return ActionResult(success=False, message=f"Snapshot '{snapshot_id}' not found")

                snapshot = self._snapshots[automation_id][snapshot_id]
                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "snapshot_id": snapshot_id,
                        "restored_state": snapshot["state_data"],
                        "restored_at": datetime.now().isoformat()
                    },
                    message=f"Snapshot '{snapshot_id}' restored for '{automation_id}'"
                )

            elif operation == "list":
                if automation_id not in self._snapshots:
                    return ActionResult(
                        success=True,
                        data={"automation_id": automation_id, "snapshots": [], "count": 0},
                        message=f"No snapshots for '{automation_id}'"
                    )

                snapshots = [
                    {"id": sid, "created_at": s["created_at"]}
                    for sid, s in self._snapshots[automation_id].items()
                ]

                return ActionResult(
                    success=True,
                    data={
                        "automation_id": automation_id,
                        "snapshots": snapshots,
                        "count": len(snapshots)
                    },
                    message=f"Found {len(snapshots)} snapshots for '{automation_id}'"
                )

            elif operation == "delete":
                if automation_id in self._snapshots and snapshot_id in self._snapshots[automation_id]:
                    del self._snapshots[automation_id][snapshot_id]
                return ActionResult(
                    success=True,
                    data={"automation_id": automation_id, "snapshot_id": snapshot_id, "deleted": True},
                    message=f"Snapshot '{snapshot_id}' deleted"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"State snapshot error: {str(e)}")
