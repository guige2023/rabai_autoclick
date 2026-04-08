"""Automation state action module for RabAI AutoClick.

Provides state management for automation:
- StateManagerAction: Manage workflow state
- StateTransitionAction: Handle state transitions
- StateHistoryAction: Track state history
- StateValidatorAction: Validate state transitions
- StateSnapshotAction: Create/restore state snapshots
"""

import time
import copy
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class StateManagerAction(BaseAction):
    """Manage workflow state."""
    action_type = "automation_state_manager"
    display_name = "状态管理器"
    description = "管理工作流状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "get")
            state_key = params.get("state_key", "default")
            initial_state = params.get("initial_state", {})

            if not hasattr(context, "_automation_states"):
                context._automation_states = {}

            if state_key not in context._automation_states:
                context._automation_states[state_key] = {
                    "state": copy.deepcopy(initial_state),
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "version": 1
                }

            state_store = context._automation_states[state_key]

            if action == "get":
                return ActionResult(
                    success=True,
                    data={
                        "state": state_store["state"],
                        "state_key": state_key,
                        "version": state_store["version"],
                        "updated_at": state_store["updated_at"]
                    },
                    message=f"Retrieved state for '{state_key}': version {state_store['version']}"
                )

            elif action == "set":
                new_state = params.get("state", {})
                merge = params.get("merge", False)

                if merge:
                    state_store["state"].update(new_state)
                else:
                    state_store["state"] = copy.deepcopy(new_state)

                state_store["updated_at"] = datetime.now().isoformat()
                state_store["version"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "state": state_store["state"],
                        "state_key": state_key,
                        "version": state_store["version"],
                        "merged": merge
                    },
                    message=f"Set state for '{state_key}': version {state_store['version']}"
                )

            elif action == "update":
                updates = params.get("updates", {})
                state_store["state"].update(updates)
                state_store["updated_at"] = datetime.now().isoformat()
                state_store["version"] += 1

                return ActionResult(
                    success=True,
                    data={
                        "state": state_store["state"],
                        "state_key": state_key,
                        "version": state_store["version"],
                        "updates": updates
                    },
                    message=f"Updated state for '{state_key}': version {state_store['version']}"
                )

            elif action == "delete":
                del context._automation_states[state_key]
                return ActionResult(
                    success=True,
                    data={"deleted": state_key},
                    message=f"Deleted state for '{state_key}'"
                )

            elif action == "list":
                keys = list(context._automation_states.keys())
                return ActionResult(
                    success=True,
                    data={
                        "states": keys,
                        "count": len(keys)
                    },
                    message=f"Found {len(keys)} states"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"State manager error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"state_key": "default", "initial_state": {}, "state": None, "merge": False, "updates": {}}


class StateTransitionAction(BaseAction):
    """Handle state transitions."""
    action_type = "automation_state_transition"
    display_name = "状态转换"
    description = "处理状态转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            current_state = params.get("current_state", "idle")
            transition_map = params.get("transition_map", {})
            event = params.get("event", "")
            guard = params.get("guard")

            if not event:
                return ActionResult(success=False, message="Event is required")

            transitions = transition_map.get(current_state, {})

            if event not in transitions:
                return ActionResult(
                    success=False,
                    data={
                        "current_state": current_state,
                        "event": event,
                        "available_transitions": list(transitions.keys()),
                        "transitioned": False
                    },
                    message=f"No transition for event '{event}' from state '{current_state}'"
                )

            next_state = transitions[event]

            if guard and not guard.get("allowed", True):
                return ActionResult(
                    success=False,
                    data={
                        "current_state": current_state,
                        "event": event,
                        "next_state": next_state,
                        "transitioned": False,
                        "blocked_by_guard": True
                    },
                    message=f"Transition blocked by guard"
                )

            return ActionResult(
                success=True,
                data={
                    "from_state": current_state,
                    "to_state": next_state,
                    "event": event,
                    "transitioned": True
                },
                message=f"Transitioned: {current_state} -> {next_state} (via {event})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"State transition error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["current_state", "event"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"transition_map": {}, "guard": None}


class StateHistoryAction(BaseAction):
    """Track state history."""
    action_type = "automation_state_history"
    display_name = "状态历史"
    description = "跟踪状态历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "record")
            state_key = params.get("state_key", "default")
            state = params.get("state")
            max_history = params.get("max_history", 100)

            if not hasattr(context, "_state_history"):
                context._state_history = {}

            if state_key not in context._state_history:
                context._state_history[state_key] = []

            history = context._state_history[state_key]

            if action == "record":
                if state is None:
                    return ActionResult(success=False, message="state is required for record action")

                entry = {
                    "state": copy.deepcopy(state),
                    "timestamp": datetime.now().isoformat(),
                    "recorded_at": time.time()
                }
                history.append(entry)

                if len(history) > max_history:
                    history = history[-max_history:]
                    context._state_history[state_key] = history

                return ActionResult(
                    success=True,
                    data={
                        "recorded": entry,
                        "history_length": len(history),
                        "state_key": state_key
                    },
                    message=f"Recorded state, history length: {len(history)}"
                )

            elif action == "get":
                limit = params.get("limit", 10)
                recent = history[-limit:] if limit > 0 else history

                return ActionResult(
                    success=True,
                    data={
                        "history": recent,
                        "total_count": len(history),
                        "state_key": state_key
                    },
                    message=f"Retrieved {len(recent)} history entries"
                )

            elif action == "clear":
                context._state_history[state_key] = []
                return ActionResult(
                    success=True,
                    data={"cleared": True, "state_key": state_key},
                    message=f"Cleared history for '{state_key}'"
                )

            elif action == "search":
                search_state = params.get("search_state")
                results = []
                for entry in reversed(history):
                    if search_state and entry.get("state") == search_state:
                        results.append(entry)

                return ActionResult(
                    success=True,
                    data={
                        "results": results,
                        "count": len(results),
                        "state_key": state_key
                    },
                    message=f"Found {len(results)} matching entries"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"State history error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"state_key": "default", "state": None, "max_history": 100, "limit": 10, "search_state": None}


class StateValidatorAction(BaseAction):
    """Validate state transitions."""
    action_type = "automation_state_validator"
    display_name = "状态验证器"
    description = "验证状态转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            from_state = params.get("from_state", "")
            to_state = params.get("to_state", "")
            allowed_transitions = params.get("allowed_transitions", {})
            required_state_fields = params.get("required_state_fields", [])

            errors = []
            warnings = []

            if allowed_transitions:
                allowed_from = allowed_transitions.get(from_state, [])
                if to_state not in allowed_from:
                    errors.append(f"Transition from '{from_state}' to '{to_state}' is not allowed")

            if required_state_fields:
                current_state = params.get("current_state", {})
                for field in required_state_fields:
                    if field not in current_state:
                        errors.append(f"Required state field missing: '{field}'")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "from_state": from_state,
                    "to_state": to_state
                },
                message=f"State transition validation: {'valid' if is_valid else f'invalid ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"State validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["from_state", "to_state"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"allowed_transitions": {}, "required_state_fields": [], "current_state": {}}


class StateSnapshotAction(BaseAction):
    """Create/restore state snapshots."""
    action_type = "automation_state_snapshot"
    display_name = "状态快照"
    description = "创建和恢复状态快照"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "create")
            snapshot_key = params.get("snapshot_key", "")
            state_key = params.get("state_key", "default")
            state = params.get("state")

            if not hasattr(context, "_state_snapshots"):
                context._state_snapshots = {}

            if snapshot_key:
                snapshot_id = snapshot_key
            else:
                snapshot_id = f"snapshot_{int(time.time())}"

            if action == "create":
                if state is None:
                    return ActionResult(success=False, message="state is required for create action")

                snapshot = {
                    "snapshot_id": snapshot_id,
                    "state": copy.deepcopy(state),
                    "created_at": datetime.now().isoformat(),
                    "version": state.get("version", 1) if isinstance(state, dict) else 1
                }

                context._state_snapshots[snapshot_id] = snapshot

                return ActionResult(
                    success=True,
                    data={
                        "snapshot": snapshot,
                        "snapshot_id": snapshot_id,
                        "snapshot_count": len(context._state_snapshots)
                    },
                    message=f"Created snapshot '{snapshot_id}'"
                )

            elif action == "restore":
                if snapshot_id not in context._state_snapshots:
                    return ActionResult(success=False, message=f"Snapshot '{snapshot_id}' not found")

                snapshot = context._state_snapshots[snapshot_id]

                return ActionResult(
                    success=True,
                    data={
                        "restored_state": snapshot["state"],
                        "snapshot_id": snapshot_id,
                        "snapshot": snapshot
                    },
                    message=f"Restored snapshot '{snapshot_id}'"
                )

            elif action == "list":
                snapshots = [
                    {"id": k, "created_at": v["created_at"]}
                    for k, v in context._state_snapshots.items()
                ]

                return ActionResult(
                    success=True,
                    data={
                        "snapshots": snapshots,
                        "count": len(snapshots)
                    },
                    message=f"Found {len(snapshots)} snapshots"
                )

            elif action == "delete":
                if snapshot_id in context._state_snapshots:
                    del context._state_snapshots[snapshot_id]

                return ActionResult(
                    success=True,
                    data={"deleted": snapshot_id},
                    message=f"Deleted snapshot '{snapshot_id}'"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"State snapshot error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"snapshot_key": "", "state_key": "default", "state": None}
