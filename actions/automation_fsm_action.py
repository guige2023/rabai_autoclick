"""Automation FSM action module for RabAI AutoClick.

Provides finite state machine operations:
- FSMCreateAction: Create FSM
- FSMTransitionAction: Perform state transition
- FSMStateAction: Get current state
- FSMValidateAction: Validate transition
- FSMHistoryAction: Get state history
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FSMCreateAction(BaseAction):
    """Create a finite state machine."""
    action_type = "fsm_create"
    display_name = "创建FSM"
    description = "创建有限状态机"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            states = params.get("states", [])
            initial_state = params.get("initial_state", "")
            transitions = params.get("transitions", [])

            if not name or not states:
                return ActionResult(success=False, message="name and states are required")

            fsm_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "fsms"):
                context.fsms = {}
            context.fsms[fsm_id] = {
                "fsm_id": fsm_id,
                "name": name,
                "states": states,
                "initial_state": initial_state,
                "current_state": initial_state,
                "transitions": transitions,
                "history": [{"state": initial_state, "timestamp": time.time()}],
                "created_at": time.time(),
            }

            return ActionResult(
                success=True,
                data={"fsm_id": fsm_id, "name": name, "initial_state": initial_state},
                message=f"FSM {fsm_id} created: {name} in state {initial_state}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FSM create failed: {e}")


class FSMTransitionAction(BaseAction):
    """Perform FSM state transition."""
    action_type = "fsm_transition"
    display_name = "FSM转换"
    description = "执行状态机转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fsm_id = params.get("fsm_id", "")
            event = params.get("event", "")

            if not fsm_id or not event:
                return ActionResult(success=False, message="fsm_id and event are required")

            fsms = getattr(context, "fsms", {})
            if fsm_id not in fsms:
                return ActionResult(success=False, message=f"FSM {fsm_id} not found")

            fsm = fsms[fsm_id]
            from_state = fsm["current_state"]

            next_state = fsm["states"][(fsm["states"].index(from_state) + 1) % len(fsm["states"])] if fsm["states"] else from_state
            fsm["current_state"] = next_state
            fsm["history"].append({"state": next_state, "event": event, "timestamp": time.time()})

            return ActionResult(
                success=True,
                data={"fsm_id": fsm_id, "from_state": from_state, "to_state": next_state, "event": event},
                message=f"FSM {fsm_id}: {from_state} -> {next_state} on event '{event}'",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FSM transition failed: {e}")


class FSMStateAction(BaseAction):
    """Get current FSM state."""
    action_type = "fsm_state"
    display_name = "FSM状态"
    description = "获取状态机当前状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fsm_id = params.get("fsm_id", "")
            if not fsm_id:
                return ActionResult(success=False, message="fsm_id is required")

            fsms = getattr(context, "fsms", {})
            if fsm_id not in fsms:
                return ActionResult(success=False, message=f"FSM {fsm_id} not found")

            fsm = fsms[fsm_id]
            return ActionResult(
                success=True,
                data={"fsm_id": fsm_id, "name": fsm["name"], "current_state": fsm["current_state"], "states": fsm["states"]},
                message=f"FSM {fsm_id} is in state: {fsm['current_state']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FSM state failed: {e}")


class FSMValidateAction(BaseAction):
    """Validate FSM transition."""
    action_type = "fsm_validate"
    display_name = "FSM验证"
    description = "验证状态转换合法性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fsm_id = params.get("fsm_id", "")
            from_state = params.get("from_state", "")
            to_state = params.get("to_state", "")

            if not fsm_id:
                return ActionResult(success=False, message="fsm_id is required")

            fsms = getattr(context, "fsms", {})
            if fsm_id not in fsms:
                return ActionResult(success=False, message=f"FSM {fsm_id} not found")

            fsm = fsms[fsm_id]
            valid_from = from_state in fsm["states"]
            valid_to = to_state in fsm["states"]
            valid = valid_from and valid_to

            return ActionResult(
                success=True,
                data={"fsm_id": fsm_id, "valid": valid, "from_state": from_state, "to_state": to_state},
                message=f"Transition {from_state} -> {to_state}: {'valid' if valid else 'invalid'}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FSM validate failed: {e}")


class FSMHistoryAction(BaseAction):
    """Get FSM state history."""
    action_type = "fsm_history"
    display_name = "FSM历史"
    description = "获取状态机历史"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fsm_id = params.get("fsm_id", "")
            limit = params.get("limit", 50)

            if not fsm_id:
                return ActionResult(success=False, message="fsm_id is required")

            fsms = getattr(context, "fsms", {})
            if fsm_id not in fsms:
                return ActionResult(success=False, message=f"FSM {fsm_id} not found")

            fsm = fsms[fsm_id]
            history = fsm["history"][-limit:]

            return ActionResult(
                success=True,
                data={"fsm_id": fsm_id, "history": history, "transition_count": len(fsm["history"])},
                message=f"FSM {fsm_id}: {len(history)} recent transitions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FSM history failed: {e}")
