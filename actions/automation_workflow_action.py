"""Automation workflow action module for RabAI AutoClick.

Provides workflow orchestration operations:
- WorkflowEngineAction: Engine for running automation workflows
- StateMachineAction: State machine workflow execution
- WorkflowSchedulerAction: Schedule and trigger workflows
- WorkflowAuditAction: Audit and log workflow execution
"""

import time
import json
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WorkflowState:
    """Maintains workflow execution state."""

    def __init__(self):
        self._lock = Lock()
        self._states: Dict[str, Dict] = {}
        self._history: List[Dict] = []

    def set_state(self, workflow_id: str, state: str, context: Dict):
        with self._lock:
            self._states[workflow_id] = {"state": state, "context": context, "updated_at": datetime.now().isoformat()}
            self._history.append({"workflow_id": workflow_id, "state": state, "timestamp": datetime.now().isoformat()})

    def get_state(self, workflow_id: str) -> Optional[Dict]:
        with self._lock:
            return self._states.get(workflow_id)

    def get_history(self, workflow_id: str) -> List[Dict]:
        with self._lock:
            return [h for h in self._history if h["workflow_id"] == workflow_id]


_workflow_state = WorkflowState()


class WorkflowEngineAction(BaseAction):
    """Engine for running automation workflows."""
    action_type = "workflow_engine"
    display_name = "工作流引擎"
    description = "运行自动化工作流的引擎"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow_def = params.get("workflow", {})
            initial_context = params.get("initial_context", {})
            workflow_id = params.get("workflow_id", f"wf_{int(time.time())}")

            if not workflow_def:
                return ActionResult(success=False, message="workflow is required")

            nodes = workflow_def.get("nodes", [])
            edges = workflow_def.get("edges", [])
            start_node = workflow_def.get("start", None)

            node_map = {n.get("id"): n for n in nodes}
            adjacency: Dict[str, List[str]] = {n.get("id"): [] for n in nodes}
            for edge in edges:
                adjacency[edge.get("from")].append(edge.get("to"))

            if not start_node:
                return ActionResult(success=False, message="start node not defined")

            execution_log = []
            current_node = start_node
            visited = set()
            max_iterations = params.get("max_iterations", 1000)
            iteration = 0

            while current_node and iteration < max_iterations:
                if current_node in visited:
                    return ActionResult(success=False, message=f"Cycle detected at node {current_node}")
                visited.add(current_node)
                iteration += 1

                node = node_map.get(current_node)
                if not node:
                    return ActionResult(success=False, message=f"Node {current_node} not found")

                node_type = node.get("type", "task")
                node_config = node.get("config", {})

                _workflow_state.set_state(workflow_id, current_node, initial_context)
                execution_log.append({
                    "node": current_node,
                    "type": node_type,
                    "timestamp": datetime.now().isoformat(),
                })

                if node_type == "task":
                    result = self._execute_task(node_config, initial_context)
                    if not result.get("success", True):
                        error_handler = node.get("error_handler")
                        if error_handler:
                            current_node = error_handler
                            continue
                        return ActionResult(success=False, message=f"Task {current_node} failed: {result.get('error')}")
                    initial_context.update(result.get("data", {}))

                elif node_type == "condition":
                    condition_met = self._evaluate_condition(node_config, initial_context)
                    current_node = node_config.get("then") if condition_met else node_config.get("else")
                    continue

                elif node_type == "delay":
                    delay_seconds = node_config.get("seconds", 1)
                    time.sleep(delay_seconds)

                elif node_type == "log":
                    message = node_config.get("message", "")
                    execution_log[-1]["log"] = message

                next_nodes = adjacency.get(current_node, [])
                current_node = next_nodes[0] if next_nodes else None

            return ActionResult(
                success=True,
                message=f"Workflow completed: {len(execution_log)} nodes executed",
                data={
                    "workflow_id": workflow_id,
                    "nodes_executed": len(execution_log),
                    "execution_log": execution_log,
                    "final_state": _workflow_state.get_state(workflow_id),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WorkflowEngine error: {e}")

    def _execute_task(self, config: Dict, ctx: Dict) -> Dict:
        return {"success": True, "data": {}}

    def _evaluate_condition(self, config: Dict, ctx: Dict) -> bool:
        field = config.get("field")
        operator = config.get("operator", "eq")
        value = config.get("value")
        ctx_value = ctx.get(field) if field else None
        if operator == "eq":
            return ctx_value == value
        elif operator == "ne":
            return ctx_value != value
        elif operator == "gt":
            return ctx_value > value
        elif operator == "lt":
            return ctx_value < value
        elif operator == "exists":
            return ctx_value is not None
        return False


class StateMachineAction(BaseAction):
    """State machine workflow execution."""
    action_type = "state_machine"
    display_name = "状态机"
    description = "状态机工作流执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            states = params.get("states", [])
            initial_state = params.get("initial_state", "")
            transitions = params.get("transitions", [])
            input_events = params.get("events", [])

            if not states:
                return ActionResult(success=False, message="states is required")

            state_map = {s.get("id"): s for s in states}
            transition_map: Dict[str, List[Dict]] = {}
            for t in transitions:
                from_state = t.get("from")
                if from_state not in transition_map:
                    transition_map[from_state] = []
                transition_map[from_state].append(t)

            current_state = initial_state
            state_history = [{"state": current_state, "timestamp": datetime.now().isoformat()}]
            event_log = []

            for event in input_events:
                event_name = event.get("name") if isinstance(event, dict) else event
                event_log.append(event_name)

                possible_transitions = transition_map.get(current_state, [])
                matched = None
                for t in possible_transitions:
                    trigger = t.get("trigger", "")
                    condition = t.get("condition")
                    if trigger == event_name:
                        if condition:
                            try:
                                if not eval(condition, {"event": event, "state": current_state}):
                                    continue
                            except Exception:
                                continue
                        matched = t
                        break

                if matched:
                    current_state = matched.get("to")
                    state_history.append({"state": current_state, "timestamp": datetime.now().isoformat(), "event": event_name})

            return ActionResult(
                success=True,
                message=f"State machine: {len(state_history)} states, final: {current_state}",
                data={
                    "current_state": current_state,
                    "state_history": state_history,
                    "event_log": event_log,
                    "state_count": len(state_history),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StateMachine error: {e}")


class WorkflowSchedulerAction(BaseAction):
    """Schedule and trigger workflows."""
    action_type = "workflow_scheduler"
    display_name = "工作流调度器"
    description = "调度和触发工作流"

    def __init__(self):
        super().__init__()
        self._scheduled: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "schedule")
            workflow_id = params.get("workflow_id", "")
            schedule = params.get("schedule", {})
            trigger_now = params.get("trigger_now", False)

            if action == "schedule":
                interval = schedule.get("interval", 60)
                max_runs = schedule.get("max_runs", None)
                workflow_def = schedule.get("workflow")

                self._scheduled[workflow_id] = {
                    "workflow_def": workflow_def,
                    "interval": interval,
                    "max_runs": max_runs,
                    "run_count": 0,
                    "last_run": None,
                    "next_run": datetime.now().isoformat(),
                }

                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id} scheduled every {interval}s",
                    data={"workflow_id": workflow_id, "interval": interval, "max_runs": max_runs},
                )

            elif action == "list":
                scheduled_list = [
                    {"workflow_id": k, "interval": v["interval"], "run_count": v["run_count"], "next_run": v["next_run"]}
                    for k, v in self._scheduled.items()
                ]
                return ActionResult(
                    success=True,
                    message=f"{len(scheduled_list)} scheduled workflows",
                    data={"scheduled": scheduled_list},
                )

            elif action == "cancel":
                if workflow_id in self._scheduled:
                    del self._scheduled[workflow_id]
                return ActionResult(success=True, message=f"Workflow {workflow_id} cancelled")

            elif action == "run":
                wf = self._scheduled.get(workflow_id, {})
                workflow_def = wf.get("workflow_def")
                if not workflow_def:
                    return ActionResult(success=False, message=f"Workflow {workflow_id} not found")

                wf["run_count"] += 1
                wf["last_run"] = datetime.now().isoformat()
                next_run = datetime.now() + timedelta(seconds=wf["interval"])
                wf["next_run"] = next_run.isoformat()

                return ActionResult(
                    success=True,
                    message=f"Workflow {workflow_id} triggered (run #{wf['run_count']})",
                    data={"workflow_id": workflow_id, "run_count": wf["run_count"], "next_run": wf["next_run"]},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"WorkflowScheduler error: {e}")


class WorkflowAuditAction(BaseAction):
    """Audit and log workflow execution."""
    action_type = "workflow_audit"
    display_name = "工作流审计"
    description = "审计和记录工作流执行"

    def __init__(self):
        super().__init__()
        self._audit_log: List[Dict] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "log")
            workflow_id = params.get("workflow_id", "")
            records = params.get("records", [])

            if action == "log":
                event_type = params.get("event_type", "info")
                message = params.get("message", "")
                metadata = params.get("metadata", {})

                entry = {
                    "workflow_id": workflow_id,
                    "event_type": event_type,
                    "message": message,
                    "metadata": metadata,
                    "timestamp": datetime.now().isoformat(),
                }
                self._audit_log.append(entry)
                return ActionResult(success=True, message="Audit entry logged", data={"entry": entry})

            elif action == "query":
                filter_workflow = params.get("workflow_id")
                filter_type = params.get("event_type")
                start_time = params.get("start_time")
                end_time = params.get("end_time")

                results = self._audit_log
                if filter_workflow:
                    results = [r for r in results if r.get("workflow_id") == filter_workflow]
                if filter_type:
                    results = [r for r in results if r.get("event_type") == filter_type]

                return ActionResult(
                    success=True,
                    message=f"Query returned {len(results)} audit entries",
                    data={"entries": results, "count": len(results)},
                )

            elif action == "summary":
                total = len(self._audit_log)
                by_type: Dict[str, int] = {}
                by_workflow: Dict[str, int] = {}
                for entry in self._audit_log:
                    et = entry.get("event_type", "unknown")
                    by_type[et] = by_type.get(et, 0) + 1
                    wid = entry.get("workflow_id", "unknown")
                    by_workflow[wid] = by_workflow.get(wid, 0) + 1

                return ActionResult(
                    success=True,
                    message=f"Audit summary: {total} total entries",
                    data={"total_entries": total, "by_type": by_type, "by_workflow": by_workflow},
                )

            elif action == "clear":
                count = len(self._audit_log)
                self._audit_log = []
                return ActionResult(success=True, message=f"Cleared {count} audit entries")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"WorkflowAudit error: {e}")
