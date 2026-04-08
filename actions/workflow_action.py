"""
Workflow and state machine utilities - FSM, DAG execution, step pipeline, branching.
"""
from typing import Any, Dict, List, Optional, Callable, Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class StateMachine:
    """Finite State Machine."""

    def __init__(self, initial: str) -> None:
        self.initial = initial
        self.current = initial
        self.states: Set[str] = {initial}
        self.transitions: Dict[str, Dict[str, str]] = {}
        self.final_states: Set[str] = set()

    def add_state(self, name: str, final: bool = False) -> None:
        self.states.add(name)
        if final:
            self.final_states.add(name)

    def add_transition(self, from_state: str, event: str, to_state: str, guard: Optional[Callable] = None) -> None:
        if from_state not in self.transitions:
            self.transitions[from_state] = {}
        self.transitions[from_state][event] = to_state

    def trigger(self, event: str, context: Optional[Dict[str, Any]] = None) -> bool:
        if self.current not in self.transitions:
            return False
        if event not in self.transitions[self.current]:
            return False
        self.current = self.transitions[self.current][event]
        return True

    def is_final(self) -> bool:
        return self.current in self.final_states

    def reset(self) -> None:
        self.current = self.initial

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current": self.current,
            "states": list(self.states),
            "transitions": self.transitions,
            "final_states": list(self.final_states),
        }


class DAG:
    """Directed Acyclic Graph for workflow execution."""

    def __init__(self) -> None:
        self.graph: Dict[str, List[str]] = {}
        self.in_degree: Dict[str, int] = {}

    def add_node(self, node: str) -> None:
        if node not in self.graph:
            self.graph[node] = []
            self.in_degree[node] = 0

    def add_edge(self, from_node: str, to_node: str) -> None:
        self.add_node(from_node)
        self.add_node(to_node)
        self.graph[from_node].append(to_node)
        self.in_degree[to_node] += 1

    def topological_sort(self) -> Optional[List[str]]:
        queue = [n for n in self.graph if self.in_degree[n] == 0]
        result = []
        while queue:
            node = queue.pop(0)
            result.append(node)
            for neighbor in self.graph[node]:
                self.in_degree[neighbor] -= 1
                if self.in_degree[neighbor] == 0:
                    queue.append(neighbor)
        if len(result) != len(self.graph):
            return None
        return result

    def has_cycle(self) -> bool:
        return self.topological_sort() is None

    def get_execution_order(self) -> Optional[List[str]]:
        return self.topological_sort()

    def dependencies_of(self, node: str) -> List[str]:
        deps = []
        for n, neighbors in self.graph.items():
            if node in neighbors:
                deps.append(n)
        return deps


class WorkflowAction(BaseAction):
    """Workflow and state machine operations.

    Provides FSM creation, DAG execution, step pipeline, conditional branching.
    """

    def __init__(self) -> None:
        self._machines: Dict[str, StateMachine] = {}
        self._dags: Dict[str, DAG] = {}

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "fsm_create")
        name = params.get("name", "default")

        try:
            if operation == "fsm_create":
                initial = params.get("initial", "start")
                machine = StateMachine(initial)
                states = params.get("states", [])
                finals = params.get("final_states", [])
                for s in states:
                    machine.add_state(s, final=s in finals)
                transitions = params.get("transitions", [])
                for t in transitions:
                    if len(t) >= 3:
                        machine.add_transition(str(t[0]), str(t[1]), str(t[2]))
                self._machines[name] = machine
                return {"success": True, "name": name, "fsm": machine.to_dict()}

            elif operation == "fsm_trigger":
                if name not in self._machines:
                    return {"success": False, "error": f"FSM {name} not found"}
                event = params.get("event", "")
                success = self._machines[name].trigger(event)
                return {"success": True, "name": name, "event": event, "current": self._machines[name].current, "triggered": success}

            elif operation == "fsm_state":
                if name not in self._machines:
                    return {"success": False, "error": f"FSM {name} not found"}
                return {"success": True, "name": name, "current": self._machines[name].current, "is_final": self._machines[name].is_final()}

            elif operation == "fsm_reset":
                if name not in self._machines:
                    return {"success": False, "error": f"FSM {name} not found"}
                self._machines[name].reset()
                return {"success": True, "name": name, "current": self._machines[name].current}

            elif operation == "fsm_list":
                return {"success": True, "machines": list(self._machines.keys()), "count": len(self._machines)}

            elif operation == "dag_create":
                dag = DAG()
                nodes = params.get("nodes", [])
                edges = params.get("edges", [])
                for n in nodes:
                    dag.add_node(str(n))
                for e in edges:
                    if isinstance(e, (list, tuple)) and len(e) >= 2:
                        dag.add_edge(str(e[0]), str(e[1]))
                self._dags[name] = dag
                return {"success": True, "name": name, "has_cycle": dag.has_cycle(), "nodes": list(dag.graph.keys())}

            elif operation == "dag_order":
                if name not in self._dags:
                    return {"success": False, "error": f"DAG {name} not found"}
                dag = self._dags[name]
                order = dag.get_execution_order()
                if order is None:
                    return {"success": False, "error": "DAG has cycle - cannot order"}
                return {"success": True, "name": name, "order": order}

            elif operation == "dag_dependencies":
                if name not in self._dags:
                    return {"success": False, "error": f"DAG {name} not found"}
                node = params.get("node", "")
                deps = self._dags[name].dependencies_of(node)
                return {"success": True, "name": name, "node": node, "dependencies": deps}

            elif operation == "dag_add_node":
                if name not in self._dags:
                    self._dags[name] = DAG()
                self._dags[name].add_node(name)
                return {"success": True, "name": name, "node": name}

            elif operation == "dag_add_edge":
                if name not in self._dags:
                    self._dags[name] = DAG()
                from_node = params.get("from", "")
                to_node = params.get("to", "")
                self._dags[name].add_edge(from_node, to_node)
                return {"success": True, "name": name, "from": from_node, "to": to_node}

            elif operation == "dag_validate":
                if name not in self._dags:
                    return {"success": False, "error": f"DAG {name} not found"}
                has_cycle = self._dags[name].has_cycle()
                return {"success": True, "name": name, "valid": not has_cycle, "has_cycle": has_cycle}

            elif operation == "step_execute":
                steps = params.get("steps", [])
                results = []
                stop_on_error = params.get("stop_on_error", True)
                for i, step in enumerate(steps):
                    step_name = step.get("name", f"step_{i}")
                    step_type = step.get("type", "pass")
                    result = {"name": step_name, "type": step_type, "success": True}
                    if step_type == "transform":
                        input_val = step.get("input")
                        fn = step.get("fn")
                        if fn == "upper":
                            result["output"] = str(input_val).upper()
                        elif fn == "lower":
                            result["output"] = str(input_val).lower()
                        elif fn == "reverse":
                            result["output"] = str(input_val)[::-1]
                        else:
                            result["output"] = input_val
                    elif step_type == "filter":
                        input_data = step.get("data", [])
                        condition = step.get("condition", "")
                        result["output"] = input_data
                        result["filtered_count"] = len(input_data)
                    elif step_type == "branch":
                        condition = step.get("condition", "")
                        branches = step.get("branches", [])
                        result["branches_executed"] = []
                        for branch in branches:
                            result["branches_executed"].append(branch.get("name", "unnamed"))
                    else:
                        result["output"] = step.get("input")
                    results.append(result)
                    if stop_on_error and not result["success"]:
                        break
                return {"success": True, "results": results, "count": len(results), "all_success": all(r["success"] for r in results)}

            elif operation == "pipeline":
                input_val = params.get("input")
                steps = params.get("steps", [])
                current = input_val
                for step in steps:
                    fn = step.get("fn", "pass")
                    if fn == "upper":
                        current = str(current).upper()
                    elif fn == "lower":
                        current = str(current).lower()
                    elif fn == "strip":
                        current = str(current).strip()
                    elif fn == "reverse":
                        current = str(current)[::-1]
                    elif fn == "len":
                        current = len(str(current))
                    elif fn == "json_parse":
                        import json
                        current = json.loads(current)
                    elif fn == "json_dump":
                        import json
                        current = json.dumps(current)
                    else:
                        pass
                return {"success": True, "input": input_val, "output": current, "steps_applied": len(steps)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"WorkflowAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for workflow operations."""
    return WorkflowAction().execute(context, params)
