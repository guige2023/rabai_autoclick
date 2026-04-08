"""Automation Agent Action.

Wraps an autonomous agent that can decide and execute automation steps.
"""
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time


class AgentState(Enum):
    IDLE = "idle"
    THINKING = "thinking"
    ACTING = "acting"
    WAITING = "waiting"
    DONE = "done"
    ERROR = "error"


@dataclass
class AgentThought:
    step: int
    thought: str
    action: Optional[str] = None
    observation: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class AutomationAgent:
    name: str
    role: str
    goal: str
    backstory: str = ""
    state: AgentState = AgentState.IDLE
    thoughts: List[AgentThought] = field(default_factory=list)

    def think(self, thought: str) -> None:
        self.state = AgentState.THINKING
        step = len(self.thoughts) + 1
        self.thoughts.append(AgentThought(step=step, thought=thought))

    def act(self, action: str, observation: str = "") -> None:
        self.state = AgentState.ACTING
        if self.thoughts:
            self.thoughts[-1].action = action
            self.thoughts[-1].observation = observation


class AutomationAgentAction:
    """An autonomous agent that plans and executes automation tasks."""

    def __init__(
        self,
        agents: Optional[List[AutomationAgent]] = None,
        max_steps: int = 50,
    ) -> None:
        self.agents = agents or []
        self.max_steps = max_steps
        self.execution_log: List[Dict[str, Any]] = []

    def add_agent(self, agent: AutomationAgent) -> None:
        self.agents.append(agent)

    def execute_task(
        self,
        task: str,
        agent_name: Optional[str] = None,
        planning_fn: Optional[Callable[[str, AutomationAgent], List[str]]] = None,
    ) -> Dict[str, Any]:
        agent = next((a for a in self.agents if a.name == agent_name), self.agents[0] if self.agents else None)
        if not agent:
            return {"status": "error", "message": "No agent available"}
        steps_executed = 0
        plan = planning_fn(task, agent) if planning_fn else [task]
        for step in plan:
            if steps_executed >= self.max_steps:
                break
            agent.think(f"Planning step: {step}")
            agent.act(step, f"Executed: {step}")
            self.execution_log.append({
                "step": steps_executed,
                "agent": agent.name,
                "action": step,
                "timestamp": time.time(),
            })
            steps_executed += 1
        return {
            "status": "done",
            "steps_executed": steps_executed,
            "agent": agent.name,
            "log": self.execution_log,
        }

    def get_agent_status(self, name: str) -> Optional[Dict[str, Any]]:
        agent = next((a for a in self.agents if a.name == name), None)
        if not agent:
            return None
        return {
            "name": agent.name,
            "state": agent.state.value,
            "thoughts_count": len(agent.thoughts),
            "role": agent.role,
        }
