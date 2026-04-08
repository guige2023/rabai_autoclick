"""Automation Policy Action Module. Defines and enforces automation policies."""
import sys, os, time, threading
from typing import Any
from dataclasses import dataclass, field
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult

@dataclass
class PolicyRule:
    name: str; action_type: str; max_per_minute: int; max_per_hour: int
    allowed: bool; conditions: dict = field(default_factory=dict)

class AutomationPolicyAction(BaseAction):
    action_type = "automation_policy"; display_name = "自动化策略"
    description = "定义和执行自动化策略"
    def __init__(self) -> None:
        super().__init__(); self._lock = threading.Lock()
        self._policies = {}; self._action_log = {}
    def add_policy(self, rule: PolicyRule) -> None:
        with self._lock: self._policies[rule.name] = rule
    def execute(self, context: Any, params: dict) -> ActionResult:
        mode = params.get("mode", "check")
        if mode == "add_policy":
            rule = PolicyRule(name=params.get("name","default"), action_type=params.get("action_type","any"),
                             max_per_minute=params.get("max_per_minute",60),
                             max_per_hour=params.get("max_per_hour",1000),
                             allowed=params.get("allowed",True),
                             conditions=params.get("conditions",{}))
            self.add_policy(rule)
            return ActionResult(success=True, message=f"Policy '{rule.name}' added")
        action_type = params.get("action_type",""); policy_name = params.get("policy_name")
        with self._lock:
            if policy_name and policy_name in self._policies: policy = self._policies[policy_name]
            elif action_type:
                policy = self._policies.get(action_type)
                if not policy:
                    policy = PolicyRule(name=action_type, action_type=action_type,
                                        max_per_minute=100, max_per_hour=5000, allowed=True)
                    self._policies[action_type] = policy
            else: return ActionResult(success=False, message="No policy_name or action_type")
        now = time.time(); key = f"{policy.name}:{action_type or policy.action_type}"
        self._action_log.setdefault(key, [])
        cutoff_min = now - 60; cutoff_hour = now - 3600
        recent = [t for t in self._action_log[key] if t > cutoff_min]
        hourly = [t for t in self._action_log[key] if t > cutoff_hour]
        self._action_log[key] = recent
        minute_ok = len(recent) < policy.max_per_minute
        hour_ok = len(hourly) < policy.max_per_hour
        allowed = policy.allowed and minute_ok and hour_ok
        if mode == "check":
            return ActionResult(success=allowed, message=f"Policy: {len(recent)}/min, {len(hourly)}/hr",
                              data={"allowed": allowed, "policy": vars(policy)})
        if not allowed:
            return ActionResult(success=False, message=f"Policy '{policy.name}' DENIED")
        self._action_log[key].append(now)
        return ActionResult(success=True, message=f"Policy '{policy.name}' ENFORCED")
