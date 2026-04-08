"""Automation guardrails action module for RabAI AutoClick.

Provides guardrails and safety checks for automation:
- AutomationGuardrails: Safety guardrails for workflows
- GuardRule: Individual guard rule
- SafetyMonitor: Monitor and enforce safety constraints
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GuardType(Enum):
    """Guard types."""
    RATE = "rate"
    QUOTA = "quota"
    TIMEOUT = "timeout"
    CONDITION = "condition"
    APPROVAL = "approval"
    SLI = "sli"


@dataclass
class GuardRule:
    """Guard rule definition."""
    rule_id: str
    name: str
    guard_type: GuardType
    threshold: float
    window: float = 60.0
    enabled: bool = True
    action: str = "block"
    scope: str = "global"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GuardResult:
    """Result of guard evaluation."""
    passed: bool
    rule_id: str
    rule_name: str
    action: str
    message: Optional[str] = None


class RateGuard:
    """Rate-based guard."""
    
    def __init__(self, rule: GuardRule):
        self.rule = rule
        self._counts: Dict[str, int] = defaultdict(int)
        self._timestamps: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.RLock()
    
    def check(self, scope: str) -> GuardResult:
        """Check if rate limit is exceeded."""
        with self._lock:
            now = time.time()
            cutoff = now - self.rule.window
            
            self._timestamps[scope] = [t for t in self._timestamps[scope] if t > cutoff]
            
            if len(self._timestamps[scope]) >= self.rule.threshold:
                return GuardResult(
                    passed=False,
                    rule_id=self.rule.rule_id,
                    rule_name=self.rule.name,
                    action=self.rule.action,
                    message=f"Rate limit exceeded: {len(self._timestamps[scope])} >= {self.rule.threshold}"
                )
            
            self._timestamps[scope].append(now)
            return GuardResult(passed=True, rule_id=self.rule.rule_id, rule_name=self.rule.name, action="allow")
    
    def reset(self, scope: Optional[str] = None):
        """Reset rate counters."""
        with self._lock:
            if scope:
                self._timestamps.pop(scope, None)
            else:
                self._timestamps.clear()


class QuotaGuard:
    """Quota-based guard."""
    
    def __init__(self, rule: GuardRule):
        self.rule = rule
        self._quotas: Dict[str, float] = defaultdict(float)
        self._lock = threading.RLock()
    
    def check(self, scope: str, amount: float = 1.0) -> GuardResult:
        """Check if quota allows operation."""
        with self._lock:
            current = self._quotas.get(scope, 0.0)
            
            if current + amount > self.rule.threshold:
                return GuardResult(
                    passed=False,
                    rule_id=self.rule.rule_id,
                    rule_name=self.rule.name,
                    action=self.rule.action,
                    message=f"Quota exceeded: {current} + {amount} > {self.rule.threshold}"
                )
            
            self._quotas[scope] = current + amount
            return GuardResult(passed=True, rule_id=self.rule.rule_id, rule_name=self.rule.name, action="allow")
    
    def reset(self, scope: Optional[str] = None):
        """Reset quota."""
        with self._lock:
            if scope:
                self._quotas.pop(scope, None)
            else:
                self._quotas.clear()


class AutomationGuardrails:
    """Guardrails enforcement for automation workflows."""
    
    def __init__(self, name: str):
        self.name = name
        self._rules: Dict[str, GuardRule] = {}
        self._guards: Dict[str, Any] = {}
        self._approvals: Dict[str, bool] = {}
        self._lock = threading.RLock()
        self._stats = {"total_checks": 0, "passed_checks": 0, "blocked_checks": 0, "total_violations": 0}
    
    def add_rule(self, rule: GuardRule):
        """Add guard rule."""
        with self._lock:
            self._rules[rule.rule_id] = rule
            
            if rule.guard_type == GuardType.RATE:
                self._guards[rule.rule_id] = RateGuard(rule)
            elif rule.guard_type == GuardType.QUOTA:
                self._guards[rule.rule_id] = QuotaGuard(rule)
    
    def remove_rule(self, rule_id: str):
        """Remove guard rule."""
        with self._lock:
            self._rules.pop(rule_id, None)
            self._guards.pop(rule_id, None)
    
    def check(self, scope: str = "global", amount: float = 1.0) -> Tuple[bool, List[GuardResult]]:
        """Check all guards for scope."""
        with self._lock:
            self._stats["total_checks"] += 1
        
        results = []
        
        with self._lock:
            rules = list(self._rules.values())
        
        for rule in rules:
            if not rule.enabled:
                continue
            
            if rule.scope != "global" and rule.scope != scope:
                continue
            
            if rule.guard_type == GuardType.RATE:
                guard = self._guards.get(rule.rule_id)
                if guard:
                    result = guard.check(scope)
                    results.append(result)
            
            elif rule.guard_type == GuardType.QUOTA:
                guard = self._guards.get(rule.rule_id)
                if guard:
                    result = guard.check(scope, amount)
                    results.append(result)
            
            elif rule.guard_type == GuardType.APPROVAL:
                approved = self._approvals.get(f"{scope}:{rule.rule_id}", False)
                if not approved:
                    results.append(GuardResult(
                        passed=False,
                        rule_id=rule.rule_id,
                        rule_name=rule.name,
                        action=rule.action,
                        message="Approval required"
                    ))
            
            elif rule.guard_type == GuardType.CONDITION:
                if "condition_fn" in rule.metadata:
                    try:
                        cond_fn = rule.metadata["condition_fn"]
                        if not cond_fn():
                            results.append(GuardResult(
                                passed=False,
                                rule_id=rule.rule_id,
                                rule_name=rule.name,
                                action=rule.action,
                                message="Condition not met"
                            ))
                    except Exception as e:
                        results.append(GuardResult(
                            passed=False,
                            rule_id=rule.rule_id,
                            rule_name=rule.name,
                            action=rule.action,
                            message=f"Condition error: {str(e)}"
                        ))
        
        with self._lock:
            blocked = any(not r.passed and r.action == "block" for r in results)
            if blocked:
                self._stats["blocked_checks"] += 1
                self._stats["total_violations"] += 1
            else:
                self._stats["passed_checks"] += 1
        
        return not blocked, results
    
    def approve(self, scope: str, rule_id: str):
        """Approve a guard for scope."""
        with self._lock:
            self._approvals[f"{scope}:{rule_id}"] = True
    
    def revoke_approval(self, scope: str, rule_id: str):
        """Revoke approval."""
        with self._lock:
            self._approvals.pop(f"{scope}:{rule_id}", None)
    
    def reset(self, rule_id: Optional[str] = None, scope: Optional[str] = None):
        """Reset guard state."""
        with self._lock:
            if rule_id and rule_id in self._guards:
                self._guards[rule_id].reset(scope)
            elif scope:
                for guard in self._guards.values():
                    guard.reset(scope)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get guardrails statistics."""
        with self._lock:
            return {
                "name": self.name,
                "rule_count": len(self._rules),
                **{k: v for k, v in self._stats.items()},
            }


class AutomationGuardrailsAction(BaseAction):
    """Automation guardrails action."""
    action_type = "automation_guardrails"
    display_name = "自动化护栏"
    description = "自动化安全护栏控制"
    
    def __init__(self):
        super().__init__()
        self._guardrails: Dict[str, AutomationGuardrails] = {}
        self._lock = threading.Lock()
    
    def _get_guardrails(self, name: str) -> AutomationGuardrails:
        """Get or create guardrails."""
        with self._lock:
            if name not in self._guardrails:
                self._guardrails[name] = AutomationGuardrails(name)
            return self._guardrails[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute guardrails operation."""
        try:
            name = params.get("name", "default")
            command = params.get("command", "check")
            
            guardrails = self._get_guardrails(name)
            
            if command == "add_rule":
                rule = GuardRule(
                    rule_id=params.get("rule_id"),
                    name=params.get("rule_name"),
                    guard_type=GuardType[params.get("guard_type", "rate").upper()],
                    threshold=params.get("threshold", 10.0),
                    window=params.get("window", 60.0),
                    action=params.get("action", "block"),
                    scope=params.get("scope", "global"),
                )
                guardrails.add_rule(rule)
                return ActionResult(success=True, message=f"Rule {rule.rule_id} added")
            
            elif command == "check":
                scope = params.get("scope", "global")
                amount = params.get("amount", 1.0)
                passed, results = guardrails.check(scope, amount)
                blocked = [r for r in results if not r.passed]
                return ActionResult(
                    success=passed,
                    message="Check passed" if passed else f"Blocked by {len(blocked)} guard(s)",
                    data={"passed": passed, "results": [{"rule": r.rule_name, "passed": r.passed, "message": r.message} for r in results]}
                )
            
            elif command == "approve":
                scope = params.get("scope", "global")
                rule_id = params.get("rule_id")
                guardrails.approve(scope, rule_id)
                return ActionResult(success=True)
            
            elif command == "reset":
                guardrails.reset()
                return ActionResult(success=True)
            
            elif command == "stats":
                stats = guardrails.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationGuardrailsAction error: {str(e)}")
