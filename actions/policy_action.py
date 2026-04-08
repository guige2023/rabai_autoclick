"""Policy Action Module.

Provides policy-based access control and enforcement
for automation workflows.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PolicyEffect(Enum):
    """Policy effect type."""
    ALLOW = "allow"
    DENY = "deny"


class PolicyType(Enum):
    """Policy type."""
    ACCESS = "access"
    RATE = "rate"
    QUOTA = "quota"
    TIME = "time"


@dataclass
class PolicyCondition:
    """Condition for policy evaluation."""
    field: str
    operator: str
    value: Any


@dataclass
class Policy:
    """Access policy definition."""
    policy_id: str
    name: str
    policy_type: PolicyType
    effect: PolicyEffect
    resources: List[str]
    actions: List[str]
    conditions: List[PolicyCondition] = field(default_factory=list)
    priority: int = 0
    enabled: bool = True


@dataclass
class PolicyEvaluation:
    """Result of policy evaluation."""
    policy_id: str
    resource: str
    action: str
    effect: PolicyEffect
    matched: bool


class PolicyEngine:
    """Evaluates policies for access control."""

    def __init__(self):
        self._policies: Dict[str, Policy] = {}
        self._audit_log: List[Dict[str, Any]] = []

    def create_policy(
        self,
        name: str,
        policy_type: PolicyType,
        effect: PolicyEffect,
        resources: List[str],
        actions: List[str],
        conditions: Optional[List[Dict]] = None,
        priority: int = 0
    ) -> str:
        """Create a new policy."""
        policy_id = hashlib.md5(
            f"{name}{time.time()}".encode()
        ).hexdigest()[:8]

        policy_conditions = []
        if conditions:
            for c in conditions:
                policy_conditions.append(PolicyCondition(
                    field=c.get("field", ""),
                    operator=c.get("operator", "eq"),
                    value=c.get("value")
                ))

        policy = Policy(
            policy_id=policy_id,
            name=name,
            policy_type=policy_type,
            effect=effect,
            resources=resources,
            actions=actions,
            conditions=policy_conditions,
            priority=priority
        )

        self._policies[policy_id] = policy
        return policy_id

    def evaluate(
        self,
        resource: str,
        action: str,
        context: Optional[Dict[str, Any]] = None
    ) -> tuple[bool, List[PolicyEvaluation]]:
        """Evaluate policies for a resource/action."""
        context = context or {}
        evaluations = []
        matched_policies = []

        for policy in self._policies.values():
            if not policy.enabled:
                continue

            if not self._matches_resource(policy, resource):
                continue

            if not self._matches_action(policy, action):
                continue

            if not self._evaluate_conditions(policy, context):
                continue

            evaluations.append(PolicyEvaluation(
                policy_id=policy.policy_id,
                resource=resource,
                action=action,
                effect=policy.effect,
                matched=True
            ))
            matched_policies.append(policy)

        matched_policies.sort(key=lambda p: p.priority, reverse=True)

        if matched_policies:
            final_effect = matched_policies[0].effect
            return final_effect == PolicyEffect.ALLOW, evaluations

        return True, evaluations

    def _matches_resource(self, policy: Policy, resource: str) -> bool:
        """Check if resource matches policy."""
        for pattern in policy.resources:
            if pattern == "*" or pattern == resource:
                return True
            if pattern.endswith("*"):
                prefix = pattern[:-1]
                if resource.startswith(prefix):
                    return True
        return False

    def _matches_action(self, policy: Policy, action: str) -> bool:
        """Check if action matches policy."""
        return action in policy.actions or "*" in policy.actions

    def _evaluate_conditions(
        self,
        policy: Policy,
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate policy conditions."""
        if not policy.conditions:
            return True

        for condition in policy.conditions:
            if not self._evaluate_single_condition(condition, context):
                return False
        return True

    def _evaluate_single_condition(
        self,
        condition: PolicyCondition,
        context: Dict[str, Any]
    ) -> bool:
        """Evaluate a single condition."""
        value = context.get(condition.field)

        if condition.operator == "eq":
            return value == condition.value
        elif condition.operator == "ne":
            return value != condition.value
        elif condition.operator == "gt":
            return value is not None and value > condition.value
        elif condition.operator == "lt":
            return value is not None and value < condition.value
        elif condition.operator == "in":
            return value in condition.value
        elif condition.operator == "contains":
            return condition.value in str(value)

        return True

    def get_policy(self, policy_id: str) -> Optional[Policy]:
        """Get policy by ID."""
        return self._policies.get(policy_id)

    def delete_policy(self, policy_id: str) -> bool:
        """Delete a policy."""
        if policy_id in self._policies:
            del self._policies[policy_id]
            return True
        return False

    def enable_policy(self, policy_id: str, enabled: bool = True) -> bool:
        """Enable or disable a policy."""
        policy = self._policies.get(policy_id)
        if policy:
            policy.enabled = enabled
            return True
        return False

    def get_audit_log(self, limit: int = 100) -> List[Dict]:
        """Get audit log."""
        return self._audit_log[-limit:]


class PolicyAction(BaseAction):
    """Action for policy operations."""

    def __init__(self):
        super().__init__("policy")
        self._engine = PolicyEngine()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute policy action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_policy(params)
            elif operation == "evaluate":
                return self._evaluate(params)
            elif operation == "delete":
                return self._delete_policy(params)
            elif operation == "enable":
                return self._enable(params)
            elif operation == "get":
                return self._get_policy(params)
            elif operation == "audit":
                return self._get_audit(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_policy(self, params: Dict) -> ActionResult:
        """Create a policy."""
        policy_id = self._engine.create_policy(
            name=params.get("name", ""),
            policy_type=PolicyType(params.get("type", "access")),
            effect=PolicyEffect(params.get("effect", "allow")),
            resources=params.get("resources", ["*"]),
            actions=params.get("actions", ["*"]),
            conditions=params.get("conditions"),
            priority=params.get("priority", 0)
        )
        return ActionResult(success=True, data={"policy_id": policy_id})

    def _evaluate(self, params: Dict) -> ActionResult:
        """Evaluate policies."""
        allowed, evals = self._engine.evaluate(
            params.get("resource", ""),
            params.get("action", ""),
            params.get("context")
        )
        return ActionResult(
            success=True,
            data={
                "allowed": allowed,
                "evaluations": [
                    {"policy_id": e.policy_id, "effect": e.effect.value}
                    for e in evals
                ]
            }
        )

    def _delete_policy(self, params: Dict) -> ActionResult:
        """Delete a policy."""
        success = self._engine.delete_policy(params.get("policy_id", ""))
        return ActionResult(success=success)

    def _enable(self, params: Dict) -> ActionResult:
        """Enable/disable a policy."""
        success = self._engine.enable_policy(
            params.get("policy_id", ""),
            params.get("enabled", True)
        )
        return ActionResult(success=success)

    def _get_policy(self, params: Dict) -> ActionResult:
        """Get policy details."""
        policy = self._engine.get_policy(params.get("policy_id", ""))
        if not policy:
            return ActionResult(success=False, message="Not found")
        return ActionResult(success=True, data={
            "policy_id": policy.policy_id,
            "name": policy.name,
            "type": policy.policy_type.value,
            "effect": policy.effect.value
        })

    def _get_audit(self, params: Dict) -> ActionResult:
        """Get audit log."""
        log = self._engine.get_audit_log(params.get("limit", 100))
        return ActionResult(success=True, data={"log": log})
