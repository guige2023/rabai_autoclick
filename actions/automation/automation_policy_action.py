"""Automation policy engine for rule-based workflow control.

Provides policy-based decision making, enforcement, and auditing
for automation workflows.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class PolicyEffect(Enum):
    """Effect of a policy rule."""
    ALLOW = "allow"
    DENY = "deny"
    Audit = "audit"
    LOG = "log"
    THROTTLE = "throttle"
    REJECT = "reject"


class PolicyResource(Enum):
    """Resource types that can be protected by policies."""
    DATA = "data"
    API = "api"
    FILE = "file"
    NETWORK = "network"
    COMPUTE = "compute"
    STORAGE = "storage"
    ACTION = "action"
    WORKFLOW = "workflow"


@dataclass
class PolicyCondition:
    """A condition that must be met for policy to apply."""
    field: str
    operator: str
    value: Any
    combiner: Optional[str] = None


@dataclass
class PolicyRule:
    """A single policy rule."""
    rule_id: str
    name: str
    description: str
    resource: PolicyResource
    effect: PolicyEffect
    priority: int
    conditions: List[PolicyCondition] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    enabled: bool = True
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyEvaluation:
    """Result of evaluating policies against a request."""
    request_id: str
    resource: str
    effect: PolicyEffect
    matched_rules: List[str] = field(default_factory=list)
    reason: str
    evaluated_at: float = field(default_factory=time.time)
    allowed: bool = True
    actions_triggered: List[str] = field(default_factory=list)


@dataclass
class PolicyAuditEntry:
    """Audit entry for policy evaluations."""
    entry_id: str
    request_id: str
    resource: str
    effect: PolicyEffect
    allowed: bool
    matched_rules: List[str]
    reason: str
    created_at: float = field(default_factory=time.time)
    request_context: Dict[str, Any] = field(default_factory=dict)


class PolicyConditionEvaluator:
    """Evaluates policy conditions against request context."""

    OPERATORS = {
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "gte": lambda a, b: a >= b,
        "lt": lambda a, b: a < b,
        "lte": lambda a, b: a <= b,
        "in": lambda a, b: a in b,
        "not_in": lambda a, b: a not in b,
        "contains": lambda a, b: b in a,
        "starts_with": lambda a, b: str(a).startswith(str(b)),
        "ends_with": lambda a, b: str(a).endswith(str(b)),
        "regex": lambda a, b: __import__("re").search(b, str(a)) is not None,
        "exists": lambda a, _: a is not None,
        "not_exists": lambda a, _: a is None,
        "is_empty": lambda a, _: not a,
        "is_not_empty": lambda a, _: bool(a),
    }

    @classmethod
    def evaluate(cls, condition: PolicyCondition, context: Dict[str, Any]) -> bool:
        """Evaluate a single condition against context."""
        field_value = cls._get_nested_field(context, condition.field)
        operator_fn = cls.OPERATORS.get(condition.operator)

        if not operator_fn:
            return False

        try:
            return operator_fn(field_value, condition.value)
        except Exception:
            return False

    @classmethod
    def _get_nested_field(cls, data: Dict[str, Any], field_path: str) -> Any:
        """Get nested field from dict using dot notation."""
        parts = field_path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None

            if current is None:
                return None

        return current


class PolicyEngine:
    """Core policy evaluation engine."""

    def __init__(self):
        self._rules: Dict[str, PolicyRule] = {}
        self._audit_log: List[PolicyAuditEntry] = []
        self._lock = threading.RLock()
        self._max_audit_entries = 10000

    def add_rule(
        self,
        name: str,
        description: str,
        resource: PolicyResource,
        effect: PolicyEffect,
        priority: int = 50,
        conditions: Optional[List[PolicyCondition]] = None,
        actions: Optional[List[str]] = None,
        enabled: bool = True,
        tags: Optional[Set[str]] = None,
        expires_at: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Add a policy rule."""
        rule_id = str(uuid.uuid4())[:12]

        rule = PolicyRule(
            rule_id=rule_id,
            name=name,
            description=description,
            resource=resource,
            effect=effect,
            priority=priority,
            conditions=conditions or [],
            actions=actions or [],
            enabled=enabled,
            tags=tags or set(),
            expires_at=expires_at,
            metadata=metadata or {},
        )

        with self._lock:
            self._rules[rule_id] = rule

        return rule_id

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a policy rule."""
        with self._lock:
            if rule_id in self._rules:
                del self._rules[rule_id]
                return True
            return False

    def enable_rule(self, rule_id: str) -> bool:
        """Enable a policy rule."""
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule:
                rule.enabled = True
                rule.updated_at = time.time()
                return True
            return False

    def disable_rule(self, rule_id: str) -> bool:
        """Disable a policy rule."""
        with self._lock:
            rule = self._rules.get(rule_id)
            if rule:
                rule.enabled = False
                rule.updated_at = time.time()
                return True
            return False

    def evaluate(
        self,
        resource: str,
        context: Dict[str, Any],
        request_id: Optional[str] = None,
    ) -> PolicyEvaluation:
        """Evaluate policies against a resource request."""
        request_id = request_id or str(uuid.uuid4())[:12]

        resource_enum = resource
        if isinstance(resource, str):
            try:
                resource_enum = PolicyResource(resource.lower())
            except ValueError:
                resource_enum = PolicyResource.ACTION

        with self._lock:
            applicable_rules = [
                r for r in self._rules.values()
                if r.enabled and r.resource == resource_enum
            ]

            applicable_rules.sort(key=lambda r: r.priority, reverse=True)

        matched_rules = []
        triggered_actions = []
        final_effect = PolicyEffect.ALLOW

        for rule in applicable_rules:
            if rule.expires_at and rule.expires_at < time.time():
                continue

            all_conditions_met = True
            for condition in rule.conditions:
                if not PolicyConditionEvaluator.evaluate(condition, context):
                    all_conditions_met = False
                    break

            if all_conditions_met or not rule.conditions:
                matched_rules.append(rule.rule_id)
                triggered_actions.extend(rule.actions)

                if rule.effect == PolicyEffect.DENY:
                    final_effect = PolicyEffect.DENY
                    break
                elif rule.effect == PolicyEffect.REJECT:
                    final_effect = PolicyEffect.REJECT
                    break
                elif rule.effect == PolicyEffect.AUDIT:
                    final_effect = PolicyEffect.AUDIT

        allowed = final_effect in (PolicyEffect.ALLOW, PolicyEffect.AUDIT, PolicyEffect.LOG)

        reason = "Allowed by default"
        if final_effect == PolicyEffect.DENY:
            reason = "Denied by matching rule"
        elif final_effect == PolicyEffect.REJECT:
            reason = "Rejected by matching rule"
        elif not matched_rules:
            reason = "No applicable rules found"

        evaluation = PolicyEvaluation(
            request_id=request_id,
            resource=resource,
            effect=final_effect,
            matched_rules=matched_rules,
            reason=reason,
            allowed=allowed,
            actions_triggered=triggered_actions,
        )

        audit_entry = PolicyAuditEntry(
            entry_id=str(uuid.uuid4())[:12],
            request_id=request_id,
            resource=resource,
            effect=final_effect,
            allowed=allowed,
            matched_rules=matched_rules,
            reason=reason,
            request_context=copy.deepcopy(context),
        )

        with self._lock:
            self._audit_log.append(audit_entry)
            if len(self._audit_log) > self._max_audit_entries:
                self._audit_log = self._audit_log[-self._max_audit_entries:]

        return evaluation

    def get_audit_log(
        self,
        resource: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        with self._lock:
            entries = list(reversed(self._audit_log))

            if resource:
                entries = [e for e in entries if e.resource == resource]

            return [
                {
                    "entry_id": e.entry_id,
                    "request_id": e.request_id,
                    "resource": e.resource,
                    "effect": e.effect.value,
                    "allowed": e.allowed,
                    "matched_rules": e.matched_rules,
                    "reason": e.reason,
                    "created_at": datetime.fromtimestamp(e.created_at).isoformat(),
                }
                for e in entries[:limit]
            ]

    def get_rules(self, resource: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all policy rules."""
        with self._lock:
            rules = list(self._rules.values())

            if resource:
                try:
                    resource_enum = PolicyResource(resource.lower())
                    rules = [r for r in rules if r.resource == resource_enum]
                except ValueError:
                    pass

            return [
                {
                    "rule_id": r.rule_id,
                    "name": r.name,
                    "description": r.description,
                    "resource": r.resource.value,
                    "effect": r.effect.value,
                    "priority": r.priority,
                    "enabled": r.enabled,
                    "condition_count": len(r.conditions),
                    "tags": list(r.tags),
                }
                for r in rules
            ]


class AutomationPolicyAction:
    """Action providing policy-based control for automation workflows."""

    def __init__(self, engine: Optional[PolicyEngine] = None):
        self._engine = engine or PolicyEngine()

    def add_rule(
        self,
        name: str,
        description: str,
        resource: str,
        effect: str,
        priority: int = 50,
        conditions: Optional[List[Dict[str, Any]]] = None,
        actions: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Add a policy rule."""
        try:
            resource_enum = PolicyResource(resource.lower())
        except ValueError:
            resource_enum = PolicyResource.ACTION

        try:
            effect_enum = PolicyEffect(effect.lower())
        except ValueError:
            effect_enum = PolicyEffect.ALLOW

        condition_objs = []
        if conditions:
            for c in conditions:
                condition_objs.append(PolicyCondition(
                    field=c.get("field", ""),
                    operator=c.get("operator", "eq"),
                    value=c.get("value"),
                ))

        rule_id = self._engine.add_rule(
            name=name,
            description=description,
            resource=resource_enum,
            effect=effect_enum,
            priority=priority,
            conditions=condition_objs,
            actions=actions,
            tags=set(tags) if tags else None,
        )

        return {"rule_id": rule_id, "name": name}

    def evaluate(
        self,
        resource: str,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Evaluate policies against a resource."""
        evaluation = self._engine.evaluate(resource, context)

        return {
            "request_id": evaluation.request_id,
            "resource": evaluation.resource,
            "effect": evaluation.effect.value,
            "allowed": evaluation.allowed,
            "matched_rules": evaluation.matched_rules,
            "reason": evaluation.reason,
            "actions_triggered": evaluation.actions_triggered,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a policy operation.

        Required params:
            operation: str - 'add_rule', 'evaluate', 'enable', 'disable', 'get_rules', 'get_audit'
            resource: str - For evaluate operation
            context: dict - For evaluate operation
        """
        operation = params.get("operation")

        if operation == "add_rule":
            return self.add_rule(
                name=params.get("name"),
                description=params.get("description", ""),
                resource=params.get("resource", "action"),
                effect=params.get("effect", "allow"),
                priority=params.get("priority", 50),
                conditions=params.get("conditions"),
                actions=params.get("actions"),
                tags=params.get("tags"),
            )

        elif operation == "evaluate":
            resource = params.get("resource")
            eval_context = params.get("context", {})

            if not resource:
                raise ValueError("resource is required")

            return self.evaluate(resource, eval_context)

        elif operation == "enable":
            rule_id = params.get("rule_id")
            if not rule_id:
                raise ValueError("rule_id is required")
            success = self._engine.enable_rule(rule_id)
            return {"success": success}

        elif operation == "disable":
            rule_id = params.get("rule_id")
            if not rule_id:
                raise ValueError("rule_id is required")
            success = self._engine.disable_rule(rule_id)
            return {"success": success}

        elif operation == "get_rules":
            return {
                "rules": self._engine.get_rules(params.get("resource")),
            }

        elif operation == "get_audit":
            return {
                "audit_log": self._engine.get_audit_log(
                    resource=params.get("resource"),
                    limit=params.get("limit", 100),
                ),
            }

        else:
            raise ValueError(f"Unknown operation: {operation}")
