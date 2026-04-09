"""
Policy Engine Action Module

Provides policy-based access control and rule evaluation for UI automation
workflows. Supports allow/deny rules, conditions, and policy composition.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PolicyEffect(Enum):
    """Policy effect types."""
    ALLOW = auto()
    DENY = auto()


class PolicyType(Enum):
    """Policy type enumeration."""
    ALLOW_DENY = auto()
    MANDATORY = auto()
    ROLE_BASED = auto()


class ConditionOp(Enum):
    """Condition operators."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IN = "in"
    NOT_IN = "not_in"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    MATCHES = "matches"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"


@dataclass
class PolicyCondition:
    """Single policy condition."""
    field: str
    operator: ConditionOp
    value: Any = None

    def evaluate(self, context: dict[str, Any]) -> bool:
        """Evaluate condition against context."""
        field_value = self._get_field_value(context, self.field)

        if self.operator == ConditionOp.EXISTS:
            return field_value is not None
        if self.operator == ConditionOp.NOT_EXISTS:
            return field_value is None

        if field_value is None:
            return False

        return self._compare(field_value, self.operator, self.value)

    def _get_field_value(self, context: dict[str, Any], field_path: str) -> Any:
        """Get nested field value from context."""
        parts = field_path.split(".")
        value = context
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
            if value is None:
                return None
        return value

    def _compare(self, field_value: Any, operator: ConditionOp, target: Any) -> bool:
        """Compare values using operator."""
        if operator == ConditionOp.EQUALS:
            return field_value == target
        if operator == ConditionOp.NOT_EQUALS:
            return field_value != target
        if operator == ConditionOp.CONTAINS:
            return target in field_value if field_value else False
        if operator == ConditionOp.NOT_CONTAINS:
            return target not in field_value if field_value else True
        if operator == ConditionOp.IN:
            return field_value in target if isinstance(target, (list, tuple)) else False
        if operator == ConditionOp.NOT_IN:
            return field_value not in target if isinstance(target, (list, tuple)) else True
        if operator == ConditionOp.GREATER_THAN:
            return field_value > target if isinstance(field_value, (int, float)) else False
        if operator == ConditionOp.LESS_THAN:
            return field_value < target if isinstance(field_value, (int, float)) else False
        if operator == ConditionOp.MATCHES:
            try:
                return bool(re.match(target, str(field_value)))
            except Exception:
                return False
        return False


@dataclass
class PolicyStatement:
    """Policy statement with effect and conditions."""
    effect: PolicyEffect
    actions: list[str]
    resources: list[str]
    conditions: list[PolicyCondition] = field(default_factory=list)
    description: str = ""


@dataclass
class Policy:
    """Access control policy."""
    id: str
    name: str
    policy_type: PolicyType = PolicyType.ALLOW_DENY
    statements: list[PolicyStatement] = field(default_factory=list)
    priority: int = 0
    enabled: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    updated_at: float = field(default_factory=lambda: datetime.utcnow().timestamp())


@dataclass
class PolicyRequest:
    """Request for policy evaluation."""
    principal: dict[str, Any] = field(default_factory=dict)
    action: str
    resource: str
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.context.setdefault("principal", self.principal)
        self.context.setdefault("action", self.action)
        self.context.setdefault("resource", self.resource)


@dataclass
class PolicyDecision:
    """Policy evaluation decision."""
    allowed: bool
    policy_id: Optional[str] = None
    reason: str = ""
    obligations: dict[str, Any] = field(default_factory=dict)


class PolicyStore:
    """In-memory policy storage."""

    def __init__(self) -> None:
        self._policies: dict[str, Policy] = {}

    def add(self, policy: Policy) -> None:
        """Add policy to store."""
        self._policies[policy.id] = policy
        logger.debug(f"Added policy: {policy.id}")

    def get(self, policy_id: str) -> Optional[Policy]:
        """Get policy by ID."""
        return self._policies.get(policy_id)

    def update(self, policy: Policy) -> None:
        """Update policy."""
        if policy.id in self._policies:
            policy.updated_at = datetime.utcnow().timestamp()
            self._policies[policy.id] = policy

    def delete(self, policy_id: str) -> bool:
        """Delete policy."""
        if policy_id in self._policies:
            del self._policies[policy_id]
            return True
        return False

    def list_policies(self) -> list[Policy]:
        """List all policies."""
        return sorted(
            [p for p in self._policies.values() if p.enabled],
            key=lambda p: p.priority,
        )

    def find_policies(self, action: Optional[str] = None, resource: Optional[str] = None) -> list[Policy]:
        """Find policies matching action and resource."""
        policies = []
        for policy in self._policies.values():
            if not policy.enabled:
                continue
            for statement in policy.statements:
                action_match = action is None or action in statement.actions or "*" in statement.actions
                resource_match = resource is None or self._matches_resource(resource, statement.resources)
                if action_match and resource_match:
                    policies.append(policy)
                    break
        return sorted(policies, key=lambda p: p.priority)

    def _matches_resource(self, resource: str, patterns: list[str]) -> bool:
        """Check if resource matches any pattern."""
        for pattern in patterns:
            if pattern == "*":
                return True
            pattern = pattern.replace("*", ".*")
            if re.match(f"^{pattern}$", resource):
                return True
        return False


class PolicyEngine:
    """
    Policy evaluation engine.

    Example:
        >>> engine = PolicyEngine(store)
        >>> decision = engine.evaluate(request)
    """

    def __init__(self, store: PolicyStore) -> None:
        self.store = store

    def evaluate(self, request: PolicyRequest) -> PolicyDecision:
        """Evaluate policy request."""
        policies = self.store.find_policies(request.action, request.resource)

        if not policies:
            return PolicyDecision(allowed=True, reason="No applicable policies")

        for policy in policies:
            for statement in policy.statements:
                if self._matches_statement(statement, request):
                    if statement.effect == PolicyEffect.ALLOW:
                        return PolicyDecision(
                            allowed=True,
                            policy_id=policy.id,
                            reason=f"Allowed by policy: {policy.name}",
                        )
                    elif statement.effect == PolicyEffect.DENY:
                        return PolicyDecision(
                            allowed=False,
                            policy_id=policy.id,
                            reason=f"Denied by policy: {policy.name}",
                        )

        return PolicyDecision(allowed=True, reason="Default allow")

    def evaluate_all(self, request: PolicyRequest) -> list[PolicyDecision]:
        """Evaluate and return all matching decisions."""
        decisions = []
        policies = self.store.find_policies(request.action, request.resource)

        for policy in policies:
            for statement in policy.statements:
                if self._matches_statement(statement, request):
                    decisions.append(PolicyDecision(
                        allowed=statement.effect == PolicyEffect.ALLOW,
                        policy_id=policy.id,
                        reason=f"{statement.effect.name} by policy: {policy.name}",
                    ))

        return decisions

    def _matches_statement(self, statement: PolicyStatement, request: PolicyRequest) -> bool:
        """Check if statement matches request."""
        action_match = request.action in statement.actions or "*" in statement.actions
        if not action_match:
            return False

        resource_match = self._matches_resource(request.resource, statement.resources)
        if not resource_match:
            return False

        for condition in statement.conditions:
            if not condition.evaluate(request.context):
                return False

        return True

    def _matches_resource(self, resource: str, patterns: list[str]) -> bool:
        """Check if resource matches any pattern."""
        for pattern in patterns:
            if pattern == "*":
                return True
            pattern = pattern.replace("*", ".*")
            if re.match(f"^{pattern}$", resource):
                return True
        return False


class RoleBasedAccessControl:
    """
    Role-based access control (RBAC) implementation.

    Example:
        >>> rbac = RoleBasedAccessControl()
        >>> rbac.assign_role("user1", "editor")
        >>> rbac.grant_permission("editor", "document:write")
        >>> can_write = rbac.check_permission("user1", "document:write")
    """

    def __init__(self) -> None:
        self._roles: dict[str, set[str]] = {}
        self._role_permissions: dict[str, set[str]] = {}
        self._user_roles: dict[str, set[str]] = {}

    def create_role(self, role_id: str) -> None:
        """Create a new role."""
        if role_id not in self._roles:
            self._roles[role_id] = set()

    def delete_role(self, role_id: str) -> None:
        """Delete a role."""
        if role_id in self._roles:
            del self._roles[role_id]
        if role_id in self._role_permissions:
            del self._role_permissions[role_id]
        for user_roles in self._user_roles.values():
            user_roles.discard(role_id)

    def grant_permission(self, role_id: str, permission: str) -> None:
        """Grant permission to role."""
        if role_id not in self._roles:
            self.create_role(role_id)
        if role_id not in self._role_permissions:
            self._role_permissions[role_id] = set()
        self._role_permissions[role_id].add(permission)

    def revoke_permission(self, role_id: str, permission: str) -> None:
        """Revoke permission from role."""
        if role_id in self._role_permissions:
            self._role_permissions[role_id].discard(permission)

    def assign_role(self, user_id: str, role_id: str) -> None:
        """Assign role to user."""
        if role_id not in self._roles:
            self.create_role(role_id)
        if user_id not in self._user_roles:
            self._user_roles[user_id] = set()
        self._user_roles[user_id].add(role_id)

    def remove_role(self, user_id: str, role_id: str) -> None:
        """Remove role from user."""
        if user_id in self._user_roles:
            self._user_roles[user_id].discard(role_id)

    def get_user_roles(self, user_id: str) -> set[str]:
        """Get all roles assigned to user."""
        return self._user_roles.get(user_id, set()).copy()

    def get_role_permissions(self, role_id: str) -> set[str]:
        """Get all permissions for role."""
        return self._role_permissions.get(role_id, set()).copy()

    def check_permission(self, user_id: str, permission: str) -> bool:
        """Check if user has permission."""
        roles = self.get_user_roles(user_id)
        for role_id in roles:
            perms = self.get_role_permissions(role_id)
            if permission in perms or "*" in perms:
                return True
            if self._check_wildcard_match(permission, perms):
                return True
        return False

    def _check_wildcard_match(self, permission: str, perms: set[str]) -> bool:
        """Check if permission matches wildcard patterns."""
        parts = permission.split(":")
        for perm in perms:
            if perm == "*":
                return True
            if "*" in perm:
                perm_parts = perm.split(":")
                if len(perm_parts) == len(parts):
                    match = True
                    for p, actual in zip(perm_parts, parts):
                        if p != "*" and p != actual:
                            match = False
                            break
                    if match:
                        return True
        return False

    def get_user_permissions(self, user_id: str) -> set[str]:
        """Get all permissions for user."""
        permissions = set()
        for role_id in self.get_user_roles(user_id):
            permissions.update(self.get_role_permissions(role_id))
        return permissions


class PolicyEngineBuilder:
    """Builder for creating common policy engine configurations."""

    @staticmethod
    def create_basic() -> tuple[PolicyStore, PolicyEngine]:
        """Create basic allow/deny policy engine."""
        store = PolicyStore()
        engine = PolicyEngine(store)

        allow_all = Policy(
            id="allow_all",
            name="Allow All",
            statements=[
                PolicyStatement(
                    effect=PolicyEffect.ALLOW,
                    actions=["*"],
                    resources=["*"],
                    description="Allow all actions by default",
                )
            ],
            priority=0,
        )
        store.add(allow_all)

        return store, engine

    @staticmethod
    def create_secure() -> tuple[PolicyStore, PolicyEngine]:
        """Create secure default-deny policy engine."""
        store = PolicyStore()
        engine = PolicyEngine(store)

        deny_all = Policy(
            id="deny_all",
            name="Deny All",
            statements=[
                PolicyStatement(
                    effect=PolicyEffect.DENY,
                    actions=["*"],
                    resources=["*"],
                    description="Deny all by default",
                )
            ],
            priority=0,
        )
        store.add(deny_all)

        return store, engine
