"""
Automation Policy Action Module

Provides policy enforcement, access control, and governance for automation.
"""
from typing import Any, Optional, Callable, Literal
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio


class PolicyEffect(Enum):
    """Policy effect types."""
    ALLOW = "allow"
    DENY = "deny"


class PolicyType(Enum):
    """Policy types."""
    ACCESS_CONTROL = "access_control"
    RATE_LIMITING = "rate_limiting"
    RESOURCE_QUOTA = "resource_quota"
    DATA_RETENTION = "data_retention"
    COMPLIANCE = "compliance"


@dataclass
class PolicyCondition:
    """A condition for policy evaluation."""
    field: str
    operator: str  # eq, ne, gt, lt, in, not_in, contains, regex
    value: Any
    logical_op: Optional[str] = None  # and, or


@dataclass
class Policy:
    """A policy definition."""
    policy_id: str
    name: str
    policy_type: PolicyType
    effect: PolicyEffect
    description: str
    conditions: list[PolicyCondition] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)  # Action patterns this applies to
    resources: list[str] = field(default_factory=list)  # Resource patterns
    principals: list[str] = field(default_factory=list)  # Who this applies to
    condition_logic: str = "all"  # all (AND), any (OR)
    metadata: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    priority: int = 0


@dataclass
class PolicyContext:
    """Context for policy evaluation."""
    principal: dict[str, Any]  # User/service making request
    action: str
    resource: str
    environment: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class PolicyDecision:
    """Result of policy evaluation."""
    allowed: bool
    policy_id: Optional[str]
    reason: str
    evaluated_policies: int
    duration_ms: float


@dataclass
class QuotaStatus:
    """Status of a resource quota."""
    quota_name: str
    limit: float
    used: float
    remaining: float
    resets_at: Optional[datetime] = None
    window: Optional[str] = None


class AutomationPolicyAction:
    """Main policy enforcement action handler."""
    
    def __init__(self):
        self._policies: dict[str, Policy] = {}
        self._quotas: dict[str, QuotaStatus] = {}
        self._policy_history: list[PolicyDecision] = []
        self._max_history: int = 10000
        self._stats: dict[str, Any] = defaultdict(int)
    
    def add_policy(self, policy: Policy) -> "AutomationPolicyAction":
        """Add a policy to the system."""
        self._policies[policy.policy_id] = policy
        self._sort_policies_by_priority()
        return self
    
    def remove_policy(self, policy_id: str) -> bool:
        """Remove a policy."""
        if policy_id in self._policies:
            del self._policies[policy_id]
            return True
        return False
    
    def _sort_policies_by_priority(self):
        """Sort policies by priority (higher first)."""
        sorted_policies = sorted(
            self._policies.values(),
            key=lambda p: p.priority,
            reverse=True
        )
        self._policies = {p.policy_id: p for p in sorted_policies}
    
    async def evaluate(
        self,
        context: PolicyContext
    ) -> PolicyDecision:
        """
        Evaluate policies for a given context.
        
        Args:
            context: Policy context with principal, action, resource
            
        Returns:
            PolicyDecision with allow/deny result
        """
        start_time = datetime.now()
        evaluated = 0
        applicable_policies = []
        
        # Find applicable policies
        for policy in self._policies.values():
            if not policy.enabled:
                continue
            
            if not self._is_applicable(policy, context):
                continue
            
            if not self._evaluate_conditions(policy, context):
                continue
            
            applicable_policies.append(policy)
            evaluated += 1
        
        # Make decision based on policy effects
        decision = self._make_decision(applicable_policies)
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        
        policy_decision = PolicyDecision(
            allowed=decision["allowed"],
            policy_id=decision.get("policy_id"),
            reason=decision["reason"],
            evaluated_policies=evaluated,
            duration_ms=duration_ms
        )
        
        # Record in history
        self._policy_history.append(policy_decision)
        if len(self._policy_history) > self._max_history:
            self._policy_history = self._policy_history[-self._max_history:]
        
        self._stats["evaluations"] += 1
        if decision["allowed"]:
            self._stats["allow_decisions"] += 1
        else:
            self._stats["deny_decisions"] += 1
        
        return policy_decision
    
    def _is_applicable(self, policy: Policy, context: PolicyContext) -> bool:
        """Check if policy applies to this context."""
        # Check actions
        if policy.actions:
            action_matches = any(
                self._match_pattern(context.action, action)
                for action in policy.actions
            )
            if not action_matches:
                return False
        
        # Check resources
        if policy.resources:
            resource_matches = any(
                self._match_pattern(context.resource, resource)
                for resource in policy.resources
            )
            if not resource_matches:
                return False
        
        # Check principals
        if policy.principals:
            principal_id = context.principal.get("id", "")
            principal_matches = any(
                self._match_pattern(principal_id, principal)
                for principal in policy.principals
            )
            if not principal_matches:
                return False
        
        return True
    
    def _match_pattern(self, value: str, pattern: str) -> bool:
        """Match value against pattern (supports * wildcards)."""
        if pattern == "*":
            return True
        if "*" in pattern:
            import fnmatch
            return fnmatch.fnmatch(value, pattern)
        return value == pattern
    
    def _evaluate_conditions(self, policy: Policy, context: PolicyContext) -> bool:
        """Evaluate policy conditions."""
        if not policy.conditions:
            return True
        
        results = []
        for condition in policy.conditions:
            result = self._evaluate_single_condition(condition, context)
            results.append(result)
        
        if policy.condition_logic == "all":
            return all(results)
        else:  # any
            return any(results)
    
    def _evaluate_single_condition(
        self,
        condition: PolicyCondition,
        context: PolicyContext
    ) -> bool:
        """Evaluate a single condition."""
        # Get value from context
        value = self._get_field_value(condition.field, context)
        
        # Apply operator
        if condition.operator == "eq":
            return value == condition.value
        elif condition.operator == "ne":
            return value != condition.value
        elif condition.operator == "gt":
            return value > condition.value
        elif condition.operator == "lt":
            return value < condition.value
        elif condition.operator == "gte":
            return value >= condition.value
        elif condition.operator == "lte":
            return value <= condition.value
        elif condition.operator == "in":
            return value in condition.value
        elif condition.operator == "not_in":
            return value not in condition.value
        elif condition.operator == "contains":
            return condition.value in value
        elif condition.operator == "regex":
            import re
            return bool(re.match(condition.value, str(value)))
        
        return True
    
    def _get_field_value(self, field: str, context: PolicyContext) -> Any:
        """Get a field value from context using dot notation."""
        parts = field.split(".")
        value = None
        
        if parts[0] == "principal":
            value = context.principal
            parts = parts[1:]
        elif parts[0] == "environment":
            value = context.environment
            parts = parts[1:]
        else:
            value = context.__dict__
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif hasattr(value, part):
                value = getattr(value, part)
            else:
                return None
        
        return value
    
    def _make_decision(
        self,
        applicable_policies: list[Policy]
    ) -> dict[str, Any]:
        """Make allow/deny decision from applicable policies."""
        if not applicable_policies:
            return {"allowed": True, "reason": "No applicable policies"}
        
        # Check for explicit deny (highest priority)
        for policy in applicable_policies:
            if policy.effect == PolicyEffect.DENY:
                return {
                    "allowed": False,
                    "policy_id": policy.policy_id,
                    "reason": f"Denied by policy: {policy.name}"
                }
        
        # Check for explicit allow
        for policy in applicable_policies:
            if policy.effect == PolicyEffect.ALLOW:
                return {
                    "allowed": True,
                    "policy_id": policy.policy_id,
                    "reason": f"Allowed by policy: {policy.name}"
                }
        
        return {"allowed": True, "reason": "Default allow"}
    
    async def check_quota(
        self,
        quota_name: str,
        cost: float = 1.0
    ) -> tuple[bool, QuotaStatus]:
        """
        Check if action is within quota limits.
        
        Returns (allowed, current_status)
        """
        if quota_name not in self._quotas:
            return True, QuotaStatus(
                quota_name=quota_name,
                limit=float('inf'),
                used=0,
                remaining=float('inf')
            )
        
        quota = self._quotas[quota_name]
        
        # Check reset window
        if quota.resets_at and datetime.now() >= quota.resets_at:
            quota.used = 0
            quota.remaining = quota.limit
        
        allowed = quota.remaining >= cost
        
        if allowed:
            quota.used += cost
            quota.remaining -= cost
        
        self._stats["quota_checks"] += 1
        if not allowed:
            self._stats["quota_violations"] += 1
        
        return allowed, quota
    
    def set_quota(
        self,
        quota_name: str,
        limit: float,
        window: Optional[str] = None  # e.g., "1h", "1d"
    ) -> QuotaStatus:
        """Set or update a quota."""
        resets_at = None
        if window:
            if window.endswith("h"):
                resets_at = datetime.now() + timedelta(hours=int(window[:-1]))
            elif window.endswith("d"):
                resets_at = datetime.now() + timedelta(days=int(window[:-1]))
        
        quota = QuotaStatus(
            quota_name=quota_name,
            limit=limit,
            used=0,
            remaining=limit,
            resets_at=resets_at,
            window=window
        )
        
        self._quotas[quota_name] = quota
        return quota
    
    async def enforce(
        self,
        context: PolicyContext,
        action_cost: float = 1.0,
        quota_name: Optional[str] = None
    ) -> PolicyDecision:
        """
        Evaluate policies and enforce quotas.
        
        Shortcut for evaluate() + optional quota check.
        """
        decision = await self.evaluate(context)
        
        if not decision.allowed:
            return decision
        
        # Check quota if specified
        if quota_name:
            quota_allowed, quota_status = await self.check_quota(
                quota_name, action_cost
            )
            
            if not quota_allowed:
                return PolicyDecision(
                    allowed=False,
                    policy_id=None,
                    reason=f"Quota exceeded: {quota_name}",
                    evaluated_policies=decision.evaluated_policies,
                    duration_ms=0
                )
        
        return decision
    
    def get_policies(
        self,
        policy_type: Optional[PolicyType] = None,
        enabled_only: bool = False
    ) -> list[Policy]:
        """Get list of policies."""
        policies = list(self._policies.values())
        
        if policy_type:
            policies = [p for p in policies if p.policy_type == policy_type]
        
        if enabled_only:
            policies = [p for p in policies if p.enabled]
        
        return policies
    
    def get_quota_status(self, quota_name: Optional[str] = None) -> dict[str, QuotaStatus]:
        """Get quota status."""
        if quota_name:
            return {quota_name: self._quotas.get(quota_name)}
        return dict(self._quotas)
    
    def get_stats(self) -> dict[str, Any]:
        """Get policy statistics."""
        return {
            **dict(self._stats),
            "total_policies": len(self._policies),
            "enabled_policies": len([p for p in self._policies.values() if p.enabled]),
            "total_quotas": len(self._quotas)
        }
