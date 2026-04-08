"""
Automation Policy Action Module.

Provides policy-based automation with rule evaluation,
condition matching, enforcement actions, and compliance checking.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import re
from collections import defaultdict

logger = logging.getLogger(__name__)


class PolicyEffect(Enum):
    """Policy effect types."""
    ALLOW = "allow"
    DENY = "deny"
    Audit = "audit"
    LOG = "log"
    NOTIFY = "notify"


class PolicyType(Enum):
    """Policy types."""
    ACCESS_CONTROL = "access_control"
    RATE_LIMITING = "rate_limiting"
    DATA_FILTERING = "data_filtering"
    COMPLIANCE = "compliance"
    SECURITY = "security"
    OPERATIONAL = "operational"


class ConditionOperator(Enum):
    """Condition operators."""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    REGEX_MATCH = "regex_match"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    IN = "in"
    NOT_IN = "not_in"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"


@dataclass
class PolicyCondition:
    """Single condition in a policy rule."""
    field: str
    operator: ConditionOperator
    value: Any
    negating: bool = False

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate condition against context."""
        field_value = self._get_field_value(context, self.field)

        result = self._compare(field_value, self.operator, self.value)

        return not result if self.negating else result

    def _get_field_value(self, context: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value from context."""
        keys = field_path.split(".")
        value = context
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list) and key.isdigit():
                value = value[int(key)] if int(key) < len(value) else None
            else:
                return None
        return value

    def _compare(self, field_value: Any, operator: ConditionOperator, expected: Any) -> bool:
        """Compare values using operator."""
        if operator == ConditionOperator.EQUALS:
            return field_value == expected
        elif operator == ConditionOperator.NOT_EQUALS:
            return field_value != expected
        elif operator == ConditionOperator.CONTAINS:
            return expected in field_value if field_value else False
        elif operator == ConditionOperator.NOT_CONTAINS:
            return expected not in field_value if field_value else True
        elif operator == ConditionOperator.REGEX_MATCH:
            return bool(re.match(str(expected), str(field_value))) if field_value else False
        elif operator == ConditionOperator.GREATER_THAN:
            return field_value > expected if field_value else False
        elif operator == ConditionOperator.LESS_THAN:
            return field_value < expected if field_value else False
        elif operator == ConditionOperator.IN:
            return field_value in expected if field_value else False
        elif operator == ConditionOperator.NOT_IN:
            return field_value not in expected if field_value else False
        elif operator == ConditionOperator.EXISTS:
            return field_value is not None
        elif operator == ConditionOperator.NOT_EXISTS:
            return field_value is None
        return False


@dataclass
class PolicyRule:
    """A single policy rule."""
    rule_id: str
    name: str
    description: str
    conditions: List[PolicyCondition]
    condition_logic: str = "AND"
    effect: PolicyEffect
    priority: int = 0
    enabled: bool = True
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate rule against context."""
        if not self.enabled:
            return False

        if self.condition_logic == "AND":
            return all(c.evaluate(context) for c in self.conditions)
        elif self.condition_logic == "OR":
            return any(c.evaluate(context) for c in self.conditions)
        elif self.condition_logic == "NOT":
            return not all(c.evaluate(context) for c in self.conditions)
        return False


@dataclass
class Policy:
    """A complete policy with rules."""
    policy_id: str
    name: str
    description: str
    policy_type: PolicyType
    version: str
    rules: List[PolicyRule]
    enabled: bool = True
    scope: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    owner: Optional[str] = None

    def evaluate(self, context: Dict[str, Any]) -> PolicyEffect:
        """Evaluate policy against context."""
        if not self.enabled:
            return PolicyEffect.LOG

        matched_rules = [r for r in self.rules if r.evaluate(context)]

        if not matched_rules:
            return PolicyEffect.LOG

        matched_rules.sort(key=lambda r: r.priority, reverse=True)
        return matched_rules[0].effect


@dataclass
class PolicyEvaluationResult:
    """Result of policy evaluation."""
    policy_id: str
    policy_name: str
    effect: PolicyEffect
    matched_rules: List[str]
    evaluation_time: float
    timestamp: datetime = field(default_factory=datetime.now)
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceCheck:
    """Compliance check result."""
    check_id: str
    check_name: str
    passed: bool
    severity: str
    details: str
    remediation: Optional[str] = None
    checked_at: datetime = field(default_factory=datetime.now)


class PolicyEngine:
    """Main policy evaluation engine."""

    def __init__(self):
        self.policies: Dict[str, Policy] = {}
        self.policy_sets: Dict[str, Set[str]] = defaultdict(set)
        self.audit_log: List[Dict[str, Any]] = []
        self._handlers: Dict[PolicyEffect, List[Callable]] = defaultdict(list)

    def add_policy(self, policy: Policy):
        """Add policy to engine."""
        self.policies[policy.policy_id] = policy

    def remove_policy(self, policy_id: str):
        """Remove policy from engine."""
        if policy_id in self.policies:
            del self.policies[policy_id]

    def get_policy(self, policy_id: str) -> Optional[Policy]:
        """Get policy by ID."""
        return self.policies.get(policy_id)

    def list_policies(
        self,
        policy_type: Optional[PolicyType] = None,
        enabled_only: bool = True
    ) -> List[Policy]:
        """List policies with optional filtering."""
        policies = self.policies.values()
        if policy_type:
            policies = [p for p in policies if p.policy_type == policy_type]
        if enabled_only:
            policies = [p for p in policies if p.enabled]
        return sorted(policies, key=lambda p: p.name)

    def register_effect_handler(self, effect: PolicyEffect, handler: Callable):
        """Register handler for policy effect."""
        self._handlers[effect].append(handler)

    async def evaluate(
        self,
        context: Dict[str, Any],
        policy_ids: Optional[List[str]] = None
    ) -> List[PolicyEvaluationResult]:
        """Evaluate policies against context."""
        results = []
        policies_to_eval = (
            [self.policies[pid] for pid in policy_ids if pid in self.policies]
            if policy_ids
            else self.list_policies()
        )

        for policy in policies_to_eval:
            start_time = datetime.now()
            effect = policy.evaluate(context)
            eval_time = (datetime.now() - start_time).total_seconds()

            matched = [r.rule_id for r in policy.rules if r.evaluate(context)]

            result = PolicyEvaluationResult(
                policy_id=policy.policy_id,
                policy_name=policy.name,
                effect=effect,
                matched_rules=matched,
                evaluation_time=eval_time,
                context=context
            )
            results.append(result)

            if effect in self._handlers:
                for handler in self._handlers[effect]:
                    await asyncio.to_thread(handler, result)

            self._audit_log.append({
                "policy_id": policy.policy_id,
                "effect": effect.value,
                "context_keys": list(context.keys()),
                "timestamp": datetime.now().isoformat()
            })

        return results

    async def enforce(
        self,
        context: Dict[str, Any],
        default_effect: PolicyEffect = PolicyEffect.DENY
    ) -> bool:
        """Enforce policies and return whether action is allowed."""
        results = await self.evaluate(context)

        deny_found = any(r.effect == PolicyEffect.DENY for r in results)
        allow_found = any(r.effect == PolicyEffect.ALLOW for r in results)

        if deny_found and not allow_found:
            return False

        return True

    def get_audit_log(
        self,
        limit: int = 100,
        policy_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        log = self._audit_log
        if policy_id:
            log = [e for e in log if e.get("policy_id") == policy_id]
        return log[-limit:]


class ComplianceChecker:
    """Policy compliance checking."""

    def __init__(self, policy_engine: PolicyEngine):
        self.policy_engine = policy_engine
        self.compliance_rules: List[Dict[str, Any]] = []

    def add_compliance_rule(
        self,
        rule_id: str,
        description: str,
        check_function: Callable[[Dict[str, Any]], bool],
        severity: str = "medium",
        remediation: Optional[str] = None
    ):
        """Add compliance check rule."""
        self.compliance_rules.append({
            "rule_id": rule_id,
            "description": description,
            "check": check_function,
            "severity": severity,
            "remediation": remediation
        })

    async def check_compliance(
        self,
        context: Dict[str, Any]
    ) -> List[ComplianceCheck]:
        """Run all compliance checks."""
        results = []

        for rule in self.compliance_rules:
            try:
                passed = await asyncio.to_thread(rule["check"], context)
                check = ComplianceCheck(
                    check_id=rule["rule_id"],
                    check_name=rule["description"],
                    passed=passed,
                    severity=rule["severity"],
                    details="Check executed successfully",
                    remediation=rule.get("remediation")
                )
            except Exception as e:
                check = ComplianceCheck(
                    check_id=rule["rule_id"],
                    check_name=rule["description"],
                    passed=False,
                    severity=rule["severity"],
                    details=f"Check failed with error: {str(e)}",
                    remediation=rule.get("remediation")
                )
            results.append(check)

        return results


def create_access_control_policy() -> Policy:
    """Create sample access control policy."""
    policy = Policy(
        policy_id="acp-001",
        name="API Access Control",
        description="Controls access to API endpoints",
        policy_type=PolicyType.ACCESS_CONTROL,
        version="1.0"
    )

    policy.rules = [
        PolicyRule(
            rule_id="acp-001-rule-1",
            name="Allow authenticated users",
            description="Allow access to authenticated users",
            conditions=[
                PolicyCondition("auth.token_valid", ConditionOperator.EQUALS, True),
                PolicyCondition("auth.user_role", ConditionOperator.IN, ["admin", "user", "guest"])
            ],
            effect=PolicyEffect.ALLOW,
            priority=10
        ),
        PolicyRule(
            rule_id="acp-001-rule-2",
            name="Deny blocked users",
            description="Block users with suspended accounts",
            conditions=[
                PolicyCondition("auth.user_status", ConditionOperator.EQUALS, "suspended")
            ],
            effect=PolicyEffect.DENY,
            priority=100
        ),
        PolicyRule(
            rule_id="acp-001-rule-3",
            name="Rate limit excessive requests",
            description="Deny requests exceeding rate limit",
            conditions=[
                PolicyCondition("request.count", ConditionOperator.GREATER_THAN, 1000)
            ],
            effect=PolicyEffect.DENY,
            priority=50
        )
    ]

    return policy


def main():
    """Demonstrate policy-based automation."""
    engine = PolicyEngine()

    access_policy = create_access_control_policy()
    engine.add_policy(access_policy)

    async def handle_deny(result: PolicyEvaluationResult):
        print(f"DENY: {result.policy_name}")

    async def handle_audit(result: PolicyEvaluationResult):
        print(f"AUDIT: {result.policy_name}")

    engine.register_effect_handler(PolicyEffect.DENY, handle_deny)
    engine.register_effect_handler(PolicyEffect.AUDIT, handle_audit)

    context = {
        "auth": {
            "token_valid": True,
            "user_role": "user",
            "user_status": "active"
        },
        "request": {
            "count": 50
        }
    }

    results = asyncio.run(engine.evaluate(context))
    for result in results:
        print(f"Policy: {result.policy_name}, Effect: {result.effect.value}")

    allowed = asyncio.run(engine.enforce(context))
    print(f"Action allowed: {allowed}")


if __name__ == "__main__":
    main()
