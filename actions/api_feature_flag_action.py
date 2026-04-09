"""
API Feature Flag Action Module

Provides feature flag evaluation with targeting rules,
percentage rollouts, and user segmentation.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

import logging

logger = logging.getLogger(__name__)


class FlagStatus(Enum):
    """Feature flag status."""

    ENABLED = auto()
    DISABLED = auto()
    CONDITIONAL = auto()


@dataclass
class TargetingRule:
    """A targeting rule for feature flag evaluation."""

    rule_id: str
    name: str
    conditions: List[Dict[str, Any]]
    serve_value: Any
    priority: int = 0

    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate if the rule matches the given context."""
        for condition in self.conditions:
            field_name = condition.get("field", "")
            operator = condition.get("op", "eq")
            expected = condition.get("value")

            actual = context.get(field_name)
            if not self._compare(actual, operator, expected):
                return False
        return True

    def _compare(self, actual: Any, operator: str, expected: Any) -> bool:
        """Compare actual value against expected using operator."""
        if operator == "eq":
            return actual == expected
        elif operator == "ne":
            return actual != expected
        elif operator == "gt":
            return actual is not None and actual > expected
        elif operator == "gte":
            return actual is not None and actual >= expected
        elif operator == "lt":
            return actual is not None and actual < expected
        elif operator == "lte":
            return actual is not None and actual <= expected
        elif operator == "in":
            return actual in expected if isinstance(expected, list) else actual == expected
        elif operator == "not_in":
            return actual not in expected if isinstance(expected, list) else actual != expected
        elif operator == "contains":
            return expected in str(actual) if actual is not None else False
        elif operator == "starts_with":
            return str(actual).startswith(str(expected)) if actual is not None else False
        elif operator == "ends_with":
            return str(actual).endswith(str(expected)) if actual is not None else False
        elif operator == "regex":
            import re
            return bool(re.match(str(expected), str(actual))) if actual is not None else False
        return False


@dataclass
class RolloutPercentage:
    """Percentage rollout configuration."""

    percentage: float
    seed: str = "default"

    def is_enabled(self, user_id: str, flag_key: str) -> bool:
        """Determine if user falls within rollout percentage."""
        hash_input = f"{flag_key}:{user_id}:{self.seed}"
        hash_val = hashlib.md5(hash_input.encode()).hexdigest()
        hash_int = int(hash_val[:8], 16)
        bucket = (hash_int % 10000) / 100.0
        return bucket < self.percentage


@dataclass
class FeatureFlag:
    """Feature flag definition."""

    flag_key: str
    name: str
    description: str = ""
    status: FlagStatus = FlagStatus.DISABLED
    default_value: Any = False
    rules: List[TargetingRule] = field(default_factory=list)
    rollout_percentage: Optional[RolloutPercentage] = None
    rollout_attribute: str = "user_id"
    variants: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    enabled_environments: Set[str] = field(default_factory=set)

    def evaluate(
        self,
        context: Dict[str, Any],
        environment: str = "production",
    ) -> Any:
        """Evaluate the feature flag for given context."""
        if environment not in self.enabled_environments:
            return self.default_value

        if self.status == FlagStatus.DISABLED:
            return self.default_value

        sorted_rules = sorted(self.rules, key=lambda r: r.priority, reverse=True)
        for rule in sorted_rules:
            if rule.evaluate(context):
                return rule.serve_value

        if self.rollout_percentage:
            rollout_val = context.get(self.rollout_attribute)
            if rollout_val and isinstance(rollout_val, str):
                if self.rollout_percentage.is_enabled(rollout_val, self.flag_key):
                    if self.variants:
                        return self.variants[0]
                    return True

        if self.status == FlagStatus.ENABLED:
            if self.variants:
                return self.variants[0]
            return True

        return self.default_value


class FeatureFlagStore:
    """In-memory store for feature flags."""

    def __init__(self) -> None:
        self._flags: Dict[str, FeatureFlag] = {}
        self._eval_history: List[Dict[str, Any]] = []

    def add_flag(self, flag: FeatureFlag) -> None:
        """Add a feature flag to the store."""
        self._flags[flag.flag_key] = flag

    def get_flag(self, flag_key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        return self._flags.get(flag_key)

    def evaluate(
        self,
        flag_key: str,
        context: Dict[str, Any],
        environment: str = "production",
    ) -> Any:
        """Evaluate a feature flag."""
        flag = self._flags.get(flag_key)
        if not flag:
            logger.warning(f"Feature flag not found: {flag_key}")
            return None

        result = flag.evaluate(context, environment)

        self._eval_history.append({
            "flag_key": flag_key,
            "context": context,
            "result": result,
            "timestamp": time.time(),
        })

        return result

    def list_flags(self) -> List[FeatureFlag]:
        """List all feature flags."""
        return list(self._flags.values())

    def get_evaluation_history(
        self,
        flag_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get evaluation history, optionally filtered by flag."""
        if flag_key:
            return [e for e in self._eval_history[-limit:] if e["flag_key"] == flag_key]
        return self._eval_history[-limit:]


class FeatureFlagAction:
    """Action class for feature flag operations."""

    def __init__(self) -> None:
        self.store = FeatureFlagStore()

    def create_flag(
        self,
        flag_key: str,
        name: str,
        default_value: Any = False,
        description: str = "",
    ) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            flag_key=flag_key,
            name=name,
            description=description,
            default_value=default_value,
        )
        self.store.add_flag(flag)
        return flag

    def evaluate(
        self,
        flag_key: str,
        user_id: Optional[str] = None,
        attributes: Optional[Dict[str, Any]] = None,
        environment: str = "production",
    ) -> Any:
        """Evaluate a feature flag."""
        context = attributes or {}
        if user_id:
            context["user_id"] = user_id

        return self.store.evaluate(flag_key, context, environment)

    def add_rule(
        self,
        flag_key: str,
        rule: TargetingRule,
    ) -> bool:
        """Add a targeting rule to a flag."""
        flag = self.store.get_flag(flag_key)
        if flag:
            flag.rules.append(rule)
            flag.updated_at = time.time()
            return True
        return False

    def enable_rollout(
        self,
        flag_key: str,
        percentage: float,
        attribute: str = "user_id",
    ) -> bool:
        """Enable percentage rollout for a flag."""
        flag = self.store.get_flag(flag_key)
        if flag:
            flag.rollout_percentage = RolloutPercentage(percentage=percentage)
            flag.rollout_attribute = attribute
            flag.status = FlagStatus.CONDITIONAL
            flag.updated_at = time.time()
            return True
        return False

    def enable_flag(self, flag_key: str, environment: str = "production") -> bool:
        """Enable a feature flag for an environment."""
        flag = self.store.get_flag(flag_key)
        if flag:
            flag.status = FlagStatus.ENABLED
            flag.enabled_environments.add(environment)
            flag.updated_at = time.time()
            return True
        return False

    def disable_flag(self, flag_key: str) -> bool:
        """Disable a feature flag."""
        flag = self.store.get_flag(flag_key)
        if flag:
            flag.status = FlagStatus.DISABLED
            flag.updated_at = time.time()
            return True
        return False

    def get_flag_status(self, flag_key: str) -> Optional[Dict[str, Any]]:
        """Get status information for a flag."""
        flag = self.store.get_flag(flag_key)
        if flag:
            return {
                "flag_key": flag.flag_key,
                "name": flag.name,
                "status": flag.status.name,
                "rules_count": len(flag.rules),
                "has_rollout": flag.rollout_percentage is not None,
                "enabled_environments": list(flag.enabled_environments),
                "updated_at": flag.updated_at,
            }
        return None
