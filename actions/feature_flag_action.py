"""Feature Flag Action Module.

Provides feature flag management with targeting rules,
percentage rollouts, user segments, and A/B testing support.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class FlagType(Enum):
    """Feature flag type."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"


class RolloutStrategy(Enum):
    """Rollout strategy."""
    ALL = "all"
    PERCENTAGE = "percentage"
    USER_LIST = "user_list"
    RULE_BASED = "rule_based"


@dataclass
class TargetingRule:
    """Targeting rule for flag."""
    attribute: str
    operator: str
    value: Any


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    id: str
    key: str
    name: str
    flag_type: FlagType
    enabled: bool
    default_value: Any
    rollout_strategy: RolloutStrategy
    rollout_percentage: float = 100.0
    user_list: List[str] = field(default_factory=list)
    rules: List[TargetingRule] = field(default_factory=list)
    variants: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class EvaluationResult:
    """Flag evaluation result."""
    key: str
    enabled: bool
    value: Any
    reason: str
    variation: Optional[str] = None


class FeatureFlagStore:
    """In-memory feature flag store."""

    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._evaluations: Dict[str, List[EvaluationResult]] = defaultdict(list)

    def create(self, key: str, name: str,
               flag_type: FlagType = FlagType.BOOLEAN,
               default_value: Any = False,
               rollout_strategy: RolloutStrategy = RolloutStrategy.ALL) -> FeatureFlag:
        """Create feature flag."""
        flag = FeatureFlag(
            id=uuid.uuid4().hex,
            key=key,
            name=name,
            flag_type=flag_type,
            enabled=True,
            default_value=default_value,
            rollout_strategy=rollout_strategy
        )
        self._flags[key] = flag
        return flag

    def get(self, key: str) -> Optional[FeatureFlag]:
        """Get flag by key."""
        return self._flags.get(key)

    def update(self, key: str, **kwargs) -> Optional[FeatureFlag]:
        """Update flag."""
        flag = self._flags.get(key)
        if not flag:
            return None

        for k, v in kwargs.items():
            if hasattr(flag, k):
                setattr(flag, k, v)
        flag.updated_at = time.time()
        return flag

    def delete(self, key: str) -> bool:
        """Delete flag."""
        if key in self._flags:
            del self._flags[key]
            return True
        return False

    def evaluate(self, key: str, user_id: Optional[str] = None,
                 attributes: Optional[Dict[str, Any]] = None) -> EvaluationResult:
        """Evaluate flag for user."""
        flag = self._flags.get(key)

        if not flag:
            return EvaluationResult(
                key=key,
                enabled=False,
                value=None,
                reason="flag_not_found"
            )

        if not flag.enabled:
            return EvaluationResult(
                key=key,
                enabled=False,
                value=flag.default_value,
                reason="flag_disabled"
            )

        attributes = attributes or {}

        if user_id and flag.user_list:
            if user_id in flag.user_list:
                return EvaluationResult(
                    key=key,
                    enabled=True,
                    value=True,
                    reason="user_in_list"
                )

        if flag.rollout_strategy == RolloutStrategy.PERCENTAGE:
            bucket = self._get_bucket(user_id or "anonymous", key)
            if bucket < flag.rollout_percentage:
                return EvaluationResult(
                    key=key,
                    enabled=True,
                    value=True,
                    reason=f"percentage_rollout_{flag.rollout_percentage}"
                )
            return EvaluationResult(
                key=key,
                enabled=False,
                value=flag.default_value,
                reason="percentage_rollout_excluded"
            )

        if flag.rules:
            for rule in flag.rules:
                attr_value = attributes.get(rule.attribute)
                if self._evaluate_condition(attr_value, rule.operator, rule.value):
                    return EvaluationResult(
                        key=key,
                        enabled=True,
                        value=True,
                        reason="rule_matched"
                    )

        return EvaluationResult(
            key=key,
            enabled=True,
            value=flag.default_value,
            reason="default"
        )

    def _get_bucket(self, user_id: str, flag_key: str) -> float:
        """Get bucket percentage for user."""
        hash_input = f"{user_id}:{flag_key}"
        hash_val = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        return (hash_val % 10000) / 100.0

    def _evaluate_condition(self, value: Any, operator: str, target: Any) -> bool:
        """Evaluate targeting condition."""
        if operator == "eq":
            return value == target
        if operator == "ne":
            return value != target
        if operator == "gt":
            return value > target
        if operator == "gte":
            return value >= target
        if operator == "lt":
            return value < target
        if operator == "lte":
            return value <= target
        if operator == "contains":
            return target in str(value)
        if operator == "in":
            return value in target
        return False


_global_store = FeatureFlagStore()


class FeatureFlagAction:
    """Feature flag action.

    Example:
        action = FeatureFlagAction()

        action.create("new-checkout", "New Checkout Flow")
        action.update("new-checkout", rollout_percentage=50)

        result = action.evaluate("new-checkout", user_id="user-123")
    """

    def __init__(self, store: Optional[FeatureFlagStore] = None):
        self._store = store or _global_store

    def create(self, key: str, name: str,
               flag_type: str = "boolean",
               default_value: Any = False,
               rollout_strategy: str = "all") -> Dict[str, Any]:
        """Create feature flag."""
        try:
            ft = FlagType(flag_type)
            rs = RolloutStrategy(rollout_strategy)
        except ValueError as e:
            return {"success": False, "message": f"Invalid type: {e}"}

        flag = self._store.create(key, name, ft, default_value, rs)

        return {
            "success": True,
            "flag": {
                "id": flag.id,
                "key": flag.key,
                "name": flag.name,
                "type": flag.flag_type.value,
                "enabled": flag.enabled,
                "default_value": flag.default_value,
                "rollout_strategy": flag.rollout_strategy.value
            },
            "message": f"Created flag: {key}"
        }

    def get(self, key: str) -> Dict[str, Any]:
        """Get flag."""
        flag = self._store.get(key)
        if flag:
            return {
                "success": True,
                "flag": {
                    "key": flag.key,
                    "name": flag.name,
                    "type": flag.flag_type.value,
                    "enabled": flag.enabled,
                    "rollout_percentage": flag.rollout_percentage,
                    "user_list_count": len(flag.user_list),
                    "rules_count": len(flag.rules)
                }
            }
        return {"success": False, "message": "Flag not found"}

    def update(self, key: str, **kwargs) -> Dict[str, Any]:
        """Update flag."""
        valid_fields = ["name", "enabled", "default_value", "rollout_strategy",
                        "rollout_percentage", "user_list", "rules"]
        update_kwargs = {k: v for k, v in kwargs.items() if k in valid_fields}

        flag = self._store.update(key, **update_kwargs)
        if flag:
            return {"success": True, "message": f"Updated flag: {key}"}
        return {"success": False, "message": "Flag not found"}

    def delete(self, key: str) -> Dict[str, Any]:
        """Delete flag."""
        if self._store.delete(key):
            return {"success": True, "message": f"Deleted flag: {key}"}
        return {"success": False, "message": "Flag not found"}

    def evaluate(self, key: str, user_id: Optional[str] = None,
                 attributes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Evaluate flag for user."""
        result = self._store.evaluate(key, user_id, attributes)

        return {
            "success": True,
            "key": result.key,
            "enabled": result.enabled,
            "value": result.value,
            "reason": result.reason,
            "variation": result.variation
        }

    def list_flags(self) -> Dict[str, Any]:
        """List all flags."""
        flags = list(self._store._flags.values())
        return {
            "success": True,
            "flags": [
                {
                    "key": f.key,
                    "name": f.name,
                    "enabled": f.enabled,
                    "rollout_percentage": f.rollout_percentage
                }
                for f in flags
            ],
            "count": len(flags)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute feature flag action."""
    operation = params.get("operation", "")
    action = FeatureFlagAction()

    try:
        if operation == "create":
            key = params.get("key", "")
            name = params.get("name", "")
            if not key or not name:
                return {"success": False, "message": "key and name required"}
            return action.create(
                key=key,
                name=name,
                flag_type=params.get("type", "boolean"),
                default_value=params.get("default_value", False),
                rollout_strategy=params.get("rollout_strategy", "all")
            )

        elif operation == "get":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.get(key)

        elif operation == "update":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.update(key, **params)

        elif operation == "delete":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.delete(key)

        elif operation == "evaluate":
            key = params.get("key", "")
            if not key:
                return {"success": False, "message": "key required"}
            return action.evaluate(
                key=key,
                user_id=params.get("user_id"),
                attributes=params.get("attributes")
            )

        elif operation == "list":
            return action.list_flags()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Feature flag error: {str(e)}"}
