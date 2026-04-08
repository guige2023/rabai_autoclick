"""
Feature flag action for managing feature toggles and A/B testing.

This module provides actions for feature flag management including
flags, rules, targeting, and percentage rollouts.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class FlagType(Enum):
    """Types of feature flags."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"


class RuleType(Enum):
    """Types of targeting rules."""
    ALWAYS = "always"
    NEVER = "never"
    PERCENTAGE = "percentage"
    USER_ID = "user_id"
    ATTRIBUTE = "attribute"
    CUSTOM = "custom"


@dataclass
class User:
    """Represents a user for targeting."""
    id: str
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TargetingRule:
    """A targeting rule for a feature flag."""
    rule_type: RuleType
    value: Any
    percentage: Optional[float] = None
    user_ids: Optional[List[str]] = None
    attribute_conditions: Optional[Dict[str, Any]] = None
    custom_rule: Optional[Callable[[User], bool]] = None
    description: Optional[str] = None

    def matches(self, user: Optional[User] = None, user_id: Optional[str] = None) -> bool:
        """Check if this rule matches for a user."""
        if self.rule_type == RuleType.ALWAYS:
            return True
        elif self.rule_type == RuleType.NEVER:
            return False
        elif self.rule_type == RuleType.PERCENTAGE:
            if user:
                return self._check_percentage(user.id)
            elif user_id:
                return self._check_percentage(user_id)
            return False
        elif self.rule_type == RuleType.USER_ID:
            if not user_id or not self.user_ids:
                return False
            return user_id in self.user_ids
        elif self.rule_type == RuleType.ATTRIBUTE:
            if not user or not self.attribute_conditions:
                return False
            return self._check_attributes(user)
        elif self.rule_type == RuleType.CUSTOM:
            if not user or not self.custom_rule:
                return False
            try:
                return self.custom_rule(user)
            except Exception:
                return False
        return False

    def _check_percentage(self, identifier: str) -> bool:
        """Check if identifier falls within percentage rollout."""
        if self.percentage is None or self.percentage >= 100:
            return True
        if self.percentage <= 0:
            return False

        hash_val = int(hashlib.md5(identifier.encode()).hexdigest()[:8], 16)
        bucket = (hash_val % 10000) / 100.0
        return bucket < self.percentage

    def _check_attributes(self, user: User) -> bool:
        """Check if user matches attribute conditions."""
        for key, expected in self.attribute_conditions.items():
            actual = user.attributes.get(key)
            if isinstance(expected, dict):
                op = expected.get("op", "eq")
                value = expected.get("value")
                if not self._compare_attribute(actual, op, value):
                    return False
            elif actual != expected:
                return False
        return True

    def _compare_attribute(self, actual: Any, op: str, expected: Any) -> bool:
        """Compare an attribute value."""
        if op == "eq":
            return actual == expected
        elif op == "ne":
            return actual != expected
        elif op == "gt":
            return actual > expected
        elif op == "gte":
            return actual >= expected
        elif op == "lt":
            return actual < expected
        elif op == "lte":
            return actual <= expected
        elif op == "in":
            return actual in expected
        elif op == "not_in":
            return actual not in expected
        elif op == "contains":
            return expected in actual
        elif op == "starts_with":
            return str(actual).startswith(str(expected))
        elif op == "ends_with":
            return str(actual).endswith(str(expected))
        return False


@dataclass
class FeatureFlag:
    """A feature flag definition."""
    name: str
    flag_type: FlagType
    enabled: bool
    default_value: Any
    rules: List[TargetingRule] = field(default_factory=list)
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    env: str = "production"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert flag to dictionary."""
        return {
            "name": self.name,
            "flag_type": self.flag_type.value,
            "enabled": self.enabled,
            "default_value": self.default_value,
            "rules": [
                {
                    "rule_type": r.rule_type.value,
                    "value": r.value,
                    "percentage": r.percentage,
                    "user_ids": r.user_ids,
                    "attribute_conditions": r.attribute_conditions,
                    "description": r.description,
                }
                for r in self.rules
            ],
            "description": self.description,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "env": self.env,
            "metadata": self.metadata,
        }


@dataclass
class FlagEvaluation:
    """Result of evaluating a feature flag."""
    flag_name: str
    value: Any
    matched_rule: Optional[str] = None
    reason: str = "default"
    evaluated_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        """Convert evaluation to dictionary."""
        return {
            "flag_name": self.flag_name,
            "value": self.value,
            "matched_rule": self.matched_rule,
            "reason": self.reason,
            "evaluated_at": self.evaluated_at.isoformat(),
        }


class FeatureFlagManager:
    """
    Manages feature flags with targeting rules and rollouts.

    Thread-safe implementation supporting boolean, string, number,
    and JSON flag types with percentage and user-based targeting.
    """

    def __init__(self, env: str = "production"):
        """
        Initialize the feature flag manager.

        Args:
            env: Environment name (production, staging, dev).
        """
        self.env = env
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = threading.RLock()
        self._change_callbacks: List[Callable[[str, FeatureFlag], None]] = []

    def create_flag(
        self,
        name: str,
        flag_type: FlagType = FlagType.BOOLEAN,
        default_value: Any = False,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> FeatureFlag:
        """
        Create a new feature flag.

        Args:
            name: Name of the flag.
            flag_type: Type of the flag.
            default_value: Default value when flag is disabled or no rules match.
            description: Optional description.
            tags: Optional tags.

        Returns:
            The created FeatureFlag.
        """
        with self._lock:
            if name in self._flags:
                raise ValueError(f"Flag already exists: {name}")

            flag = FeatureFlag(
                name=name,
                flag_type=flag_type,
                enabled=True,
                default_value=default_value,
                description=description,
                tags=tags or [],
                env=self.env,
            )

            self._flags[name] = flag
            return flag

    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get a flag by name."""
        with self._lock:
            return self._flags.get(name)

    def update_flag(
        self,
        name: str,
        enabled: Optional[bool] = None,
        default_value: Optional[Any] = None,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Optional[FeatureFlag]:
        """
        Update an existing flag.

        Args:
            name: Name of the flag to update.
            enabled: New enabled state.
            default_value: New default value.
            description: New description.
            tags: New tags.

        Returns:
            Updated FeatureFlag or None if not found.
        """
        with self._lock:
            flag = self._flags.get(name)
            if not flag:
                return None

            if enabled is not None:
                flag.enabled = enabled
            if default_value is not None:
                flag.default_value = default_value
            if description is not None:
                flag.description = description
            if tags is not None:
                flag.tags = tags

            flag.updated_at = datetime.now()
            self._notify_change(name, flag)

            return flag

    def delete_flag(self, name: str) -> bool:
        """Delete a flag by name."""
        with self._lock:
            if name in self._flags:
                del self._flags[name]
                return True
            return False

    def add_rule(
        self,
        flag_name: str,
        rule: TargetingRule,
    ) -> bool:
        """
        Add a targeting rule to a flag.

        Args:
            flag_name: Name of the flag.
            rule: The targeting rule to add.

        Returns:
            True if added, False if flag not found.
        """
        with self._lock:
            flag = self._flags.get(flag_name)
            if not flag:
                return False

            flag.rules.append(rule)
            flag.updated_at = datetime.now()
            self._notify_change(flag_name, flag)

            return True

    def remove_rule(
        self,
        flag_name: str,
        rule_index: int,
    ) -> bool:
        """Remove a rule from a flag by index."""
        with self._lock:
            flag = self._flags.get(flag_name)
            if not flag or rule_index >= len(flag.rules):
                return False

            del flag.rules[rule_index]
            flag.updated_at = datetime.now()
            self._notify_change(flag_name, flag)

            return True

    def evaluate(
        self,
        flag_name: str,
        user: Optional[User] = None,
        user_id: Optional[str] = None,
    ) -> FlagEvaluation:
        """
        Evaluate a feature flag for a user.

        Args:
            flag_name: Name of the flag to evaluate.
            user: Optional User object with attributes.
            user_id: Optional user ID string.

        Returns:
            FlagEvaluation with the result.
        """
        with self._lock:
            flag = self._flags.get(flag_name)

            if not flag:
                return FlagEvaluation(
                    flag_name=flag_name,
                    value=None,
                    reason="flag_not_found",
                )

            if not flag.enabled:
                return FlagEvaluation(
                    flag_name=flag_name,
                    value=flag.default_value,
                    reason="flag_disabled",
                )

            if not flag.rules:
                return FlagEvaluation(
                    flag_name=flag_name,
                    value=flag.default_value,
                    reason="no_rules",
                )

            effective_user = user
            if user_id and not effective_user:
                effective_user = User(id=user_id)

            for i, rule in enumerate(flag.rules):
                if rule.matches(effective_user, user_id):
                    return FlagEvaluation(
                        flag_name=flag_name,
                        value=rule.value,
                        matched_rule=str(i),
                        reason="rule_match",
                    )

            return FlagEvaluation(
                flag_name=flag_name,
                value=flag.default_value,
                reason="default",
            )

    def is_enabled(
        self,
        flag_name: str,
        user: Optional[User] = None,
        user_id: Optional[str] = None,
    ) -> bool:
        """
        Check if a boolean flag is enabled for a user.

        Args:
            flag_name: Name of the flag.
            user: Optional User object.
            user_id: Optional user ID.

        Returns:
            True if flag evaluates to True.
        """
        result = self.evaluate(flag_name, user, user_id)
        return bool(result.value)

    def list_flags(self, tag: Optional[str] = None) -> List[FeatureFlag]:
        """List all flags, optionally filtered by tag."""
        with self._lock:
            if tag:
                return [f for f in self._flags.values() if tag in f.tags]
            return list(self._flags.values())

    def on_change(
        self,
        callback: Callable[[str, FeatureFlag], None],
    ) -> None:
        """Register a callback for flag changes."""
        self._change_callbacks.append(callback)

    def _notify_change(self, name: str, flag: FeatureFlag) -> None:
        """Notify registered callbacks of a change."""
        for callback in self._change_callbacks:
            try:
                callback(name, flag)
            except Exception:
                pass

    def export_config(self) -> Dict[str, Any]:
        """Export all flags as configuration dictionary."""
        with self._lock:
            return {
                "env": self.env,
                "flags": {name: flag.to_dict() for name, flag in self._flags.items()},
            }

    def import_config(self, config: Dict[str, Any]) -> int:
        """
        Import flags from configuration.

        Args:
            config: Configuration dictionary from export_config.

        Returns:
            Number of flags imported.
        """
        with self._lock:
            flags_data = config.get("flags", {})
            imported = 0

            for name, flag_data in flags_data.items():
                rules = []
                for rule_data in flag_data.get("rules", []):
                    rules.append(TargetingRule(
                        rule_type=RuleType(rule_data["rule_type"]),
                        value=rule_data["value"],
                        percentage=rule_data.get("percentage"),
                        user_ids=rule_data.get("user_ids"),
                        attribute_conditions=rule_data.get("attribute_conditions"),
                        description=rule_data.get("description"),
                    ))

                flag_data = flag_data.copy()
                flag_data["rules"] = rules
                if "created_at" in flag_data and isinstance(flag_data["created_at"], str):
                    flag_data["created_at"] = datetime.fromisoformat(flag_data["created_at"])
                if "updated_at" in flag_data and isinstance(flag_data["updated_at"], str):
                    flag_data["updated_at"] = datetime.fromisoformat(flag_data["updated_at"])

                flag = FeatureFlag(**flag_data)
                self._flags[name] = flag
                imported += 1

            return imported


_default_manager: Optional[FeatureFlagManager] = None


def get_manager(env: str = "production") -> FeatureFlagManager:
    """Get or create the default feature flag manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = FeatureFlagManager(env)
    return _default_manager


def feature_flag_create_action(
    name: str,
    flag_type: str = "boolean",
    default_value: Any = False,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new feature flag."""
    flag_type_enum = FlagType(flag_type.lower())
    manager = get_manager()
    flag = manager.create_flag(name, flag_type_enum, default_value, description)
    return flag.to_dict()


def feature_flag_evaluate_action(
    flag_name: str,
    user_id: Optional[str] = None,
    attributes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate a feature flag for a user."""
    manager = get_manager()
    user = None
    if user_id or attributes:
        user = User(id=user_id or "anonymous", attributes=attributes or {})
    result = manager.evaluate(flag_name, user, user_id)
    return result.to_dict()


def feature_flag_is_enabled_action(
    flag_name: str,
    user_id: Optional[str] = None,
) -> bool:
    """Check if a feature flag is enabled."""
    manager = get_manager()
    return manager.is_enabled(flag_name, user_id=user_id)


def feature_flag_update_action(
    flag_name: str,
    enabled: Optional[bool] = None,
    default_value: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """Update a feature flag."""
    manager = get_manager()
    flag = manager.update_flag(flag_name, enabled, default_value)
    return flag.to_dict() if flag else None
