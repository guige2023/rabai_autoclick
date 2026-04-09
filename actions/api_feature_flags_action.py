"""
API Feature Flags Module.

Manages feature flags for API routing and behavior control.
Supports gradual rollouts, A/B testing, user targeting,
and percentage-based distribution.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class FlagType(Enum):
    """Feature flag types."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"
    PERCENTAGE = "percentage"


@dataclass
class TargetingRule:
    """Rule for targeted flag evaluation."""
    attribute: str
    operator: str
    value: Any
    flag_value: Any


@dataclass
class FeatureFlag:
    """Represents a feature flag."""
    name: str
    flag_type: FlagType
    default_value: Any
    enabled: bool = True
    rollout_percentage: float = 100.0
    targeting_rules: list[TargetingRule] = field(default_factory=list)
    variants: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class FlagContext:
    """Context for flag evaluation."""
    user_id: Optional[str] = None
    account_id: Optional[str] = None
    email: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    attributes: dict[str, Any] = field(default_factory=dict)


class FeatureFlagManager:
    """
    Manages feature flags for API behavior control.

    Supports boolean, string, number, JSON, and percentage-based flags
    with targeting rules for user-specific rollouts.

    Example:
        manager = FeatureFlagManager()
        manager.create_flag("new_api_version", FlagType.BOOLEAN, default_value=False)
        manager.add_targeting_rule("new_api_version", TargetingRule("plan", "==", "pro", True))
        result = manager.get_flag("new_api_version", context)
    """

    def __init__(self, seed: Optional[int] = None) -> None:
        self._flags: dict[str, FeatureFlag] = {}
        self._seed = seed
        self._overrides: dict[str, dict[str, Any]] = {}

    def create_flag(
        self,
        name: str,
        flag_type: FlagType,
        default_value: Any,
        enabled: bool = True,
        rollout_percentage: float = 100.0
    ) -> FeatureFlag:
        """
        Create a new feature flag.

        Args:
            name: Flag name
            flag_type: Type of flag
            default_value: Default value when flag is disabled or no match
            enabled: Whether flag is globally enabled
            rollout_percentage: Percentage of users to enable for (0-100)

        Returns:
            Created FeatureFlag
        """
        flag = FeatureFlag(
            name=name,
            flag_type=flag_type,
            default_value=default_value,
            enabled=enabled,
            rollout_percentage=rollout_percentage
        )
        self._flags[name] = flag
        return flag

    def update_flag(
        self,
        name: str,
        **updates: Any
    ) -> Optional[FeatureFlag]:
        """Update an existing feature flag."""
        flag = self._flags.get(name)
        if not flag:
            return None

        for key, value in updates.items():
            if hasattr(flag, key):
                setattr(flag, key, value)
        flag.updated_at = time.time()
        return flag

    def delete_flag(self, name: str) -> bool:
        """Delete a feature flag."""
        return self._flags.pop(name, None) is not None

    def add_targeting_rule(
        self,
        flag_name: str,
        rule: TargetingRule
    ) -> None:
        """Add a targeting rule to a flag."""
        flag = self._flags.get(flag_name)
        if not flag:
            raise ValueError(f"Flag {flag_name} not found")
        flag.targeting_rules.append(rule)
        flag.updated_at = time.time()

    def set_variant(
        self,
        flag_name: str,
        variant_name: str,
        value: Any
    ) -> None:
        """Add a variant to a flag for A/B testing."""
        flag = self._flags.get(flag_name)
        if not flag:
            raise ValueError(f"Flag {flag_name} not found")
        flag.variants[variant_name] = value

    def override(
        self,
        flag_name: str,
        user_id: str,
        value: Any
    ) -> None:
        """Override flag value for a specific user."""
        if flag_name not in self._overrides:
            self._overrides[flag_name] = {}
        self._overrides[flag_name][user_id] = value

    def clear_override(self, flag_name: str, user_id: str) -> bool:
        """Remove user override for a flag."""
        overrides = self._overrides.get(flag_name, {})
        return overrides.pop(user_id, None) is not None

    def get_flag(
        self,
        name: str,
        context: Optional[FlagContext] = None
    ) -> Any:
        """
        Evaluate a feature flag for a given context.

        Args:
            name: Flag name
            context: Evaluation context with user info

        Returns:
            Flag value based on targeting rules and rollout
        """
        flag = self._flags.get(name)
        if not flag:
            return None

        if not flag.enabled:
            return flag.default_value

        if context and self._has_user_override(name, context):
            return self._get_user_override(name, context)

        if self._matches_targeting_rules(flag, context):
            return True

        if context and flag.rollout_percentage < 100.0:
            if not self._is_in_rollout(flag, context):
                return flag.default_value

        if flag.variants and context:
            return self._get_variant(flag, context)

        return flag.default_value

    def _has_user_override(self, flag_name: str, context: FlagContext) -> bool:
        """Check if user has an override."""
        if not context or not context.user_id:
            return False
        overrides = self._overrides.get(flag_name, {})
        return context.user_id in overrides

    def _get_user_override(self, flag_name: str, context: FlagContext) -> Any:
        """Get user-specific override value."""
        overrides = self._overrides.get(flag_name, {})
        return overrides.get(context.user_id)

    def _matches_targeting_rules(
        self,
        flag: FeatureFlag,
        context: Optional[FlagContext]
    ) -> bool:
        """Check if context matches any targeting rule."""
        if not context or not flag.targeting_rules:
            return False

        attributes = {
            "user_id": context.user_id,
            "account_id": context.account_id,
            "email": context.email,
            "ip_address": context.ip_address,
            **context.attributes
        }

        for rule in flag.targeting_rules:
            if rule.attribute not in attributes:
                continue

            attr_value = attributes[rule.attribute]

            if rule.operator == "==":
                if attr_value == rule.value:
                    return True
            elif rule.operator == "!=":
                if attr_value != rule.value:
                    return True
            elif rule.operator == "in":
                if attr_value in rule.value:
                    return True
            elif rule.operator == "contains":
                if rule.value in str(attr_value):
                    return True
            elif rule.operator == "startswith":
                if str(attr_value).startswith(str(rule.value)):
                    return True
            elif rule.operator == "endswith":
                if str(attr_value).endswith(str(rule.value)):
                    return True

        return False

    def _is_in_rollout(
        self,
        flag: FeatureFlag,
        context: FlagContext
    ) -> bool:
        """Determine if user is in rollout percentage."""
        if not context or not context.user_id:
            return random() * 100 < flag.rollout_percentage

        user_hash = hashlib.sha256(
            f"{flag.name}:{context.user_id}".encode()
        ).hexdigest()

        bucket = int(user_hash[:8], 16) % 10000
        threshold = flag.rollout_percentage * 100

        return bucket < threshold

    def _get_variant(
        self,
        flag: FeatureFlag,
        context: FlagContext
    ) -> Any:
        """Get variant for A/B testing."""
        if not context or not context.user_id:
            return flag.default_value

        variant_hash = hashlib.sha256(
            f"{flag.name}:variant:{context.user_id}".encode()
        ).hexdigest()

        bucket = int(variant_hash[:8], 16) % len(flag.variants)
        variant_name = list(flag.variants.keys())[bucket]

        return flag.variants[variant_name]

    def list_flags(self, enabled_only: bool = False) -> list[FeatureFlag]:
        """List all feature flags."""
        flags = self._flags.values()
        if enabled_only:
            flags = [f for f in flags if f.enabled]
        return list(flags)

    def get_flag_info(self, name: str) -> Optional[dict[str, Any]]:
        """Get flag information as dict."""
        flag = self._flags.get(name)
        if not flag:
            return None
        return {
            "name": flag.name,
            "type": flag.flag_type.value,
            "enabled": flag.enabled,
            "default": flag.default_value,
            "rollout_percentage": flag.rollout_percentage,
            "variants": list(flag.variants.keys()),
            "rules_count": len(flag.targeting_rules),
            "created_at": flag.created_at,
            "updated_at": flag.updated_at
        }
