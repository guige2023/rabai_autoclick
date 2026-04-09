"""Feature flag action for managing feature toggles.

Provides feature flag evaluation, targeting rules,
and gradual rollout support.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class FlagState(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    GRADUAL = "gradual"


@dataclass
class TargetingRule:
    attribute: str
    operator: str
    value: Any
    enabled: bool = True


@dataclass
class FeatureFlag:
    name: str
    state: FlagState
    description: str = ""
    rollout_percentage: float = 100.0
    targeting_rules: list[TargetingRule] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    modified_at: float = field(default_factory=time.time)


class FeatureFlagAction:
    """Manage feature flags with targeting and rollouts.

    Args:
        default_state: Default state for new flags.
        enable_analytics: Enable flag evaluation analytics.
    """

    def __init__(
        self,
        default_state: FlagState = FlagState.DISABLED,
        enable_analytics: bool = True,
    ) -> None:
        self._flags: dict[str, FeatureFlag] = {}
        self._default_state = default_state
        self._enable_analytics = enable_analytics
        self._evaluation_log: list[dict[str, Any]] = []
        self._change_listeners: list[Callable[[str, FlagState], None]] = []

    def create_flag(
        self,
        name: str,
        state: FlagState = FlagState.DISABLED,
        description: str = "",
        rollout_percentage: float = 100.0,
    ) -> bool:
        """Create a new feature flag.

        Args:
            name: Flag name.
            state: Initial flag state.
            description: Flag description.
            rollout_percentage: Rollout percentage (0-100).

        Returns:
            True if created successfully.
        """
        if name in self._flags:
            logger.warning(f"Flag already exists: {name}")
            return False

        flag = FeatureFlag(
            name=name,
            state=state,
            description=description,
            rollout_percentage=rollout_percentage,
        )
        self._flags[name] = flag
        logger.debug(f"Created feature flag: {name}")
        return True

    def enable(self, name: str) -> bool:
        """Enable a feature flag.

        Args:
            name: Flag name.

        Returns:
            True if flag was found and enabled.
        """
        flag = self._flags.get(name)
        if not flag:
            return False

        flag.state = FlagState.ENABLED
        flag.modified_at = time.time()

        for listener in self._change_listeners:
            try:
                listener(name, flag.state)
            except Exception as e:
                logger.error(f"Change listener error: {e}")

        return True

    def disable(self, name: str) -> bool:
        """Disable a feature flag.

        Args:
            name: Flag name.

        Returns:
            True if flag was found and disabled.
        """
        flag = self._flags.get(name)
        if not flag:
            return False

        flag.state = FlagState.DISABLED
        flag.modified_at = time.time()

        for listener in self._change_listeners:
            try:
                listener(name, flag.state)
            except Exception as e:
                logger.error(f"Change listener error: {e}")

        return True

    def evaluate(
        self,
        name: str,
        user_id: Optional[str] = None,
        attributes: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Evaluate a feature flag for a user.

        Args:
            name: Flag name.
            user_id: Optional user ID for targeting.
            attributes: Optional user attributes.

        Returns:
            True if flag is enabled for the user.
        """
        flag = self._flags.get(name)
        if not flag:
            return False

        result = self._evaluate_flag(flag, user_id, attributes or {})

        if self._enable_analytics:
            self._log_evaluation(name, user_id, result, attributes)

        return result

    def _evaluate_flag(
        self,
        flag: FeatureFlag,
        user_id: Optional[str],
        attributes: dict[str, Any],
    ) -> bool:
        """Evaluate a flag based on its configuration.

        Args:
            flag: Feature flag.
            user_id: User ID.
            attributes: User attributes.

        Returns:
            True if flag is enabled.
        """
        if flag.state == FlagState.DISABLED:
            return False

        if flag.state == FlagState.ENABLED:
            return True

        if flag.state == FlagState.GRADUAL:
            for rule in flag.targeting_rules:
                if self._matches_rule(rule, attributes):
                    return rule.enabled

            if user_id:
                return self._in_rollout(user_id, flag.rollout_percentage)
            return flag.rollout_percentage >= 100.0

        return False

    def _matches_rule(self, rule: TargetingRule, attributes: dict[str, Any]) -> bool:
        """Check if attributes match a targeting rule.

        Args:
            rule: Targeting rule.
            attributes: User attributes.

        Returns:
            True if matches.
        """
        value = attributes.get(rule.attribute)
        if value is None:
            return False

        if rule.operator == "equals":
            return value == rule.value
        elif rule.operator == "not_equals":
            return value != rule.value
        elif rule.operator == "contains":
            return rule.value in str(value)
        elif rule.operator == "gt":
            return value > rule.value
        elif rule.operator == "lt":
            return value < rule.value

        return False

    def _in_rollout(self, user_id: str, percentage: float) -> bool:
        """Check if user is in rollout percentage.

        Args:
            user_id: User ID.
            percentage: Rollout percentage (0-100).

        Returns:
            True if user is in rollout.
        """
        if percentage >= 100.0:
            return True
        if percentage <= 0.0:
            return False

        hash_value = hash(user_id) % 100
        return hash_value < percentage

    def add_targeting_rule(
        self,
        name: str,
        attribute: str,
        operator: str,
        value: Any,
        enabled: bool = True,
    ) -> bool:
        """Add a targeting rule to a flag.

        Args:
            name: Flag name.
            attribute: Attribute to match.
            operator: Comparison operator.
            value: Value to compare.
            enabled: Result when rule matches.

        Returns:
            True if rule was added.
        """
        flag = self._flags.get(name)
        if not flag:
            return False

        rule = TargetingRule(
            attribute=attribute,
            operator=operator,
            value=value,
            enabled=enabled,
        )
        flag.targeting_rules.append(rule)
        flag.modified_at = time.time()
        return True

    def _log_evaluation(
        self,
        flag_name: str,
        user_id: Optional[str],
        result: bool,
        attributes: Optional[dict[str, Any]],
    ) -> None:
        """Log a flag evaluation.

        Args:
            flag_name: Flag name.
            user_id: User ID.
            result: Evaluation result.
            attributes: User attributes.
        """
        self._evaluation_log.append({
            "flag": flag_name,
            "user_id": user_id,
            "result": result,
            "attributes": attributes,
            "timestamp": time.time(),
        })

        if len(self._evaluation_log) > 10000:
            self._evaluation_log.pop(0)

    def register_change_listener(
        self,
        listener: Callable[[str, FlagState], None],
    ) -> None:
        """Register a listener for flag state changes.

        Args:
            listener: Callback function.
        """
        self._change_listeners.append(listener)

    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get a feature flag by name.

        Args:
            name: Flag name.

        Returns:
            Feature flag or None.
        """
        return self._flags.get(name)

    def get_all_flags(self) -> list[FeatureFlag]:
        """Get all feature flags.

        Returns:
            List of feature flags.
        """
        return list(self._flags.values())

    def get_evaluation_log(self, limit: int = 100) -> list[dict[str, Any]]:
        """Get flag evaluation log.

        Args:
            limit: Maximum entries.

        Returns:
            List of evaluations (newest first).
        """
        return self._evaluation_log[-limit:][::-1]

    def delete_flag(self, name: str) -> bool:
        """Delete a feature flag.

        Args:
            name: Flag name.

        Returns:
            True if deleted.
        """
        if name in self._flags:
            del self._flags[name]
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Get feature flag statistics.

        Returns:
            Dictionary with stats.
        """
        total = len(self._flags)
        enabled = sum(1 for f in self._flags.values() if f.state == FlagState.ENABLED)
        disabled = sum(1 for f in self._flags.values() if f.state == FlagState.DISABLED)
        gradual = sum(1 for f in self._flags.values() if f.state == FlagState.GRADUAL)

        return {
            "total_flags": total,
            "enabled": enabled,
            "disabled": disabled,
            "gradual_rollout": gradual,
            "evaluations": len(self._evaluation_log),
        }
