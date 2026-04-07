"""
Feature Flags Utilities

Provides feature flag management with targeting rules,
percentage rollouts, and user targeting.
"""

from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class FlagType(Enum):
    """Type of feature flag."""
    BOOLEAN = auto()
    STRING = auto()
    NUMBER = auto()
    JSON = auto()


@dataclass
class FlagRule:
    """A targeting rule for a feature flag."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    name: str = ""
    condition: Callable[[dict[str, Any]], bool] | None = None
    percentage: float = 100.0  # 0-100
    value: Any = None
    priority: int = 0

    def matches(self, context: dict[str, Any]) -> bool:
        """Check if this rule matches the context."""
        if self.condition:
            return self.condition(context)
        return True

    def is_enabled_for_percentage(self, user_id: str) -> bool:
        """Check if user falls within percentage rollout."""
        if self.percentage >= 100.0:
            return True
        if self.percentage <= 0.0:
            return False

        # Hash user_id for consistent bucketing
        hash_input = f"{user_id}:{self.id}".encode()
        hash_value = int(hashlib.md5(hash_input).hexdigest(), 16)
        bucket = (hash_value % 10000) / 100.0  # 0-100

        return bucket < self.percentage


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    id: str
    name: str
    flag_type: FlagType = FlagType.BOOLEAN
    enabled: bool = False
    default_value: Any = False
    rules: list[FlagRule] = field(default_factory=list)
    rollout_percentage: float = 0.0  # Overall percentage rollout
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class FlagEvaluationResult:
    """Result of flag evaluation."""
    flag_id: str
    value: Any
    matched_rule: str | None = None
    source: str = "default"  # "default", "rule", "rollout"
    reason: str = ""


class FeatureFlagProvider(ABC):
    """Abstract provider for feature flag storage."""

    @abstractmethod
    def get_flag(self, flag_id: str) -> FeatureFlag | None:
        """Get a feature flag by ID."""
        pass

    @abstractmethod
    def list_flags(self) -> list[FeatureFlag]:
        """List all feature flags."""
        pass

    @abstractmethod
    def set_flag(self, flag: FeatureFlag) -> None:
        """Set a feature flag."""
        pass

    @abstractmethod
    def delete_flag(self, flag_id: str) -> bool:
        """Delete a feature flag."""
        pass


class InMemoryFlagProvider(FeatureFlagProvider):
    """In-memory feature flag provider."""

    def __init__(self):
        self._flags: dict[str, FeatureFlag] = {}
        self._lock = threading.RLock()

    def get_flag(self, flag_id: str) -> FeatureFlag | None:
        with self._lock:
            return copy.deepcopy(self._flags.get(flag_id))

    def list_flags(self) -> list[FeatureFlag]:
        with self._lock:
            return [copy.deepcopy(f) for f in self._flags.values()]

    def set_flag(self, flag: FeatureFlag) -> None:
        with self._lock:
            self._flags[flag.id] = copy.deepcopy(flag)

    def delete_flag(self, flag_id: str) -> bool:
        with self._lock:
            if flag_id in self._flags:
                del self._flags[flag_id]
                return True
            return False


class FeatureFlagManager:
    """
    Manages feature flags and evaluates them for users.
    """

    def __init__(self, provider: FeatureFlagProvider | None = None):
        self._provider = provider or InMemoryFlagProvider()
        self._metrics: dict[str, dict[str, int]] = {}
        self._lock = threading.RLock()

    def create_flag(
        self,
        name: str,
        flag_type: FlagType = FlagType.BOOLEAN,
        default_value: Any = False,
        enabled: bool = False,
    ) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            id=f"flag_{uuid.uuid4().hex[:8]}",
            name=name,
            flag_type=flag_type,
            enabled=enabled,
            default_value=default_value,
        )
        self._provider.set_flag(flag)
        return flag

    def get_flag(self, flag_id: str) -> FeatureFlag | None:
        """Get a feature flag."""
        return self._provider.get_flag(flag_id)

    def update_flag(self, flag: FeatureFlag) -> None:
        """Update a feature flag."""
        flag.updated_at = time.time()
        self._provider.set_flag(flag)

    def delete_flag(self, flag_id: str) -> bool:
        """Delete a feature flag."""
        return self._provider.delete_flag(flag_id)

    def list_flags(self) -> list[FeatureFlag]:
        """List all feature flags."""
        return self._provider.list_flags()

    def evaluate(
        self,
        flag_id: str,
        user_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> FlagEvaluationResult:
        """
        Evaluate a feature flag for a user.

        Args:
            flag_id: The flag to evaluate.
            user_id: Optional user ID for percentage rollouts.
            context: Optional context for rule evaluation.

        Returns:
            FlagEvaluationResult with the evaluated value.
        """
        context = context or {}
        if user_id:
            context["user_id"] = user_id

        flag = self._provider.get_flag(flag_id)

        if not flag:
            return FlagEvaluationResult(
                flag_id=flag_id,
                value=None,
                reason="flag_not_found",
            )

        # Update metrics
        with self._lock:
            if flag_id not in self._metrics:
                self._metrics[flag_id] = {"true": 0, "false": 0, "errors": 0}
            self._metrics[flag_id]["total"] = self._metrics[flag_id].get("total", 0) + 1

        # Check if flag is enabled
        if not flag.enabled:
            self._record_metric(flag_id, flag.default_value)
            return FlagEvaluationResult(
                flag_id=flag_id,
                value=flag.default_value,
                source="default",
                reason="flag_disabled",
            )

        # Evaluate rules in priority order
        sorted_rules = sorted(flag.rules, key=lambda r: r.priority)

        for rule in sorted_rules:
            if rule.matches(context) and rule.is_enabled_for_percentage(user_id or ""):
                self._record_metric(flag_id, rule.value)
                return FlagEvaluationResult(
                    flag_id=flag_id,
                    value=rule.value,
                    matched_rule=rule.id,
                    source="rule",
                    reason=f"matched_rule:{rule.name}",
                )

        # Check global rollout percentage
        if user_id and flag.rollout_percentage > 0:
            hash_input = f"{user_id}:{flag.id}".encode()
            hash_value = int(hashlib.md5(hash_input).hexdigest(), 16)
            bucket = (hash_value % 10000) / 100.0

            if bucket < flag.rollout_percentage:
                self._record_metric(flag_id, True)
                return FlagEvaluationResult(
                    flag_id=flag_id,
                    value=True,
                    source="rollout",
                    reason=f"rollout_{flag.rollout_percentage}%",
                )

        self._record_metric(flag_id, flag.default_value)
        return FlagEvaluationResult(
            flag_id=flag_id,
            value=flag.default_value,
            source="default",
            reason="no_match",
        )

    def is_enabled(
        self,
        flag_id: str,
        user_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        """Check if a flag is enabled for a user."""
        result = self.evaluate(flag_id, user_id, context)
        return bool(result.value)

    def _record_metric(self, flag_id: str, value: Any) -> None:
        """Record a metric for flag evaluation."""
        with self._lock:
            if flag_id in self._metrics:
                key = "true" if value else "false"
                self._metrics[flag_id][key] += 1

    def get_metrics(self, flag_id: str | None = None) -> dict[str, dict[str, int]]:
        """Get evaluation metrics."""
        with self._lock:
            if flag_id:
                return {flag_id: copy.copy(self._metrics.get(flag_id, {}))}
            return copy.copy(self._metrics)

    def add_rule(
        self,
        flag_id: str,
        rule: FlagRule,
    ) -> bool:
        """Add a rule to a flag."""
        flag = self._provider.get_flag(flag_id)
        if not flag:
            return False

        flag.rules.append(rule)
        self.update_flag(flag)
        return True

    def set_rollout(
        self,
        flag_id: str,
        percentage: float,
    ) -> bool:
        """Set the rollout percentage for a flag."""
        flag = self._provider.get_flag(flag_id)
        if not flag:
            return False

        flag.rollout_percentage = max(0.0, min(100.0, percentage))
        self.update_flag(flag)
        return True


def create_flag_manager(
    provider: FeatureFlagProvider | None = None,
) -> FeatureFlagManager:
    """Create a configured flag manager."""
    return FeatureFlagManager(provider)
