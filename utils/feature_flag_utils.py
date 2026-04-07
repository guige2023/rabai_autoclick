"""
Feature flag utilities for progressive rollouts and A/B testing.

Provides flag evaluation, percentage rollouts, user targeting,
variant assignment, and flag change subscriptions.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class FlagVariation(Enum):
    """Feature flag variation types."""
    CONTROL = "control"
    TREATMENT = "treatment"
    A = "a"
    B = "b"


@dataclass
class FlagRule:
    """A targeting rule for a feature flag."""
    name: str
    percentage: float = 100.0
    variations: list[str] = field(default_factory=lambda: ["control", "treatment"])
    constraints: dict[str, Any] = field(default_factory=dict)

    def matches(self, context: dict[str, Any]) -> bool:
        """Check if context matches this rule's constraints."""
        for key, expected in self.constraints.items():
            actual = context.get(key)
            if isinstance(expected, list):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    key: str
    description: str = ""
    enabled: bool = True
    default_variation: str = "control"
    rules: list[FlagRule] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class FlagEvaluation:
    """Result of evaluating a feature flag."""
    flag_key: str
    variation: str
    reason: str
    context: dict[str, Any] = field(default_factory=dict)
    evaluated_at: float = field(default_factory=time.time)


class FeatureFlagStore:
    """In-memory store for feature flags."""

    def __init__(self) -> None:
        self._flags: dict[str, FeatureFlag] = {}
        self._change_listeners: list[Callable[[str, FeatureFlag], None]] = []

    def add_flag(self, flag: FeatureFlag) -> None:
        """Add a feature flag."""
        self._flags[flag.key] = flag
        logger.info("Added feature flag: %s", flag.key)

    def remove_flag(self, key: str) -> None:
        """Remove a feature flag."""
        if key in self._flags:
            del self._flags[key]

    def get_flag(self, key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        return self._flags.get(key)

    def list_flags(self) -> list[FeatureFlag]:
        """List all feature flags."""
        return list(self._flags.values())

    def update_flag(self, key: str, **kwargs: Any) -> bool:
        """Update a feature flag's properties."""
        flag = self._flags.get(key)
        if not flag:
            return False
        for k, v in kwargs.items():
            if hasattr(flag, k):
                setattr(flag, k, v)
        for listener in self._change_listeners:
            listener(key, flag)
        return True

    def on_change(self, callback: Callable[[str, FeatureFlag], None]) -> None:
        """Subscribe to flag changes."""
        self._change_listeners.append(callback)


class FeatureFlagEvaluator:
    """Evaluates feature flags for a given context."""

    def __init__(self, store: Optional[FeatureFlagStore] = None) -> None:
        self.store = store or FeatureFlagStore()
        self._cache: dict[str, tuple[float, FlagEvaluation]] = {}
        self._cache_ttl = 60.0

    def evaluate(
        self,
        flag_key: str,
        context: Optional[dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> FlagEvaluation:
        """Evaluate a feature flag for the given context."""
        cache_key = f"{flag_key}:{self._context_hash(context or {})}"
        now = time.time()

        if use_cache and cache_key in self._cache:
            cached_time, cached_eval = self._cache[cache_key]
            if now - cached_time < self._cache_ttl:
                return cached_eval

        flag = self.store.get_flag(flag_key)
        if not flag:
            return FlagEvaluation(
                flag_key=flag_key,
                variation="control",
                reason="flag_not_found",
                context=context or {},
            )

        if not flag.enabled:
            return FlagEvaluation(
                flag_key=flag_key,
                variation=flag.default_variation,
                reason="flag_disabled",
                context=context or {},
            )

        for rule in flag.rules:
            if rule.matches(context or {}):
                variation = self._select_variation(flag_key, rule, context or {})
                result = FlagEvaluation(
                    flag_key=flag_key,
                    variation=variation,
                    reason=f"rule:{rule.name}",
                    context=context or {},
                )
                self._cache[cache_key] = (now, result)
                return result

        default_variation = flag.default_variation
        result = FlagEvaluation(
            flag_key=flag_key,
            variation=default_variation,
            reason="default",
            context=context or {},
        )
        self._cache[cache_key] = (now, result)
        return result

    def is_enabled(
        self,
        flag_key: str,
        context: Optional[dict[str, Any]] = None,
        default: bool = False,
    ) -> bool:
        """Check if a flag is enabled for the given context."""
        eval_result = self.evaluate(flag_key, context)
        if eval_result.reason == "flag_not_found":
            return default
        return eval_result.variation in ("treatment", "b")

    def _select_variation(
        self,
        flag_key: str,
        rule: FlagRule,
        context: dict[str, Any],
    ) -> str:
        """Select a variation based on percentage rollout."""
        if rule.percentage >= 100:
            return rule.variations[1] if len(rule.variations) > 1 else rule.variations[0]
        if rule.percentage <= 0:
            return rule.variations[0]

        bucket = self._get_bucket(flag_key, context)
        threshold = rule.percentage / 100.0
        idx = 1 if bucket < threshold else 0
        return rule.variations[min(idx, len(rule.variations) - 1)]

    def _get_bucket(self, flag_key: str, context: dict[str, Any]) -> float:
        """Get a deterministic bucket value [0, 1) for a flag+context."""
        user_id = context.get("user_id", context.get("session_id", "anonymous"))
        seed = f"{flag_key}:{user_id}"
        hash_val = hashlib.sha256(seed.encode()).hexdigest()
        bucket = int(hash_val[:8], 16) / (16 ** 8)
        return bucket

    def _context_hash(self, context: dict[str, Any]) -> str:
        """Create a hash of the evaluation context."""
        stable_context = {k: v for k, v in sorted(context.items()) if v is not None}
        import json
        return hashlib.md5(json.dumps(stable_context, sort_keys=True).encode()).hexdigest()

    def clear_cache(self) -> None:
        """Clear the evaluation cache."""
        self._cache.clear()


class ABTestManager:
    """Manages A/B tests using feature flags."""

    def __init__(self, evaluator: Optional[FeatureFlagEvaluator] = None) -> None:
        self.evaluator = evaluator or FeatureFlagEvaluator()
        self._assignments: dict[str, dict[str, int]] = {}

    def assign(
        self,
        experiment_key: str,
        user_id: str,
        context: Optional[dict[str, Any]] = None,
    ) -> str:
        """Assign a user to an experiment variant."""
        ctx = dict(context or {})
        ctx["user_id"] = user_id
        result = self.evaluator.evaluate(experiment_key, ctx)
        self._record_assignment(experiment_key, user_id, result.variation)
        return result.variation

    def _record_assignment(self, experiment: str, user_id: str, variation: str) -> None:
        """Record a user assignment for analytics."""
        if experiment not in self._assignments:
            self._assignments[experiment] = {}
        self._assignments[experiment][user_id] = self._assignments[experiment].get(user_id, 0)

    def get_distribution(self, experiment_key: str) -> dict[str, float]:
        """Get the distribution of variants for an experiment."""
        if experiment_key not in self._assignments:
            return {}
        counts = self._assignments[experiment_key]
        total = len(counts)
        if total == 0:
            return {}
        distribution = {}
        for variant in set(counts.values()):
            distribution[variant] = round(sum(1 for v in counts.values() if v == variant) / total * 100, 2)
        return distribution
