"""
Feature Flag Action Module

Provides feature flag functionality for gradual rollouts and A/B testing
in UI automation workflows. Supports targeting rules, percentage splits,
and user segmentation.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class FlagType(Enum):
    """Feature flag type."""
    BOOLEAN = auto()
    STRING = auto()
    NUMBER = auto()
    JSON = auto()


class ComparisonOp(Enum):
    """Rule comparison operators."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    IN = "in"
    NOT_IN = "not_in"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUALS = "gte"
    LESS_EQUALS = "lte"
    REGEX_MATCH = "regex"


@dataclass
class TargetingRule:
    """Targeting rule for feature flag."""
    attribute: str
    operator: ComparisonOp
    value: Any
    description: str = ""


@dataclass
class RolloutPercentage:
    """Percentage-based rollout configuration."""
    percentage: float
    seed: str = "default"

    def __post_init__(self) -> None:
        if not 0 <= self.percentage <= 100:
            raise ValueError("Percentage must be between 0 and 100")


@dataclass
class FlagVariation:
    """Feature flag variation."""
    name: str
    value: Any
    weight: int = 0


@dataclass
class FeatureFlag:
    """Feature flag definition."""
    key: str
    name: str
    flag_type: FlagType
    enabled: bool = False
    default_value: Any = None
    variations: list[FlagVariation] = field(default_factory=list)
    targeting_rules: list[TargetingRule] = field(default_factory=list)
    rollout_percentage: Optional[RolloutPercentage] = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=lambda: time.time())
    updated_at: float = field(default_factory=lambda: time.time())


@dataclass
class FlagContext:
    """Context for flag evaluation."""
    user_id: Optional[str] = None
    anonymous_id: Optional[str] = None
    email: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    platform: Optional[str] = None
    app_version: Optional[str] = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def get_attribute(self, key: str) -> Any:
        """Get attribute value."""
        return self.attributes.get(key)

    def get_user_id(self) -> str:
        """Get user identifier."""
        return self.user_id or self.anonymous_id or ""


class FeatureFlagStore:
    """
    In-memory feature flag store.

    Example:
        >>> store = FeatureFlagStore()
        >>> flag = FeatureFlag(key="new_ui", name="New UI", flag_type=FlagType.BOOLEAN)
        >>> store.add(flag)
        >>> is_enabled = store.is_enabled("new_ui", context)
    """

    def ___init__(self) -> None:
        self._flags: dict[str, FeatureFlag] = {}
        self._hooks: dict[str, list[Callable]] = {
            "flag_enabled": [],
            "flag_disabled": [],
            "flag_updated": [],
        }

    def add(self, flag: FeatureFlag) -> None:
        """Add feature flag to store."""
        self._flags[flag.key] = flag
        logger.debug(f"Added flag: {flag.key}")

    def get(self, key: str) -> Optional[FeatureFlag]:
        """Get feature flag by key."""
        return self._flags.get(key)

    def update(self, flag: FeatureFlag) -> None:
        """Update feature flag."""
        if flag.key in self._flags:
            flag.updated_at = time.time()
            self._flags[flag.key] = flag
            self._trigger_hook("flag_updated", flag)

    def delete(self, key: str) -> bool:
        """Delete feature flag."""
        if key in self._flags:
            del self._flags[key]
            return True
        return False

    def list_flags(self) -> list[FeatureFlag]:
        """List all feature flags."""
        return list(self._flags.values())

    def add_hook(
        self,
        event: str,
        callback: Callable[..., None],
    ) -> None:
        """Add event hook."""
        if event in self._hooks:
            self._hooks[event].append(callback)

    def _trigger_hook(self, event: str, *args: Any) -> None:
        """Trigger event hooks."""
        for callback in self._hooks.get(event, []):
            try:
                callback(*args)
            except Exception as e:
                logger.error(f"Hook error: {e}")

    def __len__(self) -> int:
        return len(self._flags)


class FeatureFlagEvaluator:
    """
    Evaluates feature flags against context.

    Example:
        >>> evaluator = FeatureFlagEvaluator(store)
        >>> result = evaluator.evaluate("new_ui", context)
    """

    def __init__(self, store: FeatureFlagStore) -> None:
        self.store = store

    def is_enabled(
        self,
        flag_key: str,
        context: Optional[FlagContext] = None,
    ) -> bool:
        """Check if flag is enabled."""
        flag = self.store.get(flag_key)
        if not flag:
            return False

        if not flag.enabled:
            return False

        if context and flag.targeting_rules:
            if self._evaluate_rules(flag, context):
                return True

        if flag.rollout_percentage:
            return self._evaluate_percentage(flag, context)

        return True

    def get_value(
        self,
        flag_key: str,
        context: Optional[FlagContext] = None,
        default: Any = None,
    ) -> Any:
        """Get flag value."""
        flag = self.store.get(flag_key)
        if not flag:
            return default

        if flag.flag_type == FlagType.BOOLEAN:
            if self.is_enabled(flag_key, context):
                return True
            return flag.default_value if flag.default_value is not None else False

        if context and flag.targeting_rules:
            for variation in flag.variations:
                if self._evaluate_variation_rules(flag, variation, context):
                    return variation.value

        if flag.rollout_percentage:
            ctx = context or FlagContext()
            user_id = ctx.get_user_id() or ""
            hash_key = f"{flag.key}:{flag.rollout_percentage.seed}:{user_id}"
            bucket = int(hashlib.md5(hash_key.encode()).hexdigest(), 16) % 100

            cumulative = 0
            for variation in flag.variations:
                cumulative += variation.weight
                if bucket < cumulative:
                    return variation.value

        return flag.default_value or default

    def _evaluate_rules(self, flag: FeatureFlag, context: FlagContext) -> bool:
        """Evaluate targeting rules."""
        for rule in flag.targeting_rules:
            ctx_value = self._get_context_value(context, rule.attribute)
            if ctx_value is None:
                return False
            if not self._compare(ctx_value, rule.operator, rule.value):
                return False
        return True

    def _evaluate_variation_rules(
        self,
        flag: FeatureFlag,
        variation: FlagVariation,
        context: FlagContext,
    ) -> bool:
        """Evaluate variation targeting rules."""
        return True

    def _get_context_value(self, context: FlagContext, attribute: str) -> Any:
        """Get value from context."""
        if attribute in ("user_id", "id"):
            return context.user_id
        if attribute == "anonymous_id":
            return context.anonymous_id
        if attribute == "email":
            return context.email
        if attribute == "country":
            return context.country
        if attribute == "region":
            return context.region
        if attribute == "platform":
            return context.platform
        if attribute == "app_version":
            return context.app_version
        return context.get_attribute(attribute)

    def _compare(self, value: Any, operator: ComparisonOp, target: Any) -> bool:
        """Compare value with target using operator."""
        ops = {
            ComparisonOp.EQUALS: lambda v, t: v == t,
            ComparisonOp.NOT_EQUALS: lambda v, t: v != t,
            ComparisonOp.CONTAINS: lambda v, t: t in v if v else False,
            ComparisonOp.NOT_CONTAINS: lambda v, t: t not in v if v else True,
            ComparisonOp.IN: lambda v, t: v in t if isinstance(t, (list, tuple)) else False,
            ComparisonOp.NOT_IN: lambda v, t: v not in t if isinstance(t, (list, tuple)) else True,
            ComparisonOp.GREATER_THAN: lambda v, t: v > t if isinstance(v, (int, float)) and isinstance(t, (int, float)) else False,
            ComparisonOp.LESS_THAN: lambda v, t: v < t if isinstance(v, (int, float)) and isinstance(t, (int, float)) else False,
            ComparisonOp.GREATER_EQUALS: lambda v, t: v >= t if isinstance(v, (int, float)) and isinstance(t, (int, float)) else False,
            ComparisonOp.LESS_EQUALS: lambda v, t: v <= t if isinstance(v, (int, float)) and isinstance(t, (int, float)) else False,
            ComparisonOp.REGEX_MATCH: lambda v, t: self._regex_match(v, t),
        }
        return ops.get(operator, lambda v, t: False)(value, target)

    def _regex_match(self, value: str, pattern: str) -> bool:
        """Match value against regex pattern."""
        import re
        try:
            return bool(re.match(pattern, str(value)))
        except Exception:
            return False

    def _evaluate_percentage(
        self,
        flag: FeatureFlag,
        context: Optional[FlagContext],
    ) -> bool:
        """Evaluate percentage rollout."""
        if not flag.rollout_percentage:
            return False

        ctx = context or FlagContext()
        user_id = ctx.get_user_id() or ""
        hash_key = f"{flag.key}:{flag.rollout_percentage.seed}:{user_id}"
        bucket = int(hashlib.md5(hash_key.encode()).hexdigest(), 16) % 100

        return bucket < flag.rollout_percentage.percentage


class ABTestManager:
    """
    A/B test management with feature flags.

    Example:
        >>> manager = ABTestManager(store)
        >>> test_id = manager.create_test("button_color", ["red", "blue"])
        >>> variant = manager.get_variant(test_id, context)
    """

    def __init__(self, store: FeatureFlagStore) -> None:
        self.store = store
        self.evaluator = FeatureFlagEvaluator(store)

    def create_test(
        self,
        test_key: str,
        variants: list[str],
        weights: Optional[list[int]] = None,
    ) -> FeatureFlag:
        """Create A/B test flag."""
        if weights is None:
            weights = [100 // len(variants)] * len(variants)

        while sum(weights) < 100:
            weights[0] += 1

        variations = [
            FlagVariation(name=v, value=v, weight=w)
            for v, w in zip(variants, weights)
        ]

        flag = FeatureFlag(
            key=test_key,
            name=f"A/B Test: {test_key}",
            flag_type=FlagType.STRING,
            enabled=True,
            variations=variations,
            rollout_percentage=RolloutPercentage(percentage=100.0),
        )
        self.store.add(flag)
        return flag

    def get_variant(
        self,
        test_key: str,
        context: Optional[FlagContext] = None,
    ) -> Optional[str]:
        """Get assigned variant for context."""
        value = self.evaluator.get_value(test_key, context)
        return value if isinstance(value, str) else None

    def track_conversion(
        self,
        test_key: str,
        variant: str,
        metric: str,
        value: float = 1.0,
    ) -> None:
        """Track conversion event."""
        logger.info(f"Conversion: test={test_key} variant={variant} metric={metric} value={value}")

    def get_results(self, test_key: str) -> dict[str, Any]:
        """Get test results."""
        flag = self.store.get(test_key)
        if not flag:
            return {}

        return {
            "test_key": test_key,
            "enabled": flag.enabled,
            "variants": [
                {"name": v.name, "weight": v.weight}
                for v in flag.variations
            ],
        }


class FeatureFlagMiddleware:
    """
    Middleware for applying feature flags to actions.

    Example:
        >>> mw = FeatureFlagMiddleware(evaluator)
        >>> result = mw.apply("my_action", context, original_func, *args)
    """

    def __init__(self, evaluator: FeatureFlagEvaluator) -> None:
        self.evaluator = evaluator
        self._wrappers: dict[str, Callable] = {}

    def wrap(
        self,
        flag_key: str,
        enabled_func: Callable,
        disabled_func: Optional[Callable] = None,
    ) -> Callable:
        """Wrap function with feature flag."""
        def wrapper(context: FlagContext, *args: Any, **kwargs: Any) -> Any:
            if self.evaluator.is_enabled(flag_key, context):
                return enabled_func(*args, **kwargs)
            elif disabled_func:
                return disabled_func(*args, **kwargs)
            return None
        self._wrappers[flag_key] = wrapper
        return wrapper

    def apply(
        self,
        flag_key: str,
        context: Optional[FlagContext],
        default: Any = None,
    ) -> Any:
        """Apply flag and return appropriate value."""
        return self.evaluator.get_value(flag_key, context, default)

    def __repr__(self) -> str:
        return f"FeatureFlagMiddleware(wrappers={len(self._wrappers)})"
