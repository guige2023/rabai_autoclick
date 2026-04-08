"""
Feature flag and A/B testing module for controlled feature rollouts.

Supports targeting rules, percentage rollouts, user segmentation,
and experiment tracking.
"""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class VariationType(Enum):
    """Type of variation."""
    BOOLEAN = "boolean"
    STRING = "string"
    NUMBER = "number"
    JSON = "json"


@dataclass
class Variation:
    """A possible value for a feature flag."""
    name: str
    value: Any
    weight: int = 100
    description: str = ""


@dataclass
class TargetingRule:
    """A targeting rule for feature flag evaluation."""
    attribute: str
    operator: str
    value: Any
    variation: str


@dataclass
class Segment:
    """User segment for targeting."""
    name: str
    conditions: list[dict]
    percentage: int = 100


@dataclass
class FeatureFlag:
    """A feature flag definition."""
    key: str
    name: str
    description: str = ""
    enabled: bool = False
    variations: list[Variation] = field(default_factory=list)
    default_variation: Optional[str] = None
    targeting_rules: list[TargetingRule] = field(default_factory=list)
    segments: list[Segment] = field(default_factory=list)
    rollout_percentage: int = 100
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


@dataclass
class Experiment:
    """An A/B experiment."""
    key: str
    name: str
    flags: list[str]
    variations: list[Variation]
    status: str = "draft"
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    primary_metric: Optional[str] = None
    minimum_sample_size: int = 1000


@dataclass
class ExperimentResult:
    """Result of an experiment evaluation."""
    experiment_key: str
    variation_name: str
    variation_value: Any
    user_id: str
    assigned_at: float = field(default_factory=time.time)
    context: dict = field(default_factory=dict)


@dataclass
class MetricEvent:
    """A metric event for experiment tracking."""
    experiment_key: str
    variation_name: str
    user_id: str
    metric_key: str
    value: float
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


class FeatureFlagService:
    """
    Feature flag and A/B testing service.

    Provides flag evaluation, targeting rules, percentage rollouts,
    and experiment tracking.
    """

    def __init__(self, environment: str = "development"):
        self.environment = environment
        self._flags: dict[str, FeatureFlag] = {}
        self._experiments: dict[str, Experiment] = {}
        self._user_assignments: dict[str, dict[str, str]] = {}
        self._metric_events: list[MetricEvent] = []

    def create_flag(
        self,
        key: str,
        name: str,
        enabled: bool = False,
        variations: Optional[list[Variation]] = None,
        default_variation: Optional[str] = None,
    ) -> FeatureFlag:
        """Create a new feature flag."""
        flag = FeatureFlag(
            key=key,
            name=name,
            enabled=enabled,
            variations=variations or [Variation(name="control", value=True, weight=100)],
            default_variation=default_variation or "control",
        )
        self._flags[key] = flag
        return flag

    def get_flag(self, key: str) -> Optional[FeatureFlag]:
        """Get a feature flag by key."""
        return self._flags.get(key)

    def update_flag(self, key: str, **kwargs) -> Optional[FeatureFlag]:
        """Update a feature flag."""
        flag = self._flags.get(key)
        if not flag:
            return None

        for key_arg, value in kwargs.items():
            if hasattr(flag, key_arg):
                setattr(flag, key_arg, value)
        flag.updated_at = time.time()

        return flag

    def evaluate_flag(
        self,
        flag_key: str,
        user_id: str,
        context: Optional[dict] = None,
    ) -> Any:
        """Evaluate a feature flag for a user."""
        flag = self._flags.get(flag_key)
        if not flag:
            return None

        context = context or {}

        if not flag.enabled:
            return flag.default_variation

        for rule in flag.targeting_rules:
            if self._evaluate_condition(context.get(rule.attribute), rule.operator, rule.value):
                for variation in flag.variations:
                    if variation.name == rule.variation:
                        return variation.value

        for segment in flag.segments:
            if self._evaluate_segment(context, segment):
                assigned_variation = self._get_percentage_variation(
                    user_id, flag_key, flag.variations, segment.percentage
                )
                return assigned_variation.value

        if flag.rollout_percentage < 100:
            assigned_variation = self._get_percentage_variation(
                user_id, flag_key, flag.variations, flag.rollout_percentage
            )
            return assigned_variation.value

        return flag.variations[0].value if flag.variations else flag.default_variation

    def _evaluate_condition(self, value: Any, operator: str, target: Any) -> bool:
        """Evaluate a targeting condition."""
        if operator == "eq":
            return value == target
        elif operator == "neq":
            return value != target
        elif operator == "gt":
            return value > target
        elif operator == "gte":
            return value >= target
        elif operator == "lt":
            return value < target
        elif operator == "lte":
            return value <= target
        elif operator == "in":
            return value in target
        elif operator == "not_in":
            return value not in target
        elif operator == "contains":
            return target in value
        elif operator == "regex":
            import re
            return bool(re.match(target, str(value)))
        return False

    def _evaluate_segment(self, context: dict, segment: Segment) -> bool:
        """Evaluate if a user matches a segment."""
        for condition in segment.conditions:
            attribute = condition.get("attribute")
            operator = condition.get("operator")
            value = condition.get("value")
            if not self._evaluate_condition(context.get(attribute), operator, value):
                return False
        return True

    def _get_percentage_variation(
        self,
        user_id: str,
        flag_key: str,
        variations: list[Variation],
        percentage: int,
    ) -> Variation:
        """Get a variation based on percentage rollout."""
        hash_input = f"{user_id}:{flag_key}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        bucket = hash_value % 100

        if bucket >= percentage:
            return variations[-1]

        cumulative = 0
        for variation in variations:
            cumulative += variation.weight
            if bucket < cumulative:
                return variation

        return variations[0]

    def create_experiment(
        self,
        key: str,
        name: str,
        variations: list[Variation],
        flags: Optional[list[str]] = None,
    ) -> Experiment:
        """Create a new A/B experiment."""
        experiment = Experiment(
            key=key,
            name=name,
            flags=flags or [],
            variations=variations,
        )
        self._experiments[key] = experiment
        return experiment

    def evaluate_experiment(
        self,
        experiment_key: str,
        user_id: str,
        context: Optional[dict] = None,
    ) -> Optional[ExperimentResult]:
        """Evaluate an experiment for a user."""
        experiment = self._experiments.get(experiment_key)
        if not experiment or experiment.status != "running":
            return None

        context = context or {}

        assignment_key = f"{experiment_key}:{user_id}"
        if assignment_key in self._user_assignments:
            variation_name = self._user_assignments[assignment_key]
            for variation in experiment.variations:
                if variation.name == variation_name:
                    return ExperimentResult(
                        experiment_key=experiment_key,
                        variation_name=variation_name,
                        variation_value=variation.value,
                        user_id=user_id,
                        context=context,
                    )

        hash_input = f"{experiment_key}:{user_id}"
        hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        bucket = hash_value % 100

        cumulative = 0
        selected_variation = experiment.variations[0]
        for variation in experiment.variations:
            cumulative += variation.weight
            if bucket < cumulative:
                selected_variation = variation
                break

        self._user_assignments[assignment_key] = selected_variation.name

        return ExperimentResult(
            experiment_key=experiment_key,
            variation_name=selected_variation.name,
            variation_value=selected_variation.value,
            user_id=user_id,
            context=context,
        )

    def track_metric(
        self,
        experiment_key: str,
        variation_name: str,
        user_id: str,
        metric_key: str,
        value: float,
        metadata: Optional[dict] = None,
    ) -> None:
        """Track a metric event for an experiment."""
        event = MetricEvent(
            experiment_key=experiment_key,
            variation_name=variation_name,
            user_id=user_id,
            metric_key=metric_key,
            value=value,
            metadata=metadata or {},
        )
        self._metric_events.append(event)

    def get_experiment_stats(self, experiment_key: str) -> dict:
        """Get statistics for an experiment."""
        experiment = self._experiments.get(experiment_key)
        if not experiment:
            return {}

        relevant_events = [e for e in self._metric_events if e.experiment_key == experiment_key]

        stats = {}
        for variation in experiment.variations:
            variation_events = [e for e in relevant_events if e.variation_name == variation.name]
            count = len(variation_events)
            total_value = sum(e.value for e in variation_events)
            stats[variation.name] = {
                "sample_size": count,
                "total_value": total_value,
                "mean_value": total_value / count if count > 0 else 0,
            }

        return stats

    def list_flags(self) -> list[FeatureFlag]:
        """List all feature flags."""
        return list(self._flags.values())

    def list_experiments(self) -> list[Experiment]:
        """List all experiments."""
        return list(self._experiments.values())
