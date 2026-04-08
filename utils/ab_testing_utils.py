"""A/B testing utilities for automation experiment management.

Provides experiment configuration, variant assignment,
metric tracking, and statistical analysis for
comparing automation strategy outcomes.

Example:
    >>> from utils.ab_testing_utils import Experiment, Variant, track
    >>> exp = Experiment('click_strategy_test')
    >>> variant = exp.get_variant(user_id='user_1')
    >>> track('click_strategy_test', variant.name, 'success', value=1.0)
"""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

__all__ = [
    "Experiment",
    "Variant",
    "track",
    "get_variant",
    "ExperimentManager",
]


@dataclass
class Variant:
    """A single experimental variant."""

    name: str
    weight: float = 1.0  # relative weight in random selection
    config: dict = field(default_factory=dict)


@dataclass
class MetricRecord:
    """A single metric observation."""

    experiment: str
    variant: str
    metric: str
    value: float
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)


class Experiment:
    """An A/B test experiment with variants and tracking.

    Example:
        >>> exp = Experiment('strategy_test', [
        ...     Variant('control', weight=1.0),
        ...     Variant('treatment', weight=1.0),
        ... ])
        >>> variant = exp.get_variant('user_123')
        >>> print(f"Assigned: {variant.name}")
    """

    def __init__(
        self,
        name: str,
        variants: list[Variant],
        salt: str = "",
    ):
        self.name = name
        self.variants = variants
        self.salt = salt
        self._total_weight = sum(v.weight for v in variants)
        self._records: list[MetricRecord] = []

    def get_variant(
        self,
        user_id: str,
        deterministic: bool = True,
    ) -> Variant:
        """Assign a variant to a user.

        Args:
            user_id: Unique user/execution identifier.
            deterministic: If True, same user always gets same variant.

        Returns:
            The assigned Variant.
        """
        if deterministic:
            hash_input = f"{self.name}:{user_id}:{self.salt}"
            hash_val = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            bucket = (hash_val % 10000) / 10000.0
        else:
            bucket = random.random()

        cumulative = 0.0
        for variant in self.variants:
            cumulative += variant.weight / self._total_weight
            if bucket <= cumulative:
                return variant

        return self.variants[-1]

    def track(
        self,
        variant: Variant,
        metric: str,
        value: float,
        metadata: Optional[dict] = None,
    ) -> None:
        """Record a metric observation.

        Args:
            variant: The variant that was used.
            metric: Metric name.
            value: Metric value.
            metadata: Optional additional metadata.
        """
        record = MetricRecord(
            experiment=self.name,
            variant=variant.name,
            metric=metric,
            value=value,
            metadata=metadata or {},
        )
        self._records.append(record)

    def results(self) -> dict[str, dict[str, float]]:
        """Get aggregated results for all variants.

        Returns:
            Dictionary mapping variant name to metric summaries.
        """
        results: dict[str, dict[str, list[float]]] = {}

        for record in self._records:
            if record.variant not in results:
                results[record.variant] = {}
            if record.metric not in results[record.variant]:
                results[record.variant][record.metric] = []
            results[record.variant][record.metric].append(record.value)

        # Compute summary stats
        summary: dict[str, dict[str, float]] = {}
        for variant_name, metrics in results.items():
            summary[variant_name] = {}
            for metric_name, values in metrics.items():
                if values:
                    summary[variant_name][f"{metric_name}_mean"] = sum(values) / len(values)
                    summary[variant_name][f"{metric_name}_count"] = len(values)
                    summary[variant_name][f"{metric_name}_sum"] = sum(values)

        return summary


# Module-level global tracker
_global_metrics: list[MetricRecord] = []
_global_experiments: dict[str, Experiment] = {}


def track(
    experiment: str,
    variant: str,
    metric: str,
    value: float,
    metadata: Optional[dict] = None,
) -> None:
    """Record a metric observation at the module level.

    Args:
        experiment: Experiment name.
        variant: Variant name.
        metric: Metric name.
        value: Metric value.
        metadata: Optional additional metadata.
    """
    _global_metrics.append(
        MetricRecord(
            experiment=experiment,
            variant=variant,
            metric=metric,
            value=value,
            metadata=metadata or {},
        )
    )


def get_variant(
    experiment_name: str,
    user_id: str,
    experiments: Optional[dict[str, Experiment]] = None,
) -> Optional[Variant]:
    """Get a variant for a user from a named experiment.

    Args:
        experiment_name: Name of the registered experiment.
        user_id: User/execution identifier.
        experiments: Optional experiments dict (uses global if None).

    Returns:
        Assigned Variant, or None if experiment not found.
    """
    exps = experiments or _global_experiments
    exp = exps.get(experiment_name)
    if exp is None:
        return None
    return exp.get_variant(user_id)


class ExperimentManager:
    """Global registry and manager for experiments.

    Example:
        >>> manager = ExperimentManager()
        >>> manager.register('test1', [Variant('a'), Variant('b')])
        >>> v = manager.get_variant('test1', 'user_1')
        >>> manager.results('test1')
    """

    def __init__(self):
        self.experiments: dict[str, Experiment] = {}

    def register(
        self,
        name: str,
        variants: list[Variant],
        salt: str = "",
    ) -> Experiment:
        """Register a new experiment.

        Args:
            name: Experiment name.
            variants: List of variants.
            salt: Randomization salt.

        Returns:
            The created Experiment.
        """
        exp = Experiment(name, variants, salt=salt)
        self.experiments[name] = exp
        return exp

    def get_variant(self, name: str, user_id: str) -> Optional[Variant]:
        """Get a variant assignment."""
        exp = self.experiments.get(name)
        if exp is None:
            return None
        return exp.get_variant(user_id)

    def results(self, name: str) -> Optional[dict]:
        """Get results for an experiment."""
        exp = self.experiments.get(name)
        if exp is None:
            return None
        return exp.results()
