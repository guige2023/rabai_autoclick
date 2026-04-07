"""A/B testing utilities: experiment management, variant assignment, and result analysis."""

from __future__ import annotations

import hashlib
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Experiment",
    "Variant",
    "ABTestManager",
    "get_variant",
]


@dataclass
class Variant:
    """A test variant with its configuration."""

    name: str
    weight: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Experiment:
    """An A/B test experiment."""

    name: str
    variants: list[Variant]
    start_time: float = field(default_factory=time.time)
    end_time: float | None = None
    enabled: bool = True
    description: str = ""

    def get_variant(self, user_id: str | None = None) -> Variant | None:
        """Assign a user to a variant based on weights."""
        if not self.enabled:
            return None

        if user_id:
            bucket = self._hash_user(user_id, self.name)
        else:
            bucket = random.random()

        cumulative = 0.0
        for variant in self.variants:
            cumulative += variant.weight
            if bucket <= cumulative:
                return variant
        return self.variants[-1] if self.variants else None

    @staticmethod
    def _hash_user(user_id: str, experiment_name: str) -> float:
        """Deterministically hash user to a 0-1 float."""
        seed = f"{user_id}:{experiment_name}"
        hash_val = hashlib.md5(seed.encode()).hexdigest()
        return int(hash_val[:8], 16) / 0xFFFFFFFF


class ABTestManager:
    """Manage multiple A/B test experiments."""

    def __init__(self) -> None:
        self._experiments: dict[str, Experiment] = {}
        self._assignments: dict[str, dict[str, str]] = defaultdict(dict)
        self._conversions: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    def add_experiment(self, experiment: Experiment) -> None:
        self._experiments[experiment.name] = experiment

    def get_variant(
        self,
        experiment_name: str,
        user_id: str | None = None,
    ) -> Variant | None:
        experiment = self._experiments.get(experiment_name)
        if not experiment:
            return None

        if user_id:
            if user_id in self._assignments[experiment_name]:
                variant_name = self._assignments[experiment_name][user_id]
                return next((v for v in experiment.variants if v.name == variant_name), None)

        variant = experiment.get_variant(user_id)
        if variant and user_id:
            self._assignments[experiment_name][user_id] = variant.name
        return variant

    def record_conversion(
        self,
        experiment_name: str,
        variant_name: str,
    ) -> None:
        self._conversions[experiment_name][variant_name] += 1

    def get_results(self, experiment_name: str) -> dict[str, dict[str, Any]]:
        """Get conversion results for an experiment."""
        experiment = self._experiments.get(experiment_name)
        if not experiment:
            return {}

        total_users = len(self._assignments.get(experiment_name, {}))
        results = {}

        for variant in experiment.variants:
            conversions = self._conversions[experiment_name].get(variant.name, 0)
            variant_users = sum(
                1 for v in self._assignments.get(experiment_name, {}).values()
                if v == variant.name
            )
            rate = (conversions / variant_users) if variant_users > 0 else 0.0

            results[variant.name] = {
                "users": variant_users,
                "conversions": conversions,
                "conversion_rate": rate,
                "weight": variant.weight,
            }

        return results


def get_variant(
    experiment: Experiment,
    user_id: str | None = None,
) -> Variant | None:
    """Convenience function to get a variant."""
    return experiment.get_variant(user_id)
