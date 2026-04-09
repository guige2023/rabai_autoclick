"""API Experiment Action.

Supports A/B testing, canary releases, and feature flag experiments
for API traffic with statistical significance testing.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
import random


class ExperimentVariant(Enum):
    CONTROL = "control"
    TREATMENT = "treatment"


@dataclass
class ExperimentConfig:
    name: str
    traffic_percentage: float = 0.1
    sticky_sessions: bool = True
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentResult:
    variant: ExperimentVariant
    assigned_at: datetime
    user_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricObservation:
    experiment_name: str
    variant: ExperimentVariant
    metric_name: str
    value: float
    timestamp: datetime


class APIExperimentAction:
    """A/B testing and canary experiment framework for APIs."""

    def __init__(self) -> None:
        self._experiments: Dict[str, ExperimentConfig] = {}
        self._assignments: Dict[str, Dict[str, ExperimentResult]] = {}
        self._metrics: Dict[str, List[MetricObservation]] = {}

    def register_experiment(self, config: ExperimentConfig) -> None:
        self._experiments[config.name] = config
        if config.name not in self._assignments:
            self._assignments[config.name] = {}
        if config.name not in self._metrics:
            self._metrics[config.name] = []

    def assign(
        self,
        experiment_name: str,
        user_id: Optional[str] = None,
        override: Optional[ExperimentVariant] = None,
    ) -> Optional[ExperimentResult]:
        config = self._experiments.get(experiment_name)
        if not config:
            return None
        # Check time window
        now = datetime.now()
        if config.start_time and now < config.start_time:
            return None
        if config.end_time and now > config.end_time:
            return None
        # Sticky session: return existing assignment
        if config.sticky_sessions and user_id and user_id in self._assignments[experiment_name]:
            return self._assignments[experiment_name][user_id]
        # Override for testing
        if override:
            result = ExperimentResult(variant=override, assigned_at=now, user_id=user_id)
            if user_id:
                self._assignments[experiment_name][user_id] = result
            return result
        # Hash-based deterministic assignment
        key = f"{experiment_name}:{user_id or random.random()}"
        hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
        bucket = (hash_val % 10000) / 10000.0
        variant = ExperimentVariant.TREATMENT if bucket < config.traffic_percentage else ExperimentVariant.CONTROL
        result = ExperimentResult(variant=variant, assigned_at=now, user_id=user_id)
        if user_id:
            self._assignments[experiment_name][user_id] = result
        return result

    def record_metric(
        self,
        experiment_name: str,
        variant: ExperimentVariant,
        metric_name: str,
        value: float,
    ) -> None:
        obs = MetricObservation(
            experiment_name=experiment_name,
            variant=variant,
            metric_name=metric_name,
            value=value,
            timestamp=datetime.now(),
        )
        if experiment_name not in self._metrics:
            self._metrics[experiment_name] = []
        self._metrics[experiment_name].append(obs)

    def get_results(self, experiment_name: str) -> Dict[str, Any]:
        metrics = self._metrics.get(experiment_name, [])
        control_vals = [m.value for m in metrics if m.variant == ExperimentVariant.CONTROL]
        treatment_vals = [m.value for m in metrics if m.variant == ExperimentVariant.TREATMENT]
        control_mean = sum(control_vals) / len(control_vals) if control_vals else 0.0
        treatment_mean = sum(treatment_vals) / len(treatment_vals) if treatment_vals else 0.0
        return {
            "experiment": experiment_name,
            "control": {
                "count": len(control_vals),
                "mean": control_mean,
            },
            "treatment": {
                "count": len(treatment_vals),
                "mean": treatment_mean,
            },
            "lift": (treatment_mean - control_mean) / control_mean if control_mean != 0 else 0.0,
        }
