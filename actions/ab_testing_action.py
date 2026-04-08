"""A/B testing action module for RabAI AutoClick.

Provides A/B and multi-armed bandit experiment management:
variant assignment, statistical significance, and conversion tracking.
"""

from __future__ import annotations

import sys
import os
import math
import hashlib
import random
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Variant:
    """An experiment variant."""
    name: str
    weight: float  # 0.0 to 1.0, all weights sum to 1.0
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentResult:
    """Result of an A/B test analysis."""
    variant_name: str
    visitors: int
    conversions: int
    conversion_rate: float
    avg_value: float
    confidence_interval: Tuple[float, float]


class ABTestAssignmentAction(BaseAction):
    """Assign a user to an A/B test variant deterministically.
    
    Uses consistent hashing so the same user always gets the same
    variant. Supports weighted randomization.
    
    Args:
        experiment_name: Unique name for this experiment
        variants: List of Variant objects with weights
        hash_salt: Salt to prevent hash collisions across experiments
    """

    def __init__(self, experiment_name: str, variants: List[Variant]):
        super().__init__()
        self.experiment_name = experiment_name
        self.variants = variants
        self._normalize_weights()

    def _normalize_weights(self):
        total = sum(v.weight for v in self.variants)
        if total <= 0:
            for v in self.variants:
                v.weight = 1.0 / len(self.variants)
        else:
            for v in self.variants:
                v.weight /= total

    def execute(
        self,
        action: str,
        user_id: Optional[str] = None,
        deterministic: bool = True,
        experiment_name: Optional[str] = None,
        variants_data: Optional[List[Dict[str, Any]]] = None,
        traffic_split: Optional[Dict[str, float]] = None
    ) -> ActionResult:
        try:
            if action == "assign":
                if not user_id:
                    return ActionResult(success=False, error="user_id required")

                # Deterministic hash-based assignment
                hash_input = f"{experiment_name or self.experiment_name}:{user_id}"
                hash_val = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
                bucket = (hash_val % 10000) / 10000.0

                cumulative = 0.0
                for variant in self.variants:
                    cumulative += variant.weight
                    if bucket <= cumulative:
                        return ActionResult(success=True, data={
                            "user_id": user_id,
                            "experiment": experiment_name or self.experiment_name,
                            "variant": variant.name,
                            "deterministic": True
                        })

                return ActionResult(success=True, data={
                    "user_id": user_id,
                    "experiment": experiment_name or self.experiment_name,
                    "variant": self.variants[-1].name,
                    "deterministic": True
                })

            elif action == "random_assign":
                if not experiment_name:
                    return ActionResult(success=False, error="experiment_name required")

                vs = variants_data or []
                if not vs:
                    return ActionResult(success=False, error="variants_data required")

                # Normalize weights
                weights = [v.get("weight", 1.0) for v in vs]
                total = sum(weights)
                norm_weights = [w / total for w in weights]
                names = [v["name"] for v in vs]
                chosen = random.choices(names, weights=norm_weights, k=1)[0]

                return ActionResult(success=True, data={
                    "experiment": experiment_name,
                    "variant": chosen,
                    "deterministic": False
                })

            elif action == "create_experiment":
                if not experiment_name or not variants_data:
                    return ActionResult(success=False, error="experiment_name and variants_data required")
                return ActionResult(success=True, data={
                    "experiment": experiment_name,
                    "variants": variants_data,
                    "created": True
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class ABTestAnalysisAction(BaseAction):
    """Statistical analysis of A/B test results.
    
    Computes conversion rates, confidence intervals (Wilson & Jeffreys),
    z-scores, p-values, and determines statistical significance.
    
    Args:
        confidence_level: Confidence level (default 0.95)
    """

    def __init__(self, confidence_level: float = 0.95):
        super().__init__()
        self.confidence_level = confidence_level

    def execute(
        self,
        action: str,
        variant_data: Optional[List[Dict[str, Any]]] = None,
        control_name: str = "control",
        treatment_name: str = "treatment"
    ) -> ActionResult:
        try:
            if action == "analyze":
                if not variant_data:
                    return ActionResult(success=False, error="variant_data required")

                results: List[ExperimentResult] = []
                for vd in variant_data:
                    name = vd.get("name", "unknown")
                    visitors = vd.get("visitors", 0)
                    conversions = vd.get("conversions", 0)
                    total_value = vd.get("total_value", 0.0)

                    rate = conversions / visitors if visitors > 0 else 0.0
                    avg_value = total_value / visitors if visitors > 0 else 0.0

                    ci = self._wilson_ci(conversions, visitors)
                    results.append(ExperimentResult(
                        variant_name=name,
                        visitors=visitors,
                        conversions=conversions,
                        conversion_rate=round(rate, 6),
                        avg_value=round(avg_value, 4),
                        confidence_interval=(round(ci[0], 6), round(ci[1], 6))
                    ))

                # Compare treatment to control
                control = next((r for r in results if r.variant_name == control_name), None)
                treatment = next((r for r in results if r.variant_name == treatment_name), None)

                significance = None
                lift = None
                if control and treatment and control.visitors > 0 and treatment.visitors > 0:
                    lift = (treatment.conversion_rate - control.conversion_rate) / control.conversion_rate if control.conversion_rate > 0 else 0.0
                    z_score = self._z_score(control.conversion_rate, treatment.conversion_rate,
                                            control.visitors, treatment.visitors)
                    p_value = self._z_pvalue(z_score)
                    significance = p_value < (1.0 - self.confidence_level)

                return ActionResult(success=True, data={
                    "variants": [
                        {"name": r.variant_name, "visitors": r.visitors, "conversions": r.conversions,
                         "rate": r.conversion_rate, "ci": r.confidence_interval, "avg_value": r.avg_value}
                        for r in results
                    ],
                    "lift": round(lift, 4) if lift is not None else None,
                    "statistically_significant": significance,
                    "confidence_level": self.confidence_level
                })

            elif action == "multi_armed_bandit":
                if not variant_data:
                    return ActionResult(success=False, error="variant_data required")

                # Thompson sampling approach
                results = []
                for vd in variant_data:
                    successes = vd.get("conversions", 0)
                    trials = vd.get("visitors", 0)
                    # Beta distribution sample
                    alpha = successes + 1
                    beta_param = (trials - successes) + 1
                    sample = self._beta_sample(alpha, beta_param)
                    results.append({"name": vd.get("name"), "sample": sample, "trials": trials})

                results.sort(key=lambda x: -x["sample"])
                return ActionResult(success=True, data={
                    "recommended_variant": results[0]["name"],
                    "all_samples": [{"name": r["name"], "thompson_score": round(r["sample"], 4)} for r in results],
                    "method": "thompson_sampling"
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _wilson_ci(self, successes: int, n: int) -> Tuple[float, float]:
        """Wilson score confidence interval."""
        if n == 0:
            return (0.0, 1.0)
        z = 1.96  # 95% confidence
        p = successes / n
        denominator = 1 + z * z / n
        center = p + z * z / (2 * n)
        spread = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
        lower = (center - spread) / denominator
        upper = (center + spread) / denominator
        return (max(0.0, lower), min(1.0, upper))

    def _z_score(self, p1: float, p2: float, n1: int, n2: int) -> float:
        """Two-proportion z-score."""
        p_pooled = (p1 * n1 + p2 * n2) / (n1 + n2)
        if p_pooled == 0 or p_pooled == 1:
            return 0.0
        se = math.sqrt(p_pooled * (1 - p_pooled) * (1 / n1 + 1 / n2))
        if se == 0:
            return 0.0
        return (p2 - p1) / se

    def _z_pvalue(self, z: float) -> float:
        """Approximate two-tailed p-value from z-score."""
        return 2.0 * (1.0 - 0.5 * (1.0 + math.erf(abs(z) / math.sqrt(2))))

    def _beta_sample(self, alpha: float, beta_param: float) -> float:
        """Sample from Beta distribution (approximation)."""
        import random
        return random.betavariate(alpha, beta_param)


class ABTestTrackerAction(BaseAction):
    """Track A/B test events: exposures, conversions, and revenue.
    
    Maintains in-memory event store for experiment tracking.
    
    Args:
        experiments: Dict of experiment_name -> List[Variant]
    """

    def __init__(self):
        super().__init__()
        self._exposures: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._conversions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._values: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    def execute(
        self,
        action: str,
        experiment: Optional[str] = None,
        variant: Optional[str] = None,
        user_id: Optional[str] = None,
        value: float = 0.0
    ) -> ActionResult:
        try:
            if action == "track_exposure":
                if not experiment or not variant:
                    return ActionResult(success=False, error="experiment and variant required")
                self._exposures[experiment][variant] += 1
                return ActionResult(success=True, data={
                    "experiment": experiment, "variant": variant,
                    "total_exposures": self._exposures[experiment][variant]
                })

            elif action == "track_conversion":
                if not experiment or not variant:
                    return ActionResult(success=False, error="experiment and variant required")
                self._conversions[experiment][variant] += 1
                self._values[experiment][variant] += value
                return ActionResult(success=True, data={
                    "experiment": experiment, "variant": variant,
                    "total_conversions": self._conversions[experiment][variant]
                })

            elif action == "get_summary":
                if not experiment:
                    return ActionResult(success=False, error="experiment required")
                variants = set(self._exposures[experiment].keys()) | set(self._conversions[experiment].keys())
                summary = []
                for v in variants:
                    visitors = self._exposures[experiment][v]
                    conversions = self._conversions[experiment][v]
                    total_val = self._values[experiment][v]
                    rate = conversions / visitors if visitors > 0 else 0.0
                    summary.append({
                        "variant": v, "visitors": visitors, "conversions": conversions,
                        "conversion_rate": round(rate, 4), "total_value": round(total_val, 2)
                    })
                return ActionResult(success=True, data={
                    "experiment": experiment, "variants": summary
                })

            elif action == "list_experiments":
                return ActionResult(success=True, data={
                    "experiments": list(self._exposures.keys())
                })

            elif action == "reset":
                if experiment:
                    self._exposures.pop(experiment, None)
                    self._conversions.pop(experiment, None)
                    self._values.pop(experiment, None)
                return ActionResult(success=True, data={"reset": True})

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
