"""
Auto Scaler Action Module.

Automatically scales resources based on metrics like CPU usage,
memory, request count, or custom metrics with configurable thresholds.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ScalingAction:
    """A scaling action to take."""
    resource_id: str
    action: str  # scale_up, scale_down, scale_to
    delta: int
    reason: str
    timestamp: float


@dataclass
class ScalingResult:
    """Result of auto scaling evaluation."""
    actions: list[ScalingAction]
    current_metrics: dict[str, float]
    recommendations: list[str]


class AutoScalerAction(BaseAction):
    """Automatically scale resources based on metrics."""

    def __init__(self) -> None:
        super().__init__("auto_scaler")
        self._rules: list[dict[str, Any]] = []
        self._history: list[ScalingAction] = []
        self._current_metrics: dict[str, float] = {}

    def execute(self, context: dict, params: dict) -> ScalingResult:
        """
        Evaluate metrics and determine scaling actions.

        Args:
            context: Execution context
            params: Parameters:
                - metrics: Current metric values (e.g., {"cpu": 85.5, "memory": 70.0})
                - resource_id: Resource identifier
                - min_replicas: Minimum replicas (default: 1)
                - max_replicas: Maximum replicas (default: 10)
                - current_replicas: Current replica count
                - rules: Optional override scaling rules

        Returns:
            ScalingResult with recommended actions
        """
        import time

        metrics = params.get("metrics", {})
        resource_id = params.get("resource_id", "default")
        min_replicas = params.get("min_replicas", 1)
        max_replicas = params.get("max_replicas", 10)
        current_replicas = params.get("current_replicas", 1)
        rules = params.get("rules", self._rules)

        self._current_metrics = metrics
        actions: list[ScalingAction] = []
        recommendations: list[str] = []

        if not rules:
            rules = [
                {"metric": "cpu", "scale_up_threshold": 80, "scale_down_threshold": 20, "scale_up_delta": 2, "scale_down_delta": 1},
                {"metric": "memory", "scale_up_threshold": 85, "scale_down_threshold": 30, "scale_up_delta": 1, "scale_down_delta": 1}
            ]

        for rule in rules:
            metric_name = rule.get("metric", "")
            if metric_name not in metrics:
                continue

            value = metrics[metric_name]
            up_threshold = rule.get("scale_up_threshold", 80)
            down_threshold = rule.get("scale_down_threshold", 20)
            up_delta = rule.get("scale_up_delta", 1)
            down_delta = rule.get("scale_down_delta", 1)

            if value >= up_threshold and current_replicas < max_replicas:
                new_replicas = min(current_replicas + up_delta, max_replicas)
                actions.append(ScalingAction(
                    resource_id=resource_id,
                    action="scale_up",
                    delta=new_replicas - current_replicas,
                    reason=f"{metric_name}={value:.1f}% >= {up_threshold}%",
                    timestamp=time.time()
                ))
                recommendations.append(f"Scale up {resource_id}: {metric_name} high ({value:.1f}%)")
            elif value <= down_threshold and current_replicas > min_replicas:
                new_replicas = max(current_replicas - down_delta, min_replicas)
                actions.append(ScalingAction(
                    resource_id=resource_id,
                    action="scale_down",
                    delta=current_replicas - new_replicas,
                    reason=f"{metric_name}={value:.1f}% <= {down_threshold}%",
                    timestamp=time.time()
                ))
                recommendations.append(f"Scale down {resource_id}: {metric_name} low ({value:.1f}%)")

        self._history.extend(actions)
        return ScalingResult(actions, metrics, recommendations)

    def add_rule(self, metric: str, up_threshold: float, down_threshold: float, up_delta: int, down_delta: int) -> None:
        """Add a scaling rule."""
        self._rules.append({
            "metric": metric,
            "scale_up_threshold": up_threshold,
            "scale_down_threshold": down_threshold,
            "scale_up_delta": up_delta,
            "scale_down_delta": down_delta
        })

    def get_history(self, limit: int = 100) -> list[ScalingAction]:
        """Get scaling action history."""
        return self._history[-limit:]

    def get_current_metrics(self) -> dict[str, float]:
        """Get current metric values."""
        return self._current_metrics.copy()
