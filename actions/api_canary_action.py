"""
API Canary Deployment Action Module

Provides canary deployment capabilities for API services with traffic shifting,
metric analysis, automated rollback, and progressive rollout management.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class CanaryStatus(Enum):
    """Canary deployment status."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PASSED = "passed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"


class RolloutStrategy(Enum):
    """Traffic rollout strategies."""

    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    IMMEDIATE = "immediate"
    MANUAL = "manual"


@dataclass
class CanaryStage:
    """A single stage in canary rollout."""

    stage_id: str
    traffic_percentage: float
    duration_seconds: float
    metric_thresholds: Dict[str, float] = field(default_factory=dict)
    min_request_count: int = 100


@dataclass
class CanaryMetrics:
    """Metrics collected during canary deployment."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    error_rate: float = 0.0
    timestamp: Optional[float] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

    def calculate_error_rate(self) -> float:
        """Calculate error rate percentage."""
        if self.total_requests == 0:
            return 0.0
        return (self.failed_requests / self.total_requests) * 100


@dataclass
class CanaryDeployment:
    """A canary deployment instance."""

    deployment_id: str
    name: str
    baseline_version: str
    canary_version: str
    status: CanaryStatus = CanaryStatus.PENDING
    current_traffic_percentage: float = 0.0
    target_traffic_percentage: float = 100.0
    stages: List[CanaryStage] = field(default_factory=list)
    metrics: Optional[CanaryMetrics] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    rollback_reason: Optional[str] = None


@dataclass
class CanaryConfig:
    """Configuration for canary deployment."""

    initial_traffic_percentage: float = 5.0
    max_traffic_percentage: float = 100.0
    traffic_increment: float = 10.0
    stage_duration_seconds: float = 300.0
    auto_rollback_on_failure: bool = True
    rollback_threshold_error_rate: float = 5.0
    rollback_threshold_latency_increase: float = 50.0
    min_request_count_per_stage: int = 100
    rollout_strategy: RolloutStrategy = RolloutStrategy.LINEAR


class APIHealthChecker:
    """Health checker for API endpoints."""

    def __init__(self):
        self._health_cache: Dict[str, bool] = {}

    async def check_health(
        self,
        endpoint: str,
        timeout: float = 5.0,
    ) -> bool:
        """Check if an endpoint is healthy."""
        try:
            # Simulate health check
            await asyncio.sleep(0.01)
            return True
        except Exception:
            return False

    async def check_multiple(
        self,
        endpoints: List[str],
    ) -> Dict[str, bool]:
        """Check health of multiple endpoints."""
        results = {}
        for endpoint in endpoints:
            results[endpoint] = await self.check_health(endpoint)
        return results


class TrafficSplitter:
    """Splits traffic between baseline and canary versions."""

    def __init__(self, config: Optional[CanaryConfig] = None):
        self.config = config or CanaryConfig()

    def should_route_to_canary(self, traffic_percentage: float) -> bool:
        """Determine if a request should be routed to canary."""
        return random.random() * 100 < traffic_percentage

    def split_metrics(
        self,
        baseline_metrics: CanaryMetrics,
        canary_metrics: CanaryMetrics,
        traffic_percentage: float,
    ) -> CanaryMetrics:
        """Calculate weighted combined metrics."""
        baseline_weight = 1 - (traffic_percentage / 100)
        canary_weight = traffic_percentage / 100

        combined = CanaryMetrics(
            total_requests=baseline_metrics.total_requests + canary_metrics.total_requests,
            successful_requests=int(
                baseline_metrics.successful_requests * baseline_weight +
                canary_metrics.successful_requests * canary_weight
            ),
            failed_requests=int(
                baseline_metrics.failed_requests * baseline_weight +
                canary_metrics.failed_requests * canary_weight
            ),
            latency_p50_ms=(
                baseline_metrics.latency_p50_ms * baseline_weight +
                canary_metrics.latency_p50_ms * canary_weight
            ),
            latency_p95_ms=(
                baseline_metrics.latency_p95_ms * baseline_weight +
                canary_metrics.latency_p95_ms * canary_weight
            ),
            latency_p99_ms=(
                baseline_metrics.latency_p99_ms * baseline_weight +
                canary_metrics.latency_p99_ms * canary_weight
            ),
        )
        combined.error_rate = combined.calculate_error_rate()
        return combined


class MetricAnalyzer:
    """Analyzes canary metrics against baseline."""

    def __init__(self, config: Optional[CanaryConfig] = None):
        self.config = config or CanaryConfig()

    def analyze(
        self,
        baseline: CanaryMetrics,
        canary: CanaryMetrics,
    ) -> Dict[str, Any]:
        """
        Analyze canary metrics compared to baseline.

        Returns:
            Analysis result with pass/fail status and recommendations
        """
        error_rate_increase = canary.error_rate - baseline.error_rate
        latency_p99_increase = canary.latency_p99_ms - baseline.latency_p99_ms

        passed = True
        reasons = []

        if canary.error_rate > self.config.rollback_threshold_error_rate:
            passed = False
            reasons.append(
                f"Error rate {canary.error_rate:.2f}% exceeds threshold "
                f"{self.config.rollback_threshold_error_rate}%"
            )

        latency_increase_pct = (
            (canary.latency_p99_ms / baseline.latency_p99_ms - 1) * 100
            if baseline.latency_p99_ms > 0 else 0
        )
        if latency_increase_pct > self.config.rollback_threshold_latency_increase:
            passed = False
            reasons.append(
                f"Latency increase {latency_increase_pct:.1f}% exceeds threshold "
                f"{self.config.rollback_threshold_latency_increase}%"
            )

        if canary.total_requests < self.config.min_request_count_per_stage:
            passed = False
            reasons.append(
                f"Insufficient requests ({canary.total_requests}) "
                f"for statistical significance"
            )

        return {
            "passed": passed,
            "error_rate_increase": error_rate_increase,
            "latency_p99_increase_ms": latency_p99_increase,
            "latency_increase_percentage": latency_increase_pct,
            "reasons": reasons,
            "recommendation": "continue" if passed else "rollback",
        }


class APICanaryAction:
    """
    Canary deployment action for API services.

    Features:
    - Progressive traffic shifting (5% -> 10% -> 25% -> 50% -> 100%)
    - Automatic metric collection and analysis
    - Configurable rollback triggers
    - Multiple rollout strategies (linear, exponential, immediate)
    - Stage-based deployments with validation gates
    - Traffic percentage tracking and reporting

    Usage:
        canary = APICanaryAction(config)
        deployment = canary.create_deployment(
            name="api-v2",
            baseline_version="v1.0.0",
            canary_version="v2.0.0",
        )
        await canary.execute_deployment(deployment)
    """

    def __init__(self, config: Optional[CanaryConfig] = None):
        self.config = config or CanaryConfig()
        self._health_checker = APIHealthChecker()
        self._splitter = TrafficSplitter(self.config)
        self._analyzer = MetricAnalyzer(self.config)
        self._deployments: Dict[str, CanaryDeployment] = {}
        self._stats = {
            "deployments_started": 0,
            "deployments_passed": 0,
            "deployments_failed": 0,
            "deployments_rolled_back": 0,
        }

    def create_deployment(
        self,
        name: str,
        baseline_version: str,
        canary_version: str,
        target_traffic: float = 100.0,
    ) -> CanaryDeployment:
        """Create a new canary deployment."""
        import uuid
        deployment_id = f"canary_{uuid.uuid4().hex[:12]}"

        stages = self._generate_stages(target_traffic)

        deployment = CanaryDeployment(
            deployment_id=deployment_id,
            name=name,
            baseline_version=baseline_version,
            canary_version=canary_version,
            target_traffic_percentage=target_traffic,
            stages=stages,
        )
        self._deployments[deployment_id] = deployment
        self._stats["deployments_started"] += 1
        return deployment

    def _generate_stages(self, target_traffic: float) -> List[CanaryStage]:
        """Generate deployment stages based on config."""
        stages = []
        stage_id = 1
        current_traffic = self.config.initial_traffic_percentage

        while current_traffic <= target_traffic:
            stage = CanaryStage(
                stage_id=f"stage_{stage_id}",
                traffic_percentage=min(current_traffic, target_traffic),
                duration_seconds=self.config.stage_duration_seconds,
            )
            stages.append(stage)

            if self.config.rollout_strategy == RolloutStrategy.EXPONENTIAL:
                current_traffic *= 2
            elif self.config.rollout_strategy == RolloutStrategy.LINEAR:
                current_traffic += self.config.traffic_increment
            else:
                current_traffic = target_traffic

            stage_id += 1

        return stages

    async def execute_deployment(
        self,
        deployment: CanaryDeployment,
        request_handler: Optional[Callable[..., Any]] = None,
    ) -> CanaryDeployment:
        """
        Execute a canary deployment.

        Args:
            deployment: Deployment to execute
            request_handler: Optional handler for test requests

        Returns:
            Updated deployment with final status
        """
        logger.info(f"Starting canary deployment: {deployment.deployment_id}")
        deployment.status = CanaryStatus.IN_PROGRESS
        deployment.started_at = time.time()

        try:
            for stage in deployment.stages:
                logger.info(
                    f"Stage {stage.stage_id}: "
                    f"Traffic = {stage.traffic_percentage}%"
                )

                deployment.current_traffic_percentage = stage.traffic_percentage

                # Collect metrics during stage
                metrics = await self._collect_stage_metrics(
                    deployment, stage, request_handler
                )
                deployment.metrics = metrics

                # Analyze metrics
                analysis = self._analyze_stage(deployment)

                if not analysis["passed"]:
                    logger.warning(
                        f"Stage failed: {', '.join(analysis['reasons'])}"
                    )
                    if self.config.auto_rollback_on_failure:
                        deployment.status = CanaryStatus.FAILED
                        deployment.rollback_reason = "; ".join(analysis["reasons"])
                        await self._rollback(deployment)
                        self._stats["deployments_rolled_back"] += 1
                        return deployment
                    else:
                        deployment.status = CanaryStatus.FAILED
                        return deployment

                # Wait for stage duration
                await asyncio.sleep(min(stage.duration_seconds, 1))

            deployment.status = CanaryStatus.PASSED
            deployment.current_traffic_percentage = deployment.target_traffic_percentage
            self._stats["deployments_passed"] += 1

        except Exception as e:
            logger.error(f"Deployment error: {e}")
            deployment.status = CanaryStatus.FAILED
            deployment.rollback_reason = str(e)
            await self._rollback(deployment)

        deployment.completed_at = time.time()
        return deployment

    async def _collect_stage_metrics(
        self,
        deployment: CanaryDeployment,
        stage: CanaryStage,
        handler: Optional[Callable[..., Any]] = None,
    ) -> CanaryMetrics:
        """Collect metrics during a deployment stage."""
        # Simulate metric collection
        await asyncio.sleep(0.1)

        metrics = CanaryMetrics(
            total_requests=stage.min_request_count * 2,
            successful_requests=int(stage.min_request_count * 1.95),
            failed_requests=int(stage.min_request_count * 0.05),
            latency_p50_ms=45.0 + random.uniform(-5, 5),
            latency_p95_ms=120.0 + random.uniform(-10, 10),
            latency_p99_ms=200.0 + random.uniform(-15, 15),
        )
        metrics.error_rate = metrics.calculate_error_rate()
        return metrics

    def _analyze_stage(
        self,
        deployment: CanaryDeployment,
    ) -> Dict[str, Any]:
        """Analyze current stage metrics."""
        if deployment.metrics is None:
            return {"passed": True, "reasons": [], "recommendation": "continue"}

        # Baseline metrics (simulated historical)
        baseline = CanaryMetrics(
            total_requests=1000,
            successful_requests=985,
            failed_requests=15,
            latency_p50_ms=42.0,
            latency_p95_ms=110.0,
            latency_p99_ms=180.0,
        )
        baseline.error_rate = baseline.calculate_error_rate()

        return self._analyzer.analyze(baseline, deployment.metrics)

    async def _rollback(self, deployment: CanaryDeployment) -> None:
        """Rollback a canary deployment."""
        logger.info(f"Rolling back deployment: {deployment.deployment_id}")
        deployment.current_traffic_percentage = 0.0
        deployment.status = CanaryStatus.ROLLED_BACK

    async def pause_deployment(self, deployment_id: str) -> Optional[CanaryDeployment]:
        """Pause a running deployment."""
        deployment = self._deployments.get(deployment_id)
        if deployment and deployment.status == CanaryStatus.IN_PROGRESS:
            deployment.status = CanaryStatus.PENDING
        return deployment

    async def resume_deployment(
        self,
        deployment_id: str,
    ) -> Optional[CanaryDeployment]:
        """Resume a paused deployment."""
        deployment = self._deployments.get(deployment_id)
        if deployment and deployment.status == CanaryStatus.PENDING:
            deployment.status = CanaryStatus.IN_PROGRESS
        return deployment

    def get_deployment(self, deployment_id: str) -> Optional[CanaryDeployment]:
        """Get a deployment by ID."""
        return self._deployments.get(deployment_id)

    def get_active_deployments(self) -> List[CanaryDeployment]:
        """Get all active (in-progress) deployments."""
        return [
            d for d in self._deployments.values()
            if d.status == CanaryStatus.IN_PROGRESS
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get canary deployment statistics."""
        return self._stats.copy()


async def demo_canary():
    """Demonstrate canary deployment usage."""
    config = CanaryConfig(
        initial_traffic_percentage=10.0,
        traffic_increment=20.0,
        stage_duration_seconds=1.0,
    )
    canary = APICanaryAction(config)

    deployment = canary.create_deployment(
        name="api-v2-rollout",
        baseline_version="v1.0.0",
        canary_version="v2.0.0",
    )

    result = await canary.execute_deployment(deployment)
    print(f"Deployment status: {result.status.value}")
    print(f"Final traffic: {result.current_traffic_percentage}%")
    print(f"Stats: {canary.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_canary())
