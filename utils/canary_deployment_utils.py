"""
Canary Deployment Utilities.

Provides utilities for managing canary deployments, traffic shifting,
metrics analysis, and automated rollback decisions.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import random
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class DeploymentStatus(Enum):
    """Status of a canary deployment."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class TrafficStrategy(Enum):
    """Traffic allocation strategies."""
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    IMMEDIATE = "immediate"
    RANDOM = "random"


@dataclass
class CanaryConfig:
    """Configuration for a canary deployment."""
    name: str
    baseline_version: str
    canary_version: str
    initial_traffic_percent: float = 5.0
    max_traffic_percent: float = 100.0
    traffic_increment_percent: float = 10.0
    traffic_increment_interval_seconds: int = 300
    auto_rollback_threshold: float = 0.05
    analysis_window_seconds: int = 300
    strategy: TrafficStrategy = TrafficStrategy.LINEAR


@dataclass
class CanaryMetrics:
    """Metrics collected during canary deployment."""
    timestamp: datetime
    canary_traffic_percent: float
    request_count: int
    error_count: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    success_rate: float
    health_check_passed: bool
    custom_metrics: dict[str, float] = field(default_factory=dict)


@dataclass
class CanaryDeployment:
    """Represents an active canary deployment."""
    deployment_id: str
    config: CanaryConfig
    status: DeploymentStatus
    current_traffic_percent: float
    start_time: datetime
    end_time: Optional[datetime] = None
    metrics_history: list[CanaryMetrics] = field(default_factory=list)
    rollback_reason: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class CanaryDeploymentManager:
    """Manages canary deployments and traffic shifting."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        metrics_collector: Optional[Callable[[], CanaryMetrics]] = None,
    ) -> None:
        self.db_path = db_path or Path("canary_deployments.db")
        self.metrics_collector = metrics_collector
        self._init_db()
        self._active_deployments: dict[str, CanaryDeployment] = {}

    def _init_db(self) -> None:
        """Initialize the deployment database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS deployments (
                deployment_id TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                status TEXT NOT NULL,
                current_traffic_percent REAL NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT,
                rollback_reason TEXT,
                metadata_json TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deployment_id TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (deployment_id) REFERENCES deployments(deployment_id)
            )
        """)
        conn.commit()
        conn.close()

    def start_deployment(self, config: CanaryConfig) -> CanaryDeployment:
        """Start a new canary deployment."""
        deployment_id = f"canary_{int(time.time())}_{random.randint(1000, 9999)}"

        deployment = CanaryDeployment(
            deployment_id=deployment_id,
            config=config,
            status=DeploymentStatus.IN_PROGRESS,
            current_traffic_percent=config.initial_traffic_percent,
            start_time=datetime.now(),
        )

        self._active_deployments[deployment_id] = deployment
        self._save_deployment(deployment)

        return deployment

    def pause_deployment(self, deployment_id: str) -> bool:
        """Pause an active deployment."""
        if deployment_id not in self._active_deployments:
            return False

        deployment = self._active_deployments[deployment_id]
        deployment.status = DeploymentStatus.PAUSED
        self._save_deployment(deployment)
        return True

    def resume_deployment(self, deployment_id: str) -> bool:
        """Resume a paused deployment."""
        if deployment_id not in self._active_deployments:
            return False

        deployment = self._active_deployments[deployment_id]
        deployment.status = DeploymentStatus.IN_PROGRESS
        self._save_deployment(deployment)
        return True

    def increment_traffic(
        self,
        deployment_id: str,
        increment_percent: Optional[float] = None,
    ) -> float:
        """Increment traffic to the canary version."""
        if deployment_id not in self._active_deployments:
            raise ValueError(f"Deployment not found: {deployment_id}")

        deployment = self._active_deployments[deployment_id]

        if increment_percent is None:
            increment_percent = deployment.config.traffic_increment_percent

        new_traffic = min(
            deployment.current_traffic_percent + increment_percent,
            deployment.config.max_traffic_percent,
        )

        deployment.current_traffic_percent = new_traffic
        self._save_deployment(deployment)

        return new_traffic

    def collect_metrics(self, deployment_id: str) -> Optional[CanaryMetrics]:
        """Collect current metrics for a deployment."""
        if deployment_id not in self._active_deployments:
            return None

        if self.metrics_collector:
            metrics = self.metrics_collector()
        else:
            metrics = self._generate_sample_metrics()

        deployment = self._active_deployments[deployment_id]
        deployment.metrics_history.append(metrics)
        self._save_metrics(deployment_id, metrics)

        return metrics

    def _generate_sample_metrics(self) -> CanaryMetrics:
        """Generate sample metrics for testing."""
        return CanaryMetrics(
            timestamp=datetime.now(),
            canary_traffic_percent=random.uniform(5, 50),
            request_count=random.randint(100, 10000),
            error_count=random.randint(0, 50),
            latency_p50_ms=random.uniform(10, 100),
            latency_p95_ms=random.uniform(50, 300),
            latency_p99_ms=random.uniform(100, 500),
            success_rate=random.uniform(0.95, 0.999),
            health_check_passed=random.random() > 0.05,
            custom_metrics={},
        )

    def analyze_deployment(
        self,
        deployment_id: str,
        error_rate_threshold: float = 0.05,
        latency_threshold_p99_ms: float = 500.0,
    ) -> tuple[bool, str]:
        """
        Analyze deployment metrics and determine if it should continue.
        Returns (should_continue, reason).
        """
        if deployment_id not in self._active_deployments:
            return False, "Deployment not found"

        deployment = self._active_deployments[deployment_id]
        metrics_list = deployment.metrics_history

        if not metrics_list:
            return True, "No metrics available yet"

        recent_metrics = metrics_list[-5:]
        avg_error_rate = sum(m.error_count / max(m.request_count, 1) for m in recent_metrics) / len(recent_metrics)
        avg_p99_latency = sum(m.latency_p99_ms for m in recent_metrics) / len(recent_metrics)

        if avg_error_rate > error_rate_threshold:
            return False, f"Error rate {avg_error_rate:.2%} exceeds threshold {error_rate_threshold:.2%}"

        if avg_p99_latency > latency_threshold_p99_ms:
            return False, f"P99 latency {avg_p99_latency:.0f}ms exceeds threshold {latency_threshold_p99_ms}ms"

        health_checks_passed = sum(1 for m in recent_metrics if m.health_check_passed)
        if health_checks_passed < len(recent_metrics) * 0.9:
            return False, f"Health check pass rate {health_checks_passed/len(recent_metrics):.2%} below 90%"

        return True, "Metrics within acceptable range"

    def should_rollback(
        self,
        deployment_id: str,
        error_rate_increase_threshold: float = 0.02,
        latency_increase_threshold_ms: float = 100.0,
    ) -> tuple[bool, str]:
        """Determine if a deployment should be rolled back based on metrics."""
        if deployment_id not in self._active_deployments:
            return False, "Deployment not found"

        deployment = self._active_deployments[deployment_id]
        metrics_list = deployment.metrics_history

        if len(metrics_list) < 2:
            return False, "Insufficient metrics for comparison"

        baseline_metrics = metrics_list[:len(metrics_list)//2]
        recent_metrics = metrics_list[len(metrics_list)//2:]

        if not baseline_metrics or not recent_metrics:
            return False, "Insufficient metrics for comparison"

        baseline_error_rate = sum(m.error_count / max(m.request_count, 1) for m in baseline_metrics) / len(baseline_metrics)
        recent_error_rate = sum(m.error_count / max(m.request_count, 1) for m in recent_metrics) / len(recent_metrics)
        error_rate_increase = recent_error_rate - baseline_error_rate

        baseline_p99 = sum(m.latency_p99_ms for m in baseline_metrics) / len(baseline_metrics)
        recent_p99 = sum(m.latency_p99_ms for m in recent_metrics) / len(recent_metrics)
        latency_increase = recent_p99 - baseline_p99

        if error_rate_increase > error_rate_increase_threshold:
            return True, f"Error rate increased by {error_rate_increase:.2%}"

        if latency_increase > latency_increase_threshold_ms:
            return True, f"P99 latency increased by {latency_increase:.0f}ms"

        return False, "No rollback required"

    def complete_deployment(self, deployment_id: str) -> bool:
        """Mark a deployment as successfully completed."""
        if deployment_id not in self._active_deployments:
            return False

        deployment = self._active_deployments[deployment_id]
        deployment.status = DeploymentStatus.SUCCESS
        deployment.end_time = datetime.now()
        deployment.current_traffic_percent = 100.0
        self._save_deployment(deployment)
        return True

    def rollback_deployment(
        self,
        deployment_id: str,
        reason: str = "",
    ) -> bool:
        """Rollback a canary deployment."""
        if deployment_id not in self._active_deployments:
            return False

        deployment = self._active_deployments[deployment_id]
        deployment.status = DeploymentStatus.ROLLED_BACK
        deployment.end_time = datetime.now()
        deployment.rollback_reason = reason
        self._save_deployment(deployment)
        return True

    def get_next_traffic_percent(
        self,
        deployment: CanaryDeployment,
    ) -> float:
        """Calculate the next traffic percentage based on strategy."""
        config = deployment.config
        current = deployment.current_traffic_percent

        if config.strategy == TrafficStrategy.LINEAR:
            return min(current + config.traffic_increment_percent, config.max_traffic_percent)
        elif config.strategy == TrafficStrategy.EXPONENTIAL:
            return min(current * 1.5, config.max_traffic_percent)
        elif config.strategy == TrafficStrategy.IMMEDIATE:
            return config.max_traffic_percent
        elif config.strategy == TrafficStrategy.RANDOM:
            return random.uniform(current, min(current + 20, config.max_traffic_percent))
        else:
            return current + config.traffic_increment_percent

    def get_deployment(self, deployment_id: str) -> Optional[CanaryDeployment]:
        """Get a deployment by ID."""
        return self._active_deployments.get(deployment_id)

    def list_active_deployments(self) -> list[CanaryDeployment]:
        """List all active deployments."""
        return [d for d in self._active_deployments.values() if d.status == DeploymentStatus.IN_PROGRESS]

    def _save_deployment(self, deployment: CanaryDeployment) -> None:
        """Save deployment to database."""
        import json
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO deployments (
                deployment_id, config_json, status, current_traffic_percent,
                start_time, end_time, rollback_reason, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            deployment.deployment_id,
            json.dumps({
                "name": deployment.config.name,
                "baseline_version": deployment.config.baseline_version,
                "canary_version": deployment.config.canary_version,
                "initial_traffic_percent": deployment.config.initial_traffic_percent,
                "max_traffic_percent": deployment.config.max_traffic_percent,
                "traffic_increment_percent": deployment.config.traffic_increment_percent,
                "traffic_increment_interval_seconds": deployment.config.traffic_increment_interval_seconds,
                "auto_rollback_threshold": deployment.config.auto_rollback_threshold,
                "analysis_window_seconds": deployment.config.analysis_window_seconds,
                "strategy": deployment.config.strategy.value,
            }),
            deployment.status.value,
            deployment.current_traffic_percent,
            deployment.start_time.isoformat(),
            deployment.end_time.isoformat() if deployment.end_time else None,
            deployment.rollback_reason,
            json.dumps(deployment.metadata),
        ))
        conn.commit()
        conn.close()

    def _save_metrics(self, deployment_id: str, metrics: CanaryMetrics) -> None:
        """Save metrics to database."""
        import json
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO metrics (deployment_id, metrics_json, timestamp)
            VALUES (?, ?, ?)
        """, (
            deployment_id,
            json.dumps({
                "canary_traffic_percent": metrics.canary_traffic_percent,
                "request_count": metrics.request_count,
                "error_count": metrics.error_count,
                "latency_p50_ms": metrics.latency_p50_ms,
                "latency_p95_ms": metrics.latency_p95_ms,
                "latency_p99_ms": metrics.latency_p99_ms,
                "success_rate": metrics.success_rate,
                "health_check_passed": metrics.health_check_passed,
                "custom_metrics": metrics.custom_metrics,
            }),
            metrics.timestamp.isoformat(),
        ))
        conn.commit()
        conn.close()
