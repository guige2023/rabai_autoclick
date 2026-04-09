"""API Degradation Action.

Implements graceful API degradation: circuit breaking, fallbacks,
latency budgets, and quality-of-service preservation under load.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import time


class DegradationLevel(Enum):
    FULL = "full"
    REDUCED = "reduced"
    MINIMAL = "minimal"
    OFFLINE = "offline"


@dataclass
class DegradationPolicy:
    latency_budget_ms: float = 1000.0
    error_threshold: float = 0.5
    min_success_rate: float = 0.8
    degradation_level: DegradationLevel = DegradationLevel.FULL
    fallback_enabled: bool = True
    retry_enabled: bool = True
    max_retries: int = 3


@dataclass
class HealthSnapshot:
    timestamp: datetime
    success_rate: float
    avg_latency_ms: float
    error_count: int
    degradation_level: DegradationLevel
    circuit_open: bool


T = TypeVar("T")


class APIDegradationAction:
    """Graceful degradation framework for API calls."""

    def __init__(self, policy: Optional[DegradationPolicy] = None) -> None:
        self.policy = policy or DegradationPolicy()
        self._health_log: List[HealthSnapshot] = []
        self._circuit_open: bool = False
        self._circuit_opened_at: Optional[datetime] = None
        self._request_log: List[Dict[str, Any]] = []

    def _record_request(
        self,
        endpoint: str,
        latency_ms: float,
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        self._request_log.append({
            "endpoint": endpoint,
            "latency_ms": latency_ms,
            "success": success,
            "error": error,
            "timestamp": datetime.now(),
        })
        # Keep last 1000 entries
        if len(self._request_log) > 1000:
            self._request_log = self._request_log[-1000:]

    def _health_snapshot(self, window_minutes: int = 5) -> HealthSnapshot:
        cutoff = datetime.now() - timedelta(minutes=window_minutes)
        recent = [r for r in self._request_log if r["timestamp"] > cutoff]
        if not recent:
            return HealthSnapshot(
                timestamp=datetime.now(),
                success_rate=1.0,
                avg_latency_ms=0.0,
                error_count=0,
                degradation_level=DegradationLevel.FULL,
                circuit_open=self._circuit_open,
            )
        success_rate = sum(1 for r in recent if r["success"]) / len(recent)
        avg_latency = sum(r["latency_ms"] for r in recent) / len(recent)
        error_count = sum(1 for r in recent if not r["success"])
        level = self._determine_level(success_rate, avg_latency)
        return HealthSnapshot(
            timestamp=datetime.now(),
            success_rate=success_rate,
            avg_latency_ms=avg_latency,
            error_count=error_count,
            degradation_level=level,
            circuit_open=self._circuit_open,
        )

    def _determine_level(self, success_rate: float, avg_latency: float) -> DegradationLevel:
        if success_rate < self.policy.error_threshold or avg_latency > self.policy.latency_budget_ms * 3:
            return DegradationLevel.OFFLINE
        if success_rate < self.policy.min_success_rate:
            return DegradationLevel.MINIMAL
        if avg_latency > self.policy.latency_budget_ms:
            return DegradationLevel.REDUCED
        return DegradationLevel.FULL

    def _update_circuit(self) -> None:
        snapshot = self._health_snapshot()
        if self._circuit_open:
            if snapshot.success_rate >= self.policy.min_success_rate:
                self._circuit_open = False
                self._circuit_opened_at = None
        else:
            if snapshot.success_rate < self.policy.error_threshold:
                self._circuit_open = True
                self._circuit_opened_at = datetime.now()

    def execute(
        self,
        endpoint: str,
        primary_fn: Callable[[], T],
        fallback_fn: Optional[Callable[[], T]] = None,
    ) -> T:
        self._update_circuit()
        if self._circuit_open:
            if fallback_fn and self.policy.fallback_enabled:
                return fallback_fn()
            raise RuntimeError(f"Circuit open for {endpoint}")
        start = time.time()
        try:
            result = primary_fn()
            latency_ms = (time.time() - start) * 1000
            self._record_request(endpoint, latency_ms, success=True)
            return result
        except Exception as e:
            latency_ms = (time.time() - start) * 1000
            self._record_request(endpoint, latency_ms, success=False, error=str(e))
            self._update_circuit()
            if fallback_fn and self.policy.fallback_enabled:
                return fallback_fn()
            raise

    def get_health(self) -> Dict[str, Any]:
        snapshot = self._health_snapshot()
        return {
            "degradation_level": snapshot.degradation_level.value,
            "success_rate": round(snapshot.success_rate, 4),
            "avg_latency_ms": round(snapshot.avg_latency_ms, 2),
            "error_count": snapshot.error_count,
            "circuit_open": snapshot.circuit_open,
            "circuit_opened_at": self._circuit_opened_at.isoformat() if self._circuit_opened_at else None,
        }
