"""API Reliability Score Action.

Computes reliability scores for API endpoints based on success rates,
latency stability, and error patterns. Helps identify degraded services.
"""
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import math


@dataclass
class ReliabilityWindow:
    timestamp: datetime
    success: bool
    latency_ms: float
    status_code: int
    error_type: Optional[str] = None


class APIReliabilityScoreAction:
    """Computes reliability scores for API endpoints."""

    def __init__(self, window_minutes: int = 60) -> None:
        self.window_minutes = window_minutes
        self._data: Dict[str, List[ReliabilityWindow]] = defaultdict(list)

    def record(
        self,
        endpoint: str,
        success: bool,
        latency_ms: float,
        status_code: int,
        error_type: Optional[str] = None,
    ) -> None:
        entry = ReliabilityWindow(
            timestamp=datetime.now(),
            success=success,
            latency_ms=latency_ms,
            status_code=status_code,
            error_type=error_type,
        )
        self._data[endpoint].append(entry)
        self._prune(endpoint)

    def _prune(self, endpoint: str) -> None:
        cutoff = datetime.now() - timedelta(minutes=self.window_minutes)
        self._data[endpoint] = [
            w for w in self._data[endpoint] if w.timestamp > cutoff
        ]

    def _success_rate(self, endpoint: str) -> float:
        windows = self._data.get(endpoint, [])
        if not windows:
            return 1.0
        return sum(1 for w in windows if w.success) / len(windows)

    def _latency_stability(self, endpoint: str) -> float:
        windows = self._data.get(endpoint, [])
        if not windows:
            return 1.0
        latencies = [w.latency_ms for w in windows]
        mean = sum(latencies) / len(latencies)
        if mean == 0:
            return 1.0
        variance = sum((l - mean) ** 2 for l in latencies) / len(latencies)
        std_dev = math.sqrt(variance)
        # Lower std deviation = higher stability (score 0-1)
        # Using coefficient of variation inverted
        cv = std_dev / mean if mean > 0 else 0
        return max(0.0, 1.0 - min(cv, 1.0))

    def _error_diversity(self, endpoint: str) -> float:
        windows = self._data.get(endpoint, [])
        error_types = {w.error_type for w in windows if not w.success and w.error_type}
        if not error_types:
            return 1.0
        # Fewer unique error types = more predictable = higher score
        return max(0.0, 1.0 - (len(error_types) - 1) * 0.1)

    def reliability_score(self, endpoint: str) -> float:
        sr = self._success_rate(endpoint)
        ls = self._latency_stability(endpoint)
        ed = self._error_diversity(endpoint)
        # Weighted average: success rate most important
        return round(sr * 0.6 + ls * 0.25 + ed * 0.15, 4)

    def health_status(self, endpoint: str) -> str:
        score = self.reliability_score(endpoint)
        if score >= 0.95:
            return "healthy"
        elif score >= 0.80:
            return "degraded"
        elif score >= 0.60:
            return "unhealthy"
        return "critical"

    def all_scores(self) -> Dict[str, Dict[str, Any]]:
        return {
            ep: {
                "score": self.reliability_score(ep),
                "status": self.health_status(ep),
                "success_rate": self._success_rate(ep),
                "latency_stability": self._latency_stability(ep),
                "error_diversity": self._error_diversity(ep),
                "sample_count": len(self._data.get(ep, [])),
            }
            for ep in self._data
        }
