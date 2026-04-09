"""Data Correlator Action Module.

Provides data correlation analysis for identifying relationships
between datasets, time series, and events.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class CorrelationType(Enum):
    """Types of correlation analysis."""
    PEARSON = "pearson"
    SPEARMAN = "spearman"
    KENDALL = "kendall"
    CROSS_CORRELATION = "cross_correlation"
    AUTO_CORRELATION = "auto_correlation"
    LAG_CORRELATION = "lag_correlation"


@dataclass
class CorrelationResult:
    """Result of a correlation analysis."""
    correlation_type: CorrelationType
    coefficient: float
    p_value: Optional[float] = None
    significant: bool = False
    lag: int = 0


@dataclass
class TimeSeriesPoint:
    """A point in a time series."""
    timestamp: float
    value: float


class CorrelationAnalyzer:
    """Analyzes correlations between data series."""

    @staticmethod
    def pearson(x: List[float], y: List[float]) -> CorrelationResult:
        """Calculate Pearson correlation coefficient."""
        n = len(x)
        if n != len(y) or n < 2:
            return CorrelationResult(CorrelationType.PEARSON, 0.0)

        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return CorrelationResult(CorrelationType.PEARSON, 0.0)

        r = numerator / (denom_x * denom_y)
        p_value = CorrelationAnalyzer._calculate_p_value(r, n)

        return CorrelationResult(
            correlation_type=CorrelationType.PEARSON,
            coefficient=r,
            p_value=p_value,
            significant=p_value < 0.05 if p_value else False
        )

    @staticmethod
    def spearman(x: List[float], y: List[float]) -> CorrelationResult:
        """Calculate Spearman rank correlation."""
        n = len(x)
        if n != len(y) or n < 2:
            return CorrelationResult(CorrelationType.SPEARMAN, 0.0)

        ranks_x = CorrelationAnalyzer._rank_data(x)
        ranks_y = CorrelationAnalyzer._rank_data(y)

        return CorrelationAnalyzer.pearson(ranks_x, ranks_y)

    @staticmethod
    def kendall(x: List[float], y: List[float]) -> CorrelationResult:
        """Calculate Kendall tau correlation."""
        n = len(x)
        if n != len(y) or n < 2:
            return CorrelationResult(CorrelationType.KENDALL, 0.0)

        concordant = 0
        discordant = 0

        for i in range(n):
            for j in range(i + 1, n):
                x_diff = x[i] - x[j]
                y_diff = y[i] - y[j]
                product = x_diff * y_diff

                if product > 0:
                    concordant += 1
                elif product < 0:
                    discordant += 1

        tau = (concordant - discordant) / (n * (n - 1) / 2)

        return CorrelationResult(
            correlation_type=CorrelationType.KENDALL,
            coefficient=tau
        )

    @staticmethod
    def cross_correlation(
        x: List[float],
        y: List[float],
        max_lag: int = 10
    ) -> List[CorrelationResult]:
        """Calculate cross-correlation with different lags."""
        results = []
        n = len(x)

        for lag in range(-max_lag, max_lag + 1):
            if lag < 0:
                x_slice = x[:lag]
                y_slice = y[-lag:]
            elif lag > 0:
                x_slice = x[lag:]
                y_slice = y[:-lag]
            else:
                x_slice = x
                y_slice = y

            if len(x_slice) < 2:
                results.append(CorrelationResult(CorrelationType.CROSS_CORRELATION, 0.0, lag=lag))
                continue

            pearson_result = CorrelationAnalyzer.pearson(x_slice, y_slice)
            pearson_result.lag = lag
            pearson_result.correlation_type = CorrelationType.CROSS_CORRELATION
            results.append(pearson_result)

        return results

    @staticmethod
    def auto_correlation(
        series: List[float],
        max_lag: int = 10
    ) -> List[CorrelationResult]:
        """Calculate autocorrelation at different lags."""
        return CorrelationAnalyzer.cross_correlation(series, series, max_lag)

    @staticmethod
    def lag_correlation(
        series1: List[float],
        series2: List[float],
        max_lag: int = 10
    ) -> CorrelationResult:
        """Find the lag with maximum correlation between two series."""
        cross_results = CorrelationAnalyzer.cross_correlation(
            series1, series2, max_lag
        )

        best = max(cross_results, key=lambda r: abs(r.coefficient), default=None)

        if best:
            return best

        return CorrelationResult(CorrelationType.LAG_CORRELATION, 0.0)

    @staticmethod
    def _rank_data(data: List[float]) -> List[float]:
        """Convert data to ranks."""
        sorted_indices = sorted(range(len(data)), key=lambda i: data[i])
        ranks = [0] * len(data)
        for rank_val, idx in enumerate(sorted_indices):
            ranks[idx] = rank_val + 1
        return ranks

    @staticmethod
    def _calculate_p_value(r: float, n: int) -> float:
        """Calculate approximate p-value for correlation."""
        if n <= 2:
            return 1.0

        t = r * math.sqrt((n - 2) / (1 - r * r + 1e-10))
        df = n - 2

        # Simple approximation
        p = 1.0 / (1.0 + t * t / df)
        return 1.0 - p


class TimeSeriesCorrelator:
    """Correlates time series data."""

    def __init__(self, tolerance_seconds: float = 1.0):
        self._tolerance = tolerance_seconds

    def correlate_events(
        self,
        events1: List[TimeSeriesPoint],
        events2: List[TimeSeriesPoint],
        window_seconds: float = 60.0
    ) -> Dict[str, Any]:
        """Correlate events between two time series within windows."""
        matches = []
        unmatched_1 = []
        unmatched_2 = []

        matched_2 = set()

        for e1 in events1:
            best_match = None
            best_distance = float('inf')

            for i, e2 in enumerate(events2):
                if i in matched_2:
                    continue

                distance = abs(e1.timestamp - e2.timestamp)
                if distance <= window_seconds and distance < best_distance:
                    best_distance = distance
                    best_match = e2
                    best_idx = i

            if best_match:
                matches.append({
                    "event1": {"timestamp": e1.timestamp, "value": e1.value},
                    "event2": {"timestamp": best_match.timestamp, "value": best_match.value},
                    "distance": best_distance
                })
                matched_2.add(best_idx)
            else:
                unmatched_1.append({"timestamp": e1.timestamp, "value": e1.value})

        for i, e2 in enumerate(events2):
            if i not in matched_2:
                unmatched_2.append({"timestamp": e2.timestamp, "value": e2.value})

        return {
            "matches": matches,
            "unmatched_1": unmatched_1,
            "unmatched_2": unmatched_2,
            "match_rate": len(matches) / len(events1) if events1 else 0
        }


class EventCorrelator:
    """Correlates events from multiple sources."""

    def __init__(self):
        self._event_stores: Dict[str, List[Dict[str, Any]]] = {}

    def store_event(
        self,
        source: str,
        event_type: str,
        data: Dict[str, Any],
        timestamp: Optional[float] = None
    ) -> None:
        """Store an event from a source."""
        if source not in self._event_stores:
            self._event_stores[source] = []

        self._event_stores[source].append({
            "type": event_type,
            "data": data,
            "timestamp": timestamp or 0,
            "stored_at": len(self._event_stores[source])
        })

    def find_correlations(
        self,
        pattern: List[str],
        time_window: float = 60.0
    ) -> List[Dict[str, Any]]:
        """Find event sequences matching a pattern."""
        if not pattern:
            return []

        results = []
        first_source = pattern[0]

        for event in self._event_stores.get(first_source, []):
            sequence = [event]
            current_time = event["timestamp"]

            for next_source in pattern[1:]:
                next_event = self._find_next_event(
                    next_source, current_time, time_window
                )
                if next_event:
                    sequence.append(next_event)
                    current_time = next_event["timestamp"]
                else:
                    break
            else:
                if len(sequence) == len(pattern):
                    results.append(sequence)

        return results

    def _find_next_event(
        self,
        source: str,
        after_time: float,
        window: float
    ) -> Optional[Dict[str, Any]]:
        """Find the next event from a source after a time."""
        events = self._event_stores.get(source, [])
        for event in events:
            if event["timestamp"] >= after_time and \
               event["timestamp"] <= after_time + window:
                return event
        return None


class DataCorrelatorAction:
    """Main action class for data correlation."""

    def __init__(self):
        self._analyzer = CorrelationAnalyzer()
        self._ts_correlator = TimeSeriesCorrelator()
        self._event_correlator = EventCorrelator()

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data correlator action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with correlation results.
        """
        operation = context.get("operation", "correlate")

        if operation == "correlate":
            x = context.get("x", [])
            y = context.get("y", [])
            method = context.get("method", "pearson")

            try:
                corr_type = CorrelationType(method)
            except ValueError:
                corr_type = CorrelationType.PEARSON

            if corr_type == CorrelationType.PEARSON:
                result = self._analyzer.pearson(x, y)
            elif corr_type == CorrelationType.SPEARMAN:
                result = self._analyzer.spearman(x, y)
            elif corr_type == CorrelationType.KENDALL:
                result = self._analyzer.kendall(x, y)
            else:
                result = self._analyzer.pearson(x, y)

            return {
                "success": True,
                "correlation": round(result.coefficient, 4),
                "type": result.correlation_type.value,
                "p_value": round(result.p_value, 4) if result.p_value else None,
                "significant": result.significant
            }

        elif operation == "cross_correlate":
            x = context.get("x", [])
            y = context.get("y", [])
            max_lag = context.get("max_lag", 10)

            results = self._analyzer.cross_correlation(x, y, max_lag)
            best = max(results, key=lambda r: abs(r.coefficient), default=None)

            return {
                "success": True,
                "best_lag": best.lag if best else 0,
                "best_correlation": round(best.coefficient, 4) if best else 0,
                "correlations": [
                    {"lag": r.lag, "coefficient": round(r.coefficient, 4)}
                    for r in results
                ]
            }

        elif operation == "auto_correlate":
            series = context.get("series", [])
            max_lag = context.get("max_lag", 10)

            results = self._analyzer.auto_correlation(series, max_lag)
            return {
                "success": True,
                "correlations": [
                    {"lag": r.lag, "coefficient": round(r.coefficient, 4)}
                    for r in results
                ]
            }

        elif operation == "correlate_events":
            events1 = [
                TimeSeriesPoint(t["timestamp"], t["value"])
                for t in context.get("events1", [])
            ]
            events2 = [
                TimeSeriesPoint(t["timestamp"], t["value"])
                for t in context.get("events2", [])
            ]
            window = context.get("window_seconds", 60.0)

            result = self._ts_correlator.correlate_events(events1, events2, window)
            return {
                "success": True,
                "match_count": len(result["matches"]),
                "match_rate": round(result["match_rate"], 4),
                "unmatched_1": len(result["unmatched_1"]),
                "unmatched_2": len(result["unmatched_2"])
            }

        elif operation == "store_event":
            self._event_correlator.store_event(
                source=context.get("source", ""),
                event_type=context.get("event_type", ""),
                data=context.get("data", {}),
                timestamp=context.get("timestamp")
            )
            return {"success": True}

        elif operation == "find_pattern":
            pattern = context.get("pattern", [])
            window = context.get("time_window", 60.0)

            results = self._event_correlator.find_correlations(pattern, window)
            return {
                "success": True,
                "matches": len(results),
                "sequences": results
            }

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
