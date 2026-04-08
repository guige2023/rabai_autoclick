"""
Data Analytics Action Module.

Provides data aggregation, statistical analysis, trend detection,
anomaly identification, and reporting capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import math
import statistics
from collections import defaultdict, Counter
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class TrendDirection(Enum):
    """Trend direction indicators."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"
    UNKNOWN = "unknown"


class AnomalyType(Enum):
    """Types of anomalies."""
    POINT = "point"
    CONTEXTUAL = "contextual"
    COLLECTIVE = "collective"


@dataclass
class StatisticalSummary:
    """Statistical summary of a dataset."""
    count: int
    sum: float
    mean: float
    median: float
    mode: float
    std_dev: float
    variance: float
    min_value: float
    max_value: float
    range_value: float
    q1: float
    q2: float
    q3: float
    iqr: float

    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return {
            "count": self.count,
            "sum": self.sum,
            "mean": self.mean,
            "median": self.median,
            "mode": self.mode,
            "std_dev": self.std_dev,
            "variance": self.variance,
            "min": self.min_value,
            "max": self.max_value,
            "range": self.range_value,
            "q1": self.q1,
            "q2": self.q2,
            "q3": self.q3,
            "iqr": self.iqr
        }


@dataclass
class TimeSeriesPoint:
    """Single point in a time series."""
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrendResult:
    """Result of trend analysis."""
    direction: TrendDirection
    slope: float
    intercept: float
    r_squared: float
    confidence: float


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    anomaly_type: AnomalyType
    score: float
    threshold: float
    details: Dict[str, Any]


@dataclass
class AggregationResult:
    """Result of data aggregation."""
    group_key: Any
    count: int
    sum: float
    average: float
    min: float
    max: float
    first: Any
    last: Any


class StatisticalAnalyzer:
    """Statistical analysis operations."""

    @staticmethod
    def compute_summary(values: List[float]) -> StatisticalSummary:
        """Compute comprehensive statistical summary."""
        if not values:
            raise ValueError("Cannot compute summary of empty dataset")

        count = len(values)
        data_sum = sum(values)
        mean_val = data_sum / count
        sorted_vals = sorted(values)
        median_val = statistics.median(values)

        mode_val = statistics.mode(values) if len(set(values)) < count else values[0]

        variance_val = statistics.variance(values) if count > 1 else 0.0
        std_dev_val = math.sqrt(variance_val)

        min_val = min(values)
        max_val = max(values)
        range_val = max_val - min_val

        q1_idx = count // 4
        q2_idx = count // 2
        q3_idx = (3 * count) // 4

        q1 = sorted_vals[q1_idx] if count > 1 else min_val
        q2 = sorted_vals[q2_idx] if count > 1 else median_val
        q3 = sorted_vals[q3_idx] if count > 1 else max_val
        iqr = q3 - q1

        return StatisticalSummary(
            count=count,
            sum=data_sum,
            mean=mean_val,
            median=median_val,
            mode=mode_val,
            std_dev=std_dev_val,
            variance=variance_val,
            min_value=min_val,
            max_value=max_val,
            range_value=range_val,
            q1=q1,
            q2=q2,
            q3=q3,
            iqr=iqr
        )

    @staticmethod
    def pearson_correlation(x: List[float], y: List[float]) -> float:
        """Compute Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            raise ValueError("Lists must have same length and at least 2 elements")

        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)

    @staticmethod
    def percentile(values: List[float], p: float) -> float:
        """Compute percentile of values."""
        if not values or p < 0 or p > 100:
            raise ValueError("Invalid input")
        sorted_vals = sorted(values)
        k = (len(sorted_vals) - 1) * p / 100
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


class TimeSeriesAnalyzer:
    """Time series analysis operations."""

    def __init__(self, time_series: List[TimeSeriesPoint]):
        self.time_series = sorted(time_series, key=lambda p: p.timestamp)

    def detect_trend(self, window_size: Optional[int] = None) -> TrendResult:
        """Detect trend using linear regression."""
        if not self.time_series:
            return TrendResult(
                direction=TrendDirection.UNKNOWN,
                slope=0.0,
                intercept=0.0,
                r_squared=0.0,
                confidence=0.0
            )

        n = len(self.time_series)
        window = window_size or n

        points = self.time_series[-window:]
        times = [(p.timestamp - points[0].timestamp).total_seconds() for p in points]
        values = [p.value for p in points]

        if all(v == values[0] for v in values):
            return TrendResult(
                direction=TrendDirection.STABLE,
                slope=0.0,
                intercept=values[0],
                r_squared=1.0,
                confidence=1.0
            )

        times_mean = sum(times) / len(times)
        values_mean = sum(values) / len(values)

        numerator = sum((times[i] - times_mean) * (values[i] - values_mean) for i in range(len(times)))
        denominator = sum((t - times_mean) ** 2 for t in times)

        slope = numerator / denominator if denominator != 0 else 0.0
        intercept = values_mean - slope * times_mean

        predictions = [slope * t + intercept for t in times]
        ss_res = sum((values[i] - predictions[i]) ** 2 for i in range(len(values)))
        ss_tot = sum((v - values_mean) ** 2 for v in values)

        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

        if abs(slope) < 0.01:
            direction = TrendDirection.STABLE
        elif slope > 0:
            direction = TrendDirection.UP
        else:
            direction = TrendDirection.DOWN

        confidence = max(0.0, min(1.0, r_squared))

        return TrendResult(
            direction=direction,
            slope=slope,
            intercept=intercept,
            r_squared=r_squared,
            confidence=confidence
        )

    def detect_anomalies_zscore(
        self,
        threshold: float = 3.0
    ) -> List[Tuple[TimeSeriesPoint, AnomalyResult]]:
        """Detect anomalies using z-score method."""
        if len(self.time_series) < 3:
            return []

        values = [p.value for p in self.time_series]
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))

        if std == 0:
            return []

        anomalies = []
        for point in self.time_series:
            z_score = abs((point.value - mean) / std)
            is_anomaly = z_score > threshold
            anomalies.append((
                point,
                AnomalyResult(
                    is_anomaly=is_anomaly,
                    anomaly_type=AnomalyType.POINT,
                    score=z_score,
                    threshold=threshold,
                    details={"z_score": z_score, "mean": mean, "std": std}
                )
            ))

        return anomalies

    def detect_anomalies_iqr(
        self,
        multiplier: float = 1.5
    ) -> List[Tuple[TimeSeriesPoint, AnomalyResult]]:
        """Detect anomalies using IQR method."""
        if len(self.time_series) < 4:
            return []

        values = [p.value for p in self.time_series]
        sorted_vals = sorted(values)
        n = len(sorted_vals)

        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[(3 * n) // 4]
        iqr = q3 - q1

        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        anomalies = []
        for point in self.time_series:
            is_anomaly = point.value < lower_bound or point.value > upper_bound
            anomalies.append((
                point,
                AnomalyResult(
                    is_anomaly=is_anomaly,
                    anomaly_type=AnomalyType.CONTEXTUAL,
                    score=point.value,
                    threshold=upper_bound if point.value > upper_bound else lower_bound,
                    details={
                        "q1": q1, "q3": q3, "iqr": iqr,
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound
                    }
                )
            ))

        return anomalies

    def moving_average(self, window_size: int) -> List[TimeSeriesPoint]:
        """Compute moving average."""
        if window_size < 1 or window_size > len(self.time_series):
            return self.time_series.copy()

        result = []
        for i in range(len(self.time_series)):
            start_idx = max(0, i - window_size + 1)
            window = self.time_series[start_idx:i + 1]
            avg_value = sum(p.value for p in window) / len(window)
            result.append(TimeSeriesPoint(
                timestamp=self.time_series[i].timestamp,
                value=avg_value,
                metadata={"window": window_size}
            ))

        return result


class DataAggregator:
    """Data aggregation operations."""

    def __init__(self):
        self.aggregations: Dict[str, List[AggregationResult]] = {}

    def aggregate_by_key(
        self,
        data: List[Dict[str, Any]],
        group_key: str,
        value_key: str,
        aggregations: Optional[List[str]] = None
    ) -> List[AggregationResult]:
        """Aggregate data by key."""
        if aggregations is None:
            aggregations = ["count", "sum", "avg", "min", "max"]

        groups: Dict[Any, List[Any]] = defaultdict(list)
        for item in data:
            if group_key in item:
                groups[item[group_key]].append(item.get(value_key, 0))

        results = []
        for key, values in groups.items():
            if not values:
                continue

            result = AggregationResult(
                group_key=key,
                count=len(values),
                sum=sum(values),
                average=sum(values) / len(values),
                min=min(values),
                max=max(values),
                first=values[0],
                last=values[-1]
            )
            results.append(result)

        return sorted(results, key=lambda r: r.group_key)

    def aggregate_time_based(
        self,
        time_series: List[TimeSeriesPoint],
        interval: timedelta,
        agg_func: str = "avg"
    ) -> List[TimeSeriesPoint]:
        """Aggregate time series by interval."""
        if not time_series:
            return []

        groups: Dict[datetime, List[float]] = defaultdict(list)

        for point in time_series:
            if interval == timedelta(hours=1):
                bucket = point.timestamp.replace(minute=0, second=0, microsecond=0)
            elif interval == timedelta(days=1):
                bucket = point.timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                bucket = datetime.fromtimestamp(
                    int(point.timestamp.timestamp() / interval.total_seconds()) * interval.total_seconds()
                )
            groups[bucket].append(point.value)

        result = []
        for timestamp, values in sorted(groups.items()):
            if agg_func == "avg":
                agg_value = sum(values) / len(values)
            elif agg_func == "sum":
                agg_value = sum(values)
            elif agg_func == "min":
                agg_value = min(values)
            elif agg_func == "max":
                agg_value = max(values)
            elif agg_func == "count":
                agg_value = float(len(values))
            else:
                agg_value = sum(values) / len(values)

            result.append(TimeSeriesPoint(timestamp=timestamp, value=agg_value))

        return result


class ReportGenerator:
    """Generate analytics reports."""

    def __init__(self):
        self.sections: List[Dict[str, Any]] = []

    def add_section(
        self,
        title: str,
        content: Any,
        section_type: str = "text"
    ):
        """Add a section to the report."""
        self.sections.append({
            "title": title,
            "content": content,
            "type": section_type,
            "timestamp": datetime.now()
        })

    def add_summary(self, summary: StatisticalSummary):
        """Add statistical summary section."""
        self.add_section(
            "Statistical Summary",
            summary.to_dict(),
            "table"
        )

    def add_trend(self, trend: TrendResult):
        """Add trend analysis section."""
        self.add_section(
            "Trend Analysis",
            {
                "direction": trend.direction.value,
                "slope": trend.slope,
                "confidence": trend.confidence,
                "r_squared": trend.r_squared
            },
            "metrics"
        )

    def add_anomalies(self, anomalies: List):
        """Add anomalies section."""
        anomaly_list = [
            {
                "timestamp": a[0].timestamp.isoformat() if hasattr(a[0], 'timestamp') else str(a[0]),
                "is_anomaly": a[1].is_anomaly,
                "score": a[1].score
            }
            for a in anomalies[:10]
        ]
        self.add_section("Anomalies", anomaly_list, "list")

    def generate(self) -> Dict[str, Any]:
        """Generate final report."""
        return {
            "report_id": f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generated_at": datetime.now().isoformat(),
            "sections": self.sections
        }


def main():
    """Demonstrate data analytics capabilities."""
    data = [10, 12, 14, 15, 13, 18, 20, 22, 21, 25, 28, 30]
    summary = StatisticalAnalyzer.compute_summary(data)
    print(f"Summary: {summary.to_dict()}")

    time_series = [
        TimeSeriesPoint(datetime.now() - timedelta(hours=i), float(100 + i * 2 + (i % 3)))
        for i in range(20, 0, -1)
    ]

    analyzer = TimeSeriesAnalyzer(time_series)
    trend = analyzer.detect_trend()
    print(f"Trend: {trend.direction.value}, slope={trend.slope:.4f}")

    anomalies = analyzer.detect_anomalies_zscore()
    print(f"Anomalies found: {sum(1 for _, a in anomalies if a.is_anomaly)}")

    report = ReportGenerator()
    report.add_summary(summary)
    report.add_trend(trend)
    report.add_anomalies(anomalies)
    print(f"Report: {report.generate()}")


if __name__ == "__main__":
    main()
