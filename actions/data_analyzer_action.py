# Copyright (c) 2024. coded by claude
"""Data Analyzer Action Module.

Provides data analysis utilities for API responses including
statistical analysis, trend detection, and anomaly identification.
"""
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import statistics
import logging

logger = logging.getLogger(__name__)


class TrendDirection(Enum):
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


@dataclass
class StatisticResult:
    count: int
    sum: float
    mean: float
    median: float
    mode: Optional[float]
    min: float
    max: float
    std_dev: float
    variance: float


@dataclass
class TrendResult:
    direction: TrendDirection
    slope: float
    intercept: float
    r_squared: float


@dataclass
class AnomalyResult:
    is_anomaly: bool
    score: float
    threshold: float
    value: float
    metric_name: str


class DataAnalyzer:
    @staticmethod
    def calculate_statistics(values: List[float]) -> StatisticResult:
        if not values:
            raise ValueError("Cannot calculate statistics on empty list")
        sorted_values = sorted(values)
        return StatisticResult(
            count=len(values),
            sum=sum(values),
            mean=statistics.mean(values),
            median=statistics.median(values),
            mode=statistics.mode(sorted_values) if len(set(sorted_values)) > 1 else None,
            min=min(values),
            max=max(values),
            std_dev=statistics.stdev(values) if len(values) > 1 else 0.0,
            variance=statistics.variance(values) if len(values) > 1 else 0.0,
        )

    @staticmethod
    def detect_trend(data_points: List[Tuple[Any, float]]) -> TrendResult:
        if len(data_points) < 2:
            return TrendResult(direction=TrendDirection.STABLE, slope=0.0, intercept=0.0, r_squared=0.0)
        n = len(data_points)
        x_values = list(range(n))
        y_values = [point[1] for point in data_points]
        x_mean = sum(x_values) / n
        y_mean = sum(y_values) / n
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, y_values))
        denominator = sum((x - x_mean) ** 2 for x in x_values)
        if denominator == 0:
            return TrendResult(direction=TrendDirection.STABLE, slope=0.0, intercept=y_mean, r_squared=0.0)
        slope = numerator / denominator
        intercept = y_mean - slope * x_mean
        ss_tot = sum((y - y_mean) ** 2 for y in y_values)
        ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(x_values, y_values))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
        direction = TrendDirection.STABLE
        if slope > 0.01:
            direction = TrendDirection.UP
        elif slope < -0.01:
            direction = TrendDirection.DOWN
        return TrendResult(direction=direction, slope=slope, intercept=intercept, r_squared=r_squared)

    @staticmethod
    def detect_anomalies(values: List[float], threshold: float = 2.0) -> List[AnomalyResult]:
        if len(values) < 3:
            return []
        stats = DataAnalyzer.calculate_statistics(values)
        results = []
        for i, value in enumerate(values):
            z_score = abs((value - stats.mean) / stats.std_dev) if stats.std_dev > 0 else 0
            is_anomaly = z_score > threshold
            results.append(AnomalyResult(
                is_anomaly=is_anomaly,
                score=z_score,
                threshold=threshold,
                value=value,
                metric_name=f"value_{i}",
            ))
        return results

    @staticmethod
    def calculate_percentiles(values: List[float], percentiles: List[float]) -> Dict[str, float]:
        if not values:
            return {}
        sorted_values = sorted(values)
        result = {}
        for p in percentiles:
            idx = int(len(sorted_values) * p / 100)
            if idx >= len(sorted_values):
                idx = len(sorted_values) - 1
            result[f"p{int(p)}"] = sorted_values[idx]
        return result

    @staticmethod
    def moving_average(values: List[float], window_size: int) -> List[float]:
        if window_size < 1:
            raise ValueError("Window size must be at least 1")
        if len(values) < window_size:
            return [statistics.mean(values)]
        result = []
        for i in range(len(values) - window_size + 1):
            window = values[i:i + window_size]
            result.append(statistics.mean(window))
        return result
