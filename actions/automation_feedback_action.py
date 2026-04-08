"""
Automation Feedback Action - Feedback loops and adaptive automation.

This module provides feedback mechanisms for automation workflows
including result evaluation, adaptive retry, and self-tuning.
"""

from __future__ import annotations

import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class FeedbackMetric(Enum):
    """Types of feedback metrics."""
    SUCCESS_RATE = "success_rate"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    THROUGHPUT = "throughput"


@dataclass
class FeedbackThreshold:
    """Threshold configuration for feedback metrics."""
    metric: FeedbackMetric
    min_value: float | None = None
    max_value: float | None = None
    target_value: float | None = None


@dataclass
class FeedbackResult:
    """Result of feedback evaluation."""
    passed: bool
    metric: FeedbackMetric
    value: float
    threshold: FeedbackThreshold
    adjustment: Any = None


@dataclass
class AdaptiveConfig:
    """Configuration for adaptive behavior."""
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: float = 0.1
    success_reset: bool = True


@dataclass
class FeedbackState:
    """State tracking for feedback system."""
    total_attempts: int = 0
    successful_attempts: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    latencies: list[float] = field(default_factory=list)
    last_attempt: float = 0.0
    current_delay: float = 1.0


class AdaptiveRetry:
    """
    Adaptive retry mechanism with feedback.
    
    Example:
        retry = AdaptiveRetry(AdaptiveConfig(max_delay=30.0))
        result = await retry.execute(my_task, should_retry)
    """
    
    def __init__(self, config: AdaptiveConfig | None = None) -> None:
        self.config = config or AdaptiveConfig()
        self._state = FeedbackState()
    
    async def execute(
        self,
        task: Callable[[], Any],
        should_retry: Callable[[Exception], bool],
        max_attempts: int = 5,
    ) -> Any:
        """Execute task with adaptive retry."""
        last_error: Exception | None = None
        
        for attempt in range(max_attempts):
            self._state.total_attempts += 1
            self._state.last_attempt = time.time()
            
            try:
                start = time.time()
                result = task()
                if hasattr(result, "__await__"):
                    result = await result
                
                latency = (time.time() - start) * 1000
                self._state.latencies.append(latency)
                self._state.successful_attempts += 1
                self._state.consecutive_successes += 1
                self._state.consecutive_failures = 0
                
                if self.config.success_reset:
                    self._state.current_delay = self.config.initial_delay
                
                return result
            
            except Exception as e:
                last_error = e
                self._state.consecutive_failures += 1
                self._state.consecutive_successes = 0
                
                if not should_retry(e) or attempt == max_attempts - 1:
                    raise
                
                await self._wait_before_retry()
        
        raise last_error
    
    async def _wait_before_retry(self) -> None:
        """Wait with exponential backoff and jitter."""
        import random
        jitter_range = self._state.current_delay * self.config.jitter
        jitter_amount = random.uniform(-jitter_range, jitter_range)
        wait_time = self._state.current_delay + jitter_amount
        await asyncio.sleep(wait_time)
        self._state.current_delay = min(
            self._state.current_delay * self.config.backoff_multiplier,
            self.config.max_delay,
        )


class FeedbackEvaluator:
    """Evaluates automation results against thresholds."""
    
    def __init__(
        self,
        thresholds: list[FeedbackThreshold] | None = None,
    ) -> None:
        self.thresholds = thresholds or []
        self._history: list[FeedbackResult] = []
    
    def add_threshold(self, threshold: FeedbackThreshold) -> None:
        """Add a feedback threshold."""
        self.thresholds.append(threshold)
    
    def evaluate(
        self,
        metric: FeedbackMetric,
        value: float,
    ) -> FeedbackResult:
        """Evaluate a metric against thresholds."""
        matching = [t for t in self.thresholds if t.metric == metric]
        
        if not matching:
            return FeedbackResult(
                passed=True,
                metric=metric,
                value=value,
                threshold=FeedbackThreshold(metric=metric),
            )
        
        for threshold in matching:
            passed = True
            if threshold.min_value is not None and value < threshold.min_value:
                passed = False
            if threshold.max_value is not None and value > threshold.max_value:
                passed = False
            if threshold.target_value is not None:
                deviation = abs(value - threshold.target_value) / threshold.target_value
                if deviation > 0.1:
                    passed = False
            
            result = FeedbackResult(
                passed=passed,
                metric=metric,
                value=value,
                threshold=threshold,
            )
            self._history.append(result)
            return result
        
        return FeedbackResult(
            passed=True,
            metric=metric,
            value=value,
            threshold=FeedbackThreshold(metric=metric),
        )
    
    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of all feedback metrics."""
        summary = {}
        for metric in FeedbackMetric:
            values = [
                r.value for r in self._history
                if r.metric == metric
            ]
            if values:
                summary[metric.value] = {
                    "count": len(values),
                    "mean": statistics.mean(values),
                    "min": min(values),
                    "max": max(values),
                }
        return summary


class AutomationFeedbackAction:
    """
    Feedback-based automation action with adaptive behavior.
    
    Example:
        action = AutomationFeedbackAction()
        action.add_threshold(FeedbackThreshold(
            metric=FeedbackMetric.LATENCY,
            max_value=1000.0,
        ))
        
        @action.adaptive()
        async def my_task():
            return await risky_operation()
    """
    
    def __init__(self) -> None:
        self._evaluator = FeedbackEvaluator()
        self._adaptive_config = AdaptiveConfig()
    
    def add_threshold(
        self,
        metric: FeedbackMetric,
        min_value: float | None = None,
        max_value: float | None = None,
        target_value: float | None = None,
    ) -> None:
        """Add a feedback threshold."""
        self._evaluator.add_threshold(
            FeedbackThreshold(
                metric=metric,
                min_value=min_value,
                max_value=max_value,
                target_value=target_value,
            )
        )
    
    def adaptive(
        self,
        max_attempts: int = 3,
        should_retry: Callable[[Exception], bool] | None = None,
    ) -> Callable:
        """Decorator for adaptive execution."""
        def decorator(func: Callable) -> Callable:
            async def wrapper(*args, **kwargs):
                retry = AdaptiveRetry(self._adaptive_config)
                default_should_retry = lambda e: True
                return await retry.execute(
                    lambda: func(*args, **kwargs),
                    should_retry or default_should_retry,
                    max_attempts,
                )
            return wrapper
        return decorator
    
    async def execute_with_feedback(
        self,
        task: Callable[[], Any],
        metrics_to_collect: list[FeedbackMetric] | None = None,
    ) -> tuple[Any, dict[str, FeedbackResult]]:
        """Execute task and collect feedback metrics."""
        start_time = time.time()
        results: dict[str, FeedbackResult] = {}
        
        try:
            result = task()
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as e:
            results["error"] = FeedbackResult(
                passed=False,
                metric=FeedbackMetric.ERROR_RATE,
                value=1.0,
                threshold=FeedbackThreshold(metric=FeedbackMetric.ERROR_RATE),
                adjustment={"error": str(e)},
            )
            raise
        
        latency = (time.time() - start_time) * 1000
        results["latency"] = self._evaluator.evaluate(FeedbackMetric.LATENCY, latency)
        
        return result, results
    
    def get_feedback_summary(self) -> dict[str, Any]:
        """Get feedback metrics summary."""
        return self._evaluator.get_metrics_summary()


import asyncio


# Export public API
__all__ = [
    "FeedbackMetric",
    "FeedbackThreshold",
    "FeedbackResult",
    "AdaptiveConfig",
    "FeedbackState",
    "AdaptiveRetry",
    "FeedbackEvaluator",
    "AutomationFeedbackAction",
]
