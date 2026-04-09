"""
Comparison Threshold Utilities for UI Element Matching.

This module provides utilities for managing comparison thresholds
for visual element matching, image comparison, and fuzzy matching.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Any, List, Tuple
from enum import Enum
import statistics


class ThresholdStrategy(Enum):
    """Threshold calculation strategies."""
    FIXED = "fixed"
    ADAPTIVE = "adaptive"
    PERCENTILE = "percentile"
    STANDARD_DEVIATION = "standard_deviation"
    HISTOGRAM = "histogram"


@dataclass
class ThresholdConfig:
    """Configuration for threshold calculations."""
    base_threshold: float = 0.85
    min_threshold: float = 0.5
    max_threshold: float = 1.0
    strategy: ThresholdStrategy = ThresholdStrategy.FIXED
    confidence_interval: float = 0.95
    sample_size: int = 100


@dataclass
class ThresholdResult:
    """Result of threshold calculation."""
    threshold: float
    confidence: float
    samples_used: int
    strategy: ThresholdStrategy
    metadata: Dict[str, Any] = field(default_factory=dict)


class ThresholdCalculator:
    """
    Calculate and manage comparison thresholds.
    """

    def __init__(self, config: Optional[ThresholdConfig] = None):
        """
        Initialize threshold calculator.

        Args:
            config: Threshold configuration
        """
        self.config = config or ThresholdConfig()
        self._samples: List[float] = []
        self._history: List[ThresholdResult] = []

    def add_sample(self, value: float) -> ThresholdResult:
        """
        Add a sample value and recalculate threshold.

        Args:
            value: Sample value to add

        Returns:
            Updated ThresholdResult
        """
        self._samples.append(value)

        if len(self._samples) >= self.config.sample_size:
            self._samples = self._samples[-self.config.sample_size:]

        result = self.calculate()
        self._history.append(result)
        return result

    def calculate(self) -> ThresholdResult:
        """
        Calculate threshold based on current strategy.

        Returns:
            ThresholdResult
        """
        if not self._samples:
            return ThresholdResult(
                threshold=self.config.base_threshold,
                confidence=0.0,
                samples_used=0,
                strategy=self.config.strategy
            )

        if self.config.strategy == ThresholdStrategy.FIXED:
            return self._calculate_fixed()
        elif self.config.strategy == ThresholdStrategy.ADAPTIVE:
            return self._calculate_adaptive()
        elif self.config.strategy == ThresholdStrategy.PERCENTILE:
            return self._calculate_percentile()
        elif self.config.strategy == ThresholdStrategy.STANDARD_DEVIATION:
            return self._calculate_stddev()
        else:
            return self._calculate_fixed()

    def _calculate_fixed(self) -> ThresholdResult:
        """Fixed threshold calculation."""
        return ThresholdResult(
            threshold=self.config.base_threshold,
            confidence=1.0,
            samples_used=len(self._samples),
            strategy=self.config.strategy
        )

    def _calculate_adaptive(self) -> ThresholdResult:
        """Adaptive threshold based on sample distribution."""
        if len(self._samples) < 2:
            return self._calculate_fixed()

        mean = statistics.mean(self._samples)
        stdev = statistics.stdev(self._samples)

        threshold = mean - stdev
        threshold = max(self.config.min_threshold, min(self.config.max_threshold, threshold))

        confidence = min(1.0, len(self._samples) / self.config.sample_size)

        return ThresholdResult(
            threshold=threshold,
            confidence=confidence,
            samples_used=len(self._samples),
            strategy=self.config.strategy,
            metadata={"mean": mean, "stdev": stdev}
        )

    def _calculate_percentile(self) -> ThresholdResult:
        """Percentile-based threshold."""
        if len(self._samples) < 2:
            return self._calculate_fixed()

        sorted_samples = sorted(self._samples)
        percentile_idx = int(len(sorted_samples) * self.config.confidence_interval)
        threshold = sorted_samples[percentile_idx]

        threshold = max(self.config.min_threshold, min(self.config.max_threshold, threshold))
        confidence = min(1.0, len(self._samples) / self.config.sample_size)

        return ThresholdResult(
            threshold=threshold,
            confidence=confidence,
            samples_used=len(self._samples),
            strategy=self.config.strategy
        )

    def _calculate_stddev(self) -> ThresholdResult:
        """Standard deviation based threshold."""
        if len(self._samples) < 2:
            return self._calculate_fixed()

        mean = statistics.mean(self._samples)
        stdev = statistics.stdev(self._samples)

        z_score = 1.96 if self.config.confidence_interval >= 0.95 else 1.645
        threshold = mean - z_score * stdev

        threshold = max(self.config.min_threshold, min(self.config.max_threshold, threshold))
        confidence = min(1.0, len(self._samples) / self.config.sample_size)

        return ThresholdResult(
            threshold=threshold,
            confidence=confidence,
            samples_used=len(self._samples),
            strategy=self.config.strategy,
            metadata={"mean": mean, "stdev": stdev, "z_score": z_score}
        )

    def reset(self) -> None:
        """Reset samples and history."""
        self._samples.clear()
        self._history.clear()

    def get_threshold(self) -> float:
        """Get current threshold value."""
        return self.calculate().threshold


class MultiThresholdManager:
    """
    Manage multiple thresholds for different comparison types.
    """

    def __init__(self):
        """Initialize multi-threshold manager."""
        self._calculators: Dict[str, ThresholdCalculator] = {}

    def get_or_create(
        self,
        name: str,
        config: Optional[ThresholdConfig] = None
    ) -> ThresholdCalculator:
        """Get or create a threshold calculator."""
        if name not in self._calculators:
            self._calculators[name] = ThresholdCalculator(config)
        return self._calculators[name]

    def get_threshold(self, name: str) -> float:
        """Get threshold for a named comparison type."""
        if name in self._calculators:
            return self._calculators[name].get_threshold()
        return 0.85

    def add_sample(self, name: str, value: float, config: Optional[ThresholdConfig] = None) -> ThresholdResult:
        """Add sample to named threshold calculator."""
        calc = self.get_or_create(name, config)
        return calc.add_sample(value)


def calculate_match_score(
    similarity: float,
    threshold: float,
    penalty_factor: float = 0.1
) -> Tuple[float, str]:
    """
    Calculate match score with penalty for values below threshold.

    Args:
        similarity: Similarity value (0.0-1.0)
        threshold: Matching threshold
        penalty_factor: Penalty factor for below-threshold values

    Returns:
        Tuple of (adjusted_score, match_quality)
    """
    if similarity >= threshold:
        return similarity, "exact"

    gap = threshold - similarity
    penalty = gap * penalty_factor
    adjusted = max(0.0, similarity - penalty)

    if adjusted >= threshold:
        return adjusted, "close"
    elif adjusted >= threshold * 0.8:
        return adjusted, "partial"
    else:
        return adjusted, "poor"
