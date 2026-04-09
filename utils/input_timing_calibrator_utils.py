"""
Input timing calibrator utilities.

This module provides utilities for calibrating input timing parameters
to match system responsiveness characteristics.
"""

from __future__ import annotations

import time
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field


@dataclass
class TimingSample:
    """A single timing measurement."""
    expected_ms: float
    actual_ms: float
    timestamp: float
    input_type: str = "click"


@dataclass
class CalibrationResult:
    """Result of timing calibration."""
    offset_ms: float
    scale_factor: float
    confidence: float
    sample_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimingCalibratorConfig:
    """Configuration for timing calibration."""
    min_samples: int = 10
    max_samples: int = 100
    outlier_threshold_std: float = 2.0
    convergence_threshold: float = 0.01
    target_latency_ms: float = 50.0


class InputTimingCalibrator:
    """Calibrates input timing parameters based on measurements."""

    def __init__(self, config: Optional[TimingCalibratorConfig] = None):
        self.config = config or TimingCalibratorConfig()
        self._samples: List[TimingSample] = []
        self._last_calibration: Optional[CalibrationResult] = None

    def add_sample(self, expected_ms: float, actual_ms: float, input_type: str = "click") -> None:
        """
        Add a timing sample.

        Args:
            expected_ms: Expected/desired timing in ms.
            actual_ms: Actual observed timing in ms.
            input_type: Type of input (click, swipe, etc.).
        """
        self._samples.append(TimingSample(
            expected_ms=expected_ms,
            actual_ms=actual_ms,
            timestamp=time.time(),
            input_type=input_type,
        ))

        # Keep samples within limit
        if len(self._samples) > self.config.max_samples:
            self._samples = self._samples[-self.config.max_samples:]

    def calibrate(self) -> CalibrationResult:
        """
        Compute calibration parameters from collected samples.

        Returns:
            CalibrationResult with offset and scale.
        """
        if len(self._samples) < self.config.min_samples:
            return CalibrationResult(
                offset_ms=0.0,
                scale_factor=1.0,
                confidence=0.0,
                sample_count=len(self._samples),
            )

        # Filter outliers
        errors = [s.actual_ms - s.expected_ms for s in self._samples]
        mean_error = sum(errors) / len(errors)
        variance = sum((e - mean_error) ** 2 for e in errors) / len(errors)
        std = variance ** 0.5
        threshold = self.config.outlier_threshold_std * std

        filtered = [e for e in errors if abs(e - mean_error) <= threshold]

        if len(filtered) < self.config.min_samples:
            filtered = errors  # Fall back to all samples

        # Compute offset and scale
        mean_offset = sum(filtered) / len(filtered)
        scale = self.config.target_latency_ms / max(mean_offset, 1.0) if mean_offset > 0 else 1.0

        confidence = min(1.0, len(filtered) / self.config.max_samples)

        self._last_calibration = CalibrationResult(
            offset_ms=mean_offset,
            scale_factor=scale,
            confidence=confidence,
            sample_count=len(self._samples),
            metadata={
                "filtered_count": len(filtered),
                "mean_error": mean_error,
                "std_error": std,
            },
        )

        return self._last_calibration

    def apply_calibration(self, raw_timing_ms: float) -> float:
        """
        Apply calibration to raw timing value.

        Args:
            raw_timing_ms: Raw timing measurement.

        Returns:
            Calibrated timing.
        """
        if self._last_calibration is None:
            self.calibrate()

        cal = self._last_calibration
        if cal is None or cal.confidence < 0.5:
            return raw_timing_ms

        # Apply: calibrated = (raw - offset) * scale
        adjusted = (raw_timing_ms - cal.offset_ms) * cal.scale_factor
        return max(0.0, adjusted)

    def reset(self) -> None:
        """Reset all samples and calibration state."""
        self._samples.clear()
        self._last_calibration = None

    @property
    def sample_count(self) -> int:
        return len(self._samples)

    @property
    def is_ready(self) -> bool:
        return len(self._samples) >= self.config.min_samples


def estimate_system_latency(samples: List[Tuple[float, float]], window_seconds: float = 5.0) -> float:
    """
    Estimate average system latency from input event samples.

    Args:
        samples: List of (input_time, response_time) tuples.
        window_seconds: Time window to consider.

    Returns:
        Estimated latency in milliseconds.
    """
    now = time.time()
    recent = [(inp, resp) for inp, resp in samples if now - inp <= window_seconds]
    if not recent:
        return 0.0

    latencies = [(resp - inp) * 1000.0 for inp, resp in recent]
    return sum(latencies) / len(latencies)
