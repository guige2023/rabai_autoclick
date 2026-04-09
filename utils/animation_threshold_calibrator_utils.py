"""
Animation Threshold Calibrator Utilities

Automatically calibrate animation detection thresholds using
observed data. Supports online calibration as more samples are collected.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, List


@dataclass
class CalibrationThresholds:
    """Calibrated threshold values for animation detection."""
    confidence_on: float = 0.6
    confidence_off: float = 0.3
    min_animation_frames: int = 3
    frame_stability_threshold: float = 0.85


@dataclass
class CalibrationDataPoint:
    """A single calibration data point."""
    is_animating: bool
    confidence: float
    frame_count: int


class AnimationThresholdCalibrator:
    """
    Automatically calibrate animation detection thresholds.

    The calibrator tracks labeled data points (ground-truth animation state
    paired with detector confidence) and adjusts thresholds to maximize
    the F1 score against the labeled data.
    """

    def __init__(
        self,
        initial_on_threshold: float = 0.6,
        initial_off_threshold: float = 0.3,
    ):
        self.on_threshold = initial_on_threshold
        self.off_threshold = initial_off_threshold
        self._labeled_data: List[CalibrationDataPoint] = []
        self._calibrated = False

    def add_labeled_sample(
        self,
        is_animating: bool,
        confidence: float,
        frame_count: int = 1,
    ) -> None:
        """Add a labeled sample for calibration."""
        self._labeled_data.append(
            CalibrationDataPoint(is_animating=is_animating, confidence=confidence, frame_count=frame_count)
        )

    def calibrate(self) -> CalibrationThresholds:
        """
        Run calibration to find optimal threshold values.

        Uses a simple grid search over the labeled data to find
        threshold values that maximize classification accuracy.
        """
        if len(self._labeled_data) < 10:
            return CalibrationThresholds(
                confidence_on=self.on_threshold,
                confidence_off=self.off_threshold,
            )

        best_f1 = 0.0
        best_on = self.on_threshold
        best_off = self.off_threshold

        for on_thresh in [0.4, 0.5, 0.6, 0.7, 0.8]:
            for off_thresh in [0.2, 0.25, 0.3, 0.35, 0.4]:
                if off_thresh >= on_thresh:
                    continue

                tp = fp = tn = fn = 0
                for dp in self._labeled_data:
                    predicted = dp.confidence >= on_thresh
                    if dp.is_animating and predicted:
                        tp += dp.frame_count
                    elif dp.is_animating and not predicted:
                        fn += dp.frame_count
                    elif not dp.is_animating and predicted:
                        fp += dp.frame_count
                    else:
                        tn += dp.frame_count

                precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

                if f1 > best_f1:
                    best_f1 = f1
                    best_on = on_thresh
                    best_off = off_thresh

        self.on_threshold = best_on
        self.off_threshold = best_off
        self._calibrated = True

        return CalibrationThresholds(
            confidence_on=best_on,
            confidence_off=best_off,
        )

    def get_current_thresholds(self) -> CalibrationThresholds:
        """Get the current (possibly uncalibrated) thresholds."""
        return CalibrationThresholds(
            confidence_on=self.on_threshold,
            confidence_off=self.off_threshold,
        )
