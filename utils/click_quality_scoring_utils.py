"""
Click quality scoring utilities.

This module provides utilities for scoring the quality of clicks,
including precision, timing, and target selection metrics.
"""

from __future__ import annotations

import math
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class ClickTarget:
    """Information about a click target."""
    x: float
    y: float
    width: float = 0.0
    height: float = 0.0
    element_type: str = ""
    label: str = ""
    is_interactive: bool = True


@dataclass
class ClickResult:
    """Result of a click action."""
    target_x: float
    target_y: float
    actual_x: float
    actual_y: float
    success: bool
    timestamp: float


@dataclass
class ClickQualityScore:
    """Quality score for a click."""
    overall_score: float
    precision_score: float
    target_selection_score: float
    timing_score: float
    precision_px: float
    is_within_target: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


def compute_click_precision(target: Tuple[float, float], actual: Tuple[float, float]) -> float:
    """
    Compute click precision in pixels.

    Args:
        target: Intended click coordinates (x, y).
        actual: Actual click coordinates (x, y).

    Returns:
        Euclidean distance in pixels.
    """
    dx = target[0] - actual[0]
    dy = target[1] - actual[1]
    return math.sqrt(dx * dx + dy * dy)


def compute_precision_score(precision_px: float, excellent_threshold: float = 5.0, acceptable_threshold: float = 20.0) -> float:
    """
    Score precision from 0-1 based on pixel error.

    Args:
        precision_px: Distance from target in pixels.
        excellent_threshold: Distance considered excellent.
        acceptable_threshold: Distance considered acceptable.

    Returns:
        Score between 0 and 1.
    """
    if precision_px <= excellent_threshold:
        return 1.0
    if precision_px >= acceptable_threshold:
        return 0.0
    # Linear interpolation
    ratio = (acceptable_threshold - precision_px) / (acceptable_threshold - excellent_threshold)
    return max(0.0, min(1.0, ratio))


def is_click_within_target(
    click_pos: Tuple[float, float],
    target: ClickTarget,
) -> bool:
    """
    Check if a click position is within the target bounds.

    Args:
        click_pos: Click coordinates (x, y).
        target: Target element.

    Returns:
        True if click is within target bounds.
    """
    cx, cy = click_pos
    tx, ty = target.x, target.y
    half_w = target.width / 2
    half_h = target.height / 2

    return (
        tx - half_w <= cx <= tx + half_w
        and ty - half_h <= cy <= ty + half_h
    )


def score_target_selection(
    result: ClickResult,
    target: ClickTarget,
) -> float:
    """
    Score how well the click targeted the intended element.

    Args:
        result: Click result.
        target: Target element information.

    Returns:
        Score between 0 and 1.
    """
    if not result.success:
        return 0.0

    # Check if click was within target bounds
    if is_click_within_target((result.actual_x, result.actual_y), target):
        base_score = 0.8
    else:
        base_score = 0.3

    # Bonus for targeting interactive elements
    if target.is_interactive:
        base_score += 0.1

    # Bonus for precise center hits
    if target.width > 0 and target.height > 0:
        dx = abs(result.actual_x - target.x)
        dy = abs(result.actual_y - target.y)
        max_offset = math.sqrt((target.width / 2) ** 2 + (target.height / 2) ** 2)
        offset = math.sqrt(dx * dx + dy * dy)
        center_score = max(0.0, 1.0 - offset / max_offset)
        base_score += 0.1 * center_score

    return min(1.0, base_score)


def score_timing(
    click_time: float,
    ideal_time: Optional[float] = None,
    max_wait_ms: float = 5000.0,
) -> float:
    """
    Score click timing relative to ideal.

    Args:
        click_time: Actual click timestamp.
        ideal_time: Ideal click timestamp (None = no scoring).
        max_wait_ms: Maximum wait time considered valid.

    Returns:
        Score between 0 and 1.
    """
    if ideal_time is None:
        return 1.0  # No timing requirement

    delay_ms = abs(click_time - ideal_time) * 1000.0
    if delay_ms <= 100:
        return 1.0
    if delay_ms >= max_wait_ms:
        return 0.0
    return max(0.0, 1.0 - (delay_ms - 100) / (max_wait_ms - 100))


def compute_overall_score(
    precision_score: float,
    target_score: float,
    timing_score: float,
    weights: Tuple[float, float, float] = (0.4, 0.4, 0.2),
) -> float:
    """
    Compute weighted overall quality score.

    Args:
        precision_score: Precision component score.
        target_score: Target selection component score.
        timing_score: Timing component score.
        weights: (precision, target, timing) weights.

    Returns:
        Weighted overall score.
    """
    w_precision, w_target, w_timing = weights
    return w_precision * precision_score + w_target * target_score + w_timing * timing_score


def score_click(
    result: ClickResult,
    target: ClickTarget,
    click_time: Optional[float] = None,
) -> ClickQualityScore:
    """
    Compute full quality score for a click.

    Args:
        result: Click result.
        target: Target element.
        click_time: Optional ideal click time for timing score.

    Returns:
        Complete ClickQualityScore.
    """
    precision_px = compute_click_precision(
        (result.target_x, result.target_y),
        (result.actual_x, result.actual_y),
    )
    precision_score = compute_precision_score(precision_px)
    target_score = score_target_selection(result, target)
    timing_score = score_timing(click_time or 0.0) if click_time is not None else 1.0
    overall = compute_overall_score(precision_score, target_score, timing_score)
    within_target = is_click_within_target((result.actual_x, result.actual_y), target)

    return ClickQualityScore(
        overall_score=overall,
        precision_score=precision_score,
        target_selection_score=target_score,
        timing_score=timing_score,
        precision_px=precision_px,
        is_within_target=within_target,
        metadata={
            "target_type": target.element_type,
            "target_label": target.label,
            "success": result.success,
        },
    )
