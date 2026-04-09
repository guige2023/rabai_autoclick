"""
Prediction error metrics utilities.

This module provides error metrics for comparing predicted vs actual
input trajectories, including MSE, MAE, RMSE, and directional accuracy.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
Trajectory = List[Point2D]


@dataclass
class PredictionErrorMetrics:
    """Comprehensive error metrics for trajectory prediction."""
    mse: float = 0.0
    mae: float = 0.0
    rmse: float = 0.0
    max_error: float = 0.0
    mean_euclidean: float = 0.0
    directional_accuracy: float = 0.0
    endpoint_error: float = 0.0
    path_length_ratio: float = 1.0
    sample_count: int = 0

    def to_dict(self) -> Dict[str, float]:
        """Convert metrics to dictionary."""
        return {
            "mse": self.mse,
            "mae": self.mae,
            "rmse": self.rmse,
            "max_error": self.max_error,
            "mean_euclidean": self.mean_euclidean,
            "directional_accuracy": self.directional_accuracy,
            "endpoint_error": self.endpoint_error,
            "path_length_ratio": self.path_length_ratio,
            "sample_count": self.sample_count,
        }


def point_euclidean_distance(p1: Point2D, p2: Point2D) -> float:
    """Compute Euclidean distance between two points."""
    dx = p1[0] - p2[0]
    dy = p1[1] - p2[1]
    return math.sqrt(dx * dx + dy * dy)


def compute_mse(actual: Trajectory, predicted: Trajectory) -> float:
    """
    Compute Mean Squared Error between trajectories.

    Args:
        actual: Ground truth trajectory.
        predicted: Predicted trajectory.

    Returns:
        MSE value.
    """
    if len(actual) != len(predicted):
        min_len = min(len(actual), len(predicted))
        actual = actual[:min_len]
        predicted = predicted[:min_len]

    if not actual:
        return 0.0

    total_se = 0.0
    for p_actual, p_pred in zip(actual, predicted):
        dist = point_euclidean_distance(p_actual, p_pred)
        total_se += dist * dist

    return total_se / len(actual)


def compute_mae(actual: Trajectory, predicted: Trajectory) -> float:
    """
    Compute Mean Absolute Error between trajectories.

    Args:
        actual: Ground truth trajectory.
        predicted: Predicted trajectory.

    Returns:
        MAE value.
    """
    if len(actual) != len(predicted):
        min_len = min(len(actual), len(predicted))
        actual = actual[:min_len]
        predicted = predicted[:min_len]

    if not actual:
        return 0.0

    total_ae = 0.0
    for p_actual, p_pred in zip(actual, predicted):
        total_ae += point_euclidean_distance(p_actual, p_pred)

    return total_ae / len(actual)


def compute_rmse(actual: Trajectory, predicted: Trajectory) -> float:
    """Compute Root Mean Squared Error."""
    return math.sqrt(compute_mse(actual, predicted))


def compute_directional_accuracy(actual: Trajectory, predicted: Trajectory) -> float:
    """
    Compute directional accuracy (percentage of steps with correct direction).

    Args:
        actual: Ground truth trajectory.
        predicted: Predicted trajectory.

    Returns:
        Accuracy value between 0 and 1.
    """
    if len(actual) < 3 or len(predicted) < 3:
        return 0.0

    correct = 0
    total = 0

    for i in range(1, len(actual) - 1):
        # Direction vectors
        actual_dx = actual[i + 1][0] - actual[i][0]
        actual_dy = actual[i + 1][1] - actual[i][1]
        pred_dx = predicted[i + 1][0] - predicted[i][0]
        pred_dy = predicted[i + 1][1] - predicted[i][1]

        # Dot product for angle similarity
        dot = actual_dx * pred_dx + actual_dy * pred_dy
        mag_actual = math.sqrt(actual_dx * actual_dx + actual_dy * actual_dy)
        mag_pred = math.sqrt(pred_dx * pred_dx + pred_dy * pred_dy)

        if mag_actual > 0.1 and mag_pred > 0.1:
            cos_angle = dot / (mag_actual * mag_pred)
            if cos_angle > 0.5:  # Within ~60 degrees
                correct += 1
        total += 1

    return correct / total if total > 0 else 0.0


def compute_path_length(trajectory: Trajectory) -> float:
    """Compute total path length of a trajectory."""
    if len(trajectory) < 2:
        return 0.0
    return sum(
        point_euclidean_distance(trajectory[i], trajectory[i + 1])
        for i in range(len(trajectory) - 1)
    )


def compute_full_metrics(actual: Trajectory, predicted: Trajectory) -> PredictionErrorMetrics:
    """
    Compute all error metrics for trajectory comparison.

    Args:
        actual: Ground truth trajectory.
        predicted: Predicted trajectory.

    Returns:
        PredictionErrorMetrics with all computed values.
    """
    if len(actual) != len(predicted):
        min_len = min(len(actual), len(predicted))
        actual = actual[:min_len]
        predicted = predicted[:min_len]

    if not actual:
        return PredictionErrorMetrics()

    # Euclidean errors per point
    errors = [point_euclidean_distance(a, p) for a, p in zip(actual, predicted)]

    mse = compute_mse(actual, predicted)
    mae = compute_mae(actual, predicted)

    actual_length = compute_path_length(actual)
    predicted_length = compute_path_length(predicted)
    path_ratio = predicted_length / actual_length if actual_length > 0.1 else 1.0

    return PredictionErrorMetrics(
        mse=mse,
        mae=mae,
        rmse=math.sqrt(mse),
        max_error=max(errors) if errors else 0.0,
        mean_euclidean=sum(errors) / len(errors) if errors else 0.0,
        directional_accuracy=compute_directional_accuracy(actual, predicted),
        endpoint_error=errors[-1] if errors else 0.0,
        path_length_ratio=path_ratio,
        sample_count=len(actual),
    )


def is_prediction_acceptable(metrics: PredictionErrorMetrics, mae_threshold: float = 10.0, accuracy_threshold: float = 0.7) -> bool:
    """
    Check if prediction metrics meet acceptable thresholds.

    Args:
        metrics: Computed error metrics.
        mae_threshold: Maximum acceptable MAE in pixels.
        accuracy_threshold: Minimum acceptable directional accuracy.

    Returns:
        True if prediction is acceptable.
    """
    return metrics.mae <= mae_threshold and metrics.directional_accuracy >= accuracy_threshold
