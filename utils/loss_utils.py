"""
Neural network loss functions.

Provides common loss functions for classification, regression,
and generative models.
"""
from __future__ import annotations

from typing import Callable, Optional

import numpy as np


def mse_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Squared Error loss.

    Args:
        y_true: Ground truth values
        y_pred: Predicted values

    Returns:
        MSE value

    Example:
        >>> mse_loss(np.array([1, 2, 3]), np.array([1.1, 2.1, 2.9]))
        0.01
    """
    return np.mean((y_true - y_pred) ** 2)


def mse_derivative(y_true: np.ndarray, y_pred: np.ndarray) -> np.ndarray:
    """
    Derivative of MSE with respect to predictions.

    Args:
        y_true: Ground truth values
        y_pred: Predicted values

    Returns:
        Gradient array
    """
    return 2 * (y_pred - y_true) / len(y_true)


def mae_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Mean Absolute Error loss.

    Args:
        y_true: Ground truth values
        y_pred: Predicted values

    Returns:
        MAE value

    Example:
        >>> mae_loss(np.array([1, 2, 3]), np.array([1, 2, 4]))
        0.3333333333333333
    """
    return np.mean(np.abs(y_true - y_pred))


def mae_derivative(y_true: np.ndarray, y_pred: np.ndarray) -> np.ndarray:
    """
    Derivative of MAE with respect to predictions.

    Returns:
        Gradient array (subgradient at zero)
    """
    diff = y_pred - y_true
    return np.sign(diff) / len(y_true)


def binary_cross_entropy(
    y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-15
) -> float:
    """
    Binary cross-entropy loss.

    Args:
        y_true: Ground truth labels (0 or 1)
        y_pred: Predicted probabilities
        eps: Small constant for numerical stability

    Returns:
        BCE value

    Example:
        >>> binary_cross_entropy(np.array([1, 0]), np.array([0.9, 0.1]))
        0.10536051565782628
    """
    y_pred = np.clip(y_pred, eps, 1 - eps)
    return -np.mean(y_true * np.log(y_pred) + (1 - y_true) * np.log(1 - y_pred))


def binary_cross_entropy_derivative(
    y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-15
) -> np.ndarray:
    """
    Derivative of BCE with respect to predictions.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted probabilities
        eps: Small constant

    Returns:
        Gradient array
    """
    y_pred = np.clip(y_pred, eps, 1 - eps)
    return -(y_true / y_pred) + (1 - y_true) / (1 - y_pred)


def categorical_cross_entropy(
    y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-15
) -> float:
    """
    Categorical cross-entropy loss.

    Args:
        y_true: One-hot encoded ground truth labels
        y_pred: Predicted probabilities (softmax output)
        eps: Small constant for numerical stability

    Returns:
        Cross-entropy value

    Example:
        >>> y_true = np.array([[1, 0], [0, 1]])
        >>> y_pred = np.array([[0.8, 0.2], [0.2, 0.8]])
        >>> categorical_cross_entropy(y_true, y_pred)
        0.500179...
    """
    y_pred = np.clip(y_pred, eps, 1 - eps)
    return -np.sum(y_true * np.log(y_pred)) / len(y_true)


def sparse_categorical_cross_entropy(
    y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-15
) -> float:
    """
    Sparse categorical cross-entropy loss.

    Args:
        y_true: Integer class labels
        y_pred: Predicted logits or probabilities

    Returns:
        Cross-entropy value
    """
    y_pred = np.clip(y_pred, eps, 1 - eps)
    n_classes = y_pred.shape[-1]
    y_true_onehot = np.eye(n_classes)[y_true.flatten()]
    return categorical_cross_entropy(y_true_onehot, y_pred)


def huber_loss(
    y_true: np.ndarray, y_pred: np.ndarray, delta: float = 1.0
) -> float:
    """
    Huber loss (smooth L1 loss).

    Args:
        y_true: Ground truth values
        y_pred: Predicted values
        delta: Threshold between quadratic and linear loss

    Returns:
        Huber loss value

    Example:
        >>> huber_loss(np.array([1, 2, 3]), np.array([1, 2.5, 3.5]))
        0.125
    """
    error = y_pred - y_true
    abs_error = np.abs(error)
    quadratic = np.minimum(abs_error, delta)
    linear = abs_error - quadratic
    return np.mean(0.5 * quadratic ** 2 + delta * linear)


def hinge_loss(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Hinge loss (SVM loss).

    Args:
        y_true: Ground truth labels (-1 or 1)
        y_pred: Predicted scores

    Returns:
        Hinge loss value

    Example:
        >>> hinge_loss(np.array([1, -1]), np.array([0.5, -0.5]))
        0.0
    """
    return np.mean(np.maximum(0, 1 - y_true * y_pred))


def focal_loss(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    alpha: float = 0.25,
    gamma: float = 2.0,
    eps: float = 1e-15,
) -> float:
    """
    Focal loss for dense object detection.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted probabilities
        alpha: Weighting factor
        gamma: Focusing parameter
        eps: Small constant

    Returns:
        Focal loss value
    """
    y_pred = np.clip(y_pred, eps, 1 - eps)
    p_t = y_true * y_pred + (1 - y_true) * (1 - y_pred)
    alpha_t = y_true * alpha + (1 - y_true) * (1 - alpha)
    focal_weight = alpha_t * ((1 - p_t) ** gamma)
    return -np.mean(focal_weight * np.log(p_t))


def triplet_loss(
    anchor: np.ndarray, positive: np.ndarray, negative: np.ndarray, margin: float = 1.0
) -> float:
    """
    Triplet loss for metric learning.

    Args:
        anchor: Anchor embedding
        positive: Positive sample embedding
        negative: Negative sample embedding
        margin: Distance margin

    Returns:
        Triplet loss value

    Example:
        >>> anchor = np.array([1, 0])
        >>> positive = np.array([1, 0.1])
        >>> negative = np.array([0, 1])
        >>> triplet_loss(anchor, positive, negative)
        0.0
    """
    pos_dist = np.sum((anchor - positive) ** 2)
    neg_dist = np.sum((anchor - negative) ** 2)
    loss = np.maximum(0, pos_dist - neg_dist + margin)
    return np.mean(loss)


def gan_generator_loss(
    fake_predictions: np.ndarray, mode: str = "bce"
) -> float:
    """
    GAN generator loss.

    Args:
        fake_predictions: Discriminator predictions on generated samples
        mode: Loss mode (bce or wasserstein)

    Returns:
        Generator loss value
    """
    if mode == "bce":
        return binary_cross_entropy(np.ones_like(fake_predictions), fake_predictions)
    elif mode == "wasserstein":
        return -np.mean(fake_predictions)
    else:
        raise ValueError(f"Unknown mode: {mode}")


def gan_discriminator_loss(
    real_predictions: np.ndarray, fake_predictions: np.ndarray, mode: str = "bce"
) -> float:
    """
    GAN discriminator loss.

    Args:
        real_predictions: Discriminator predictions on real samples
        fake_predictions: Discriminator predictions on fake samples
        mode: Loss mode (bce or wasserstein)

    Returns:
        Discriminator loss value
    """
    if mode == "bce":
        real_loss = binary_cross_entropy(np.ones_like(real_predictions), real_predictions)
        fake_loss = binary_cross_entropy(np.zeros_like(fake_predictions), fake_predictions)
        return (real_loss + fake_loss) / 2
    elif mode == "wasserstein":
        return np.mean(fake_predictions) - np.mean(real_predictions)
    else:
        raise ValueError(f"Unknown mode: {mode}")


class LossFunction:
    """Wrapper for loss functions with forward and backward."""

    def __init__(self, loss_fn: Callable, grad_fn: Callable = None):
        self.loss_fn = loss_fn
        self.grad_fn = grad_fn or mse_derivative

    def __call__(self, y_true: np.ndarray, y_pred: np.ndarray) -> float:
        return self.loss_fn(y_true, y_pred)

    def gradient(self, y_true: np.ndarray, y_pred: np.ndarray) -> np.ndarray:
        return self.grad_fn(y_true, y_pred)


def get_loss(name: str) -> LossFunction:
    """
    Get loss function by name.

    Args:
        name: Loss name (mse, mae, bce, cce, huber, hinge, focal, triplet)

    Returns:
        LossFunction instance
    """
    losses = {
        "mse": (mse_loss, mse_derivative),
        "mae": (mae_loss, mae_derivative),
        "bce": (binary_cross_entropy, binary_cross_entropy_derivative),
        "cce": (categorical_cross_entropy, None),
        "huber": (huber_loss, None),
        "hinge": (hinge_loss, None),
        "focal": (focal_loss, None),
    }
    name = name.lower()
    if name not in losses:
        raise ValueError(f"Unknown loss: {name}")
    return LossFunction(*losses[name])
