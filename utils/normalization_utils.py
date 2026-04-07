"""
Normalization and regularization utilities.

Provides layer normalization, batch normalization, weight decay,
and other normalization techniques for neural networks.
"""
from __future__ import annotations

from typing import Optional, Tuple

import numpy as np


def layer_norm(
    x: np.ndarray, gamma: np.ndarray = None, beta: np.ndarray = None, eps: float = 1e-8
) -> np.ndarray:
    """
    Layer normalization.

    Args:
        x: Input array of shape (batch, seq_len, features) or (batch, features)
        gamma: Scale parameter (optional)
        beta: Shift parameter (optional)
        eps: Small constant for numerical stability

    Returns:
        Normalized output

    Example:
        >>> x = np.random.randn(2, 4, 8)
        >>> normed = layer_norm(x)
        >>> normed.shape
        (2, 4, 8)
    """
    if len(x.shape) == 3:
        axis = (1, 2)
    else:
        axis = 1
    mean = np.mean(x, axis=axis, keepdims=True)
    var = np.var(x, axis=axis, keepdims=True)
    x_norm = (x - mean) / np.sqrt(var + eps)
    if gamma is not None:
        x_norm = x_norm * gamma
    if beta is not None:
        x_norm = x_norm + beta
    return x_norm


def batch_norm(
    x: np.ndarray,
    gamma: np.ndarray = None,
    beta: np.ndarray = None,
    running_mean: np.ndarray = None,
    running_var: np.ndarray = None,
    momentum: float = 0.9,
    eps: float = 1e-5,
    training: bool = True,
) -> np.ndarray:
    """
    Batch normalization.

    Args:
        x: Input array of shape (batch, channels, height, width) or (batch, features)
        gamma: Scale parameter
        beta: Shift parameter
        running_mean: Running mean for inference
        running_var: Running variance for inference
        momentum: Momentum for running statistics
        eps: Small constant
        training: Whether in training mode

    Returns:
        Normalized output
    """
    if training:
        mean = np.mean(x, axis=0, keepdims=True)
        var = np.var(x, axis=0, keepdims=True)
        if running_mean is not None:
            running_mean = momentum * running_mean + (1 - momentum) * mean.squeeze()
        if running_var is not None:
            running_var = momentum * running_var + (1 - momentum) * var.squeeze()
        x_norm = (x - mean) / np.sqrt(var + eps)
    else:
        if running_mean is None or running_var is None:
            raise ValueError("Running statistics required for inference")
        x_norm = (x - running_mean.reshape(1, -1, 1, 1) if len(x.shape) == 4 else running_mean.reshape(1, -1)) / np.sqrt(
            running_var.reshape(1, -1, 1, 1) if len(x.shape) == 4 else running_var.reshape(1, -1) + eps
        )
    if gamma is not None:
        x_norm = x_norm * gamma
    if beta is not None:
        x_norm = x_norm + beta
    return x_norm


def instance_norm(
    x: np.ndarray, gamma: np.ndarray = None, beta: np.ndarray = None, eps: float = 1e-8
) -> np.ndarray:
    """
    Instance normalization (style transfer).

    Args:
        x: Input array of shape (batch, channels, height, width)
        gamma: Scale parameter
        beta: Shift parameter
        eps: Small constant

    Returns:
        Normalized output
    """
    mean = np.mean(x, axis=(2, 3), keepdims=True)
    var = np.var(x, axis=(2, 3), keepdims=True)
    x_norm = (x - mean) / np.sqrt(var + eps)
    if gamma is not None:
        x_norm = x_norm * gamma
    if beta is not None:
        x_norm = x_norm + beta
    return x_norm


def group_norm(
    x: np.ndarray,
    num_groups: int = 32,
    gamma: np.ndarray = None,
    beta: np.ndarray = None,
    eps: float = 1e-8,
) -> np.ndarray:
    """
    Group normalization.

    Args:
        x: Input array of shape (batch, channels, height, width)
        num_groups: Number of groups to split channels into
        gamma: Scale parameter
        beta: Shift parameter
        eps: Small constant

    Returns:
        Normalized output
    """
    batch, channels, height, width = x.shape
    x_reshaped = x.reshape(batch, num_groups, channels // num_groups, height, width)
    mean = np.mean(x_reshaped, axis=(2, 3, 4), keepdims=True)
    var = np.var(x_reshaped, axis=(2, 3, 4), keepdims=True)
    x_norm = ((x_reshaped - mean) / np.sqrt(var + eps)).reshape(batch, channels, height, width)
    if gamma is not None:
        x_norm = x_norm * gamma
    if beta is not None:
        x_norm = x_norm + beta
    return x_norm


def weight_normalization(
    W: np.ndarray, dim: int = 0
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Weight normalization: reparameterize weights as g * (W / ||W||).

    Args:
        W: Weight matrix
        dim: Dimension to normalize along

    Returns:
        Tuple of (normalized_weights, scale_factor)
    """
    norm = np.linalg.norm(W, axis=dim, keepdims=True)
    W_norm = W / (norm + 1e-8)
    g = norm
    return W_norm, g


def l2_normalize(x: np.ndarray, axis: int = -1, eps: float = 1e-8) -> np.ndarray:
    """
    L2 normalization.

    Args:
        x: Input array
        axis: Axis along which to normalize
        eps: Small constant

    Returns:
        L2 normalized array
    """
    norm = np.linalg.norm(x, axis=axis, keepdims=True)
    return x / (norm + eps)


def spectral_normalization(W: np.ndarray, num_iters: int = 1) -> np.ndarray:
    """
    Spectral normalization: divide weights by their spectral norm.

    Args:
        W: Weight matrix
        num_iters: Number of power iterations

    Returns:
        Spectrally normalized weight matrix
    """
    if len(W.shape) != 2:
        return W
    u = np.random.randn(W.shape[0], 1)
    u = u / (np.linalg.norm(u) + 1e-8)
    for _ in range(num_iters):
        v = np.dot(W.T, u)
        v = v / (np.linalg.norm(v) + 1e-8)
        u = np.dot(W, v)
        u = u / (np.linalg.norm(u) + 1e-8)
    sigma = np.dot(np.dot(u.T, W), v)[0, 0]
    return W / sigma


class LayerNorm:
    """Layer normalization module."""

    def __init__(self, normalized_shape: int, eps: float = 1e-8):
        self.normalized_shape = normalized_shape
        self.eps = eps
        self.gamma = np.ones(normalized_shape)
        self.beta = np.zeros(normalized_shape)

    def __call__(self, x: np.ndarray) -> np.ndarray:
        return layer_norm(x, self.gamma, self.beta, self.eps)


class BatchNorm:
    """Batch normalization module."""

    def __init__(
        self,
        num_features: int,
        momentum: float = 0.9,
        eps: float = 1e-5,
        affine: bool = True,
    ):
        self.num_features = num_features
        self.momentum = momentum
        self.eps = eps
        self.running_mean = np.zeros(num_features)
        self.running_var = np.ones(num_features)
        self.gamma = np.ones(num_features) if affine else None
        self.beta = np.zeros(num_features) if affine else None
        self.training = True

    def __call__(
        self, x: np.ndarray, training: bool = True
    ) -> np.ndarray:
        return batch_norm(
            x,
            self.gamma,
            self.beta,
            self.running_mean,
            self.running_var,
            self.momentum,
            self.eps,
            training,
        )


class L2Penalty:
    """L2 regularization penalty."""

    def __init__(self, weight: float = 1e-4):
        self.weight = weight

    def loss(self, params: np.ndarray) -> float:
        """Compute L2 penalty loss."""
        return self.weight * np.sum(params ** 2)

    def gradient(self, params: np.ndarray) -> np.ndarray:
        """Compute gradient of L2 penalty."""
        return 2 * self.weight * params


class Dropout:
    """Dropout regularization."""

    def __init__(self, rate: float = 0.5):
        self.rate = rate
        self.mask = None

    def __call__(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        if not training or self.rate == 0:
            return x
        self.mask = np.random.binomial(1, 1 - self.rate, x.shape) / (1 - self.rate)
        return x * self.mask

    def backward(self, grad: np.ndarray) -> np.ndarray:
        if self.mask is None:
            return grad
        return grad * self.mask


class SpatialDropout(Dropout):
    """Spatial dropout (drops entire channels)."""

    def __call__(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        if not training or self.rate == 0:
            return x
        if len(x.shape) == 4:
            batch, channels, height, width = x.shape
            self.mask = np.random.binomial(1, 1 - self.rate, (batch, channels, 1, 1)) / (1 - self.rate)
        else:
            self.mask = np.random.binomial(1, 1 - self.rate, x.shape) / (1 - self.rate)
        return x * self.mask


class LabelSmoothing:
    """Label smoothing regularization."""

    def __init__(self, num_classes: int, smoothing: float = 0.1):
        self.num_classes = num_classes
        self.smoothing = smoothing

    def __call__(self, y_true: np.ndarray) -> np.ndarray:
        """
        Convert hard labels to smoothed soft labels.

        Args:
            y_true: Integer class labels

        Returns:
            Smoothed probability distribution
        """
        smooth_value = self.smoothing / (self.num_classes - 1)
        result = np.full((len(y_true), self.num_classes), smooth_value)
        for i, label in enumerate(y_true):
            result[i, label] = 1.0 - self.smoothing
        return result


def mixup_augment(
    x1: np.ndarray, y1: np.ndarray, x2: np.ndarray, y2: np.ndarray, alpha: float = 0.2
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Mixup data augmentation.

    Args:
        x1, y1: First sample
        x2, y2: Second sample
        alpha: Beta distribution parameter

    Returns:
        Tuple of (mixed_x, mixed_y, lambda)
    """
    lam = np.random.beta(alpha, alpha)
    mixed_x = lam * x1 + (1 - lam) * x2
    mixed_y = lam * y1 + (1 - lam) * y2
    return mixed_x, mixed_y, lam


def cutmix_augment(
    x1: np.ndarray, y1: np.ndarray, x2: np.ndarray, y2: np.ndarray, alpha: float = 1.0
) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    CutMix data augmentation.

    Returns:
        Tuple of (cutmixed_x, cutmixed_y, lambda)
    """
    lam = np.random.beta(alpha, alpha)
    if len(x1.shape) == 4:
        batch, channels, h, w = x1.shape
        cut_ratio = np.sqrt(1.0 - lam)
        cut_h = int(h * cut_ratio)
        cut_w = int(w * cut_ratio)
        cy = np.random.randint(h)
        cx = np.random.randint(w)
        y1_min = max(0, cy - cut_h // 2)
        y1_max = min(h, cy + cut_h // 2)
        x1_min = max(0, cx - cut_w // 2)
        x1_max = min(w, cx + cut_w // 2)
        x1_copy = x1.copy()
        x1_copy[:, :, y1_min:y1_max, x1_min:x1_max] = x2[:, :, y1_min:y1_max, x1_min:x1_max]
        lam = 1 - ((y1_max - y1_min) * (x1_max - x1_min) / (h * w))
        return x1_copy, lam * y1 + (1 - lam) * y2, lam
    return x1, lam * y1 + (1 - lam) * y2, lam
