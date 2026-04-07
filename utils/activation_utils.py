"""
Activation functions utility module.

Provides common neural network activation functions and their derivatives.
"""
from __future__ import annotations

from typing import Callable, Sequence

import numpy as np


def sigmoid(x: np.ndarray) -> np.ndarray:
    """
    Sigmoid activation: 1 / (1 + exp(-x))

    Args:
        x: Input array

    Returns:
        Sigmoid activation values in range (0, 1)

    Example:
        >>> sigmoid(np.array([0, 1, -1]))
        array([0.5       , 0.73105858, 0.26894142])
    """
    x = np.asarray(x, dtype=np.float64)
    return np.where(x >= 0, 1 / (1 + np.exp(-x)), np.exp(x) / (1 + np.exp(x)))


def sigmoid_derivative(x: np.ndarray) -> np.ndarray:
    """
    Derivative of sigmoid: sigmoid(x) * (1 - sigmoid(x))

    Args:
        x: Input array (or sigmoid output)

    Returns:
        Sigmoid derivative values

    Example:
        >>> sigmoid_derivative(np.array([0, 1, -1]))
        array([0.25      , 0.19661193, 0.19661193])
    """
    s = sigmoid(x)
    return s * (1 - s)


def tanh(x: np.ndarray) -> np.ndarray:
    """
    Hyperbolic tangent activation.

    Args:
        x: Input array

    Returns:
        Tanh activation values in range (-1, 1)

    Example:
        >>> tanh(np.array([0, 1, -1]))
        array([ 0.        ,  0.76159416, -0.76159416])
    """
    x = np.asarray(x, dtype=np.float64)
    return np.tanh(x)


def tanh_derivative(x: np.ndarray) -> np.ndarray:
    """
    Derivative of tanh: 1 - tanh^2(x)

    Args:
        x: Input array (or tanh output)

    Returns:
        Tanh derivative values

    Example:
        >>> tanh_derivative(np.array([0, 1, -1]))
        array([1.        , 0.41997434, 0.41997434])
    """
    t = tanh(x)
    return 1 - t ** 2


def relu(x: np.ndarray, alpha: float = 0.0) -> np.ndarray:
    """
    Rectified Linear Unit activation.

    Args:
        x: Input array
        alpha: Leaky slope for negative values (default: 0.0)

    Returns:
        ReLU activation values

    Example:
        >>> relu(np.array([-1, 0, 1]))
        array([0., 0., 1.])
        >>> relu(np.array([-1, 0, 1]), alpha=0.1)
        array([-0.1,  0. ,  1. ])
    """
    x = np.asarray(x, dtype=np.float64)
    return np.where(x > 0, x, alpha * x)


def relu_derivative(x: np.ndarray, alpha: float = 0.0) -> np.ndarray:
    """
    Derivative of ReLU.

    Args:
        x: Input array
        alpha: Leaky slope (default: 0.0)

    Returns:
        ReLU derivative values (0 or 1, or alpha for negative inputs)

    Example:
        >>> relu_derivative(np.array([-1, 0, 1]))
        array([0., 0., 1.])
    """
    x = np.asarray(x, dtype=np.float64)
    return np.where(x > 0, 1.0, alpha)


def leaky_relu(x: np.ndarray, alpha: float = 0.01) -> np.ndarray:
    """
    Leaky ReLU: x if x > 0 else alpha * x

    Args:
        x: Input array
        alpha: Slope for negative values (default: 0.01)

    Returns:
        Leaky ReLU activation values

    Example:
        >>> leaky_relu(np.array([-1, 0, 1]))
        array([-0.01,  0.  ,  1.  ])
    """
    return relu(x, alpha=alpha)


def elu(x: np.ndarray, alpha: float = 1.0) -> np.ndarray:
    """
    Exponential Linear Unit activation.

    Args:
        x: Input array
        alpha: Coefficient for negative values (default: 1.0)

    Returns:
        ELU activation values

    Example:
        >>> elu(np.array([-1, 0, 1]))
        array([-0.63212056,  0.        ,  1.        ])
    """
    x = np.asarray(x, dtype=np.float64)
    return np.where(x > 0, x, alpha * (np.exp(x) - 1))


def selu(x: np.ndarray) -> np.ndarray:
    """
    Scaled Exponential Linear Unit (SELU).

    Args:
        x: Input array

    Returns:
        SELU activation with fixed scale (~1.0507) and alpha (~1.6733)

    Example:
        >>> selu(np.array([-1, 0, 1]))[:2]
        array([-1.111,  0.    ])
    """
    alpha = 1.6732632423543772848170429916717
    scale = 1.0507009873554804934193349852946
    x = np.asarray(x, dtype=np.float64)
    return scale * np.where(x > 0, x, alpha * (np.exp(x) - 1))


def softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """
    Softmax activation function.

    Args:
        x: Input array (logits)
        axis: Axis along which to compute softmax (default: -1)

    Returns:
        Softmax probabilities summing to 1

    Example:
        >>> softmax(np.array([1, 2, 3]))
        array([0.09003057, 0.24472847, 0.66524096])
    """
    x = np.asarray(x, dtype=np.float64)
    x_max = np.max(x, axis=axis, keepdims=True)
    exp_x = np.exp(x - x_max)
    return exp_x / np.sum(exp_x, axis=axis, keepdims=True)


def log_softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """
    Log-Softmax activation (numerically stable).

    Args:
        x: Input array (logits)
        axis: Axis along which to compute (default: -1)

    Returns:
        Log-softmax values

    Example:
        >>> log_softmax(np.array([1, 2, 3]))
        array([-2.40760596, -1.40760596, -0.40760596])
    """
    x = np.asarray(x, dtype=np.float64)
    x_max = np.max(x, axis=axis, keepdims=True)
    return x - x_max - np.log(np.sum(np.exp(x - x_max), axis=axis, keepdims=True))


def swish(x: np.ndarray) -> np.ndarray:
    """
    Swish activation: x * sigmoid(x)

    Args:
        x: Input array

    Returns:
        Swish activation values

    Example:
        >>> swish(np.array([0, 1, -1]))
        array([ 0.        ,  0.73105858, -0.26894142])
    """
    x = np.asarray(x, dtype=np.float64)
    return x * sigmoid(x)


def mish(x: np.ndarray) -> np.ndarray:
    """
    Mish activation: x * tanh(softplus(x))

    Args:
        x: Input array

    Returns:
        Mish activation values

    Example:
        >>> mish(np.array([0, 1, -1]))
        array([ 0.        ,  0.86509746, -0.30340193])
    """
    x = np.asarray(x, dtype=np.float64)
    return x * tanh(np.log1p(np.exp(x)))


def gelu(x: np.ndarray) -> np.ndarray:
    """
    Gaussian Error Linear Unit (GELU).

    Args:
        x: Input array

    Returns:
        GELU activation values

    Example:
        >>> gelu(np.array([0, 1, -1]))
        array([ 0.        ,  0.84119199, -0.15880801])
    """
    x = np.asarray(x, dtype=np.float64)
    return 0.5 * x * (1 + np.tanh(np.sqrt(2 / np.pi) * (x + 0.044715 * x ** 3)))


def get_activation(name: str) -> Callable[[np.ndarray], np.ndarray]:
    """
    Get activation function by name.

    Args:
        name: Activation name (sigmoid, tanh, relu, leaky_relu, elu, selu, softmax, swish, mish, gelu)

    Returns:
        Activation function

    Example:
        >>> act = get_activation('relu')
        >>> act(np.array([-1, 0, 1]))
        array([0., 0., 1.])
    """
    activations = {
        "sigmoid": sigmoid,
        "tanh": tanh,
        "relu": relu,
        "leaky_relu": leaky_relu,
        "elu": elu,
        "selu": selu,
        "softmax": softmax,
        "log_softmax": log_softmax,
        "swish": swish,
        "mish": mish,
        "gelu": gelu,
    }
    name = name.lower()
    if name not in activations:
        raise ValueError(f"Unknown activation: {name}. Available: {list(activations.keys())}")
    return activations[name]


class ActivationLayer:
    """Simple activation layer wrapper."""

    def __init__(self, activation: Union[str, Callable]):
        if isinstance(activation, str):
            self.activation_fn = get_activation(activation)
        else:
            self.activation_fn = activation

    def forward(self, x: np.ndarray) -> np.ndarray:
        """Apply activation."""
        return self.activation_fn(x)

    def __call__(self, x: np.ndarray) -> np.ndarray:
        return self.forward(x)
