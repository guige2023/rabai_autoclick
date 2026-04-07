"""
Neural network layer utilities.

Provides common layer implementations, initializers,
and layer building blocks.
"""
from __future__ import annotations

from typing import Callable, List, Optional, Sequence, Tuple

import numpy as np


def xavier_init(fan_in: int, fan_out: int, constant: float = 1.0) -> np.ndarray:
    """
    Xavier/Glorot initialization.

    Args:
        fan_in: Number of input units
        fan_out: Number of output units
        constant: Scaling constant

    Returns:
        Weight matrix

    Example:
        >>> xavier_init(128, 64).shape
        (128, 64)
    """
    std = constant * np.sqrt(2.0 / (fan_in + fan_out))
    return np.random.randn(fan_in, fan_out) * std


def kaiming_init(fan_in: int, fan_out: int, mode: str = "fan_out") -> np.ndarray:
    """
    Kaiming/He initialization.

    Args:
        fan_in: Number of input units
        fan_out: Number of output units
        mode: 'fan_out' or 'fan_in'

    Returns:
        Weight matrix
    """
    if mode == "fan_out":
        std = np.sqrt(2.0 / fan_out)
    else:
        std = np.sqrt(2.0 / fan_in)
    return np.random.randn(fan_in, fan_out) * std


def orthogonal_init(shape: Tuple[int, ...], gain: float = 1.0) -> np.ndarray:
    """
    Orthogonal initialization.

    Args:
        shape: Weight shape
        gain: Scaling factor

    Returns:
        Orthogonal weight matrix
    """
    from scipy.linalg import orth
    if len(shape) != 2:
        flat_shape = (shape[0], np.prod(shape[1:]))
    else:
        flat_shape = shape
    a = np.random.randn(*flat_shape)
    u, _, vt = np.linalg.svd(a, full_matrices=False)
    q = u if u.shape == flat_shape else vt.T
    q = q.reshape(shape)
    return gain * q


class Linear:
    """Fully connected linear layer."""

    def __init__(
        self,
        in_features: int,
        out_features: int,
        bias: bool = True,
        init: str = "xavier",
    ):
        self.in_features = in_features
        self.out_features = out_features
        self.has_bias = bias
        if init == "xavier":
            self.weight = xavier_init(in_features, out_features)
        elif init == "kaiming":
            self.weight = kaiming_init(in_features, out_features)
        elif init == "orthogonal":
            self.weight = orthogonal_init((in_features, out_features))
        else:
            self.weight = np.random.randn(in_features, out_features) * 0.01
        self.bias = np.zeros(out_features) if bias else None
        self._input = None

    def forward(self, x: np.ndarray) -> np.ndarray:
        """Forward pass."""
        self._input = x
        if len(x.shape) == 3:
            batch, seq_len, _ = x.shape
            output = np.einsum("bsi,io->bso", x, self.weight)
        else:
            output = np.dot(x, self.weight)
        if self.has_bias:
            output = output + self.bias
        return output

    def __call__(self, x: np.ndarray) -> np.ndarray:
        return self.forward(x)

    def get_weight_norm(self) -> float:
        """Get L2 norm of weights."""
        return np.linalg.norm(self.weight)


class Layer:
    """Base layer class."""

    def forward(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def __call__(self, x: np.ndarray) -> np.ndarray:
        return self.forward(x)


class ReLU(Layer):
    """ReLU activation layer."""

    def forward(self, x: np.ndarray) -> np.ndarray:
        return np.maximum(0, x)


class Sigmoid(Layer):
    """Sigmoid activation layer."""

    def forward(self, x: np.ndarray) -> np.ndarray:
        return 1 / (1 + np.exp(-np.clip(x, -500, 500)))


class Tanh(Layer):
    """Tanh activation layer."""

    def forward(self, x: np.ndarray) -> np.ndarray:
        return np.tanh(x)


class Softmax(Layer):
    """Softmax activation layer."""

    def __init__(self, axis: int = -1):
        self.axis = axis

    def forward(self, x: np.ndarray) -> np.ndarray:
        x_max = np.max(x, axis=self.axis, keepdims=True)
        exp_x = np.exp(x - x_max)
        return exp_x / np.sum(exp_x, axis=self.axis, keepdims=True)


class Flatten(Layer):
    """Flatten layer."""

    def forward(self, x: np.ndarray) -> np.ndarray:
        batch = x.shape[0]
        return x.reshape(batch, -1)


class Reshape(Layer):
    """Reshape layer."""

    def __init__(self, shape: Tuple[int, ...]):
        self.shape = shape

    def forward(self, x: np.ndarray) -> np.ndarray:
        batch = x.shape[0]
        return x.reshape(batch, *self.shape)


class Dropout(Layer):
    """Dropout layer."""

    def __init__(self, p: float = 0.5):
        self.p = p
        self.mask = None

    def forward(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        if not training or self.p == 0:
            return x
        self.mask = np.random.binomial(1, 1 - self.p, x.shape) / (1 - self.p)
        return x * self.mask

    def __call__(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        return self.forward(x, training)


class Sequential:
    """Sequential container for layers."""

    def __init__(self, layers: List[Layer] = None):
        self.layers = layers or []

    def add(self, layer: Layer) -> "Sequential":
        """Add a layer."""
        self.layers.append(layer)
        return self

    def forward(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        """Forward pass through all layers."""
        for layer in self.layers:
            if hasattr(layer, "forward"):
                if "training" in layer.forward.__code__.co_varnames:
                    x = layer.forward(x, training=training)
                else:
                    x = layer.forward(x)
        return x

    def __call__(self, x: np.ndarray, training: bool = True) -> np.ndarray:
        return self.forward(x, training)

    def __getitem__(self, idx: int) -> Layer:
        return self.layers[idx]

    def __len__(self) -> int:
        return len(self.layers)


class MLP(Sequential):
    """Multi-layer perceptron."""

    def __init__(
        self,
        input_size: int,
        hidden_sizes: List[int],
        output_size: int,
        activation: str = "relu",
        dropout: float = 0.0,
    ):
        super().__init__()
        sizes = [input_size] + hidden_sizes + [output_size]
        for i in range(len(sizes) - 1):
            self.add(Linear(sizes[i], sizes[i + 1]))
            if i < len(sizes) - 2:
                if activation == "relu":
                    self.add(ReLU())
                elif activation == "sigmoid":
                    self.add(Sigmoid())
                elif activation == "tanh":
                    self.add(Tanh())
                if dropout > 0:
                    self.add(Dropout(dropout))


class Conv2D(Layer):
    """2D Convolution layer."""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        kernel_size: int,
        stride: int = 1,
        padding: int = 0,
        init: str = "kaiming",
    ):
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.kernel_size = kernel_size
        self.stride = stride
        self.padding = padding
        if init == "kaiming":
            self.weight = kaiming_init(kernel_size * kernel_size * in_channels, out_channels * kernel_size * kernel_size).reshape(
                kernel_size, kernel_size, in_channels, out_channels
            )
        else:
            self.weight = np.random.randn(kernel_size, kernel_size, in_channels, out_channels) * 0.01
        self.bias = np.zeros(out_channels)
        self._input = None

    def forward(self, x: np.ndarray) -> np.ndarray:
        """Forward pass (simplified)."""
        from utils.cnn_utils import conv2d
        self._input = x
        return conv2d(x, self.weight, stride=(self.stride, self.stride), padding="same" if self.padding else "valid") + self.bias


class MaxPool2D(Layer):
    """2D Max pooling layer."""

    def __init__(self, pool_size: int = 2, stride: int = None):
        self.pool_size = pool_size
        self.stride = stride or pool_size

    def forward(self, x: np.ndarray) -> np.ndarray:
        from utils.cnn_utils import max_pool2d
        return max_pool2d(x, pool_size=(self.pool_size, self.pool_size), stride=(self.stride, self.stride))


class BatchNorm1D(Layer):
    """1D Batch normalization."""

    def __init__(self, num_features: int, momentum: float = 0.9, eps: float = 1e-5):
        self.num_features = num_features
        self.momentum = momentum
        self.eps = eps
        self.running_mean = np.zeros(num_features)
        self.running_var = np.ones(num_features)
        self.gamma = np.ones(num_features)
        self.beta = np.zeros(num_features)
        self.training = True

    def forward(self, x: np.ndarray) -> np.ndarray:
        from utils.normalization_utils import batch_norm
        return batch_norm(
            x,
            self.gamma,
            self.beta,
            self.running_mean,
            self.running_var,
            self.momentum,
            self.eps,
            self.training,
        )


class EmbeddingLayer(Layer):
    """Embedding lookup layer."""

    def __init__(self, num_embeddings: int, embedding_dim: int):
        self.num_embeddings = num_embeddings
        self.embedding_dim = embedding_dim
        scale = 1.0 / np.sqrt(embedding_dim)
        self.weight = np.random.uniform(-scale, scale, (num_embeddings, embedding_dim))

    def forward(self, indices: np.ndarray) -> np.ndarray:
        """Look up embeddings."""
        return self.weight[indices]


class LSTMLayer(Layer):
    """LSTM layer."""

    def __init__(self, input_size: int, hidden_size: int, num_layers: int = 1):
        from utils.rnn_utils import LSTMCell
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.cells = [LSTMCell(input_size if i == 0 else hidden_size, hidden_size) for i in range(num_layers)]

    def forward(self, x: np.ndarray, h0: np.ndarray = None, c0: np.ndarray = None) -> Tuple[np.ndarray, Tuple[np.ndarray, np.ndarray]]:
        """Forward pass through LSTM."""
        from utils.rnn_utils import lstm_forward
        outputs = x
        final_states = (h0, c0)
        for cell in self.cells:
            outputs, final_states = lstm_forward(outputs, cell, final_states[0], final_states[1])
        return outputs, final_states
