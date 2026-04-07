"""
Convolutional Neural Network utilities.

Provides CNN layer operations, padding, pooling, and
convolutional filter utilities.
"""
from __future__ import annotations

from typing import Tuple

import numpy as np


def conv2d(
    input: np.ndarray,
    kernel: np.ndarray,
    stride: Tuple[int, int] = (1, 1),
    padding: str = "valid",
    padding_value: int = 0,
) -> np.ndarray:
    """
    2D convolution operation.

    Args:
        input: Input tensor of shape (H, W, C_in) or (B, H, W, C_in)
        kernel: Convolution kernel of shape (K_h, K_w, C_in, C_out)
        stride: Stride in (height, width)
        padding: Padding mode ('valid' or 'same')
        padding_value: Value for constant padding

    Returns:
        Convolved output

    Example:
        >>> img = np.ones((6, 6, 1))
        >>> kernel = np.ones((3, 3, 1, 1))
        >>> out = conv2d(img, kernel, stride=(1, 1), padding='valid')
        >>> out.shape
        (4, 4, 1)
    """
    if len(input.shape) == 3:
        input = input[np.newaxis, ...]
    batch, h, w, c_in = input.shape
    k_h, k_w, c_in_k, c_out = kernel.shape
    s_h, s_w = stride

    if padding == "same":
        pad_h = ((h - 1) * s_h + k_h - h) // 2
        pad_w = ((w - 1) * s_w + k_w - w) // 2
        input_padded = np.pad(
            input, ((0, 0), (pad_h, pad_h), (pad_w, pad_w), (0, 0)), mode="constant", constant_values=padding_value
        )
    else:
        input_padded = input
        pad_h, pad_w = 0, 0

    out_h = (input_padded.shape[1] - k_h) // s_h + 1
    out_w = (input_padded.shape[2] - k_w) // s_w + 1

    output = np.zeros((batch, out_h, out_w, c_out))

    for b in range(batch):
        for i in range(out_h):
            for j in range(out_w):
                h_start = i * s_h
                w_start = j * s_w
                region = input_padded[b, h_start : h_start + k_h, w_start : w_start + k_w, :]
                output[b, i, j] = np.einsum("hwc,hwc->c", region, kernel[:, :, :, 0])

    return output[0] if batch == 1 else output


def conv2d_batch(
    input: np.ndarray,
    kernels: np.ndarray,
    biases: np.ndarray = None,
    stride: Tuple[int, int] = (1, 1),
    padding: str = "valid",
) -> np.ndarray:
    """
    Multi-kernel 2D convolution with biases.

    Args:
        input: Input tensor (H, W, C_in) or (B, H, W, C_in)
        kernels: Stack of kernels (N_kernels, K_h, K_w, C_in)
        biases: Optional biases (N_kernels,)
        stride: Stride tuple
        padding: Padding mode

    Returns:
        Output with shape (H_out, W_out, N_kernels)
    """
    outputs = []
    for kernel in kernels:
        out = conv2d(input, kernel[np.newaxis, ...], stride=stride, padding=padding)
        outputs.append(out[..., np.newaxis])
    output = np.concatenate(outputs, axis=-1)
    if biases is not None:
        output = output + biases
    return output


def max_pool2d(
    input: np.ndarray, pool_size: Tuple[int, int] = (2, 2), stride: Tuple[int, int] = None
) -> np.ndarray:
    """
    2D max pooling.

    Args:
        input: Input tensor (H, W, C) or (B, H, W, C)
        pool_size: Pooling window size (H, W)
        stride: Stride (defaults to pool_size)

    Returns:
        Pooled output

    Example:
        >>> img = np.arange(16).reshape(4, 4, 1).astype(float)
        >>> max_pool2d(img, pool_size=(2, 2)).shape
        (2, 2, 1)
    """
    if len(input.shape) == 3:
        input = input[np.newaxis, ...]
    if stride is None:
        stride = pool_size
    batch, h, w, c = input.shape
    p_h, p_w = pool_size
    s_h, s_w = stride

    out_h = (h - p_h) // s_h + 1
    out_w = (w - p_w) // s_w + 1

    output = np.zeros((batch, out_h, out_w, c))

    for i in range(out_h):
        for j in range(out_w):
            h_start = i * s_h
            w_start = j * s_w
            region = input[:, h_start : h_start + p_h, w_start : w_start + p_w, :]
            output[:, i, j, :] = np.max(region, axis=(1, 2))

    return output[0] if batch == 1 else output


def avg_pool2d(
    input: np.ndarray, pool_size: Tuple[int, int] = (2, 2), stride: Tuple[int, int] = None
) -> np.ndarray:
    """
    2D average pooling.

    Args:
        input: Input tensor (H, W, C) or (B, H, W, C)
        pool_size: Pooling window size
        stride: Stride (defaults to pool_size)

    Returns:
        Pooled output
    """
    if len(input.shape) == 3:
        input = input[np.newaxis, ...]
    if stride is None:
        stride = pool_size
    batch, h, w, c = input.shape
    p_h, p_w = pool_size
    s_h, s_w = stride

    out_h = (h - p_h) // s_h + 1
    out_w = (w - p_w) // s_w + 1

    output = np.zeros((batch, out_h, out_w, c))

    for i in range(out_h):
        for j in range(out_w):
            h_start = i * s_h
            w_start = j * s_w
            region = input[:, h_start : h_start + p_h, w_start : w_start + p_w, :]
            output[:, i, j, :] = np.mean(region, axis=(1, 2))

    return output[0] if batch == 1 else output


def global_avg_pool2d(input: np.ndarray) -> np.ndarray:
    """
    Global average pooling (pool over spatial dimensions).

    Args:
        input: Input tensor (H, W, C) or (B, H, W, C)

    Returns:
        Pooled vector of shape (C,) or (B, C)

    Example:
        >>> global_avg_pool2d(np.ones((4, 4, 3)))
        array([1., 1., 1.])
    """
    if len(input.shape) == 3:
        return np.mean(input, axis=(0, 1))
    return np.mean(input, axis=(1, 2))


def spatial_pyramid_pool(
    input: np.ndarray, bins: list = (1, 2, 4)
) -> np.ndarray:
    """
    Spatial Pyramid Pooling.

    Args:
        input: Input tensor (H, W, C)
        bins: List of bin sizes for pooling

    Returns:
        Concatenated pooled features
    """
    if len(input.shape) == 3:
        input = input[np.newaxis, ...]
    h, w = input.shape[1:3]
    features = []
    for bin_size in bins:
        pooled = avg_pool2d(input[0], pool_size=(bin_size, bin_size))
        features.append(pooled.flatten())
    return np.concatenate(features)


def padding_same(input_size: int, stride: int, kernel_size: int) -> int:
    """Calculate padding for 'same' output size."""
    return ((input_size - 1) * stride + kernel_size - input_size) // 2


def padding_calc(
    input_size: int, output_size: int, kernel_size: int, stride: int
) -> int:
    """
    Calculate padding given desired input/output sizes.

    Args:
        input_size: Input dimension
        output_size: Desired output dimension
        kernel_size: Convolution kernel size
        stride: Stride

    Returns:
        Required padding
    """
    return ((output_size - 1) * stride + kernel_size - input_size) // 2


def create_gaussian_kernel(size: int, sigma: float = 1.0) -> np.ndarray:
    """
    Create Gaussian kernel.

    Args:
        size: Kernel size (odd number)
        sigma: Standard deviation

    Returns:
        Gaussian kernel

    Example:
        >>> kernel = create_gaussian_kernel(3, sigma=1.0)
        >>> kernel.sum() > 0
        True
    """
    if size % 2 == 0:
        size += 1
    x = np.arange(size) - size // 2
    gauss_1d = np.exp(-(x ** 2) / (2 * sigma ** 2))
    kernel = np.outer(gauss_1d, gauss_1d)
    return kernel / kernel.sum()


def create_sobel_kernels() -> Tuple[np.ndarray, np.ndarray]:
    """
    Create Sobel edge detection kernels.

    Returns:
        Tuple of (sobel_x, sobel_y) 3x3 kernels
    """
    sobel_x = np.array([[-1, 0, 1], [-2, 0, 2], [-1, 0, 1]], dtype=np.float32)
    sobel_y = np.array([[-1, -2, -1], [0, 0, 0], [1, 2, 1]], dtype=np.float32)
    return sobel_x, sobel_y


def create_laplacian_kernel() -> np.ndarray:
    """Create Laplacian of Gaussian kernel."""
    return np.array([[0, 1, 0], [1, -4, 1], [0, 1, 0]], dtype=np.float32)


class Conv2DLayer:
    """Simple 2D convolution layer."""

    def __init__(
        self,
        in_channels: int,
        out_channels: int,
        kernel_size: int,
        stride: int = 1,
        padding: int = 0,
        activation: str = "relu",
    ):
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.kernel_size = kernel_size
        self.stride = stride
        self.padding = padding
        scale = np.sqrt(2.0 / (in_channels * kernel_size * kernel_size))
        self.kernel = np.random.randn(kernel_size, kernel_size, in_channels, out_channels) * scale
        self.bias = np.zeros(out_channels)
        self.activation = activation

    def forward(self, input: np.ndarray) -> np.ndarray:
        """Forward pass."""
        out = conv2d(input, self.kernel, stride=(self.stride, self.stride), padding="same" if self.padding else "valid")
        out = out + self.bias
        if self.activation == "relu":
            return np.maximum(0, out)
        return out

    def __call__(self, input: np.ndarray) -> np.ndarray:
        return self.forward(input)


class DepthwiseSeparableConv:
    """Depthwise separable convolution layer."""

    def __init__(
        self, in_channels: int, out_channels: int, kernel_size: int, stride: int = 1, padding: int = 0
    ):
        self.depthwise_kernel = create_gaussian_kernel(kernel_size)
        self.pointwise_kernel = np.random.randn(1, 1, in_channels, out_channels) * 0.01
        self.bias = np.zeros(out_channels)
        self.stride = stride
        self.padding = padding

    def forward(self, input: np.ndarray) -> np.ndarray:
        """Forward pass."""
        dw_out = conv2d(input, self.depthwise_kernel[:, :, np.newaxis, np.newaxis], stride=(self.stride, self.stride), padding="same")
        pw_out = conv2d(dw_out, self.pointwise_kernel, padding="valid")
        return pw_out + self.bias

    def __call__(self, input: np.ndarray) -> np.ndarray:
        return self.forward(input)
