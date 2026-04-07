"""
Tensor operations utility module.

Provides common tensor operations including reshaping, splitting,
concatenation, broadcasting, and mathematical operations.
"""
from __future__ import annotations

import math
from typing import Any, Callable, Sequence, Tuple, Union

import numpy as np


def ensure_tensor(data: Any, dtype: str = "float32") -> np.ndarray:
    """
    Convert input data to a numpy tensor.

    Args:
        data: Input data (list, tuple, numpy array, or scalar)
        dtype: Target data type (default: float32)

    Returns:
        Numpy ndarray with specified dtype

    Example:
        >>> ensure_tensor([[1, 2], [3, 4]])
        array([[1., 2.],
               [3., 4.]])
    """
    if isinstance(data, np.ndarray):
        return data.astype(dtype)
    if isinstance(data, (list, tuple)):
        return np.array(data, dtype=dtype)
    return np.array([data], dtype=dtype)


def tensor_reshape(tensor: np.ndarray, shape: Tuple[int, ...]) -> np.ndarray:
    """
    Reshape tensor with validation.

    Args:
        tensor: Input tensor
        shape: Target shape (must match total elements)

    Returns:
        Reshaped tensor

    Example:
        >>> tensor_reshape(np.arange(6), (2, 3))
        array([[0, 1, 2],
               [3, 4, 5]])
    """
    tensor = ensure_tensor(tensor)
    shape = tuple(shape)
    if np.prod(shape) != np.prod(tensor.shape):
        raise ValueError(
            f"Cannot reshape {tensor.shape} to {shape}: "
            f"total elements {np.prod(tensor.shape)} != {np.prod(shape)}"
        )
    return tensor.reshape(shape)


def tensor_split(
    tensor: np.ndarray, indices_or_sections: Union[int, Sequence[int]], axis: int = 0
) -> list[np.ndarray]:
    """
    Split tensor along axis.

    Args:
        tensor: Input tensor
        indices_or_sections: Number of sections or list of split points
        axis: Axis along which to split (default: 0)

    Returns:
        List of tensor slices

    Example:
        >>> tensor_split(np.arange(6), 3)
        [array([0, 1]), array([2, 3]), array([4, 5])]
    """
    tensor = ensure_tensor(tensor)
    return np.split(tensor, indices_or_sections, axis=axis)


def tensor_concatenate(
    tensors: Sequence[np.ndarray], axis: int = 0
) -> np.ndarray:
    """
    Concatenate tensors along axis.

    Args:
        tensors: Sequence of tensors to concatenate
        axis: Axis along which to concatenate (default: 0)

    Returns:
        Concatenated tensor

    Example:
        >>> tensor_concatenate([np.array([1, 2]), np.array([3, 4])])
        array([1, 2, 3, 4])
    """
    tensors = [ensure_tensor(t) for t in tensors]
    return np.concatenate(tensors, axis=axis)


def tensor_stack(tensors: Sequence[np.ndarray], axis: int = 0) -> np.ndarray:
    """
    Stack tensors along new axis.

    Args:
        tensors: Sequence of tensors to stack
        axis: Axis along which to stack (default: 0)

    Returns:
        Stacked tensor with one more dimension

    Example:
        >>> tensor_stack([np.array([1, 2]), np.array([3, 4])])
        array([[1, 2],
               [3, 4]])
    """
    tensors = [ensure_tensor(t) for t in tensors]
    return np.stack(tensors, axis=axis)


def tensor_broadcast(a: np.ndarray, b: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Broadcast two tensors to compatible shapes.

    Args:
        a: First tensor
        b: Second tensor

    Returns:
        Tuple of (broadcast_a, broadcast_b) with compatible shapes

    Example:
        >>> a, b = tensor_broadcast(np.ones((3, 1)), np.ones((1, 4)))
        >>> a.shape, b.shape
        ((3, 4), (3, 4))
    """
    a, b = ensure_tensor(a), ensure_tensor(b)
    shape = np.broadcast_shapes(a.shape, b.shape)
    return np.broadcast_to(a, shape), np.broadcast_to(b, shape)


def tensor_transpose(tensor: np.ndarray, axes: Tuple[int, ...]) -> np.ndarray:
    """
    Transpose tensor with specified axes.

    Args:
        tensor: Input tensor
        axes: Tuple specifying new axis order

    Returns:
        Transposed tensor

    Example:
        >>> tensor_transpose(np.arange(6).reshape(2, 3), (1, 0))
        array([[0, 3],
               [1, 4],
               [2, 5]])
    """
    return ensure_tensor(tensor).transpose(axes)


def tensor_squeeze(tensor: np.ndarray, axis: Union[int, Tuple[int, ...], None] = None) -> np.ndarray:
    """
    Remove dimensions of size 1.

    Args:
        tensor: Input tensor
        axis: Specific axis to squeeze, or None for all

    Returns:
        Squeezed tensor

    Example:
        >>> tensor_squeeze(np.ones((1, 3, 1, 2)), axis=0)
        array([[[1., 2.]]])
    """
    return ensure_tensor(tensor).squeeze(axis=axis)


def tensor_expand_dims(tensor: np.ndarray, axis: int) -> np.ndarray:
    """
    Add dimension of size 1 at specified position.

    Args:
        tensor: Input tensor
        axis: Position for new dimension

    Returns:
        Expanded tensor

    Example:
        >>> tensor_expand_dims(np.array([1, 2, 3]), axis=0).shape
        (1, 3)
    """
    return ensure_tensor(tensor)[np.newaxis, :] if axis == 0 else ensure_tensor(tensor)[..., np.newaxis]


def tensor_clip(
    tensor: np.ndarray, min_val: float = None, max_val: float = None
) -> np.ndarray:
    """
    Clip tensor values to range [min_val, max_val].

    Args:
        tensor: Input tensor
        min_val: Minimum value (default: None)
        max_val: Maximum value (default: None)

    Returns:
        Clipped tensor

    Example:
        >>> tensor_clip(np.array([-1, 0, 1, 2]), min_val=0, max_val=1)
        array([0, 0, 1, 1])
    """
    tensor = ensure_tensor(tensor)
    return np.clip(tensor, min_val, max_val)


def tensor_normalize(
    tensor: np.ndarray, mean: Union[float, Sequence[float]] = 0.0, std: Union[float, Sequence[float]] = 1.0
) -> np.ndarray:
    """
    Normalize tensor with mean and standard deviation.

    Args:
        tensor: Input tensor
        mean: Mean value(s) for normalization
        std: Standard deviation value(s) for normalization

    Returns:
        Normalized tensor

    Example:
        >>> tensor_normalize(np.array([0, 1, 2]), mean=1, std=2)
        array([-0.5,  0. ,  0.5])
    """
    tensor = ensure_tensor(tensor, dtype="float64")
    return (tensor - np.array(mean)) / np.array(std)


def tensor_one_hot(indices: np.ndarray, num_classes: int) -> np.ndarray:
    """
    Convert indices to one-hot encoded tensor.

    Args:
        indices: Array of class indices
        num_classes: Total number of classes

    Returns:
        One-hot encoded tensor

    Example:
        >>> tensor_one_hot(np.array([0, 2, 1]), num_classes=3)
        array([[1., 0., 0.],
               [0., 0., 1.],
               [0., 1., 0.]])
    """
    indices = ensure_tensor(indices, dtype="int64")
    return np.eye(num_classes, dtype="float32")[indices.flatten()].reshape(*indices.shape, num_classes)


def tensor_flatten(tensor: np.ndarray, start_dim: int = 0) -> np.ndarray:
    """
    Flatten tensor from start_dim.

    Args:
        tensor: Input tensor
        start_dim: First dimension to flatten (default: 0)

    Returns:
        Flattened tensor

    Example:
        >>> tensor_flatten(np.ones((2, 3, 4)), start_dim=1).shape
        (2, 12)
    """
    tensor = ensure_tensor(tensor)
    if start_dim == 0:
        return tensor.flatten()
    shape = tensor.shape[:start_dim] + (np.prod(tensor.shape[start_dim:]),)
    return tensor.reshape(shape)


def tensor_dot(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Compute dot product of two tensors.

    Args:
        a: First tensor
        b: Second tensor

    Returns:
        Dot product result

    Example:
        >>> tensor_dot(np.array([1, 2]), np.array([3, 4]))
        11.0
    """
    a, b = ensure_tensor(a, dtype="float64"), ensure_tensor(b, dtype="float64")
    return np.dot(a.flatten(), b.flatten())


def tensor_outer(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Compute outer product of two tensors.

    Args:
        a: First tensor
        b: Second tensor

    Returns:
        Outer product result

    Example:
        >>> tensor_outer(np.array([1, 2]), np.array([3, 4]))
        array([[3., 4.],
               [6., 8.]])
    """
    a, b = ensure_tensor(a, dtype="float64"), ensure_tensor(b, dtype="float64")
    return np.outer(a.flatten(), b.flatten())


def batched_dot(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """
    Compute batched dot product along last axis.

    Args:
        a: First tensor with shape (..., n)
        b: Second tensor with shape (..., n)

    Returns:
        Tensor with shape (...,)

    Example:
        >>> batched_dot(np.ones((3, 4)), np.ones((3, 4)))
        array([4., 4., 4.])
    """
    a, b = ensure_tensor(a, dtype="float64"), ensure_tensor(b, dtype="float64")
    return np.sum(a * b, axis=-1)


def tensor_gather(
    tensor: np.ndarray, indices: np.ndarray, axis: int = 0
) -> np.ndarray:
    """
    Gather values from tensor at specified indices.

    Args:
        tensor: Source tensor
        indices: Indices to gather
        axis: Axis along which to gather (default: 0)

    Returns:
        Gathered tensor

    Example:
        >>> tensor_gather(np.array(['a', 'b', 'c']), np.array([0, 2]))
        array(['a', 'c'], dtype='<U1')
    """
    tensor, indices = ensure_tensor(tensor), ensure_tensor(indices, dtype="int64")
    return np.take(tensor, indices, axis=axis)


def tensor_scatter(
    base: np.ndarray, indices: np.ndarray, updates: np.ndarray, axis: int = 0
) -> np.ndarray:
    """
    Scatter updates into base tensor at indices.

    Args:
        base: Base tensor to update
        indices: Indices where to place updates
        updates: Values to insert
        axis: Axis along which to scatter (default: 0)

    Returns:
        Updated tensor copy

    Example:
        >>> tensor_scatter(np.zeros(3), np.array([0, 2]), np.array([1, 2]))
        array([1., 0., 2.])
    """
    base = ensure_tensor(base).copy()
    indices = ensure_tensor(indices, dtype="int64").flatten()
    updates = ensure_tensor(updates).flatten()
    np.put(base, indices, updates)
    return base
