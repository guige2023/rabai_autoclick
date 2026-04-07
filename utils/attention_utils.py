"""
Attention mechanism utilities.

Provides various attention mechanisms including scaled dot-product,
multi-head, and relative position attention.
"""
from __future__ import annotations

from typing import Optional, Tuple

import numpy as np


def scaled_dot_product_attention(
    query: np.ndarray,
    key: np.ndarray,
    value: np.ndarray,
    mask: np.ndarray = None,
    scale: bool = True,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Scaled dot-product attention mechanism.

    Args:
        query: Query tensor of shape (..., seq_len_q, d_k)
        key: Key tensor of shape (..., seq_len_k, d_k)
        value: Value tensor of shape (..., seq_len_v, d_v)
        mask: Optional attention mask
        scale: Whether to scale by sqrt(d_k)

    Returns:
        Tuple of (output, attention_weights)

    Example:
        >>> q = np.random.randn(2, 4, 8)
        >>> k = np.random.randn(2, 4, 8)
        >>> v = np.random.randn(2, 4, 8)
        >>> out, attn = scaled_dot_product_attention(q, k, v)
        >>> out.shape
        (2, 4, 8)
    """
    d_k = query.shape[-1]
    scores = np.matmul(query, key.transpose(0, 1, 3, 2))
    if scale:
        scores = scores / np.sqrt(d_k)
    if mask is not None:
        scores = np.where(mask == 0, -1e9, scores)
    attention_weights = softmax(scores, axis=-1)
    output = np.matmul(attention_weights, value)
    return output, attention_weights


def softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Numerically stable softmax."""
    x_max = np.max(x, axis=axis, keepdims=True)
    exp_x = np.exp(x - x_max)
    return exp_x / np.sum(exp_x, axis=axis, keepdims=True)


def multihead_attention(
    query: np.ndarray,
    key: np.ndarray,
    value: np.ndarray,
    num_heads: int = 8,
    mask: np.ndarray = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Multi-head attention mechanism.

    Args:
        query: Query tensor (batch, seq_len, d_model)
        key: Key tensor (batch, seq_len, d_model)
        value: Value tensor (batch, seq_len, d_model)
        num_heads: Number of attention heads
        mask: Optional attention mask

    Returns:
        Tuple of (output, attention_weights)

    Example:
        >>> q = np.random.randn(2, 10, 64)
        >>> k = np.random.randn(2, 10, 64)
        >>> v = np.random.randn(2, 10, 64)
        >>> out, _ = multihead_attention(q, k, v, num_heads=8)
        >>> out.shape
        (2, 10, 64)
    """
    batch, seq_len, d_model = query.shape
    d_k = d_model // num_heads

    Q = reshape_for_heads(query, num_heads)
    K = reshape_for_heads(key, num_heads)
    V = reshape_for_heads(value, num_heads)

    attention_outputs = []
    attention_weights_list = []
    for h in range(num_heads):
        out, attn = scaled_dot_product_attention(Q[:, h], K[:, h], V[:, h], mask)
        attention_outputs.append(out[:, np.newaxis, :, :])
        attention_weights_list.append(attn[:, np.newaxis, :, :])

    output = np.concatenate(attention_outputs, axis=1)
    output = output.reshape(batch, seq_len, d_model)
    attention_weights = np.concatenate(attention_weights_list, axis=1)

    return output, attention_weights


def reshape_for_heads(x: np.ndarray, num_heads: int) -> np.ndarray:
    """Reshape tensor for multi-head processing."""
    batch, seq_len, d_model = x.shape
    d_k = d_model // num_heads
    return x.reshape(batch, num_heads, seq_len, d_k)


def causal_mask(seq_len: int) -> np.ndarray:
    """
    Create causal mask for autoregressive models.

    Args:
        seq_len: Sequence length

    Returns:
        Boolean mask where future positions are masked

    Example:
        >>> causal_mask(4)
        array([[ True, False, False, False],
               [ True,  True, False, False],
               [ True,  True,  True, False],
               [ True,  True,  True,  True]])
    """
    mask = np.triu(np.ones((seq_len, seq_len)), k=1).astype(bool)
    return ~mask


def create_padding_mask(sequence: np.ndarray, pad_token: int = 0) -> np.ndarray:
    """
    Create padding mask for sequences.

    Args:
        sequence: Input sequence of token IDs
        pad_token: Token ID used for padding

    Returns:
        Boolean mask where padding positions are False

    Example:
        >>> create_padding_mask(np.array([[1, 2, 0, 0]]))
        array([[[ True,  True, False, False]]])
    """
    mask = sequence != pad_token
    return mask[:, np.newaxis, np.newaxis, :]


def create_causal_padding_mask(
    sequence: np.ndarray, pad_token: int = 0
) -> np.ndarray:
    """Create combined causal and padding mask."""
    padding_mask = create_padding_mask(sequence, pad_token)
    causal = causal_mask(sequence.shape[1])
    combined = padding_mask & causal
    return combined


class SelfAttention:
    """Self-attention layer."""

    def __init__(self, d_model: int, num_heads: int = 8):
        self.d_model = d_model
        self.num_heads = num_heads
        self.d_k = d_model // num_heads
        scale = np.sqrt(2.0 / d_model)
        self.W_q = np.random.randn(d_model, d_model) * scale
        self.W_k = np.random.randn(d_model, d_model) * scale
        self.W_v = np.random.randn(d_model, d_model) * scale
        self.W_o = np.random.randn(d_model, d_model) * scale

    def forward(self, x: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        """Forward pass."""
        batch, seq_len, _ = x.shape
        Q = np.einsum("bij,jk->bik", x, self.W_q)
        K = np.einsum("bij,jk->bik", x, self.W_k)
        V = np.einsum("bij,jk->bik", x, self.W_v)
        out, _ = multihead_attention(Q, K, V, self.num_heads, mask)
        out = np.einsum("bij,jk->bik", out, self.W_o)
        return out

    def __call__(self, x: np.ndarray, mask: np.ndarray = None) -> np.ndarray:
        return self.forward(x, mask)


class CrossAttention:
    """Cross-attention layer (encoder-decoder attention)."""

    def __init__(self, d_model: int, num_heads: int = 8):
        self.d_model = d_model
        self.num_heads = num_heads
        self.W_q = np.random.randn(d_model, d_model) * np.sqrt(2.0 / d_model)
        self.W_k = np.random.randn(d_model, d_model) * np.sqrt(2.0 / d_model)
        self.W_v = np.random.randn(d_model, d_model) * np.sqrt(2.0 / d_model)
        self.W_o = np.random.randn(d_model, d_model) * np.sqrt(2.0 / d_model)

    def forward(
        self, query: np.ndarray, key: np.ndarray, value: np.ndarray, mask: np.ndarray = None
    ) -> np.ndarray:
        """Forward pass."""
        Q = np.einsum("bij,jk->bik", query, self.W_q)
        K = np.einsum("bij,jk->bik", key, self.W_k)
        V = np.einsum("bij,jk->bik", value, self.W_v)
        out, _ = multihead_attention(Q, K, V, self.num_heads, mask)
        out = np.einsum("bij,jk->bik", out, self.W_o)
        return out

    def __call__(
        self, query: np.ndarray, key: np.ndarray, value: np.ndarray, mask: np.ndarray = None
    ) -> np.ndarray:
        return self.forward(query, key, value, mask)


class PositionalEncoding:
    """Sinusoidal positional encoding."""

    def __init__(self, d_model: int, max_len: int = 5000):
        self.d_model = d_model
        self.pe = self._create_encoding(max_len, d_model)

    def _create_encoding(self, max_len: int, d_model: int) -> np.ndarray:
        """Create positional encoding matrix."""
        pe = np.zeros((max_len, d_model))
        position = np.arange(0, max_len).reshape(-1, 1)
        div_term = np.exp(np.arange(0, d_model, 2) * -(np.log(10000.0) / d_model))
        pe[:, 0::2] = np.sin(position * div_term)
        pe[:, 1::2] = np.cos(position * div_term)
        return pe

    def forward(self, seq_len: int) -> np.ndarray:
        """Get positional encoding for sequence length."""
        return self.pe[:seq_len]

    def __call__(self, seq_len: int) -> np.ndarray:
        return self.forward(seq_len)


def additive_attention(query: np.ndarray, key: np.ndarray, value: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Additive attention (Bahdanau attention).

    Args:
        query: Decoder hidden state (batch, d_q)
        key: Encoder outputs (batch, seq_len, d_k)
        value: Encoder outputs (batch, seq_len, d_v)

    Returns:
        Tuple of (context vector, attention weights)
    """
    d_k = key.shape[-1]
    scores = np.sum(np.tanh(query[:, np.newaxis, :] + key[:, :, :d_k]) * np.ones((1, 1, d_k)), axis=-1)
    attention_weights = softmax(scores, axis=-1)
    context = np.einsum("bs,bv->bv", attention_weights, value[:, :, 0]) if len(value.shape) == 3 else np.einsum("bs,bv->bv", attention_weights, value)
    return context, attention_weights


def multiplicative_attention(query: np.ndarray, key: np.ndarray, value: np.ndarray, scale: bool = True) -> Tuple[np.ndarray, np.ndarray]:
    """
    Multiplicative attention (Luong attention).

    Args:
        query: Query vector (batch, d_q)
        key: Key vectors (batch, seq_len, d_k)
        value: Value vectors (batch, seq_len, d_v)
        scale: Whether to scale scores

    Returns:
        Tuple of (context vector, attention weights)
    """
    d_k = key.shape[-1]
    if scale:
        d_k = np.sqrt(d_k)
    scores = np.sum(query[:, np.newaxis, :] * key, axis=-1) / d_k
    attention_weights = softmax(scores, axis=-1)
    context = np.einsum("bs,bsv->bv", attention_weights, value)
    return context, attention_weights
