"""
Recurrent Neural Network utilities.

Provides RNN cell implementations, sequence processing,
and gradient clipping utilities.
"""
from __future__ import annotations

from typing import Optional, Tuple

import numpy as np


class RNNCell:
    """Simple RNN cell."""

    def __init__(self, input_size: int, hidden_size: int):
        self.input_size = input_size
        self.hidden_size = hidden_size
        scale = np.sqrt(1.0 / input_size)
        self.W_xh = np.random.randn(input_size, hidden_size) * scale
        self.W_hh = np.random.randn(hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.b_h = np.zeros(hidden_size)

    def forward(self, x: np.ndarray, h_prev: np.ndarray) -> np.ndarray:
        """
        Single RNN step.

        Args:
            x: Input at timestep (batch, input_size)
            h_prev: Previous hidden state (batch, hidden_size)

        Returns:
            New hidden state (batch, hidden_size)
        """
        h_raw = np.dot(x, self.W_xh) + np.dot(h_prev, self.W_hh) + self.b_h
        h_new = np.tanh(h_raw)
        return h_new

    def __call__(self, x: np.ndarray, h_prev: np.ndarray) -> np.ndarray:
        return self.forward(x, h_prev)


class LSTMCell:
    """LSTM cell with forget, input, and output gates."""

    def __init__(self, input_size: int, hidden_size: int):
        self.input_size = input_size
        self.hidden_size = hidden_size
        scale = np.sqrt(1.0 / input_size)
        self.W_xi = np.random.randn(input_size, hidden_size) * scale
        self.W_xf = np.random.randn(input_size, hidden_size) * scale
        self.W_xo = np.random.randn(input_size, hidden_size) * scale
        self.W_xc = np.random.randn(input_size, hidden_size) * scale
        self.W_hi = np.random.randn(hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.W_hf = np.random.randn(hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.W_ho = np.random.randn(hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.W_hc = np.random.randn(hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.b_i = np.zeros(hidden_size)
        self.b_f = np.zeros(hidden_size)
        self.b_o = np.zeros(hidden_size)
        self.b_c = np.zeros(hidden_size)

    def sigmoid(self, x: np.ndarray) -> np.ndarray:
        return 1 / (1 + np.exp(-np.clip(x, -500, 500)))

    def forward(self, x: np.ndarray, state: Tuple[np.ndarray, np.ndarray]) -> Tuple[np.ndarray, np.ndarray]:
        """
        Single LSTM step.

        Args:
            x: Input at timestep (batch, input_size)
            state: Tuple of (hidden_state, cell_state)

        Returns:
            Tuple of (new_hidden, new_cell)
        """
        h_prev, c_prev = state
        i = self.sigmoid(np.dot(x, self.W_xi) + np.dot(h_prev, self.W_hi) + self.b_i)
        f = self.sigmoid(np.dot(x, self.W_xf) + np.dot(h_prev, self.W_hf) + self.b_f)
        o = self.sigmoid(np.dot(x, self.W_xo) + np.dot(h_prev, self.W_ho) + self.b_o)
        c_tilde = np.tanh(np.dot(x, self.W_xc) + np.dot(h_prev, self.W_hc) + self.b_c)
        c_new = f * c_prev + i * c_tilde
        h_new = o * np.tanh(c_new)
        return h_new, c_new

    def __call__(self, x: np.ndarray, state: Tuple[np.ndarray, np.ndarray]) -> Tuple[np.ndarray, np.ndarray]:
        return self.forward(x, state)


class GRUCell:
    """Gated Recurrent Unit cell."""

    def __init__(self, input_size: int, hidden_size: int):
        self.input_size = input_size
        self.hidden_size = hidden_size
        scale = np.sqrt(1.0 / input_size)
        self.W_zr = np.random.randn(input_size + hidden_size, hidden_size * 2) * scale
        self.W_h = np.random.randn(input_size + hidden_size, hidden_size) * np.sqrt(1.0 / hidden_size)
        self.b_zr = np.zeros(hidden_size * 2)
        self.b_h = np.zeros(hidden_size)

    def sigmoid(self, x: np.ndarray) -> np.ndarray:
        return 1 / (1 + np.exp(-np.clip(x, -500, 500)))

    def forward(self, x: np.ndarray, h_prev: np.ndarray) -> np.ndarray:
        """
        Single GRU step.

        Args:
            x: Input at timestep (batch, input_size)
            h_prev: Previous hidden state (batch, hidden_size)

        Returns:
            New hidden state
        """
        concat = np.concatenate([x, h_prev], axis=-1)
        zr = self.sigmoid(np.dot(concat, self.W_zr) + self.b_zr)
        z, r = zr[:, :self.hidden_size], zr[:, self.hidden_size:]
        h_tilde = np.tanh(np.dot(np.concatenate([x, r * h_prev], axis=-1), self.W_h) + self.b_h)
        h_new = (1 - z) * h_prev + z * h_tilde
        return h_new

    def __call__(self, x: np.ndarray, h_prev: np.ndarray) -> np.ndarray:
        return self.forward(x, h_prev)


def rnn_forward(
    x_seq: np.ndarray, cell: RNNCell, h0: np.ndarray = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Forward pass through RNN sequence.

    Args:
        x_seq: Input sequence (batch, seq_len, input_size)
        cell: RNN cell
        h0: Initial hidden state

    Returns:
        Tuple of (outputs, final_hidden)
    """
    batch, seq_len, _ = x_seq.shape
    if h0 is None:
        h0 = np.zeros((batch, cell.hidden_size))
    h = h0
    outputs = []
    for t in range(seq_len):
        h = cell.forward(x_seq[:, t, :], h)
        outputs.append(h)
    return np.stack(outputs, axis=1), h


def lstm_forward(
    x_seq: np.ndarray, cell: LSTMCell, h0: np.ndarray = None, c0: np.ndarray = None
) -> Tuple[np.ndarray, Tuple[np.ndarray, np.ndarray]]:
    """
    Forward pass through LSTM sequence.

    Args:
        x_seq: Input sequence (batch, seq_len, input_size)
        cell: LSTM cell
        h0: Initial hidden state
        c0: Initial cell state

    Returns:
        Tuple of (outputs, (final_hidden, final_cell))
    """
    batch, seq_len, _ = x_seq.shape
    if h0 is None:
        h0 = np.zeros((batch, cell.hidden_size))
    if c0 is None:
        c0 = np.zeros((batch, cell.hidden_size))
    h, c = h0, c0
    outputs = []
    for t in range(seq_len):
        h, c = cell.forward(x_seq[:, t, :], (h, c))
        outputs.append(h)
    return np.stack(outputs, axis=1), (h, c)


def gru_forward(
    x_seq: np.ndarray, cell: GRUCell, h0: np.ndarray = None
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Forward pass through GRU sequence.

    Args:
        x_seq: Input sequence (batch, seq_len, input_size)
        cell: GRU cell
        h0: Initial hidden state

    Returns:
        Tuple of (outputs, final_hidden)
    """
    batch, seq_len, _ = x_seq.shape
    if h0 is None:
        h0 = np.zeros((batch, cell.hidden_size))
    h = h0
    outputs = []
    for t in range(seq_len):
        h = cell.forward(x_seq[:, t, :], h)
        outputs.append(h)
    return np.stack(outputs, axis=1), h


def bidirectional_rnn(
    x_seq: np.ndarray, cell: RNNCell
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Bidirectional RNN forward pass.

    Returns:
        Tuple of (forward_outputs, backward_outputs, combined)
    """
    batch, seq_len, _ = x_seq.shape
    fwd_outputs, fwd_h = rnn_forward(x_seq, cell)
    bwd_outputs, _ = rnn_forward(x_seq[:, ::-1, :], cell)
    bwd_outputs = bwd_outputs[:, ::-1, :]
    combined = np.concatenate([fwd_outputs, bwd_outputs], axis=-1)
    return fwd_outputs, bwd_outputs, combined


def clip_gradients(grads: dict, clip_value: float = 5.0) -> dict:
    """
    Clip gradients by value.

    Args:
        grads: Dictionary of parameter gradients
        clip_value: Maximum gradient magnitude

    Returns:
        Clipped gradients dictionary
    """
    return {k: np.clip(v, -clip_value, clip_value) for k, v in grads.items()}


def clip_gradients_by_norm(grads: dict, max_norm: float = 5.0) -> dict:
    """
    Clip gradients by global norm.

    Args:
        grads: Dictionary of parameter gradients
        max_norm: Maximum global norm

    Returns:
        Clipped gradients dictionary
    """
    total_norm = np.sqrt(sum(np.sum(g ** 2) for g in grads.values()))
    clip_coef = max_norm / (total_norm + 1e-6)
    if clip_coef < 1:
        return {k: g * clip_coef for k, g in grads.items()}
    return grads


class SequenceClassifier:
    """Simple sequence classification using last hidden state."""

    def __init__(self, hidden_size: int, num_classes: int):
        self.hidden_size = hidden_size
        self.num_classes = num_classes
        self.W = np.random.randn(hidden_size, num_classes) * np.sqrt(1.0 / hidden_size)
        self.b = np.zeros(num_classes)

    def forward(self, hidden_final: np.ndarray) -> np.ndarray:
        """Classify from final hidden state."""
        logits = np.dot(hidden_final, self.W) + self.b
        return softmax(logits, axis=-1)


def softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Numerically stable softmax."""
    x_max = np.max(x, axis=axis, keepdims=True)
    exp_x = np.exp(x - x_max)
    return exp_x / np.sum(exp_x, axis=axis, keepdims=True)
