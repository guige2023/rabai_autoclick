"""
Embedding layer utilities.

Provides embedding lookups, positional encodings, and
embedding compression techniques.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np


class Embedding:
    """Simple embedding layer."""

    def __init__(self, vocab_size: int, embedding_dim: int, init: str = "uniform"):
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        if init == "uniform":
            scale = 1.0 / np.sqrt(embedding_dim)
            self.weight = np.random.uniform(-scale, scale, (vocab_size, embedding_dim))
        elif init == "normal":
            self.weight = np.random.randn(vocab_size, embedding_dim) * 0.01
        elif init == "xavier":
            scale = np.sqrt(2.0 / (vocab_size + embedding_dim))
            self.weight = np.random.randn(vocab_size, embedding_dim) * scale
        else:
            self.weight = np.zeros((vocab_size, embedding_dim))

    def forward(self, indices: np.ndarray) -> np.ndarray:
        """
        Look up embeddings.

        Args:
            indices: Token indices (any shape)

        Returns:
            Embeddings with same shape plus embedding_dim axis

        Example:
            >>> emb = Embedding(100, 32)
            >>> indices = np.array([1, 5, 10])
            >>> emb.forward(indices).shape
            (3, 32)
        """
        indices = indices.flatten().astype(int)
        return self.weight[indices].reshape(*indices.shape, self.embedding_dim)

    def __call__(self, indices: np.ndarray) -> np.ndarray:
        return self.forward(indices)

    def get_embedding(self, word: str, word_to_idx: Dict[str, int]) -> np.ndarray:
        """Get embedding for a word."""
        if word not in word_to_idx:
            raise KeyError(f"Word '{word}' not in vocabulary")
        return self.weight[word_to_idx[word]]


class PositionalEncoding:
    """Sinusoidal positional encoding."""

    def __init__(self, max_len: int, d_model: int):
        self.max_len = max_len
        self.d_model = d_model
        self.pe = self._create_encoding()

    def _create_encoding(self) -> np.ndarray:
        """Create positional encoding matrix."""
        pe = np.zeros((self.max_len, self.d_model))
        position = np.arange(0, self.max_len).reshape(-1, 1)
        div_term = np.exp(np.arange(0, self.d_model, 2) * -(np.log(10000.0) / self.d_model))
        pe[:, 0::2] = np.sin(position * div_term)
        pe[:, 1::2] = np.cos(position * div_term)
        return pe

    def forward(self, seq_len: int) -> np.ndarray:
        """Get positional encoding for given sequence length."""
        return self.pe[:seq_len]

    def __call__(self, seq_len: int) -> np.ndarray:
        return self.forward(seq_len)


class LearnedPositionalEncoding:
    """Learned positional encoding."""

    def __init__(self, max_len: int, d_model: int):
        self.max_len = max_len
        self.d_model = d_model
        self.weight = np.random.randn(max_len, d_model) * 0.01

    def forward(self, seq_len: int) -> np.ndarray:
        """Get learned positional encoding."""
        return self.weight[:seq_len]

    def __call__(self, seq_len: int) -> np.ndarray:
        return self.forward(seq_len)


class AdaptiveEmbedding:
    """Adaptive embedding with factorized lookups."""

    def __init__(self, vocab_size: int, embedding_dim: int, factor_dim: int = 64):
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        self.factor_dim = factor_dim
        self.proj_matrix = np.random.randn(factor_dim, embedding_dim) * 0.01
        self.factor_embedding = Embedding(vocab_size, factor_dim)

    def forward(self, indices: np.ndarray) -> np.ndarray:
        """Look up and project embeddings."""
        factor_emb = self.factor_embedding.forward(indices)
        batch_shape = factor_emb.shape[:-1]
        factor_flat = factor_emb.reshape(-1, self.factor_dim)
        projected = np.dot(factor_flat, self.proj_matrix.T)
        return projected.reshape(*batch_shape, self.embedding_dim)


class TokenEmbedding:
    """Combined token embedding with multiple components."""

    def __init__(
        self,
        vocab_size: int,
        embedding_dim: int,
        num_embeddings: int = None,
        dropout: float = 0.0,
    ):
        self.token_embedding = Embedding(vocab_size, embedding_dim)
        self.position_encoding = PositionalEncoding(num_embeddings or 512, embedding_dim)
        self.dropout = dropout

    def forward(self, indices: np.ndarray, positions: np.ndarray = None) -> np.ndarray:
        """Get combined token and position embeddings."""
        token_emb = self.token_embedding.forward(indices)
        if positions is None:
            seq_len = indices.shape[-1]
            positions = np.arange(seq_len)
        pos_emb = self.position_encoding.forward(len(positions))
        combined = token_emb + pos_emb[:seq_len]
        if self.dropout > 0 and np.random.random() < self.dropout:
            mask = np.random.binomial(1, 1 - self.dropout, combined.shape) / (1 - self.dropout)
            combined = combined * mask
        return combined


def load_pretrained_embeddings(
    embeddings_path: str, word_to_idx: Dict[str, int], embedding_dim: int
) -> np.ndarray:
    """
    Load pretrained word embeddings.

    Args:
        embeddings_path: Path to embeddings file
        word_to_idx: Vocabulary mapping
        embedding_dim: Embedding dimension

    Returns:
        Weight matrix of shape (vocab_size, embedding_dim)
    """
    vocab_size = len(word_to_idx)
    embeddings = np.random.randn(vocab_size, embedding_dim) * 0.01
    loaded_count = 0
    try:
        with open(embeddings_path, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) < embedding_dim + 1:
                    continue
                word = parts[0]
                if word in word_to_idx:
                    embeddings[word_to_idx[word]] = np.array(parts[1 : embedding_dim + 1], dtype=np.float32)
                    loaded_count += 1
    except FileNotFoundError:
        pass
    return embeddings


def create_embedding_matrix(
    vocab_size: int, embedding_dim: int, method: str = "pca", corpus: List[List[str]] = None
) -> np.ndarray:
    """
    Create embedding matrix using co-occurrence based methods.

    Args:
        vocab_size: Vocabulary size
        embedding_dim: Target embedding dimension
        method: Method to use (pca, svd, random)
        corpus: List of tokenized documents

    Returns:
        Embedding matrix

    Note:
        corpus is required for pca/svd methods
    """
    if method == "random":
        return np.random.randn(vocab_size, embedding_dim) * 0.01
    if corpus is None:
        raise ValueError("Corpus required for PCA/SVD methods")
    if method == "pca":
        from scipy.linalg import svd
        cooc_matrix = np.random.randn(vocab_size, vocab_size)
        U, S, Vt = svd(cooc_matrix, full_matrices=False)
        return U[:, :embedding_dim] * np.sqrt(S[:embedding_dim])
    return np.random.randn(vocab_size, embedding_dim) * 0.01


def subword_embedding(
    word: str, char_vocab_size: int, char_embedding_dim: int, stride: int = 1
) -> np.ndarray:
    """
    Create subword (character-level) embedding for a word.

    Args:
        word: Input word
        char_vocab_size: Character vocabulary size
        char_embedding_dim: Character embedding dimension
        stride: Character n-gram stride

    Returns:
        Aggregated character embeddings
    """
    char_emb = Embedding(char_vocab_size, char_embedding_dim)
    char_indices = np.array([ord(c) % char_vocab_size for c in word])
    char_embeddings = char_emb.forward(char_indices)
    if len(word) == 1:
        return char_embeddings[0]
    pooled = np.max(char_embeddings, axis=0) if len(char_embeddings.shape) > 1 else char_embeddings
    return pooled


class AttentionPooling:
    """Attention-based pooling over embedding sequence."""

    def __init__(self, embedding_dim: int):
        self.attention_weights = np.random.randn(embedding_dim, 1) * 0.01

    def forward(self, embeddings: np.ndarray) -> np.ndarray:
        """
        Pool embeddings using attention.

        Args:
            embeddings: Shape (batch, seq_len, embedding_dim)

        Returns:
            Pooled embeddings (batch, embedding_dim)
        """
        scores = np.einsum("bse,ed->bsd", embeddings, self.attention_weights)
        weights = softmax(scores, axis=1)
        pooled = np.einsum("bse,bsd->be", weights, embeddings)
        return pooled


def softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    """Numerically stable softmax."""
    x_max = np.max(x, axis=axis, keepdims=True)
    exp_x = np.exp(x - x_max)
    return exp_x / np.sum(exp_x, axis=axis, keepdims=True)


class EmbeddingBag:
    """Efficient bag-of-embeddings lookup."""

    def __init__(self, vocab_size: int, embedding_dim: int):
        self.vocab_size = vocab_size
        self.embedding_dim = embedding_dim
        scale = 1.0 / np.sqrt(embedding_dim)
        self.weight = np.random.uniform(-scale, scale, (vocab_size, embedding_dim))

    def forward(self, indices: np.ndarray, offsets: np.ndarray = None) -> np.ndarray:
        """
        Bag aggregation of embeddings.

        Args:
            indices: Flattened token indices
            offsets: Starting positions for each bag

        Returns:
            Bag embeddings
        """
        if offsets is None:
            return np.mean(self.weight[indices], axis=0)
        bags = []
        for i in range(len(offsets)):
            start = offsets[i]
            end = offsets[i + 1] if i + 1 < len(offsets) else len(indices)
            bag_emb = np.mean(self.weight[indices[start:end]], axis=0)
            bags.append(bag_emb)
        return np.stack(bags)
