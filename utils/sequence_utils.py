"""
Sequence processing utilities.

Provides tools for sequence manipulation, alignment, padding,
and common sequence operations.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple, Union

import numpy as np


def pad_sequence_1d(
    sequence: Sequence[int],
    max_length: int,
    pad_value: int = 0,
    truncating: str = "post",
) -> List[int]:
    """
    Pad or truncate a 1D sequence.

    Args:
        sequence: Input sequence
        max_length: Target length
        pad_value: Value for padding
        truncating: Truncation strategy (post or pre)

    Returns:
        Padded sequence

    Example:
        >>> pad_sequence_1d([1, 2, 3], 5)
        [1, 2, 3, 0, 0]
        >>> pad_sequence_1d([1, 2, 3, 4, 5], 3)
        [1, 2, 3]
    """
    if len(sequence) > max_length:
        if truncating == "pre":
            return list(sequence[-max_length:])
        return list(sequence[:max_length])
    padding = [pad_value] * (max_length - len(sequence))
    if truncating == "pre":
        return padding + list(sequence)
    return list(sequence) + padding


def pad_sequences_2d(
    sequences: Sequence[Sequence[Sequence]]],
    max_length: Optional[int] = None,
    pad_value: int = 0,
) -> np.ndarray:
    """
    Pad a list of 2D sequences (e.g., batch of sequences).

    Args:
        sequences: List of sequences (each sequence is a list of vectors)
        max_length: Maximum sequence length (auto if None)
        pad_value: Padding value

    Returns:
        Padded array of shape (batch, max_length, features)

    Example:
        >>> pad_sequences_2d([[[1, 2], [3, 4]], [[5, 6]]])
        array([[[1, 2],
                [3, 4]],
               [[5, 6],
                [0, 0]]])
    """
    if not sequences:
        return np.array([])
    if max_length is None:
        max_length = max(len(seq) for seq in sequences)
    batch_size = len(sequences)
    feature_dim = len(sequences[0][0]) if sequences[0] else 0
    result = np.full((batch_size, max_length, feature_dim), pad_value, dtype=np.float32)
    for i, seq in enumerate(sequences):
        length = min(len(seq), max_length)
        result[i, :length] = seq[:length]
    return result


def sequence_mask(lengths: np.ndarray, max_length: int = None) -> np.ndarray:
    """
    Create boolean mask for variable-length sequences.

    Args:
        lengths: Sequence lengths
        max_length: Maximum length (or use max of lengths)

    Returns:
        Boolean mask array

    Example:
        >>> sequence_mask(np.array([2, 3, 1]), max_length=4)
        array([[ True,  True, False, False],
               [ True,  True,  True, False],
               [ True, False, False, False]])
    """
    if max_length is None:
        max_length = int(lengths.max())
    indices = np.arange(max_length).reshape(1, -1)
    mask = indices < lengths.reshape(-1, 1)
    return mask


def pack_sequence(sequences: List[np.ndarray]) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Pack variable-length sequences into packed sequence.

    Args:
        sequences: List of variable-length arrays

    Returns:
        Tuple of (packed_data, batch_sizes, sorted_indices)

    Example:
        >>> data, sizes, indices = pack_sequence([np.array([1,2]), np.array([3,4,5])])
        >>> data
        array([1, 2, 3, 4, 5])
        >>> sizes
        array([2, 2, 1])
    """
    lengths = np.array([len(seq) for seq in sequences])
    sorted_indices = np.argsort(-lengths)
    sorted_lengths = lengths[sorted_indices]
    batch_sizes = sorted_lengths
    packed_data = np.concatenate([sequences[i] for i in sorted_indices])
    return packed_data, batch_sizes, sorted_indices


def unpacks_sequence(packed_data: np.ndarray, batch_sizes: np.ndarray, sorted_indices: np.ndarray) -> List[np.ndarray]:
    """
    Unpack packed sequence back to list of arrays.

    Args:
        packed_data: Concatenated data
        batch_sizes: Batch sizes for each step
        sorted_indices: Original indices

    Returns:
        List of unpadded sequences
    """
    sequences = []
    idx = 0
    for size in batch_sizes:
        seq = packed_data[idx : idx + size]
        sequences.append(seq)
        idx += size
    unsorted = [None] * len(sequences)
    for new_idx, orig_idx in enumerate(sorted_indices):
        unsorted[orig_idx] = sequences[new_idx]
    return unsorted


def sequence_similarity(seq1: Sequence, seq2: Sequence, mode: str = "hamming") -> float:
    """
    Calculate similarity between two sequences.

    Args:
        seq1: First sequence
        seq2: Second sequence
        mode: Similarity mode (hamming, jaccard, cosine)

    Returns:
        Similarity score

    Example:
        >>> sequence_similarity([1, 2, 3], [1, 2, 4], mode='hamming')
        0.6666666666666666
    """
    if mode == "hamming":
        if len(seq1) != len(seq2):
            raise ValueError("Sequences must have same length for Hamming similarity")
        matches = sum(a == b for a, b in zip(seq1, seq2))
        return matches / len(seq1)
    elif mode == "jaccard":
        set1, set2 = set(seq1), set(seq2)
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0
    elif mode == "cosine":
        vec1 = np.array(seq1)
        vec2 = np.array(seq2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return np.dot(vec1, vec2) / (norm1 * norm2)
    else:
        raise ValueError(f"Unknown mode: {mode}")


def longest_common_subsequence(seq1: Sequence, seq2: Sequence) -> int:
    """
    Find longest common subsequence length.

    Args:
        seq1: First sequence
        seq2: Second sequence

    Returns:
        Length of LCS

    Example:
        >>> longest_common_subsequence("ABCD", "ACBD")
        3
    """
    m, n = len(seq1), len(seq2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if seq1[i - 1] == seq2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1] + 1
            else:
                dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])
    return dp[m][n]


def edit_distance(seq1: Sequence, seq2: Sequence) -> int:
    """
    Calculate Levenshtein edit distance.

    Args:
        seq1: First sequence
        seq2: Second sequence

    Returns:
        Edit distance

    Example:
        >>> edit_distance("kitten", "sitting")
        3
    """
    m, n = len(seq1), len(seq2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if seq1[i - 1] == seq2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[m][n]


def n_gram_features(
    sequences: Sequence[Sequence],
    n: int = 2,
    vocab_size: int = None,
) -> np.ndarray:
    """
    Extract n-gram features from sequences.

    Args:
        sequences: List of sequences
        n: N-gram size
        vocab_size: Vocabulary size for hashing

    Returns:
        Feature matrix

    Example:
        >>> features = n_gram_features([[1, 2, 3], [2, 3, 4]], n=2)
        >>> features.shape[1] > 0
        True
    """
    def hash_ngram(ngram):
        return hash(tuple(ngram)) % (vocab_size or 10000)
    features = []
    for seq in sequences:
        ngram_counts = {}
        for i in range(len(seq) - n + 1):
            ngram = tuple(seq[i : i + n])
            idx = hash_ngram(ngram)
            ngram_counts[idx] = ngram_counts.get(idx, 0) + 1
        features.append(ngram_counts)
    if vocab_size is None:
        all_keys = set()
        for f in features:
            all_keys.update(f.keys())
        vocab_size = len(all_keys)
    matrix = np.zeros((len(sequences), vocab_size))
    for i, f in enumerate(features):
        for idx, count in f.items():
            matrix[i, idx] = count
    return matrix


def rolling_window_features(
    sequence: Sequence,
    window_size: int,
    step: int = 1,
    aggregator: str = "mean",
) -> np.ndarray:
    """
    Extract features using rolling window.

    Args:
        sequence: Input sequence
        window_size: Size of rolling window
        step: Step size between windows
        aggregator: Aggregation function (mean, sum, max, min, std)

    Returns:
        Array of aggregated values

    Example:
        >>> rolling_window_features([1, 2, 3, 4, 5], window_size=3)
        array([2., 3., 4.])
    """
    aggregators = {
        "mean": np.mean,
        "sum": np.sum,
        "max": np.max,
        "min": np.min,
        "std": np.std,
    }
    agg_func = aggregators.get(aggregator, np.mean)
    result = []
    for i in range(0, len(sequence) - window_size + 1, step):
        window = sequence[i : i + window_size]
        result.append(agg_func(window))
    return np.array(result)


def sequence_to_chunks(
    sequence: Sequence, chunk_size: int, overlap: int = 0
) -> List[List]:
    """
    Split sequence into overlapping chunks.

    Args:
        sequence: Input sequence
        chunk_size: Size of each chunk
        overlap: Number of overlapping elements

    Returns:
        List of chunks

    Example:
        >>> sequence_to_chunks([1, 2, 3, 4, 5], chunk_size=3, overlap=1)
        [[1, 2, 3], [3, 4, 5]]
    """
    step = chunk_size - overlap
    chunks = []
    for i in range(0, len(sequence) - overlap, step):
        chunk = sequence[i : i + chunk_size]
        if len(chunk) == chunk_size:
            chunks.append(list(chunk))
    return chunks
