"""
Text similarity and embedding utilities.

Provides text similarity metrics, sentence embeddings,
and semantic search utilities.
"""
from __future__ import annotations

from typing import List, Optional, Sequence, Tuple

import numpy as np


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """
    Compute cosine similarity between two vectors.

    Args:
        a: First vector
        b: Second vector

    Returns:
        Cosine similarity in range [-1, 1]

    Example:
        >>> cosine_similarity(np.array([1, 0]), np.array([1, 0]))
        1.0
        >>> cosine_similarity(np.array([1, 0]), np.array([0, 1]))
        0.0
    """
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return np.dot(a, b) / (norm_a * norm_b)


def euclidean_distance(a: np.ndarray, b: np.ndarray) -> float:
    """
    Compute Euclidean distance between two vectors.

    Args:
        a: First vector
        b: Second vector

    Returns:
        Euclidean distance

    Example:
        >>> euclidean_distance(np.array([0, 0]), np.array([3, 4]))
        5.0
    """
    return np.linalg.norm(a - b)


def manhattan_distance(a: np.ndarray, b: np.ndarray) -> float:
    """
    Compute Manhattan (L1) distance between two vectors.

    Args:
        a: First vector
        b: Second vector

    Returns:
        Manhattan distance

    Example:
        >>> manhattan_distance(np.array([0, 0]), np.array([3, 4]))
        7.0
    """
    return np.sum(np.abs(a - b))


def jaccard_similarity(a: Sequence, b: Sequence) -> float:
    """
    Compute Jaccard similarity between two sequences.

    Args:
        a: First sequence
        b: Second sequence

    Returns:
        Jaccard similarity in range [0, 1]

    Example:
        >>> jaccard_similarity([1, 2, 3], [2, 3, 4])
        0.5
    """
    set_a, set_b = set(a), set(b)
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def dice_coefficient(a: Sequence, b: Sequence) -> float:
    """
    Compute Dice coefficient between two sequences.

    Args:
        a: First sequence
        b: Second sequence

    Returns:
        Dice coefficient in range [0, 1]

    Example:
        >>> dice_coefficient([1, 2, 3], [2, 3, 4])
        0.6
    """
    set_a, set_b = set(a), set(b)
    intersection = len(set_a & set_b)
    return 2 * intersection / (len(set_a) + len(set_b)) if (len(set_a) + len(set_b)) > 0 else 0.0


def levenshtein_similarity(s1: str, s2: str) -> float:
    """
    Compute normalized Levenshtein (edit) similarity.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Similarity in range [0, 1]

    Example:
        >>> levenshtein_similarity("kitten", "sitting")
        0.5714285714285714
    """
    if len(s1) == 0 and len(s2) == 0:
        return 1.0
    distance = _levenshtein_distance(s1, s2)
    max_len = max(len(s1), len(s2))
    return 1 - distance / max_len


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance."""
    m, n = len(s1), len(s2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s1[i - 1] == s2[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[m][n]


def soft_cosine_similarity(
    a: np.ndarray, b: np.ndarray, similarity_matrix: np.ndarray
) -> float:
    """
    Compute soft cosine similarity using a similarity matrix.

    Args:
        a: First vector
        b: Second vector
        similarity_matrix: Matrix defining term similarities

    Returns:
        Soft cosine similarity
    """
    norm_a = np.sqrt(np.dot(a, np.dot(similarity_matrix, a)))
    norm_b = np.sqrt(np.dot(b, np.dot(similarity_matrix, b)))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return np.dot(a, np.dot(similarity_matrix, b)) / (norm_a * norm_b)


class SentenceEmbedder:
    """Simple sentence embedder using bag-of-words."""

    def __init__(self, vocab: List[str]):
        self.vocab = {w: i for i, w in enumerate(vocab)}
        self.vocab_size = len(vocab)

    def embed(self, text: str) -> np.ndarray:
        """Embed a single text as bag-of-words."""
        words = text.lower().split()
        vector = np.zeros(self.vocab_size)
        for word in words:
            if word in self.vocab:
                vector[self.vocab[word]] += 1
        norm = np.linalg.norm(vector)
        if norm > 0:
            vector = vector / norm
        return vector

    def embed_batch(self, texts: List[str]) -> np.ndarray:
        """Embed multiple texts."""
        return np.array([self.embed(text) for text in texts])


class BM25:
    """Okapi BM25 ranking for information retrieval."""

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.doc_freqs = {}
        self.avgdl = 0
        self.doc_len = []
        self.corpus_size = 0

    def index(self, corpus: List[List[str]]):
        """Index corpus for BM25."""
        self.corpus_size = len(corpus)
        self.doc_len = [len(doc) for doc in corpus]
        self.avgdl = sum(self.doc_len) / self.corpus_size if self.corpus_size > 0 else 0
        for doc in corpus:
            seen = set()
            for term in doc:
                if term not in seen:
                    self.doc_freqs[term] = self.doc_freqs.get(term, 0) + 1
                    seen.add(term)

    def score(self, query: List[str], document: List[str]) -> float:
        """Score a document against a query."""
        doc_len = len(document)
        score = 0.0
        term_freqs = {}
        for term in document:
            term_freqs[term] = term_freqs.get(term, 0) + 1
        for term in query:
            if term not in term_freqs:
                continue
            tf = term_freqs[term]
            df = self.doc_freqs.get(term, 0)
            idf = np.log((self.corpus_size - df + 0.5) / (df + 0.5) + 1)
            score += idf * (tf * (self.k1 + 1)) / (tf + self.k1 * (1 - self.b + self.b * doc_len / self.avgdl))
        return score

    def search(self, query: List[str], top_k: int = 10) -> List[Tuple[int, float]]:
        """Search corpus and return top-k results."""
        scores = [(i, self.score(query, self.corpus[i])) for i in range(self.corpus_size)]
        return sorted(scores, key=lambda x: -x[1])[:top_k]


def semantic_search(
    query_embedding: np.ndarray,
    document_embeddings: np.ndarray,
    top_k: int = 10,
) -> List[Tuple[int, float]]:
    """
    Perform semantic search using cosine similarity.

    Args:
        query_embedding: Query vector
        document_embeddings: Matrix of document vectors (N, D)
        top_k: Number of results to return

    Returns:
        List of (doc_index, score) tuples

    Example:
        >>> query = np.array([1, 0, 0])
        >>> docs = np.array([[1, 0, 0], [0, 1, 0], [0.5, 0.5, 0]])
        >>> semantic_search(query, docs, top_k=2)
        [(0, 1.0), (2, 0.7071067811865475)]
    """
    scores = [cosine_similarity(query_embedding, doc) for doc in document_embeddings]
    indexed = list(enumerate(scores))
    return sorted(indexed, key=lambda x: -x[1])[:top_k]


class WordMoversDistance:
    """Word Mover's Distance using word embeddings."""

    def __init__(self, embeddings: dict):
        """
        Initialize with word embeddings dictionary.

        Args:
            embeddings: Dict mapping word to embedding vector
        """
        self.embeddings = embeddings
        self.vocab = list(embeddings.keys())
        self.word_vectors = np.array([embeddings[w] for w in self.vocab])
        self.word_to_idx = {w: i for i, w in enumerate(self.vocab)}

    def wmd(self, text1: str, text2: str) -> float:
        """
        Compute Word Mover's Distance between two texts.

        Uses only first two moments as approximation.
        """
        words1 = text1.lower().split()
        words2 = text2.lower().split()
        emb1 = [self.embeddings[w] for w in words1 if w in self.embeddings]
        emb2 = [self.embeddings[w] for w in words2 if w in self.embeddings]
        if not emb1 or not emb2:
            return float("inf")
        mean1 = np.mean(emb1, axis=0)
        mean2 = np.mean(emb2, axis=0)
        std1 = np.std(emb1, axis=0) if len(emb1) > 1 else np.zeros_like(mean1)
        std2 = np.std(emb2, axis=0) if len(emb2) > 1 else np.zeros_like(mean2)
        return euclidean_distance(mean1, mean2) + euclidean_distance(std1, std2)
