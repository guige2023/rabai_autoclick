"""
Feature extraction utilities for machine learning.

Provides common feature extraction methods including TF-IDF,
count vectors, and statistical features.
"""
from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Sequence, Tuple

import numpy as np


def bag_of_words(
    documents: Sequence[str],
    max_features: int = None,
    min_df: int = 1,
) -> Tuple[np.ndarray, List[str]]:
    """
    Bag of words feature extraction.

    Args:
        documents: List of text documents
        max_features: Maximum vocabulary size
        min_df: Minimum document frequency

    Returns:
        Tuple of (feature_matrix, vocabulary)

    Example:
        >>> bow, vocab = bag_of_words(["hello world", "hello"])
        >>> vocab
        ['hello', 'world']
    """
    word_docs = {}
    vocab = []
    for doc in documents:
        words = doc.lower().split()
        seen = set()
        for word in words:
            if word not in seen:
                seen.add(word)
                word_docs[word] = word_docs.get(word, 0) + 1
    for word, count in sorted(word_docs.items(), key=lambda x: -x[1]):
        if count >= min_df:
            vocab.append(word)
        if max_features and len(vocab) >= max_features:
            break
    word_to_idx = {w: i for i, w in enumerate(vocab)}
    matrix = np.zeros((len(documents), len(vocab)))
    for i, doc in enumerate(documents):
        for word in doc.lower().split():
            if word in word_to_idx:
                matrix[i, word_to_idx[word]] += 1
    return matrix, vocab


def tf_idf(
    documents: Sequence[str],
    max_features: int = None,
    min_df: int = 1,
    smooth_idf: bool = True,
) -> Tuple[np.ndarray, List[str]]:
    """
    TF-IDF feature extraction.

    Args:
        documents: List of text documents
        max_features: Maximum vocabulary size
        min_df: Minimum document frequency
        smooth_idf: Smooth IDF weights

    Returns:
        Tuple of (tfidf_matrix, vocabulary)

    Example:
        >>> tfidf, vocab = tf_idf(["hello world", "hello hello"])
    """
    n_docs = len(documents)
    tf_matrix, vocab = bag_of_words(documents, max_features=max_features, min_df=min_df)
    doc_freq = np.sum(tf_matrix > 0, axis=0)
    if smooth_idf:
        idf = np.log((n_docs + 1) / (doc_freq + 1)) + 1
    else:
        idf = np.log(n_docs / (doc_freq + 1e-10)) + 1
    tfidf_matrix = tf_matrix * idf
    norms = np.linalg.norm(tfidf_matrix, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    tfidf_matrix = tfidf_matrix / norms
    return tfidf_matrix, vocab


class CountVectorizer:
    """Count vectorizer for text feature extraction."""

    def __init__(
        self,
        max_features: int = None,
        min_df: int = 1,
        max_df: float = 1.0,
        binary: bool = False,
    ):
        self.max_features = max_features
        self.min_df = min_df
        self.max_df = max_df
        self.binary = binary
        self.vocabulary_ = None

    def fit(self, documents: Sequence[str]) -> "CountVectorizer":
        """Fit vocabulary to documents."""
        word_doc_count = {}
        for doc in documents:
            words = set(doc.lower().split())
            for word in words:
                word_doc_count[word] = word_doc_count.get(word, 0) + 1
        n_docs = len(documents)
        vocab = []
        for word, count in sorted(word_doc_count.items(), key=lambda x: -x[1]):
            if self.min_df <= count <= n_docs * self.max_df:
                vocab.append(word)
            if self.max_features and len(vocab) >= self.max_features:
                break
        self.vocabulary_ = {w: i for i, w in enumerate(vocab)}
        return self

    def transform(self, documents: Sequence[str]) -> np.ndarray:
        """Transform documents to feature matrix."""
        if self.vocabulary_ is None:
            raise ValueError("Vectorizer not fitted")
        matrix = np.zeros((len(documents), len(self.vocabulary_)))
        for i, doc in enumerate(documents):
            for word in doc.lower().split():
                if word in self.vocabulary_:
                    if self.binary:
                        matrix[i, self.vocabulary_[word]] = 1
                    else:
                        matrix[i, self.vocabulary_[word]] += 1
        return matrix

    def fit_transform(self, documents: Sequence[str]) -> np.ndarray:
        """Fit and transform in one step."""
        self.fit(documents)
        return self.transform(documents)


class TfidfVectorizer:
    """TF-IDF vectorizer for text feature extraction."""

    def __init__(
        self,
        max_features: int = None,
        min_df: int = 1,
        max_df: float = 1.0,
        norm: str = "l2",
        use_idf: bool = True,
        smooth_idf: bool = True,
    ):
        self.max_features = max_features
        self.min_df = min_df
        self.max_df = max_df
        self.norm = norm
        self.use_idf = use_idf
        self.smooth_idf = smooth_idf
        self.vocabulary_ = None
        self.idf_ = None

    def fit(self, documents: Sequence[str]) -> "TfidfVectorizer":
        """Fit TF-IDF to documents."""
        self.count_vectorizer = CountVectorizer(
            max_features=self.max_features,
            min_df=self.min_df,
            max_df=self.max_df,
        )
        count_matrix = self.count_vectorizer.fit_transform(documents)
        self.vocabulary_ = self.count_vectorizer.vocabulary_
        n_docs, n_terms = count_matrix.shape
        doc_freq = np.sum(count_matrix > 0, axis=0)
        if self.smooth_idf:
            self.idf_ = np.log((n_docs + 1) / (doc_freq + 1)) + 1
        else:
            self.idf_ = np.log(n_docs / (doc_freq + 1e-10)) + 1
        return self

    def transform(self, documents: Sequence[str]) -> np.ndarray:
        """Transform documents to TF-IDF matrix."""
        if self.vocabulary_ is None:
            raise ValueError("Vectorizer not fitted")
        count_matrix = self.count_vectorizer.transform(documents)
        tfidf_matrix = count_matrix * self.idf_
        if self.norm == "l2":
            norms = np.linalg.norm(tfidf_matrix, axis=1, keepdims=True)
            norms = np.where(norms == 0, 1, norms)
            tfidf_matrix = tfidf_matrix / norms
        return tfidf_matrix

    def fit_transform(self, documents: Sequence[str]) -> np.ndarray:
        """Fit and transform in one step."""
        self.fit(documents)
        return self.transform(documents)


def extract_statistical_features(data: np.ndarray) -> Dict[str, float]:
    """
    Extract statistical features from data.

    Args:
        data: Input array

    Returns:
        Dictionary of statistical features

    Example:
        >>> features = extract_statistical_features(np.array([1, 2, 3, 4, 5]))
        >>> 'mean' in features
        True
    """
    return {
        "mean": float(np.mean(data)),
        "median": float(np.median(data)),
        "std": float(np.std(data)),
        "var": float(np.var(data)),
        "min": float(np.min(data)),
        "max": float(np.max(data)),
        "range": float(np.max(data) - np.min(data)),
        "q25": float(np.percentile(data, 25)),
        "q75": float(np.percentile(data, 75)),
        "iqr": float(np.percentile(data, 75) - np.percentile(data, 25)),
        "skewness": float(_skewness(data)),
        "kurtosis": float(_kurtosis(data)),
    }


def _skewness(data: np.ndarray) -> float:
    """Calculate skewness."""
    mean, std = np.mean(data), np.std(data)
    if std == 0:
        return 0.0
    n = len(data)
    return np.sum(((data - mean) / std) ** 3) * n / ((n - 1) * (n - 2))


def _kurtosis(data: np.ndarray) -> float:
    """Calculate kurtosis."""
    mean, std = np.mean(data), np.std(data)
    if std == 0:
        return 0.0
    n = len(data)
    return np.sum(((data - mean) / std) ** 4) * n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) - 3 * (n - 1) ** 2 / ((n - 2) * (n - 3))


def extract_histogram_features(data: np.ndarray, n_bins: int = 10) -> np.ndarray:
    """
    Extract histogram-based features.

    Args:
        data: Input array
        n_bins: Number of histogram bins

    Returns:
        Array of bin counts normalized

    Example:
        >>> features = extract_histogram_features(np.array([1, 2, 3, 4, 5, 6]))
        >>> len(features)
        10
    """
    hist, _ = np.histogram(data, bins=n_bins, range=(data.min(), data.max()))
    return hist / hist.sum()


def polynomial_features(x: np.ndarray, degree: int = 2) -> np.ndarray:
    """
    Generate polynomial features.

    Args:
        x: Input array of shape (n_samples, n_features)
        degree: Polynomial degree

    Returns:
        Feature matrix with polynomial terms

    Example:
        >>> poly_features(np.array([[1], [2]]), degree=2)
        array([[1., 1., 1.],
               [1., 2., 4.]])
    """
    n_samples = x.shape[0]
    features = [np.ones(n_samples)]
    for d in range(1, degree + 1):
        features.append(x.flatten() ** d)
    return np.column_stack(features)


def interaction_features(x: np.ndarray) -> np.ndarray:
    """
    Generate interaction (cross-product) features.

    Args:
        x: Input array of shape (n_samples, n_features)

    Returns:
        Feature matrix with interaction terms

    Example:
        >>> interaction_features(np.array([[1, 2], [3, 4]]))
        array([[1, 2, 1, 2, 4],
               [3, 4, 3, 4, 16]])
    """
    n_samples, n_features = x.shape
    result = [x]
    for i in range(n_features):
        for j in range(i + 1, n_features):
            result.append((x[:, i] * x[:, j]).reshape(-1, 1))
    return np.hstack(result)


class FeatureSelector:
    """Select top K features based on variance or correlation."""

    def __init__(self, k: int = 10, method: str = "variance"):
        self.k = k
        self.method = method
        self.selected_indices_ = None

    def fit(self, X: np.ndarray, y: np.ndarray = None) -> "FeatureSelector":
        """Fit feature selector."""
        if self.method == "variance":
            variances = np.var(X, axis=0)
            self.selected_indices_ = np.argsort(variances)[-self.k :]
        elif self.method == "correlation" and y is not None:
            correlations = [abs(np.corrcoef(X[:, i], y)[0, 1]) for i in range(X.shape[1])]
            correlations = np.nan_to_num(correlations, 0)
            self.selected_indices_ = np.argsort(correlations)[-self.k :]
        else:
            self.selected_indices_ = np.arange(X.shape[1])[: self.k]
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        """Transform data with selected features."""
        return X[:, self.selected_indices_]

    def fit_transform(self, X: np.ndarray, y: np.ndarray = None) -> np.ndarray:
        """Fit and transform."""
        self.fit(X, y)
        return self.transform(X)
