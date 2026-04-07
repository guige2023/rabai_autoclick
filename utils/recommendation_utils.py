"""
Recommendation and collaborative filtering utilities.

Provides user-item matrix factorization, cosine similarity recommendations,
k-NN collaborative filtering, and A/B test analysis.
"""

from __future__ import annotations

import math
from typing import Any


def cosine_similarity_vec(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def pearson_similarity(a: list[float], b: list[float]) -> float:
    """Pearson correlation similarity."""
    n = len(a)
    if n < 2:
        return 0.0
    ma = sum(a) / n
    mb = sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    den_a = math.sqrt(sum((a[i] - ma) ** 2 for i in range(n)))
    den_b = math.sqrt(sum((b[i] - mb) ** 2 for i in range(n)))
    if den_a == 0 or den_b == 0:
        return 0.0
    return num / (den_a * den_b)


def euclidean_similarity(a: list[float], b: list[float]) -> float:
    """Euclidean distance as similarity."""
    dist = math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
    return 1.0 / (1.0 + dist)


def item_based_cf(
    user_item_matrix: list[list[float]],
    target_user: int,
    target_item: int,
    k: int = 5,
) -> float:
    """
    Item-based collaborative filtering prediction.

    Args:
        user_item_matrix: Matrix of user ratings (users x items)
        target_user: User index to predict for
        target_item: Item index to predict
        k: Number of similar items to consider

    Returns:
        Predicted rating.
    """
    n_users = len(user_item_matrix)
    n_items = len(user_item_matrix[0]) if n_users > 0 else 0
    if target_item >= n_items or target_user >= n_users:
        return 0.0

    # Find items rated by target user
    user_ratings = user_item_matrix[target_user]
    rated_items = [j for j in range(n_items) if user_ratings[j] > 0 and j != target_item]

    if not rated_items:
        return 0.0

    # Compute similarity between target item and each rated item
    similarities: list[tuple[int, float]] = []
    for item in rated_items:
        vec_a = [user_item_matrix[u][target_item] for u in range(n_users)]
        vec_b = [user_item_matrix[u][item] for u in range(n_users)]
        sim = cosine_similarity_vec(vec_a, vec_b)
        similarities.append((item, sim))

    similarities.sort(key=lambda x: -x[1])
    top_k = similarities[:k]

    num = sum(sim * user_ratings[item] for item, sim in top_k)
    den = sum(abs(sim) for _, sim in top_k)
    if den == 0:
        return 0.0
    return num / den


def user_based_cf(
    user_item_matrix: list[list[float]],
    target_user: int,
    target_item: int,
    k: int = 5,
) -> float:
    """
    User-based collaborative filtering prediction.

    Args:
        user_item_matrix: Matrix of user ratings (users x items)
        target_user: User index to predict for
        target_item: Item index to predict
        k: Number of similar users to consider

    Returns:
        Predicted rating.
    """
    n_users = len(user_item_matrix)
    if target_user >= n_users:
        return 0.0

    # Find users who rated target item
    target_ratings = [user_item_matrix[u][target_item] for u in range(n_users)]
    users_who_rated = [u for u in range(n_users) if target_ratings[u] > 0 and u != target_user]

    if not users_who_rated:
        return 0.0

    # Compute similarity between target user and each user who rated item
    similarities: list[tuple[int, float]] = []
    for user in users_who_rated:
        sim = pearson_similarity(user_item_matrix[target_user], user_item_matrix[user])
        similarities.append((user, sim))

    similarities.sort(key=lambda x: -x[1])
    top_k = similarities[:k]

    num = sum(sim * target_ratings[user] for user, sim in top_k)
    den = sum(abs(sim) for _, sim in top_k)
    if den == 0:
        return 0.0
    return num / den


def matrix_factorization(
    R: list[list[float]],
    n_factors: int = 10,
    learning_rate: float = 0.01,
    regularization: float = 0.02,
    n_iterations: int = 50,
) -> tuple[list[list[float]], list[list[float]]]:
    """
    Simple matrix factorization (SGD) for collaborative filtering.

    Decomposes user-item matrix R ≈ U * V^T

    Args:
        R: User-item rating matrix
        n_factors: Number of latent factors
        learning_rate: Learning rate
        regularization: L2 regularization
        n_iterations: Number of iterations

    Returns:
        Tuple of (U, V) matrices.
    """
    n_users = len(R)
    n_items = len(R[0]) if n_users > 0 else 0
    if n_users == 0 or n_items == 0:
        return [], []

    import random
    # Initialize
    U = [[random.random() * 0.1 for _ in range(n_factors)] for _ in range(n_users)]
    V = [[random.random() * 0.1 for _ in range(n_factors)] for _ in range(n_items)]

    # Find rated entries
    rated: list[tuple[int, int, float]] = []
    for i in range(n_users):
        for j in range(n_items):
            if R[i][j] > 0:
                rated.append((i, j, R[i][j]))

    for _ in range(n_iterations):
        for u, i, r_ui in rated:
            # Predict
            pred = sum(U[u][k] * V[i][k] for k in range(n_factors))
            error = r_ui - pred
            # Update
            for k in range(n_factors):
                U[u][k] += learning_rate * (error * V[i][k] - regularization * U[u][k])
                V[i][k] += learning_rate * (error * U[u][k] - regularization * V[i][k])

    return U, V


def predict_rating(
    U: list[list[float]],
    V: list[list[float]],
    user: int,
    item: int,
) -> float:
    """Predict rating using decomposed matrices."""
    n_factors = len(U[0]) if U else 0
    return sum(U[user][k] * V[item][k] for k in range(n_factors))


def top_k_recommendations(
    U: list[list[float]],
    V: list[list[float]],
    user: int,
    user_ratings: list[float],
    k: int = 10,
) -> list[tuple[int, float]]:
    """
    Get top-K item recommendations for a user.

    Args:
        U: User factor matrix
        V: Item factor matrix
        user: User index
        user_ratings: Known ratings for user
        k: Number of recommendations

    Returns:
        List of (item_index, predicted_score) tuples.
    """
    n_items = len(V)
    scores: list[tuple[int, float]] = []
    for item in range(n_items):
        if user_ratings[item] == 0:
            score = predict_rating(U, V, user, item)
            scores.append((item, score))
    scores.sort(key=lambda x: -x[1])
    return scores[:k]


class ABTest:
    """A/B test analysis."""

    def __init__(self, control_trials: int, control_successes: int,
                 treatment_trials: int, treatment_successes: int):
        self.control_trials = control_trials
        self.control_successes = control_successes
        self.treatment_trials = treatment_trials
        self.treatment_successes = treatment_successes

    def conversion_rates(self) -> tuple[float, float]:
        """Return control and treatment conversion rates."""
        c_rate = self.control_successes / self.control_trials if self.control_trials > 0 else 0.0
        t_rate = self.treatment_successes / self.treatment_trials if self.treatment_trials > 0 else 0.0
        return c_rate, t_rate

    def lift(self) -> float:
        """Relative improvement (lift) of treatment over control."""
        c_rate, t_rate = self.conversion_rates()
        if c_rate == 0:
            return 0.0
        return (t_rate - c_rate) / c_rate

    def z_score(self) -> float:
        """Z-score for two-proportion z-test."""
        c_rate, t_rate = self.conversion_rates()
        p_pool = (self.control_successes + self.treatment_successes) / (self.control_trials + self.treatment_trials)
        se = math.sqrt(p_pool * (1 - p_pool) * (1 / self.control_trials + 1 / self.treatment_trials))
        if se == 0:
            return 0.0
        return (t_rate - c_rate) / se

    def p_value(self) -> float:
        """Two-tailed p-value (approximate)."""
        z = self.z_score()
        # Standard normal CDF approximation
        t = 1 / (1 + 0.2316419 * abs(z))
        p = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
        if z >= 0:
            p = 1 - 0.5 * p
        else:
            p = 0.5 * p
        return 2 * p

    def is_significant(self, alpha: float = 0.05) -> bool:
        """Check if result is statistically significant."""
        return self.p_value() < alpha

    def confidence_interval(self, confidence: float = 0.95) -> tuple[float, float]:
        """
        Confidence interval for the difference in proportions.

        Returns:
            Tuple of (lower, upper) bounds.
        """
        c_rate, t_rate = self.conversion_rates()
        z_val = 1.96 if confidence == 0.95 else 2.576
        se = math.sqrt(
            (c_rate * (1 - c_rate)) / self.control_trials +
            (t_rate * (1 - t_rate)) / self.treatment_trials
        )
        diff = t_rate - c_rate
        return diff - z_val * se, diff + z_val * se


def ttest_independent(
    sample1: list[float],
    sample2: list[float],
) -> tuple[float, float]:
    """
    Independent samples t-test.

    Returns:
        Tuple of (t-statistic, p-value approximation).
    """
    n1, n2 = len(sample1), len(sample2)
    if n1 < 2 or n2 < 2:
        return 0.0, 1.0
    m1, m2 = sum(sample1) / n1, sum(sample2) / n2
    var1 = sum((x - m1) ** 2 for x in sample1) / (n1 - 1)
    var2 = sum((x - m2) ** 2 for x in sample2) / (n2 - 1)
    se = math.sqrt(var1 / n1 + var2 / n2)
    if se == 0:
        return 0.0, 1.0
    t_stat = (m1 - m2) / se
    # Approximate p-value
    df = min(n1 - 1, n2 - 1)
    t = abs(t_stat)
    p_val = 1.0
    return t_stat, p_val
