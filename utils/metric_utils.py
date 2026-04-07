"""
Metric and evaluation utilities.

Provides classification metrics, regression metrics, ranking metrics,
confusion matrix analysis, and threshold optimization.
"""

from __future__ import annotations

import math
from typing import Any


def accuracy(y_true: list, y_pred: list) -> float:
    """Accuracy score."""
    if not y_true:
        return 0.0
    return sum(1 for t, p in zip(y_true, y_pred) if t == p) / len(y_true)


def precision(y_true: list, y_pred: list, pos_label: Any = 1) -> float:
    """Precision: TP / (TP + FP)"""
    tp = sum(1 for t, p in zip(y_true, y_pred) if t == pos_label and p == pos_label)
    fp = sum(1 for t, p in zip(y_true, y_pred) if t != pos_label and p == pos_label)
    if tp + fp == 0:
        return 0.0
    return tp / (tp + fp)


def recall(y_true: list, y_pred: list, pos_label: Any = 1) -> float:
    """Recall: TP / (TP + FN)"""
    tp = sum(1 for t, p in zip(y_true, y_pred) if t == pos_label and p == pos_label)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t == pos_label and p != pos_label)
    if tp + fn == 0:
        return 0.0
    return tp / (tp + fn)


def f1_score(y_true: list, y_pred: list, pos_label: Any = 1) -> float:
    """F1 score: 2 * precision * recall / (precision + recall)"""
    p = precision(y_true, y_pred, pos_label)
    r = recall(y_true, y_pred, pos_label)
    if p + r == 0:
        return 0.0
    return 2 * p * r / (p + r)


def f_beta_score(y_true: list, y_pred: list, beta: float = 1.0, pos_label: Any = 1) -> float:
    """F-beta score generalization of F1."""
    p = precision(y_true, y_pred, pos_label)
    r = recall(y_true, y_pred, pos_label)
    if p + r == 0:
        return 0.0
    return (1 + beta ** 2) * p * r / (beta ** 2 * p + r)


def confusion_matrix(y_true: list, y_pred: list) -> dict[tuple[Any, Any], int]:
    """Compute confusion matrix entries."""
    matrix: dict[tuple[Any, Any], int] = {}
    for t, p in zip(y_true, y_pred):
        matrix[(t, p)] = matrix.get((t, p), 0) + 1
    return matrix


def specificity(y_true: list, y_pred: list, neg_label: Any = 0) -> float:
    """Specificity: TN / (TN + FP)"""
    tn = sum(1 for t, p in zip(y_true, y_pred) if t == neg_label and p == neg_label)
    fp = sum(1 for t, p in zip(y_true, y_pred) if t != neg_label and p == neg_label)
    if tn + fp == 0:
        return 0.0
    return tn / (tn + fp)


def matthews_corrcoef(y_true: list, y_pred: list) -> float:
    """Matthews correlation coefficient."""
    if not y_true:
        return 0.0
    tp = sum(1 for t, p in zip(y_true, y_pred) if t == 1 and p == 1)
    tn = sum(1 for t, p in zip(y_true, y_pred) if t == 0 and p == 0)
    fp = sum(1 for t, p in zip(y_true, y_pred) if t == 0 and p == 1)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t == 1 and p == 0)
    denom = math.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    if denom == 0:
        return 0.0
    return (tp * tn - fp * fn) / denom


def roc_auc_score(y_true: list, y_scores: list) -> float:
    """
    Approximate AUC-ROC using Mann-Whitney U statistic.

    Args:
        y_true: True binary labels
        y_scores: Predicted scores/probabilities

    Returns:
        AUC value in [0, 1].
    """
    pos = [y_scores[i] for i in range(len(y_true)) if y_true[i] == 1]
    neg = [y_scores[i] for i in range(len(y_true)) if y_true[i] == 0]
    if not pos or not neg:
        return 0.0
    # Count pairs where pos > neg
    n_pos, n_neg = len(pos), len(neg)
    concordant = sum(1 for p in pos for n in neg if p > n)
    ties = sum(1 for p in pos for n in neg if p == n)
    return (concordant + 0.5 * ties) / (n_pos * n_neg)


def pr_auc_score(y_true: list, y_scores: list) -> float:
    """
    Approximate AUC-PR (area under precision-recall curve).

    Uses piecewise constant approximation.
    """
    if not y_true or sum(y_true) == 0 or sum(y_true) == len(y_true):
        return 0.0
    # Sort by score descending
    pairs = sorted(zip(y_true, y_scores), key=lambda x: -x[1])
    tp = 0
    fp = 0
    prev_score = None
    precisions = []
    recalls = []
    total_pos = sum(y_true)
    total_neg = len(y_true) - total_pos
    for label, score in pairs:
        if score != prev_score and (tp + fp) > 0:
            prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            rec = tp / total_pos if total_pos > 0 else 0.0
            precisions.append(prec)
            recalls.append(rec)
            prev_score = score
        if label == 1:
            tp += 1
        else:
            fp += 1
    if precisions:
        precisions.append(tp / (tp + fp) if (tp + fp) > 0 else 0.0)
        recalls.append(tp / total_pos if total_pos > 0 else 0.0)
    # Trapezoidal integration
    auc = 0.0
    for i in range(len(precisions) - 1):
        auc += (recalls[i + 1] - recalls[i]) * (precisions[i] + precisions[i + 1]) / 2
    return auc


def mean_absolute_error(y_true: list[float], y_pred: list[float]) -> float:
    """MAE."""
    return sum(abs(t - p) for t, p in zip(y_true, y_pred)) / len(y_true)


def mean_squared_error(y_true: list[float], y_pred: list[float]) -> float:
    """MSE."""
    return sum((t - p) ** 2 for t, p in zip(y_true, y_pred)) / len(y_true)


def root_mean_squared_error(y_true: list[float], y_pred: list[float]) -> float:
    """RMSE."""
    return math.sqrt(mean_squared_error(y_true, y_pred))


def mean_absolute_percentage_error(y_true: list[float], y_pred: list[float]) -> float:
    """MAPE."""
    n = len(y_true)
    if n == 0:
        return 0.0
    pct_errors = [abs((t - p) / t) for t, p in zip(y_true, y_pred) if abs(t) > 1e-12]
    return sum(pct_errors) / len(pct_errors) if pct_errors else 0.0


def r2_score(y_true: list[float], y_pred: list[float]) -> float:
    """R-squared (coefficient of determination)."""
    n = len(y_true)
    if n < 2:
        return 0.0
    mean_true = sum(y_true) / n
    ss_tot = sum((t - mean_true) ** 2 for t in y_true)
    ss_res = sum((t - p) ** 2 for t, p in zip(y_true, y_pred))
    if ss_tot < 1e-12:
        return 0.0
    return 1.0 - ss_res / ss_tot


def explained_variance(y_true: list[float], y_pred: list[float]) -> float:
    """Explained variance score."""
    n = len(y_true)
    if n < 2:
        return 0.0
    mean_true = sum(y_true) / n
    var_true = sum((t - mean_true) ** 2 for t in y_true) / n
    var_res = sum((t - p) ** 2 for t, p in zip(y_true, y_pred)) / n
    if var_true < 1e-12:
        return 0.0
    return 1.0 - var_res / var_true


def ndcg_score(y_true: list[list[float]], y_pred: list[list[float]], k: int | None = None) -> float:
    """
    Normalized Discounted Cumulative Gain.

    Args:
        y_true: True relevance scores per query
        y_pred: Predicted scores per query
        k: Cutoff position
    """
    def dcg(scores: list[float], k: int | None = None) -> float:
        if k:
            scores = scores[:k]
        return sum((2 ** rel - 1) / math.log2(i + 2) for i, rel in enumerate(scores))

    def ndcg_single(true: list[float], pred: list[float], k: int | None = None) -> float:
        sorted_true = sorted(true, reverse=True)
        ideal_dcg = dcg(sorted_true, k)
        if ideal_dcg == 0:
            return 0.0
        # Sort by predicted
        paired = sorted(zip(pred, true), reverse=True)
        actual_dcg = dcg([t for _, t in paired], k)
        return actual_dcg / ideal_dcg

    if k is None:
        k = max(len(y) for y in y_true)
    return sum(ndcg_single(t, p, k) for t, p in zip(y_true, y_pred)) / len(y_true)


def map_score(y_true: list[list[Any]], y_pred: list[list[Any]]) -> float:
    """
    Mean Average Precision.

    Args:
        y_true: True relevant items per query
        y_pred: Predicted items per query
    """
    def ap(true: list, pred: list) -> float:
        hits = 0
        sum_prec = 0.0
        for i, p in enumerate(pred):
            if p in true:
                hits += 1
                sum_prec += hits / (i + 1)
        return sum_prec / len(true) if true else 0.0

    return sum(ap(t, p) for t, p in zip(y_true, y_pred)) / len(y_true)


def optimal_f1_threshold(y_true: list, y_scores: list) -> float:
    """
    Find threshold that maximizes F1 score.

    Args:
        y_true: True binary labels
        y_scores: Predicted probabilities/scores

    Returns:
        Optimal threshold.
    """
    thresholds = sorted(set(y_scores))
    best_f1 = 0.0
    best_thresh = thresholds[0] if thresholds else 0.0
    for thresh in thresholds:
        y_pred = [1 if s >= thresh else 0 for s in y_scores]
        f1 = f1_score(y_true, y_pred)
        if f1 > best_f1:
            best_f1 = f1
            best_thresh = thresh
    return best_thresh


def bootstrap_confidence_interval(
    metric_fn: Callable[[list, list], float],
    y_true: list,
    y_pred: list,
    n_bootstrap: int = 1000,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """
    Bootstrap confidence interval for a metric.

    Args:
        metric_fn: Metric function (e.g., accuracy)
        y_true: True values
        y_pred: Predicted values
        n_bootstrap: Number of bootstrap samples
        confidence: Confidence level

    Returns:
        Tuple of (lower, upper) bounds.
    """
    import random
    n = len(y_true)
    scores = []
    for _ in range(n_bootstrap):
        indices = [random.randint(0, n - 1) for _ in range(n)]
        t_sample = [y_true[i] for i in indices]
        p_sample = [y_pred[i] for i in indices]
        scores.append(metric_fn(t_sample, p_sample))
    scores.sort()
    alpha = (1 - confidence) / 2
    lower_idx = int(alpha * n_bootstrap)
    upper_idx = int((1 - alpha) * n_bootstrap)
    return scores[lower_idx], scores[upper_idx]


from typing import Callable
