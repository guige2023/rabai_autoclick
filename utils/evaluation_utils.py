"""
Model evaluation metrics utilities.

Provides classification, regression, and ranking metrics
with both numpy and sklearn-style interfaces.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np


def accuracy_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Classification accuracy.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels

    Returns:
        Accuracy in range [0, 1]

    Example:
        >>> accuracy_score(np.array([1, 2, 3]), np.array([1, 2, 4]))
        0.6666666666666666
    """
    return np.mean(y_true == y_pred)


def precision_score(
    y_true: np.ndarray, y_pred: np.ndarray, average: str = "binary"
) -> np.ndarray:
    """
    Precision score.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels
        average: Averaging method (binary, macro, micro, weighted)

    Returns:
        Precision value(s)

    Example:
        >>> precision_score(np.array([1, 0, 1]), np.array([1, 1, 0]))
        0.5
    """
    if average == "binary":
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fp = np.sum((y_true == 0) & (y_pred == 1))
        return tp / (tp + fp) if (tp + fp) > 0 else 0.0
    classes = np.unique(np.concatenate([y_true, y_pred]))
    precisions = []
    for cls in classes:
        tp = np.sum((y_true == cls) & (y_pred == cls))
        fp = np.sum((y_true != cls) & (y_pred == cls))
        precisions.append(tp / (tp + fp) if (tp + fp) > 0 else 0.0)
    precisions = np.array(precisions)
    if average == "macro":
        return np.mean(precisions)
    elif average == "micro":
        return np.sum(precisions) / len(classes)
    return precisions


def recall_score(
    y_true: np.ndarray, y_pred: np.ndarray, average: str = "binary"
) -> np.ndarray:
    """
    Recall score.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels
        average: Averaging method

    Returns:
        Recall value(s)
    """
    if average == "binary":
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fn = np.sum((y_true == 1) & (y_pred == 0))
        return tp / (tp + fn) if (tp + fn) > 0 else 0.0
    classes = np.unique(np.concatenate([y_true, y_pred]))
    recalls = []
    for cls in classes:
        tp = np.sum((y_true == cls) & (y_pred == cls))
        fn = np.sum((y_true == cls) & (y_pred != cls))
        recalls.append(tp / (tp + fn) if (tp + fn) > 0 else 0.0)
    recalls = np.array(recalls)
    if average == "macro":
        return np.mean(recalls)
    elif average == "micro":
        return np.sum(recalls) / len(classes)
    return recalls


def f1_score(
    y_true: np.ndarray, y_pred: np.ndarray, average: str = "binary"
) -> float:
    """
    F1 score (harmonic mean of precision and recall).

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels
        average: Averaging method

    Returns:
        F1 score
    """
    prec = precision_score(y_true, y_pred, average)
    rec = recall_score(y_true, y_pred, average)
    if isinstance(prec, np.ndarray):
        f1 = 2 * prec * rec / (prec + rec + 1e-10)
        if average == "macro":
            return np.mean(f1)
        return f1
    return 2 * prec * rec / (prec + rec + 1e-10)


def confusion_matrix(y_true: np.ndarray, y_pred: np.ndarray) -> np.ndarray:
    """
    Compute confusion matrix.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels

    Returns:
        Confusion matrix

    Example:
        >>> confusion_matrix(np.array([1, 0, 1, 0]), np.array([1, 0, 0, 1]))
        array([[1, 1],
               [1, 1]])
    """
    classes = np.unique(np.concatenate([y_true, y_pred]))
    n_classes = len(classes)
    cm = np.zeros((n_classes, n_classes), dtype=int)
    for i, true_cls in enumerate(classes):
        for j, pred_cls in enumerate(classes):
            cm[i, j] = np.sum((y_true == true_cls) & (y_pred == pred_cls))
    return cm


def roc_auc_score(y_true: np.ndarray, y_scores: np.ndarray) -> float:
    """
    ROC AUC score.

    Args:
        y_true: Ground truth binary labels
        y_scores: Predicted probabilities/scores

    Returns:
        ROC AUC score
    """
    indices = np.argsort(y_scores)[::-1]
    y_true_sorted = y_true[indices]
    n_pos = np.sum(y_true == 1)
    n_neg = np.sum(y_true == 0)
    if n_pos == 0 or n_neg == 0:
        return 0.5
    auc = 0.0
    tp = 0
    fp_prev = 0
    for i in range(len(y_scores)):
        if i > 0 and y_scores[indices[i]] != y_scores[indices[i - 1]]:
            auc += (fp_prev - fp) * (2 * tp + (fp - fp_prev)) / (2 * n_pos)
            fp_prev = fp
        if y_true_sorted[i] == 1:
            tp += 1
    auc += (fp_prev - fp) * (2 * tp + (fp - fp_prev)) / (2 * n_pos)
    return auc / (2 * n_pos * n_neg)


def mean_absolute_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Absolute Error."""
    return np.mean(np.abs(y_true - y_pred))


def mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Mean Squared Error."""
    return np.mean((y_true - y_pred) ** 2)


def root_mean_squared_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error."""
    return np.sqrt(mean_squared_error(y_true, y_pred))


def r2_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    R-squared (coefficient of determination).

    Args:
        y_true: Ground truth values
        y_pred: Predicted values

    Returns:
        R² score

    Example:
        >>> r2_score(np.array([1, 2, 3, 4]), np.array([1.1, 2.1, 2.9, 4.2]))
        0.985...
    """
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    return 1 - ss_res / ss_tot if ss_tot > 0 else 0.0


def explained_variance(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Explained variance score."""
    var_res = np.var(y_true - y_pred)
    var_tot = np.var(y_true)
    return 1 - var_res / var_tot if var_tot > 0 else 0.0


def mean_absolute_percentage_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """MAPE (Mean Absolute Percentage Error)."""
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def top_k_accuracy(y_true: np.ndarray, y_scores: np.ndarray, k: int = 5) -> float:
    """
    Top-k accuracy.

    Args:
        y_true: Ground truth labels
        y_scores: Predicted scores for each class
        k: Number of top predictions to consider

    Returns:
        Top-k accuracy
    """
    top_k_preds = np.argsort(y_scores, axis=-1)[:, -k:]
    correct = 0
    for i, true_label in enumerate(y_true):
        if true_label in top_k_preds[i]:
            correct += 1
    return correct / len(y_true)


def ndcg_score(y_true: np.ndarray, y_pred: np.ndarray, k: int = None) -> float:
    """
    Normalized Discounted Cumulative Gain.

    Args:
        y_true: Ground truth relevance scores
        y_pred: Predicted scores
        k: Truncation parameter

    Returns:
        NDCG score
    """
    if k is None:
        k = len(y_true)
    order = np.argsort(y_pred)[::-1][:k]
    y_true_sorted = y_true[order]
    gains = 2 ** y_true_sorted - 1
    discounts = np.log2(np.arange(len(y_true_sorted)) + 2)
    dcg = np.sum(gains / discounts)
    ideal_order = np.argsort(y_true)[::-1][:k]
    ideal_sorted = y_true[ideal_order]
    ideal_gains = 2 ** ideal_sorted - 1
    idcg = np.sum(ideal_gains / discounts)
    return dcg / idcg if idcg > 0 else 0.0


def classification_report(
    y_true: np.ndarray, y_pred: np.ndarray, target_names: List[str] = None
) -> Dict:
    """
    Generate classification report.

    Args:
        y_true: Ground truth labels
        y_pred: Predicted labels
        target_names: Optional class names

    Returns:
        Dictionary with precision, recall, f1 for each class
    """
    classes = np.unique(y_true)
    if target_names is None:
        target_names = [f"class_{c}" for c in classes]
    report = {}
    for cls, name in zip(classes, target_names):
        mask = y_true == cls
        tp = np.sum((y_true == cls) & (y_pred == cls))
        fp = np.sum((y_true != cls) & (y_pred == cls))
        fn = np.sum((y_true == cls) & (y_pred != cls))
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall + 1e-10)
        support = np.sum(mask)
        report[name] = {
            "precision": precision,
            "recall": recall,
            "f1-score": f1,
            "support": int(support),
        }
    report["accuracy"] = accuracy_score(y_true, y_pred)
    return report


def precision_recall_curve(
    y_true: np.ndarray, y_scores: np.ndarray, num_thresholds: int = 100
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute precision-recall curve.

    Args:
        y_true: Ground truth binary labels
        y_scores: Predicted probabilities
        num_thresholds: Number of thresholds to evaluate

    Returns:
        Tuple of (precision, recall, thresholds)
    """
    thresholds = np.linspace(0, 1, num_thresholds)
    precisions = []
    recalls = []
    for thresh in thresholds:
        y_pred = (y_scores >= thresh).astype(int)
        precisions.append(precision_score(y_true, y_pred))
        recalls.append(recall_score(y_true, y_pred))
    return np.array(precisions), np.array(recalls), thresholds


def roc_curve(
    y_true: np.ndarray, y_scores: np.ndarray, num_thresholds: int = 100
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute ROC curve.

    Args:
        y_true: Ground truth binary labels
        y_scores: Predicted probabilities
        num_thresholds: Number of thresholds

    Returns:
        Tuple of (fpr, tpr, thresholds)
    """
    thresholds = np.linspace(0, 1, num_thresholds)
    fprs = []
    tprs = []
    for thresh in thresholds:
        y_pred = (y_scores >= thresh).astype(int)
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fp = np.sum((y_true == 0) & (y_pred == 1))
        tn = np.sum((y_true == 0) & (y_pred == 0))
        fn = np.sum((y_true == 1) & (y_pred == 0))
        fprs.append(fp / (fp + tn) if (fp + tn) > 0 else 0.0)
        tprs.append(tp / (tp + fn) if (tp + fn) > 0 else 0.0)
    return np.array(fprs), np.array(tprs), thresholds
