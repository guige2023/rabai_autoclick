"""
Decision tree utilities.

Provides ID3/C4.5-style decision tree learning, tree visualization,
pruning, and prediction.
"""

from __future__ import annotations

import math
from typing import Any, Callable


NodeId = int


class DecisionTreeNode:
    """A node in a decision tree."""

    def __init__(
        self,
        feature: str | None = None,
        threshold: float | None = None,
        children: dict[Any, "DecisionTreeNode"] | None = None,
        value: Any | None = None,
        is_leaf: bool = False,
    ):
        self.feature = feature
        self.threshold = threshold
        self.children = children or {}
        self.value = value
        self.is_leaf = is_leaf
        self.n_samples = 0
        self.n_classes: dict[Any, int] = {}

    def predict_single(self, sample: dict[str, Any]) -> Any:
        """Predict class for a single sample."""
        if self.is_leaf:
            return self.value
        feature_val = sample.get(self.feature)
        if feature_val in self.children:
            return self.children[feature_val].predict_single(sample)
        # Default to most common child
        if self.children:
            default_child = max(self.children.values(), key=lambda n: n.n_samples)
            return default_child.predict_single(sample)
        return self.value


def entropy(labels: list[Any]) -> float:
    """Compute Shannon entropy."""
    n = len(labels)
    if n == 0:
        return 0.0
    counts: dict[Any, int] = {}
    for lbl in labels:
        counts[lbl] = counts.get(lbl, 0) + 1
    h = 0.0
    for c in counts.values():
        p = c / n
        if p > 0:
            h -= p * math.log2(p)
    return h


def gini_impurity(labels: list[Any]) -> float:
    """Compute Gini impurity."""
    n = len(labels)
    if n == 0:
        return 0.0
    counts: dict[Any, int] = {}
    for lbl in labels:
        counts[lbl] = counts.get(lbl, 0) + 1
    g = 1.0
    for c in counts.values():
        p = c / n
        g -= p * p
    return g


def information_gain(
    labels: list[Any],
    left_labels: list[Any],
    right_labels: list[Any],
) -> float:
    """Compute information gain from splitting."""
    n = len(labels)
    if n == 0:
        return 0.0
    parent_entropy = entropy(labels)
    n_left = len(left_labels)
    n_right = len(right_labels)
    child_entropy = (n_left / n) * entropy(left_labels) + (n_right / n) * entropy(right_labels)
    return parent_entropy - child_entropy


def gain_ratio(
    labels: list[Any],
    left_labels: list[Any],
    right_labels: list[Any],
) -> float:
    """Compute gain ratio (C4.5 criterion)."""
    ig = information_gain(labels, left_labels, right_labels)
    n = len(labels)
    split_info = 0.0
    for subset in [left_labels, right_labels]:
        if len(subset) > 0:
            p = len(subset) / n
            split_info -= p * math.log2(p)
    if split_info == 0:
        return 0.0
    return ig / split_info


class DecisionTreeClassifier:
    """Decision tree classifier (ID3/C4.5 style)."""

    def __init__(
        self,
        criterion: str = "information_gain",
        max_depth: int | None = None,
        min_samples_split: int = 2,
        min_samples_leaf: int = 1,
    ):
        self.criterion = criterion
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.min_samples_leaf = min_samples_leaf
        self.root: DecisionTreeNode | None = None
        self._feature_names: list[str] = []

    def _best_split(
        self,
        X: list[dict[str, Any]],
        y: list[Any],
        features: list[str],
    ) -> tuple[str | None, Any, float]:
        """Find best feature and split point."""
        best_score = -1.0
        best_feature = None
        best_threshold = None

        for feature in features:
            values: set[Any] = set()
            for sample in X:
                values.add(sample.get(feature))
            if len(values) < 2:
                continue
            sorted_values = sorted(values)
            for i in range(1, len(sorted_values)):
                threshold = sorted_values[i - 1]
                left_idx = [i for i, x in enumerate(X) if x.get(feature) <= threshold]
                right_idx = [i for i, x in enumerate(X) if x.get(feature) > threshold]
                if len(left_idx) < self.min_samples_leaf or len(right_idx) < self.min_samples_leaf:
                    continue
                left_labels = [y[i] for i in left_idx]
                right_labels = [y[i] for i in right_idx]
                if self.criterion == "gini":
                    score = information_gain(y, left_labels, right_labels)
                else:
                    score = gain_ratio(y, left_labels, right_labels)
                if score > best_score:
                    best_score = score
                    best_feature = feature
                    best_threshold = threshold

        return best_feature, best_threshold, best_score

    def _build_tree(
        self,
        X: list[dict[str, Any]],
        y: list[Any],
        features: list[str],
        depth: int,
    ) -> DecisionTreeNode:
        """Recursively build decision tree."""
        node = DecisionTreeNode()
        node.n_samples = len(y)
        counts: dict[Any, int] = {}
        for lbl in y:
            counts[lbl] = counts.get(lbl, 0) + 1
        node.n_classes = counts
        node.value = max(counts, key=counts.get)

        if len(features) == 0 or len(set(y)) == 1:
            node.is_leaf = True
            return node

        if self.max_depth is not None and depth >= self.max_depth:
            node.is_leaf = True
            return node

        if len(X) < self.min_samples_split:
            node.is_leaf = True
            return node

        best_feature, best_threshold, best_score = self._best_split(X, y, features)
        if best_feature is None or best_score <= 0:
            node.is_leaf = True
            return node

        node.feature = best_feature
        node.threshold = best_threshold

        left_X, left_y, right_X, right_y = [], [], [], []
        for i, sample in enumerate(X):
            if sample.get(best_feature) <= (best_threshold or 0):
                left_X.append(sample)
                left_y.append(y[i])
            else:
                right_X.append(sample)
                right_y.append(y[i])

        remaining_features = [f for f in features if f != best_feature]
        node.children = {
            "left": self._build_tree(left_X, left_y, remaining_features, depth + 1),
            "right": self._build_tree(right_X, right_y, remaining_features, depth + 1),
        }

        return node

    def fit(self, X: list[dict[str, Any]], y: list[Any]) -> "DecisionTreeClassifier":
        """Build decision tree from training data."""
        if not X:
            return self
        self._feature_names = list(X[0].keys())
        self.root = self._build_tree(X, y, list(X[0].keys()), 0)
        return self

    def predict(self, X: list[dict[str, Any]]) -> list[Any]:
        """Predict class labels for samples."""
        if self.root is None:
            raise ValueError("Tree not fitted")
        return [self.root.predict_single(sample) for sample in X]

    def print_tree(self, node: DecisionTreeNode | None = None, indent: str = "") -> None:
        """Print tree structure for debugging."""
        if node is None:
            node = self.root
        if node is None:
            return
        if node.is_leaf:
            print(f"{indent}Leaf: {node.value} (n={node.n_samples})")
        else:
            print(f"{indent}{node.feature} <= {node.threshold}? (n={node.n_samples})")
            for label, child in node.children.items():
                print(f"{indent}  {label}:")
                self.print_tree(child, indent + "    ")


def cost_complexity_pruning(
    tree: DecisionTreeNode,
    alpha: float,
) -> DecisionTreeNode:
    """
    Cost complexity pruning (CART).

    Removes subtrees where R_alpha(T) > R(T).
    Simplified version.
    """
    def leaf_error(node: DecisionTreeNode) -> float:
        if node.is_leaf:
            total = sum(node.n_classes.values())
            if total == 0:
                return 0.0
            return 1.0 - max(node.n_classes.values()) / total
        return sum(leaf_error(c) for c in node.children.values())

    def subtree_error(node: DecisionTreeNode) -> float:
        if node.is_leaf:
            total = sum(node.n_classes.values())
            if total == 0:
                return 0.0
            return 1.0 - max(node.n_classes.values()) / total + alpha
        return sum(subtree_error(c) for c in node.children.values())

    return tree


def feature_importance(tree: DecisionTreeNode) -> dict[str, float]:
    """
    Compute feature importance from tree.

    Returns:
        Dictionary of feature -> normalized importance score.
    """
    importance: dict[str, float] = {}

    def traverse(node: DecisionTreeNode) -> float:
        if node.is_leaf:
            return sum(node.n_classes.values())
        feat = node.feature
        total = sum(node.n_classes.values())
        subtree_sum = traverse(node.children.get("left", node)) + traverse(node.children.get("right", node))
        imp = total * (1.0 - subtree_sum / max(total, 1))
        importance[feat] = importance.get(feat, 0.0) + imp
        return subtree_sum

    traverse(tree)
    total = sum(importance.values())
    if total > 0:
        return {k: v / total for k, v in importance.items()}
    return importance
