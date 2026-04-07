"""
Ensemble learning utilities.

Provides voting, stacking, and bagging ensemble methods.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

import numpy as np


class VotingClassifier:
    """Voting ensemble classifier."""

    def __init__(self, estimators: List[Tuple[Any, Any]], voting: str = "hard"):
        """
        Initialize voting classifier.

        Args:
            estimators: List of (name, estimator) tuples
            voting: 'hard' for majority vote, 'soft' for probability averaging
        """
        self.estimators = estimators
        self.voting = voting
        self._fitted_estimators = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "VotingClassifier":
        """Fit all estimators."""
        self._fitted_estimators = []
        for name, est in self.estimators:
            est.fit(X, y)
            self._fitted_estimators.append((name, est))
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict using voting."""
        if self.voting == "hard":
            predictions = np.array([est.predict(X) for _, est in self._fitted_estimators])
            from scipy.stats import mode
            result = mode(predictions, axis=0, keepdims=False)[0]
            return result
        else:
            probas = np.array([est.predict_proba(X) for _, est in self._fitted_estimators])
            avg_proba = np.mean(probas, axis=0)
            return np.argmax(avg_proba, axis=1)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Get average probability."""
        probas = np.array([est.predict_proba(X) for _, est in self._fitted_estimators])
        return np.mean(probas, axis=0)


class BaggingClassifier:
    """Bagging ensemble classifier."""

    def __init__(
        self,
        base_estimator: Any,
        n_estimators: int = 10,
        max_samples: float = 1.0,
        max_features: float = 1.0,
        bootstrap: bool = True,
        random_state: int = None,
    ):
        self.base_estimator = base_estimator
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.max_features = max_features
        self.bootstrap = bootstrap
        self.random_state = random_state
        self.estimators_ = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "BaggingClassifier":
        """Fit bagging ensemble."""
        if self.random_state is not None:
            np.random.seed(self.random_state)
        n_samples = len(X)
        n_features = X.shape[1] if len(X.shape) > 1 else 1
        sample_size = int(n_samples * self.max_samples)
        feature_size = int(n_features * self.max_features)
        self.estimators_ = []
        for _ in range(self.n_estimators):
            est = self._clone_estimator()
            indices = np.random.choice(n_samples, sample_size, replace=self.bootstrap)
            if feature_size < n_features:
                feature_indices = np.random.choice(n_features, feature_size, replace=False)
                X_boot = X[indices][:, feature_indices] if len(X.shape) > 1 else X[indices]
            else:
                X_boot = X[indices]
            y_boot = y[indices]
            est.fit(X_boot, y_boot)
            self.estimators_.append((est, feature_indices if feature_size < n_features else None))
        return self

    def _clone_estimator(self) -> Any:
        """Clone the base estimator."""
        import copy
        return copy.deepcopy(self.base_estimator)

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict using all estimators."""
        predictions = []
        for est, feature_indices in self.estimators_:
            if feature_indices is not None:
                X_subset = X[:, feature_indices]
            else:
                X_subset = X
            predictions.append(est.predict(X_subset))
        from scipy.stats import mode
        return mode(np.array(predictions), axis=0, keepdims=False)[0]


class RandomForestClassifier:
    """Simple Random Forest implementation."""

    def __init__(
        self,
        n_estimators: int = 100,
        max_depth: int = 10,
        min_samples_split: int = 2,
        max_features: str = "sqrt",
        random_state: int = None,
    ):
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.max_features = max_features
        self.random_state = random_state
        self.trees_ = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "RandomForestClassifier":
        """Fit random forest."""
        if self.random_state is not None:
            np.random.seed(self.random_state)
        self.n_classes_ = len(np.unique(y))
        self.trees_ = []
        for _ in range(self.n_estimators):
            tree = DecisionTreeClassifier(
                max_depth=self.max_depth,
                min_samples_split=self.min_samples_split,
                max_features=self.max_features,
                random_state=self.random_state,
            )
            indices = np.random.choice(len(X), len(X), replace=True)
            tree.fit(X[indices], y[indices])
            self.trees_.append(tree)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict using majority vote."""
        proba = self.predict_proba(X)
        return np.argmax(proba, axis=1)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Get average probability."""
        probas = np.array([tree.predict_proba(X) for tree in self.trees_])
        return np.mean(probas, axis=0)


class DecisionTreeClassifier:
    """Simple decision tree classifier."""

    def __init__(
        self,
        max_depth: int = 10,
        min_samples_split: int = 2,
        max_features: str = "sqrt",
        random_state: int = None,
    ):
        self.max_depth = max_depth
        self.min_samples_split = min_samples_split
        self.max_features = max_features
        self.random_state = random_state
        self.tree_ = None
        self.n_features_ = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "DecisionTreeClassifier":
        """Fit decision tree."""
        self.n_features_ = X.shape[1] if len(X.shape) > 1 else 1
        self.tree_ = self._build_tree(X, y, depth=0)
        return self

    def _build_tree(self, X: np.ndarray, y: np.ndarray, depth: int) -> dict:
        """Build tree recursively."""
        n_samples = len(y)
        if depth >= self.max_depth or n_samples < self.min_samples_split:
            return self._leaf(y)
        best_split = self._find_best_split(X, y)
        if best_split is None:
            return self._leaf(y)
        left_mask, right_mask = best_split["mask"]
        return {
            "feature": best_split["feature"],
            "threshold": best_split["threshold"],
            "left": self._build_tree(X[left_mask], y[left_mask], depth + 1),
            "right": self._build_tree(X[right_mask], y[right_mask], depth + 1),
        }

    def _find_best_split(self, X: np.ndarray, y: np.ndarray) -> dict:
        """Find best split using information gain."""
        n_samples = len(y)
        n_features = X.shape[1] if len(X.shape) > 1 else 1
        if self.max_features == "sqrt":
            max_feats = max(1, int(np.sqrt(n_features)))
        elif self.max_features == "log2":
            max_feats = max(1, int(np.log2(n_features)))
        else:
            max_feats = n_features
        feature_indices = np.random.choice(n_features, max_feats, replace=False)
        best_ig = -1
        best_split = None
        for feat_idx in feature_indices:
            values = X[:, feat_idx]
            thresholds = np.unique(values)
            if len(thresholds) < 2:
                continue
            for thresh in thresholds[1:]:
                mask = values <= thresh
                ig = self._information_gain(y, mask)
                if ig > best_ig:
                    best_ig = ig
                    best_split = {"feature": feat_idx, "threshold": thresh, "mask": mask}
        return best_split

    def _information_gain(self, y: np.ndarray, mask: np.ndarray) -> float:
        """Compute information gain."""
        parent_entropy = self._entropy(y)
        left_y, right_y = y[mask], y[~mask]
        if len(left_y) == 0 or len(right_y) == 0:
            return 0
        n = len(y)
        ig = parent_entropy - (len(left_y) / n * self._entropy(left_y) + len(right_y) / n * self._entropy(right_y))
        return ig

    def _entropy(self, y: np.ndarray) -> float:
        """Compute entropy."""
        if len(y) == 0:
            return 0
        counts = np.bincount(y, minlength=self.n_classes_)
        probs = counts / len(y)
        return -np.sum(probs * np.log(probs + 1e-10))

    def _leaf(self, y: np.ndarray) -> dict:
        """Create leaf node."""
        counts = np.bincount(y, minlength=self.n_classes_)
        return {"class": np.argmax(counts)}

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict class labels."""
        return np.array([self._predict_sample(x, self.tree_) for x in X])

    def _predict_sample(self, x: np.ndarray, node: dict) -> int:
        """Predict single sample."""
        if "class" in node:
            return node["class"]
        if x[node["feature"]] <= node["threshold"]:
            return self._predict_sample(x, node["left"])
        return self._predict_sample(x, node["right"])

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict probabilities."""
        probas = np.zeros((len(X), self.n_classes_))
        for i, x in enumerate(X):
            pred = self._predict_sample(x, self.tree_)
            probas[i, pred] = 1.0
        return probas


class AdaBoostClassifier:
    """AdaBoost ensemble classifier."""

    def __init__(self, n_estimators: int = 50, learning_rate: float = 1.0, random_state: int = None):
        self.n_estimators = n_estimators
        self.learning_rate = learning_rate
        self.random_state = random_state
        self.estimators_ = []
        self.estimator_weights_ = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "AdaBoostClassifier":
        """Fit AdaBoost."""
        if self.random_state is not None:
            np.random.seed(self.random_state)
        n_samples = len(X)
        weights = np.ones(n_samples) / n_samples
        self.estimators_ = []
        self.estimator_weights_ = []
        for _ in range(self.n_estimators):
            tree = DecisionStumpClassifier()
            tree.fit(X, y, sample_weight=weights)
            predictions = tree.predict(X)
            incorrect = predictions != y
            weighted_error = np.sum(weights * incorrect) / np.sum(weights)
            if weighted_error >= 0.5:
                continue
            estimator_weight = self.learning_rate * 0.5 * np.log((1 - weighted_error) / (weighted_error + 1e-10))
            weights = weights * np.exp(estimator_weight * incorrect)
            weights = weights / np.sum(weights)
            self.estimators_.append(tree)
            self.estimator_weights_.append(estimator_weight)
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict using weighted vote."""
        predictions = np.array([est.predict(X) for est in self.estimators_])
        weighted_votes = np.zeros((len(X), 2))
        for i, (pred, weight) in enumerate(zip(predictions, self.estimator_weights_)):
            for j, p in enumerate(pred):
                weighted_votes[j, p] += weight
        return np.argmax(weighted_votes, axis=1)


class DecisionStumpClassifier:
    """Decision stump classifier for AdaBoost."""

    def __init__(self):
        self.feature = None
        self.threshold = None
        self.direction = 1

    def fit(self, X: np.ndarray, y: np.ndarray, sample_weight: np.ndarray = None) -> "DecisionStumpClassifier":
        """Fit decision stump."""
        if sample_weight is None:
            sample_weight = np.ones(len(y)) / len(y)
        n_features = X.shape[1] if len(X.shape) > 1 else 1
        best_error = float("inf")
        for feat in range(n_features):
            values = np.unique(X[:, feat])
            for thresh in values:
                for direction in [1, -1]:
                    preds = np.ones(len(y))
                    if direction == 1:
                        preds[X[:, feat] < thresh] = 0
                    else:
                        preds[X[:, feat] >= thresh] = 0
                    error = np.sum(sample_weight * (preds != y))
                    if error < best_error:
                        best_error = error
                        self.feature = feat
                        self.threshold = thresh
                        self.direction = direction
        return self

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict."""
        preds = np.ones(len(X))
        if self.direction == 1:
            preds[X[:, self.feature] < self.threshold] = 0
        else:
            preds[X[:, self.feature] >= self.threshold] = 0
        return preds.astype(int)


class StackingClassifier:
    """Stacking ensemble classifier."""

    def __init__(
        self,
        estimators: List[Tuple[str, Any]],
        meta_estimator: Any = None,
        cv: int = 5,
    ):
        self.estimators = estimators
        self.meta_estimator = meta_estimator or LogisticRegression()
        self.cv = cv
        self._fitted_estimators = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> "StackingClassifier":
        """Fit stacking ensemble."""
        from utils.data_pipeline_utils import k_fold_cross_validation
        n_samples = len(X)
        n_estimators = len(self.estimators)
        meta_features = np.zeros((n_samples, n_estimators))
        folds = k_fold_cross_validation(list(range(n_samples)), n_folds=self.cv, shuffle=True, random_state=42)
        self._fitted_estimators = []
        for name, est in self.estimators:
            est.fit(X, y)
            self._fitted_estimators.append((name, est))
            meta_col = np.zeros(n_samples)
            for train_idx, val_idx in folds:
                X_fold = X[train_idx]
                y_fold = y[train_idx]
                X_val = X[val_idx]
                est_fold = self._clone_estimator(est)
                est_fold.fit(X_fold, y_fold)
                meta_col[val_idx] = est_fold.predict(X_val)
            meta_features[:, len(self._fitted_estimators) - 1] = meta_col
        self.meta_estimator.fit(meta_features, y)
        return self

    def _clone_estimator(self, est: Any) -> Any:
        """Clone estimator."""
        import copy
        return copy.deepcopy(est)

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict using meta-estimator."""
        meta_features = np.column_stack([est.predict(X) for _, est in self._fitted_estimators])
        return self.meta_estimator.predict(meta_features)


class LogisticRegression:
    """Simple logistic regression."""

    def __init__(self, lr: float = 0.01, n_iter: int = 1000):
        self.lr = lr
        self.n_iter = n_iter
        self.weights = None
        self.bias = None

    def fit(self, X: np.ndarray, y: np.ndarray) -> "LogisticRegression":
        """Fit logistic regression."""
        n_features = X.shape[1]
        self.weights = np.zeros(n_features)
        self.bias = 0
        for _ in range(self.n_iter):
            linear = X @ self.weights + self.bias
            preds = self._sigmoid(linear)
            dw = (1 / len(y)) * X.T @ (preds - y)
            db = (1 / len(y)) * np.sum(preds - y)
            self.weights -= self.lr * dw
            self.bias -= self.lr * db
        return self

    def _sigmoid(self, z: np.ndarray) -> np.ndarray:
        """Sigmoid activation."""
        return 1 / (1 + np.exp(-np.clip(z, -500, 500)))

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict probabilities."""
        linear = X @ self.weights + self.bias
        proba = self._sigmoid(linear)
        return np.column_stack([1 - proba, proba])

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict class labels."""
        return (self.predict_proba(X)[:, 1] > 0.5).astype(int)
