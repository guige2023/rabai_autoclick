"""
Hyperparameter optimization utilities.

Provides grid search, random search, and Bayesian optimization
utilities for hyperparameter tuning.
"""
from __future__ import annotations

import copy
import random
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np


def grid_search(
    param_grid: Dict[str, List],
    objective_fn: Callable[[Dict], float],
    maximize: bool = True,
    verbose: int = 0,
) -> Tuple[Dict, float, List[Dict]]:
    """
    Grid search over parameter space.

    Args:
        param_grid: Dictionary mapping parameter names to value lists
        objective_fn: Function that takes params and returns score
        maximize: Whether to maximize or minimize objective
        verbose: Verbosity level

    Returns:
        Tuple of (best_params, best_score, all_results)

    Example:
        >>> def objective(params):
        ...     return -(params['lr'] ** 2 + params['C'] ** 2)
        >>> best_params, best_score, _ = grid_search(
        ...     {'lr': [0.01, 0.1], 'C': [1, 10]}, objective, maximize=False
        ... )
    """
    import itertools
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())
    all_combinations = list(itertools.product(*param_values))
    all_results = []
    best_score = float("-inf") if maximize else float("inf")
    best_params = None
    for combo in all_combinations:
        params = dict(zip(param_names, combo))
        score = objective_fn(params)
        all_results.append({"params": copy.deepcopy(params), "score": score})
        if verbose > 0:
            print(f"Params: {params}, Score: {score:.4f}")
        if (maximize and score > best_score) or (not maximize and score < best_score):
            best_score = score
            best_params = copy.deepcopy(params)
    return best_params, best_score, all_results


def random_search(
    param_distributions: Dict[str, Union[List, Tuple]],
    objective_fn: Callable[[Dict], float],
    n_trials: int = 100,
    maximize: bool = True,
    seed: int = None,
    verbose: int = 0,
) -> Tuple[Dict, float, List[Dict]]:
    """
    Random search over parameter space.

    Args:
        param_distributions: Dict mapping param names to (low, high) tuples or lists
        objective_fn: Function that takes params and returns score
        n_trials: Number of random trials
        maximize: Whether to maximize or minimize objective
        seed: Random seed
        verbose: Verbosity level

    Returns:
        Tuple of (best_params, best_score, all_results)
    """
    if seed is not None:
        np.random.seed(seed)
    param_names = list(param_distributions.keys())
    all_results = []
    best_score = float("-inf") if maximize else float("inf")
    best_params = None
    for _ in range(n_trials):
        params = {}
        for name in param_names:
            dist = param_distributions[name]
            if isinstance(dist, (list, tuple)) and len(dist) == 2 and isinstance(dist[0], (int, float)):
                params[name] = np.random.uniform(dist[0], dist[1])
            else:
                params[name] = random.choice(dist)
        score = objective_fn(params)
        all_results.append({"params": copy.deepcopy(params), "score": score})
        if verbose > 0:
            print(f"Params: {params}, Score: {score:.4f}")
        if (maximize and score > best_score) or (not maximize and score < best_score):
            best_score = score
            best_params = copy.deepcopy(params)
    return best_params, best_score, all_results


class Hyperband:
    """Hyperband hyperparameter optimization."""

    def __init__(
        self,
        objective_fn: Callable[[Dict], float],
        max_iter: int = 81,
        eta: float = 3.0,
        maximize: bool = True,
    ):
        self.objective_fn = objective_fn
        self.max_iter = max_iter
        self.eta = eta
        self.maximize = maximize
        self.logeta = np.log(eta)
        self.s_max = int(np.log(max_iter) / self.logeta)
        self.B = (self.s_max + 1) * max_iter

    def run(self, param_space: Dict[str, Tuple]) -> Tuple[Dict, float]:
        """
        Run Hyperband optimization.

        Args:
            param_space: Dict mapping param names to (low, high) tuples

        Returns:
            Tuple of (best_params, best_score)
        """
        best_score = float("-inf") if self.maximize else float("inf")
        best_params = None
        for s in range(self.s_max, -1, -1):
            n = int(np.ceil(self.B / max_iter / (s + 1) * self.eta ** s))
            r = int(max_iter * self.eta ** (-s))
            brackets = self._successive_halving(param_space, n, r)
            for result in brackets:
                if (self.maximize and result["score"] > best_score) or (
                    not self.maximize and result["score"] < best_score
                ):
                    best_score = result["score"]
                    best_params = result["params"]
        return best_params, best_score

    def _successive_halving(
        self, param_space: Dict, n: int, r: int
    ) -> List[Dict]:
        """Run successive halving within a bracket."""
        params_list = [self._sample_params(param_space) for _ in range(n)]
        for i in range(int(np.log(n) / np.log(self.eta))):
            scores = []
            for params in params_list:
                budgets = [r * self.eta ** (-i)] * len(params_list)
                score = self.objective_fn(params)
                scores.append(score)
            n_keep = n // self.eta
            keep_indices = np.argsort(scores)[-n_keep:]
            params_list = [params_list[i] for i in keep_indices]
        return [
            {"params": p, "score": self.objective_fn(p)}
            for p in params_list
        ]

    def _sample_params(self, param_space: Dict) -> Dict:
        """Sample parameters from space."""
        params = {}
        for name, (low, high) in param_space.items():
            if isinstance(low, int) and isinstance(high, int):
                params[name] = np.random.randint(low, high + 1)
            else:
                params[name] = np.random.uniform(low, high)
        return params


class EarlyStopping:
    """Early stopping callback."""

    def __init__(
        self,
        patience: int = 10,
        min_delta: float = 0.0,
        maximize: bool = True,
    ):
        self.patience = patience
        self.min_delta = min_delta
        self.maximize = maximize
        self.best_score = float("-inf") if maximize else float("inf")
        self.counter = 0
        self.should_stop = False

    def __call__(self, score: float) -> bool:
        """Check if training should stop."""
        if (self.maximize and score > self.best_score + self.min_delta) or (
            not self.maximize and score < self.best_score - self.min_delta
        ):
            self.best_score = score
            self.counter = 0
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.should_stop = True
        return self.should_stop


class LearningRateScheduler:
    """Learning rate scheduler."""

    def __init__(self, optimizer_lr: float, schedule_type: str = "step", **kwargs):
        self.lr = optimizer_lr
        self.schedule_type = schedule_type
        self.kwargs = kwargs

    def step(self, epoch: int) -> float:
        """Get learning rate for epoch."""
        if self.schedule_type == "step":
            gamma = self.kwargs.get("gamma", 0.1)
            step_size = self.kwargs.get("step_size", 10)
            return self.lr * gamma ** (epoch // step_size)
        elif self.schedule_type == "exp":
            gamma = self.kwargs.get("gamma", 0.95)
            return self.lr * gamma ** epoch
        elif self.schedule_type == "cosine":
            T_max = self.kwargs.get("T_max", 50)
            eta_min = self.kwargs.get("eta_min", 0)
            return eta_min + (self.lr - eta_min) * (1 + np.cos(np.pi * epoch / T_max)) / 2
        return self.lr


class ModelCheckpoint:
    """Save model checkpoints based on metric."""

    def __init__(self, filepath: str, maximize: bool = True, save_best_only: bool = True):
        self.filepath = filepath
        self.maximize = maximize
        self.save_best_only = save_best_only
        self.best_score = float("-inf") if maximize else float("inf")

    def __call__(self, model_state: Dict, score: float) -> bool:
        """Check if model should be saved."""
        is_best = (self.maximize and score > self.best_score) or (
            not self.maximize and score < self.best_score
        )
        if is_best:
            self.best_score = score
            return True
        return not self.save_best_only


def cross_validate(
    X: np.ndarray,
    y: np.ndarray,
    model_fn: Callable[[Dict], Any],
    param_grid: Dict,
    cv: int = 5,
    scoring: Callable[[np.ndarray, np.ndarray], float] = None,
    maximize: bool = True,
) -> Tuple[Dict, float]:
    """
    Cross-validation for hyperparameter tuning.

    Args:
        X: Feature matrix
        y: Labels
        model_fn: Function that creates model with given params
        param_grid: Parameter grid to search
        cv: Number of folds
        scoring: Scoring function
        maximize: Whether higher is better

    Returns:
        Tuple of (best_params, best_cv_score)
    """
    from utils.data_pipeline_utils import k_fold_cross_validation
    if scoring is None:
        from utils.evaluation_utils import accuracy_score
        scoring = accuracy_score
    folds = k_fold_cross_validation(list(range(len(X))), n_folds=cv, shuffle=True, random_state=42)
    best_score = float("-inf") if maximize else float("inf")
    best_params = None
    for params in _iter_param_combinations(param_grid):
        scores = []
        for train_idx, val_idx in folds:
            model = model_fn(params)
            X_train = X[train_idx]
            y_train = y[train_idx]
            X_val = X[val_idx]
            y_val = y[val_idx]
            if hasattr(model, "fit"):
                model.fit(X_train, y_train)
            score = scoring(y_val, model.predict(X_val))
            scores.append(score)
        mean_score = np.mean(scores)
        if (maximize and mean_score > best_score) or (not maximize and mean_score < best_score):
            best_score = mean_score
            best_params = copy.deepcopy(params)
    return best_params, best_score


def _iter_param_combinations(param_grid: Dict) -> List[Dict]:
    """Iterate over all parameter combinations."""
    import itertools
    keys = list(param_grid.keys())
    values = list(param_grid.values())
    for combo in itertools.product(*values):
        yield dict(zip(keys, combo))


class BayesianOptimizer:
    """Simple Bayesian optimization (placeholder using random sampling)."""

    def __init__(
        self,
        objective_fn: Callable[[Dict], float],
        param_space: Dict[str, Tuple],
        n_iter: int = 50,
        maximize: bool = True,
    ):
        self.objective_fn = objective_fn
        self.param_space = param_space
        self.n_iter = n_iter
        self.maximize = maximize
        self.history = []

    def run(self) -> Tuple[Dict, float]:
        """Run optimization."""
        best_score = float("-inf") if self.maximize else float("inf")
        best_params = None
        for _ in range(self.n_iter):
            params = self._sample_params()
            score = self.objective_fn(params)
            self.history.append({"params": params, "score": score})
            if (self.maximize and score > best_score) or (not self.maximize and score < best_score):
                best_score = score
                best_params = copy.deepcopy(params)
        return best_params, best_score

    def _sample_params(self) -> Dict:
        """Sample parameters."""
        params = {}
        for name, (low, high) in self.param_space.items():
            if isinstance(low, int) and isinstance(high, int):
                params[name] = np.random.randint(low, high + 1)
            else:
                params[name] = np.random.uniform(low, high)
        return params
