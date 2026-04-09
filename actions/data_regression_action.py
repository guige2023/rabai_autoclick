"""Data Regression Action.

Performs regression analysis including linear, polynomial, and
logistic regression with cross-validation and feature importance.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class RegressionType(Enum):
    """Types of regression supported."""
    LINEAR = "linear"
    POLYNOMIAL = "polynomial"
    LOGISTIC = "logistic"
    RIDGE = "ridge"
    LASSO = "lasso"
    ELASTIC_NET = "elastic_net"


@dataclass
class RegressionResult:
    """Result of a regression fit."""
    regression_type: str
    coefficients: np.ndarray
    intercept: float
    r_squared: float
    adjusted_r_squared: float
    rmse: float
    mae: float
    feature_names: List[str] = field(default_factory=list)


@dataclass
class CVResult:
    """Cross-validation result."""
    fold_scores: List[float]
    mean_score: float
    std_score: float


class DataRegressionAction:
    """Regression analysis for numerical predictions."""

    def __init__(self, random_state: int = 42) -> None:
        self.random_state = random_state
        self._last_result: Optional[RegressionResult] = None

    def fit_linear(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: Optional[List[str]] = None,
    ) -> RegressionResult:
        """Fit a simple linear regression."""
        from sklearn.linear_model import LinearRegression

        model = LinearRegression()
        model.fit(X, y)

        y_pred = model.predict(X)

        result = self._build_result(
            RegressionType.LINEAR.value,
            model.coef_,
            model.intercept_,
            X, y, y_pred,
            feature_names,
        )

        self._last_result = result
        return result

    def fit_polynomial(
        self,
        X: np.ndarray,
        y: np.ndarray,
        degree: int = 2,
        feature_names: Optional[List[str]] = None,
    ) -> RegressionResult:
        """Fit a polynomial regression."""
        from sklearn.preprocessing import PolynomialFeatures
        from sklearn.linear_model import LinearRegression

        poly = PolynomialFeatures(degree=degree, include_bias=False)
        X_poly = poly.fit_transform(X)

        model = LinearRegression()
        model.fit(X_poly, y)

        y_pred = model.predict(X_poly)

        result = self._build_result(
            RegressionType.POLYNOMIAL.value,
            model.coef_,
            model.intercept_,
            X_poly, y, y_pred,
            feature_names or [f"poly_{i}" for i in range(X_poly.shape[1])],
        )

        self._last_result = result
        return result

    def fit_ridge(
        self,
        X: np.ndarray,
        y: np.ndarray,
        alpha: float = 1.0,
        feature_names: Optional[List[str]] = None,
    ) -> RegressionResult:
        """Fit ridge regression (L2 regularization)."""
        from sklearn.linear_model import Ridge

        model = Ridge(alpha=alpha, random_state=self.random_state)
        model.fit(X, y)

        y_pred = model.predict(X)

        result = self._build_result(
            RegressionType.RIDGE.value,
            model.coef_,
            model.intercept_,
            X, y, y_pred,
            feature_names,
        )

        self._last_result = result
        return result

    def fit_lasso(
        self,
        X: np.ndarray,
        y: np.ndarray,
        alpha: float = 1.0,
        feature_names: Optional[List[str]] = None,
    ) -> RegressionResult:
        """Fit lasso regression (L1 regularization)."""
        from sklearn.linear_model import Lasso

        model = Lasso(alpha=alpha, random_state=self.random_state, max_iter=10000)
        model.fit(X, y)

        y_pred = model.predict(X)

        result = self._build_result(
            RegressionType.LASSO.value,
            model.coef_,
            model.intercept_,
            X, y, y_pred,
            feature_names,
        )

        self._last_result = result
        return result

    def fit_logistic(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: Optional[List[str]] = None,
    ) -> RegressionResult:
        """Fit logistic regression for classification."""
        from sklearn.linear_model import LogisticRegression

        model = LogisticRegression(random_state=self.random_state, max_iter=10000)
        model.fit(X, y)

        y_pred = model.predict(X)

        result = self._build_result(
            RegressionType.LOGISTIC.value,
            model.coef_.flatten(),
            model.intercept_[0],
            X, y, y_pred,
            feature_names,
        )

        self._last_result = result
        return result

    def cross_validate(
        self,
        X: np.ndarray,
        y: np.ndarray,
        n_splits: int = 5,
        scoring: str = "r2",
    ) -> CVResult:
        """Perform k-fold cross-validation."""
        from sklearn.model_selection import cross_val_score
        from sklearn.linear_model import LinearRegression

        if self._last_result is None:
            raise RuntimeError("Must fit a model before cross-validation")

        model_type = self._last_result.regression_type

        if model_type == RegressionType.LINEAR.value:
            model = LinearRegression()
        elif model_type == RegressionType.RIDGE.value:
            model = Ridge(random_state=self.random_state)
        elif model_type == RegressionType.LASSO.value:
            model = Lasso(random_state=self.random_state, max_iter=10000)
        else:
            model = LinearRegression()

        scores = cross_val_score(model, X, y, cv=n_splits, scoring=scoring)

        return CVResult(
            fold_scores=[float(s) for s in scores],
            mean_score=float(np.mean(scores)),
            std_score=float(np.std(scores)),
        )

    def predict(
        self,
        X: np.ndarray,
        result: Optional[RegressionResult] = None,
    ) -> np.ndarray:
        """Make predictions using a fitted model."""
        reg_result = result or self._last_result
        if reg_result is None:
            raise RuntimeError("No fitted model available")

        X = np.asarray(X)
        if X.ndim == 1:
            X = X.reshape(-1, 1)

        return reg_result.coefficients @ X.T + reg_result.intercept

    def feature_importance(
        self,
        result: Optional[RegressionResult] = None,
    ) -> List[Tuple[str, float]]:
        """Get feature importance as absolute coefficient values."""
        reg_result = result or self._last_result
        if reg_result is None:
            raise RuntimeError("No fitted model available")

        names = reg_result.feature_names or [f"f{i}" for i in range(len(reg_result.coefficients))]
        coefs = np.abs(reg_result.coefficients)

        importance = sorted(zip(names, coefs), key=lambda x: x[1], reverse=True)
        return importance

    def _build_result(
        self,
        reg_type: str,
        coefficients: np.ndarray,
        intercept: float,
        X: np.ndarray,
        y: np.ndarray,
        y_pred: np.ndarray,
        feature_names: Optional[List[str]],
    ) -> RegressionResult:
        """Build a regression result with metrics."""
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

        n = len(y)
        p = len(coefficients)
        adj_r2 = 1 - (1 - r2) * (n - 1) / (n - p - 1) if n > p + 1 else r2

        rmse = float(np.sqrt(np.mean((y - y_pred) ** 2)))
        mae = float(np.mean(np.abs(y - y_pred)))

        names = feature_names or [f"x{i}" for i in range(len(coefficients))]

        return RegressionResult(
            regression_type=reg_type,
            coefficients=coefficients,
            intercept=float(intercept),
            r_squared=float(r2),
            adjusted_r_squared=float(adj_r2),
            rmse=rmse,
            mae=mae,
            feature_names=names,
        )

    def get_last_result(self) -> Optional[RegressionResult]:
        """Get the last regression result."""
        return self._last_result
