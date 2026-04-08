"""Data regression action module for RabAI AutoClick.

Provides regression analysis: simple linear, multiple linear,
polynomial regression, and basic forecasting.
"""

from __future__ import annotations

import sys
import os
import math
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LinearRegressionAction(BaseAction):
    """Simple and multiple linear regression.
    
    Fits y = a + bx using ordinary least squares.
    Supports simple (one feature) and multiple (many features) regression.
    
    Args:
        fit_intercept: Whether to fit intercept term
    """

    def __init__(self, fit_intercept: bool = True):
        super().__init__()
        self.fit_intercept = fit_intercept
        self.coefficients: List[float] = []
        self.intercept: float = 0.0
        self._is_fitted = False
        self._r_squared: float = 0.0

    def execute(
        self,
        data: List[Dict[str, Any]],
        target_column: str,
        feature_columns: Optional[List[str]] = None,
        single_feature: Optional[str] = None
    ) -> ActionResult:
        try:
            features = [single_feature] if single_feature else (feature_columns or [])
            if not features:
                return ActionResult(success=False, error="No features specified")

            # Extract vectors
            rows = []
            targets = []
            for row in data:
                try:
                    vec = [float(row[f]) for f in features if f in row and row[f] is not None]
                    tgt = float(row[target_column])
                    if len(vec) == len(features) and tgt is not None:
                        rows.append(vec)
                        targets.append(tgt)
                except (ValueError, TypeError):
                    continue

            if len(rows) < len(features) + 2:
                return ActionResult(success=False, error="Insufficient data")

            n = len(rows)

            if len(features) == 1:
                # Simple linear regression
                x = [r[0] for r in rows]
                y = targets
                x_mean = sum(x) / n
                y_mean = sum(y) / n

                cov = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
                var_x = sum((xi - x_mean) ** 2 for xi in x)

                if var_x == 0:
                    return ActionResult(success=False, error="Zero variance in feature")

                slope = cov / var_x
                intercept = y_mean - slope * x_mean if self.fit_intercept else 0.0

                # R-squared
                y_pred = [intercept + slope * xi for xi in x]
                ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
                ss_tot = sum((yi - y_mean) ** 2 for yi in y)
                r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0

                self.coefficients = [slope]
                self.intercept = intercept
                self._r_squared = r_squared
                self._is_fitted = True

                return ActionResult(success=True, data={
                    "intercept": round(intercept, 6),
                    "slope": round(slope, 6),
                    "r_squared": round(r_squared, 4),
                    "n_samples": n,
                    "feature": features[0]
                })
            else:
                # Multiple linear regression via normal equations
                X = [[1.0] + r for r in rows] if self.fit_intercept else rows
                y = targets

                # X^T X
                m = len(X[0])
                XtX = [[0.0] * m for _ in range(m)]
                for row in X:
                    for i in range(m):
                        for j in range(m):
                            XtX[i][j] += row[i] * row[j]

                # X^T y
                Xty = [0.0] * m
                for row in X:
                    for i in range(m):
                        Xty[i] += row[i] * y[X.index(row)]

                # Gaussian elimination
                coeffs = self._solve_linear(XtX, Xty)

                # Compute R-squared
                y_mean = sum(y) / n
                ss_tot = sum((yi - y_mean) ** 2 for yi in y)
                y_pred = [sum(coeffs[i] * row[i] for i in range(m)) for row in X]
                ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y, y_pred))
                r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0

                if self.fit_intercept:
                    self.intercept = coeffs[0]
                    self.coefficients = coeffs[1:]
                else:
                    self.intercept = 0.0
                    self.coefficients = coeffs

                self._r_squared = r_squared
                self._is_fitted = True

                coef_dict = {f: round(c, 6) for f, c in zip(features, self.coefficients)}
                return ActionResult(success=True, data={
                    "intercept": round(self.intercept, 6),
                    "coefficients": coef_dict,
                    "r_squared": round(r_squared, 4),
                    "n_samples": n
                })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _solve_linear(self, A: List[List[float]], b: List[float]) -> List[float]:
        """Solve Ax = b via Gaussian elimination with partial pivoting."""
        n = len(A)
        M = [row[:] + [b[i]] for i, row in enumerate(A)]

        for i in range(n):
            # Find pivot
            max_row = max(range(i, n), key=lambda r: abs(M[r][i]))
            M[i], M[max_row] = M[max_row], M[i]

            if abs(M[i][i]) < 1e-10:
                continue

            for j in range(i + 1, n):
                factor = M[j][i] / M[i][i]
                for k in range(i, n + 1):
                    M[j][k] -= factor * M[i][k]

        # Back substitution
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            if abs(M[i][i]) < 1e-10:
                x[i] = 0.0
                continue
            x[i] = M[i][n]
            for j in range(i + 1, n):
                x[i] -= M[i][j] * x[j]
            x[i] /= M[i][i]

        return x


class PolynomialRegressionAction(BaseAction):
    """Polynomial regression for non-linear relationships.
    
    Fits degree-n polynomial using the same least-squares approach.
    
    Args:
        degree: Polynomial degree (2 = quadratic, 3 = cubic, etc.)
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        target_column: str,
        feature_column: str,
        degree: int = 2
    ) -> ActionResult:
        try:
            if degree < 2:
                return ActionResult(success=False, error="degree must be >= 2")

            # Extract (x, y) pairs
            pairs = []
            for row in data:
                try:
                    x_val = float(row[feature_column])
                    y_val = float(row[target_column])
                    pairs.append((x_val, y_val))
                except (ValueError, TypeError):
                    continue

            if len(pairs) < degree + 2:
                return ActionResult(success=False, error="Insufficient data")

            # Build design matrix
            n = len(pairs)
            X = []
            y = []
            for x_val, y_val in pairs:
                row = [x_val ** d for d in range(degree + 1)]
                X.append(row)
                y.append(y_val)

            # Normal equations: (X^T X) beta = X^T y
            m = degree + 1
            XtX = [[0.0] * m for _ in range(m)]
            for row in X:
                for i in range(m):
                    for j in range(m):
                        XtX[i][j] += row[i] * row[j]

            Xty = [0.0] * m
            for row in X:
                for i in range(m):
                    Xty[i] += row[i] * y[X.index(row)]

            # Solve
            coeffs = self._solve_linear(XtX, Xty)

            # R-squared
            y_vals = [p[1] for p in pairs]
            y_mean = sum(y_vals) / n
            y_pred = [sum(coeffs[d] * (p[0] ** d) for d in range(degree + 1)) for p in pairs]
            ss_res = sum((yi - yp) ** 2 for yi, yp in zip(y_vals, y_pred))
            ss_tot = sum((yi - y_mean) ** 2 for yi in y_vals)
            r_squared = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0

            coef_dict = {f"x^{d}": round(coeffs[d], 6) for d in range(degree + 1)}
            return ActionResult(success=True, data={
                "coefficients": coef_dict,
                "r_squared": round(r_squared, 4),
                "degree": degree,
                "n_samples": n
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _solve_linear(self, A: List[List[float]], b: List[float]) -> List[float]:
        n = len(A)
        M = [row[:] + [b[i]] for i, row in enumerate(A)]
        for i in range(n):
            max_row = max(range(i, n), key=lambda r: abs(M[r][i]))
            M[i], M[max_row] = M[max_row], M[i]
            if abs(M[i][i]) < 1e-10:
                continue
            for j in range(i + 1, n):
                factor = M[j][i] / M[i][i]
                for k in range(i, n + 1):
                    M[j][k] -= factor * M[i][k]
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            if abs(M[i][i]) < 1e-10:
                x[i] = 0.0
                continue
            x[i] = M[i][n]
            for j in range(i + 1, n):
                x[i] -= M[i][j] * x[j]
            x[i] /= M[i][i]
        return x


class ForecastAction(BaseAction):
    """Simple time series forecasting.
    
    Supports moving average, weighted moving average,
    and exponential smoothing forecasting.
    
    Args:
        window_size: Window for moving average methods
        alpha: Smoothing factor for exponential smoothing (0-1)
    """

    def execute(
        self,
        data: List[Dict[str, Any]],
        value_column: str,
        time_column: Optional[str] = None,
        method: str = "moving_average",
        window_size: int = 3,
        alpha: float = 0.3,
        forecast_periods: int = 3
    ) -> ActionResult:
        try:
            # Extract values in order
            if time_column:
                sorted_data = sorted(data, key=lambda d: str(d.get(time_column, "")))
            else:
                sorted_data = data

            values = []
            for row in sorted_data:
                if value_column in row and row[value_column] is not None:
                    try:
                        values.append(float(row[value_column]))
                    except (ValueError, TypeError):
                        continue

            if len(values) < window_size + 1:
                return ActionResult(success=False, error="Insufficient data for forecasting")

            forecasts = []
            if method == "moving_average":
                for i in range(forecast_periods):
                    window = values[-(window_size):]
                    forecast = sum(window) / len(window)
                    forecasts.append(round(forecast, 4))
                    values.append(forecast)

            elif method == "weighted_moving_average":
                weights = list(range(1, window_size + 1))
                total_weight = sum(weights)
                for i in range(forecast_periods):
                    window = values[-(window_size):]
                    weighted_avg = sum(w * v for w, v in zip(weights, window)) / total_weight
                    forecasts.append(round(weighted_avg, 4))
                    values.append(weighted_avg)

            elif method == "exponential_smoothing":
                # Initialize with first value
                smoothed = [values[0]]
                for v in values[1:]:
                    s = alpha * v + (1 - alpha) * smoothed[-1]
                    smoothed.append(s)

                for i in range(forecast_periods):
                    forecasts.append(round(smoothed[-1], 4))
                    smoothed.append(smoothed[-1])

            else:
                return ActionResult(success=False, error=f"Unknown method: {method}")

            return ActionResult(success=True, data={
                "method": method,
                "forecasts": forecasts,
                "n_forecast_periods": forecast_periods,
                "last_actual": values[-(forecast_periods + 1)]
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))
