"""
Regression analysis module for predictive modeling.

Provides linear, polynomial, and multiple regression with
metrics, predictions, and residual analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class RegressionCoefficients:
    """Regression model coefficients."""
    intercept: float
    coefficients: list[float]
    r_squared: float
    adjusted_r_squared: float
    standard_errors: list[float]


@dataclass
class PredictionResult:
    """Single prediction result."""
    input_values: list[float]
    predicted: float
    confidence_lower: float
    confidence_upper: float
    prediction_interval_lower: float
    prediction_interval_upper: float


@dataclass
class RegressionResult:
    """Complete regression analysis result."""
    coefficients: RegressionCoefficients
    predictions: list[float]
    residuals: list[float]
    metrics: dict[str, float]
    sample_size: int
    degrees_of_freedom: int


class RegressionAnalyzer:
    """
    Performs regression analysis for predictive modeling.
    
    Example:
        analyzer = RegressionAnalyzer()
        x = [[1], [2], [3], [4], [5]]
        y = [2.1, 4.0, 5.2, 7.8, 10.1]
        result = analyzer.linear_regression(x, y)
    """

    def __init__(self) -> None:
        """Initialize regression analyzer."""
        self._last_coefficients: Optional[RegressionCoefficients] = None

    def _transpose(self, matrix: list[list[float]]) -> list[list[float]]:
        """Transpose a matrix."""
        if not matrix:
            return []
        return [[matrix[i][j] for i in range(len(matrix))] for j in range(len(matrix[0]))]

    def _matrix_multiply(
        self,
        a: list[list[float]],
        b: list[list[float]]
    ) -> list[list[float]]:
        """Multiply two matrices."""
        if not a or not b:
            return []
        n, m, p = len(a), len(a[0]), len(b[0])
        result = [[0.0] * p for _ in range(n)]
        for i in range(n):
            for j in range(p):
                for k in range(m):
                    result[i][j] += a[i][k] * b[k][j]
        return result

    def _matrix_inverse(self, matrix: list[list[float]]) -> Optional[list[list[float]]]:
        """Simple matrix inverse using Gauss-Jordan elimination."""
        n = len(matrix)
        aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(matrix)]

        for col in range(n):
            # Find pivot
            max_row = max(range(col, n), key=lambda r: abs(aug[r][col]))
            if abs(aug[max_row][col]) < 1e-10:
                return None
            aug[col], aug[max_row] = aug[max_row], aug[col]

            # Scale pivot row
            pivot = aug[col][col]
            for j in range(2 * n):
                aug[col][j] /= pivot

            # Eliminate column
            for i in range(n):
                if i != col:
                    factor = aug[i][col]
                    for j in range(2 * n):
                        aug[i][j] -= factor * aug[col][j]

        return [row[n:] for row in aug]

    def _mean(self, data: list[float]) -> float:
        """Compute mean."""
        return sum(data) / len(data) if data else 0.0

    def _variance(self, data: list[float]) -> float:
        """Compute variance."""
        if len(data) < 2:
            return 0.0
        mean = self._mean(data)
        return sum((x - mean) ** 2 for x in data) / (len(data) - 1)

    def linear_regression(
        self,
        x: list[list[float]],
        y: list[float],
        add_intercept: bool = True
    ) -> RegressionResult:
        """
        Perform simple or multiple linear regression.
        
        Args:
            x: Independent variables (list of feature vectors).
            y: Dependent variable values.
            add_intercept: Whether to add a bias/intercept term.
            
        Returns:
            RegressionResult with coefficients, predictions, and metrics.
            
        Raises:
            ValueError: If input dimensions don't match.
        """
        n = len(x)
        if n != len(y):
            raise ValueError("x and y must have the same number of samples")
        if n < 2:
            raise ValueError("Need at least 2 samples")

        m = len(x[0]) if x else 0

        # Design matrix with intercept
        if add_intercept:
            X = [[1.0] + x[i] for i in range(n)]
            k = m + 1  # number of coefficients (including intercept)
        else:
            X = x
            k = m

        # Compute beta = (X'X)^(-1) X'y
        Xt = self._transpose(X)
        XtX = self._matrix_multiply(Xt, X)
        XtX_inv = self._matrix_inverse(XtX)

        if XtX_inv is None:
            raise ValueError("Matrix is singular, cannot compute regression")

        Xty = [[sum(Xt[i][j] * y[j] for j in range(n))] for i in range(k)]
        beta = self._matrix_multiply(XtX_inv, Xty)

        intercept = beta[0][0] if add_intercept else 0.0
        coefficients = [beta[i][0] for i in range(1 if add_intercept else 0, k)]

        # Predictions
        predictions = []
        for i in range(n):
            pred = intercept + sum(coefficients[j] * x[i][j] for j in range(m))
            predictions.append(pred)

        # Residuals
        residuals = [y[i] - predictions[i] for i in range(n)]

        # R-squared
        y_mean = self._mean(y)
        ss_res = sum(r ** 2 for r in residuals)
        ss_tot = sum((yi - y_mean) ** 2 for yi in y)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        # Adjusted R-squared
        if n > k + 1:
            adj_r_squared = 1 - (1 - r_squared) * (n - 1) / (n - k - 1)
        else:
            adj_r_squared = r_squared

        # Standard errors of coefficients
        mse = ss_res / (n - k)
        se = []
        for i in range(k):
            se.append(math.sqrt(mse * XtX_inv[i][i]))

        coeffs = RegressionCoefficients(
            intercept=round(intercept, 6),
            coefficients=[round(c, 6) for c in coefficients],
            r_squared=round(r_squared, 6),
            adjusted_r_squared=round(adj_r_squared, 6),
            standard_errors=[round(s, 6) for s in se]
        )

        # Metrics
        rmse = math.sqrt(ss_res / n)
        mae = sum(abs(r) for r in residuals) / n

        metrics = {
            'r_squared': r_squared,
            'adjusted_r_squared': adj_r_squared,
            'rmse': rmse,
            'mae': mae,
            'mse': ss_res / n,
            'ss_res': ss_res,
            'ss_tot': ss_tot,
        }

        self._last_coefficients = coeffs

        return RegressionResult(
            coefficients=coeffs,
            predictions=[round(p, 6) for p in predictions],
            residuals=[round(r, 6) for r in residuals],
            metrics={k: round(v, 6) for k, v in metrics.items()},
            sample_size=n,
            degrees_of_freedom=n - k
        )

    def polynomial_regression(
        self,
        x: list[float],
        y: list[float],
        degree: int = 2
    ) -> RegressionResult:
        """
        Perform polynomial regression.
        
        Args:
            x: Independent variable values.
            y: Dependent variable values.
            degree: Polynomial degree.
            
        Returns:
            RegressionResult with polynomial coefficients.
        """
        if len(x) != len(y):
            raise ValueError("x and y must have the same length")
        if degree < 1:
            raise ValueError("Degree must be at least 1")

        # Create polynomial features
        x_poly: list[list[float]] = []
        for xi in x:
            row = [xi ** d for d in range(degree + 1)]
            x_poly.append(row)

        return self.linear_regression(x_poly, y, add_intercept=False)

    def predict_single(
        self,
        x_values: list[float],
        coefficients: Optional[RegressionCoefficients] = None,
        confidence: float = 0.95
    ) -> PredictionResult:
        """
        Make a prediction with confidence intervals.
        
        Args:
            x_values: Feature values for prediction.
            coefficients: Model coefficients (uses last if None).
            confidence: Confidence level (e.g., 0.95 for 95%).
            
        Returns:
            PredictionResult with point estimate and intervals.
        """
        if coefficients is None:
            coefficients = self._last_coefficients
        if coefficients is None:
            raise ValueError("No coefficients available")

        # Point prediction
        predicted = coefficients.intercept + sum(
            coefficients.coefficients[i] * x_values[i]
            for i in range(len(x_values))
        )

        # Simple approximation for intervals
        z = 1.96 if confidence >= 0.95 else 1.645  # z-score for confidence
        se_pred = sum(coefficients.standard_errors) * z

        return PredictionResult(
            input_values=x_values,
            predicted=round(predicted, 6),
            confidence_lower=round(predicted - se_pred, 6),
            confidence_upper=round(predicted + se_pred, 6),
            prediction_interval_lower=round(predicted - 2 * se_pred, 6),
            prediction_interval_upper=round(predicted + 2 * se_pred, 6)
        )

    def predict_batch(
        self,
        x_values: list[list[float]],
        coefficients: Optional[RegressionCoefficients] = None
    ) -> list[float]:
        """
        Make batch predictions.
        
        Args:
            x_values: List of feature vectors.
            coefficients: Model coefficients (uses last if None).
            
        Returns:
            List of predictions.
        """
        if coefficients is None:
            coefficients = self._last_coefficients
        if coefficients is None:
            raise ValueError("No coefficients available")

        predictions = []
        for x in x_values:
            pred = coefficients.intercept + sum(
                coefficients.coefficients[i] * x[i]
                for i in range(len(x))
            )
            predictions.append(round(pred, 6))

        return predictions
