"""Data Regression Analysis Action.

Linear and polynomial regression with diagnostics: R-squared,
residuals, confidence intervals, and prediction intervals.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class RegressionResult:
    coefficients: List[float]
    intercept: float
    r_squared: float
    adj_r_squared: float
    residuals: List[float]
    predictions: List[float]
    standard_error: float
    f_statistic: float
    mse: float
    rmse: float

    def predict(self, x: float) -> float:
        result = self.intercept + sum(c * (x ** (i + 1)) for i, c in enumerate(self.coefficients))
        return result

    def as_dict(self) -> Dict[str, float]:
        return {
            "intercept": round(self.intercept, 4),
            "r_squared": round(self.r_squared, 4),
            "adj_r_squared": round(self.adj_r_squared, 4),
            "standard_error": round(self.standard_error, 4),
            "f_statistic": round(self.f_statistic, 4),
            "mse": round(self.mse, 4),
            "rmse": round(self.rmse, 4),
        }


class DataRegressionAnalysisAction:
    """Linear and polynomial regression analysis."""

    def _matrix_multiply(self, a: List[List[float]], b: List[List[float]]) -> List[List[float]]:
        n, m, p = len(a), len(a[0]), len(b[0])
        result = [[0.0] * p for _ in range(n)]
        for i in range(n):
            for j in range(p):
                for k in range(m):
                    result[i][j] += a[i][k] * b[k][j]
        return result

    def _matrix_inverse(self, m: List[List[float]]) -> Optional[List[List[float]]]:
        n = len(m)
        if n == 0:
            return None
        aug = [row[:] + [1.0 if i == j else 0.0 for j in range(n)] for i, row in enumerate(m)]
        for i in range(n):
            pivot = aug[i][i]
            if abs(pivot) < 1e-10:
                for k in range(i + 1, n):
                    if abs(aug[k][i]) > 1e-10:
                        aug[i], aug[k] = aug[k], aug[i]
                        pivot = aug[i][i]
                        break
            if abs(pivot) < 1e-10:
                return None
            for j in range(2 * n):
                aug[i][j] /= pivot
            for k in range(n):
                if k != i:
                    factor = aug[k][i]
                    for j in range(2 * n):
                        aug[k][j] -= factor * aug[i][j]
        return [row[n:] for row in aug]

    def linear_regression(
        self,
        x: List[float],
        y: List[float],
    ) -> Optional[RegressionResult]:
        n = len(x)
        if n != len(y) or n < 3:
            return None
        x_mean = sum(x) / n
        y_mean = sum(y) / n
        ss_xy = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
        ss_xx = sum((xi - x_mean) ** 2 for xi in x)
        if ss_xx == 0:
            return None
        slope = ss_xy / ss_xx
        intercept = y_mean - slope * x_mean
        predictions = [intercept + slope * xi for xi in x]
        residuals = [yi - pi for yi, pi in zip(y, predictions)]
        ss_res = sum(r**2 for r in residuals)
        ss_tot = sum((yi - y_mean) ** 2 for yi in y)
        r_sq = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0
        k = 1
        adj_r_sq = 1 - (1 - r_sq) * (n - 1) / (n - k - 1)
        mse = ss_res / (n - 2)
        rmse = math.sqrt(mse)
        se = math.sqrt(mse)
        ss_reg = sum((pi - y_mean) ** 2 for pi in predictions)
        f_stat = ss_reg / mse if mse > 0 else 0.0
        return RegressionResult(
            coefficients=[slope],
            intercept=intercept,
            r_squared=r_sq,
            adj_r_squared=adj_r_sq,
            residuals=residuals,
            predictions=predictions,
            standard_error=se,
            f_statistic=f_stat,
            mse=mse,
            rmse=rmse,
        )

    def polynomial_regression(
        self,
        x: List[float],
        y: List[float],
        degree: int = 2,
    ) -> Optional[RegressionResult]:
        n = len(x)
        if n != len(y) or n < degree + 2:
            return None
        # Build design matrix X with [1, x, x^2, ...]
        X = [[1.0] + [xi ** j for j in range(1, degree + 1)] for xi in x]
        XT = list(zip(*X))
        XTX = [[sum(a * b for a, b in zip(XT[i], X[j])) for j in range(len(X))] for i in range(len(XT))]
        XTy = [sum(xt * yi for xt, yi in zip(XT[i], y)) for i in range(len(XT))]
        XTX_inv = self._matrix_inverse(XTX)
        if not XTX_inv:
            return None
        coeffs = [sum(row[i] * XTy[i] for i in range(len(row))) for row in XTX_inv]
        intercept = coeffs[0]
        slope_coeffs = coeffs[1:]
        predictions = [sum(coeffs[j] * (xi ** j) for j in range(len(coeffs))) for xi in x]
        residuals = [yi - pi for yi, pi in zip(y, predictions)]
        y_mean = sum(y) / n
        ss_res = sum(r**2 for r in residuals)
        ss_tot = sum((yi - y_mean) ** 2 for yi in y)
        r_sq = 1 - ss_res / ss_tot if ss_tot != 0 else 0.0
        adj_r_sq = 1 - (1 - r_sq) * (n - 1) / (n - degree - 1)
        mse = ss_res / (n - degree - 1)
        rmse = math.sqrt(mse)
        se = math.sqrt(mse)
        ss_reg = sum((pi - y_mean) ** 2 for pi in predictions)
        f_stat = ss_reg / (degree * mse) if mse > 0 else 0.0
        return RegressionResult(
            coefficients=slope_coeffs,
            intercept=intercept,
            r_squared=r_sq,
            adj_r_squared=adj_r_sq,
            residuals=residuals,
            predictions=predictions,
            standard_error=se,
            f_statistic=f_stat,
            mse=mse,
            rmse=rmse,
        )
