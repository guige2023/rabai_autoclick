"""Data normalization action for normalizing data values.

Provides various normalization strategies including
min-max, z-score, and log normalization.
"""

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class NormalizationMethod(Enum):
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    LOG = "log"
    ROBUST = "robust"
    DECIMAL = "decimal"


@dataclass
class NormalizationConfig:
    method: NormalizationMethod
    feature_range: tuple[float, float] = (0.0, 1.0)
    epsilon: float = 1e-8


class DataNormalizationAction:
    """Normalize data using various methods.

    Args:
        default_method: Default normalization method.
    """

    def __init__(
        self,
        default_method: NormalizationMethod = NormalizationMethod.MIN_MAX,
    ) -> None:
        self._default_method = default_method
        self._fitted_params: dict[str, dict[str, Any]] = {}

    def fit(
        self,
        values: list[float],
        field_name: str,
        method: Optional[NormalizationMethod] = None,
    ) -> dict[str, Any]:
        """Fit normalization parameters to values.

        Args:
            values: Training values.
            field_name: Field name for storage.
            method: Normalization method.

        Returns:
            Fitted parameters.
        """
        method = method or self._default_method

        if method == NormalizationMethod.MIN_MAX:
            params = {
                "min": min(values),
                "max": max(values),
            }
        elif method == NormalizationMethod.Z_SCORE:
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            params = {
                "mean": mean,
                "std": math.sqrt(variance),
            }
        elif method == NormalizationMethod.LOG:
            positive_values = [v for v in values if v > 0]
            params = {
                "min_positive": min(positive_values) if positive_values else 0,
            }
        elif method == NormalizationMethod.ROBUST:
            sorted_values = sorted(values)
            n = len(sorted_values)
            params = {
                "median": sorted_values[n // 2],
                "q1": sorted_values[n // 4],
                "q3": sorted_values[3 * n // 4],
            }
        elif method == NormalizationMethod.DECIMAL:
            max_val = max(abs(v) for v in values)
            params = {
                "scale": 10 ** len(str(int(max_val))),
            }
        else:
            params = {}

        params["method"] = method.value
        self._fitted_params[field_name] = params
        return params

    def transform(
        self,
        values: list[float],
        field_name: str,
        method: Optional[NormalizationMethod] = None,
    ) -> list[float]:
        """Transform values using fitted parameters.

        Args:
            values: Values to transform.
            field_name: Field name for parameters.
            method: Override method.

        Returns:
            Transformed values.
        """
        params = self._fitted_params.get(field_name)
        if not params:
            logger.warning(f"No fitted params for {field_name}, fitting now")
            if values:
                self.fit(values, field_name, method)
                params = self._fitted_params[field_name]

        method_str = method.value if method else params.get("method", self._default_method.value)
        method_enum = NormalizationMethod(method_str)

        if method_enum == NormalizationMethod.MIN_MAX:
            return self._min_max_transform(values, params)
        elif method_enum == NormalizationMethod.Z_SCORE:
            return self._z_score_transform(values, params)
        elif method_enum == NormalizationMethod.LOG:
            return self._log_transform(values, params)
        elif method_enum == NormalizationMethod.ROBUST:
            return self._robust_transform(values, params)
        elif method_enum == NormalizationMethod.DECIMAL:
            return self._decimal_transform(values, params)

        return values

    def fit_transform(
        self,
        values: list[float],
        field_name: str,
        method: Optional[NormalizationMethod] = None,
    ) -> list[float]:
        """Fit and transform in one step.

        Args:
            values: Values to fit and transform.
            field_name: Field name.
            method: Normalization method.

        Returns:
            Transformed values.
        """
        self.fit(values, field_name, method)
        return self.transform(values, field_name, method)

    def _min_max_transform(
        self,
        values: list[float],
        params: dict[str, Any],
    ) -> list[float]:
        """Apply min-max normalization.

        Args:
            values: Values to transform.
            params: Fitted parameters.

        Returns:
            Normalized values.
        """
        min_val = params["min"]
        max_val = params["max"]
        range_val = max_val - min_val

        if range_val == 0:
            return [0.0] * len(values)

        return [(v - min_val) / range_val for v in values]

    def _z_score_transform(
        self,
        values: list[float],
        params: dict[str, Any],
    ) -> list[float]:
        """Apply z-score normalization.

        Args:
            values: Values to transform.
            params: Fitted parameters.

        Returns:
            Normalized values.
        """
        mean = params["mean"]
        std = params["std"]

        if std == 0:
            return [0.0] * len(values)

        return [(v - mean) / std for v in values]

    def _log_transform(
        self,
        values: list[float],
        params: dict[str, Any],
    ) -> list[float]:
        """Apply log normalization.

        Args:
            values: Values to transform.
            params: Fitted parameters.

        Returns:
            Normalized values.
        """
        epsilon = params.get("min_positive", 1e-8)
        return [math.log(v + epsilon) for v in values]

    def _robust_transform(
        self,
        values: list[float],
        params: dict[str, Any],
    ) -> list[float]:
        """Apply robust normalization.

        Args:
            values: Values to transform.
            params: Fitted parameters.

        Returns:
            Normalized values.
        """
        median = params["median"]
        q1 = params["q1"]
        q3 = params["q3"]
        iqr = q3 - q1

        if iqr == 0:
            return [0.0] * len(values)

        return [(v - median) / iqr for v in values]

    def _decimal_transform(
        self,
        values: list[float],
        params: dict[str, Any],
    ) -> list[float]:
        """Apply decimal scaling normalization.

        Args:
            values: Values to transform.
            params: Fitted parameters.

        Returns:
            Normalized values.
        """
        scale = params.get("scale", 1.0)
        return [v / scale for v in values]

    def inverse_transform(
        self,
        values: list[float],
        field_name: str,
    ) -> list[float]:
        """Inverse transform normalized values.

        Args:
            values: Normalized values.
            field_name: Field name.

        Returns:
            Original scale values.
        """
        params = self._fitted_params.get(field_name)
        if not params:
            raise ValueError(f"No fitted params for {field_name}")

        method_str = params.get("method", self._default_method.value)
        method_enum = NormalizationMethod(method_str)

        if method_enum == NormalizationMethod.MIN_MAX:
            min_val = params["min"]
            max_val = params["max"]
            return [v * (max_val - min_val) + min_val for v in values]

        elif method_enum == NormalizationMethod.Z_SCORE:
            mean = params["mean"]
            std = params["std"]
            return [v * std + mean for v in values]

        elif method_enum == NormalizationMethod.ROBUST:
            median = params["median"]
            q1 = params["q1"]
            q3 = params["q3"]
            iqr = q3 - q1
            return [v * iqr + median for v in values]

        return values

    def get_fitted_params(self, field_name: str) -> Optional[dict[str, Any]]:
        """Get fitted parameters for a field.

        Args:
            field_name: Field name.

        Returns:
            Fitted parameters or None.
        """
        return self._fitted_params.get(field_name)
