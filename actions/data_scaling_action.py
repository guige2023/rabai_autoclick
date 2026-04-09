"""Data Scaling Action Module.

Scales and normalizes numerical data using various strategies:
min-max, z-score, robust, log, and power transforms.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
import math
import statistics

logger = logging.getLogger(__name__)


@dataclass
class ScaleParams:
    """Parameters for a scaling transform."""
    method: str
    min_val: Optional[float] = None
    max_val: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    median: Optional[float] = None
    iqr: Optional[float] = None


class DataScalingAction:
    """Scales and normalizes numerical data.
    
    Supports multiple scaling strategies with fit/transform
    separation for train/test consistency.
    """

    def fit(self, values: List[float]) -> ScaleParams:
        """Fit scaling parameters from training data.
        
        Args:
            values: Training data to compute parameters from.
        
        Returns:
            ScaleParams to use for transform.
        """
        if not values:
            raise ValueError("Cannot fit on empty data")
        return ScaleParams(
            method="unknown",
            min_val=min(values),
            max_val=max(values),
            mean=statistics.mean(values),
            std=statistics.stdev(values) if len(values) > 1 else 1.0,
            median=statistics.median(values),
            iqr=self._iqr(values),
        )

    def _iqr(self, values: List[float]) -> float:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        return q3 - q1

    def min_max(
        self,
        values: List[float],
        feature_range: Tuple[float, float] = (0.0, 1.0),
    ) -> List[float]:
        """Min-max scaling to a given range.
        
        Args:
            values: Values to scale.
            feature_range: Target (min, max) range.
        
        Returns:
            Scaled values.
        """
        if not values:
            return []
        v_min, v_max = min(values), max(values)
        if v_min == v_max:
            return [feature_range[0]] * len(values)
        target_min, target_max = feature_range
        return [
            target_min + (v - v_min) / (v_max - v_min) * (target_max - target_min)
            for v in values
        ]

    def z_score(self, values: List[float]) -> List[float]:
        """Standard z-score normalization.
        
        Args:
            values: Values to normalize.
        
        Returns:
            Z-score normalized values.
        """
        if not values:
            return []
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 1.0
        if std == 0:
            return [0.0] * len(values)
        return [(v - mean) / std for v in values]

    def robust(self, values: List[float]) -> List[float]:
        """Robust scaling using median and IQR.
        
        Args:
            values: Values to scale.
        
        Returns:
            Robust scaled values.
        """
        if not values:
            return []
        median = statistics.median(values)
        iqr = self._iqr(values)
        if iqr == 0:
            return [0.0] * len(values)
        return [(v - median) / iqr for v in values]

    def log_transform(self, values: List[float]) -> List[float]:
        """Natural log transform.
        
        Args:
            values: Values to transform.
        
        Returns:
            Log-transformed values (clamps negatives to 0).
        """
        result: List[float] = []
        for v in values:
            if v <= 0:
                result.append(0.0)
            else:
                result.append(math.log(v))
        return result

    def power_transform(
        self,
        values: List[float],
        power: float = 0.5,
    ) -> List[float]:
        """Power (Box-Cox style) transform.
        
        Args:
            values: Values to transform.
            power: Power parameter (0.5 = sqrt, 0 = log-like).
        
        Returns:
            Power-transformed values.
        """
        if power == 0:
            return self.log_transform(values)
        offset = abs(min(values)) + 1 if min(values) <= 0 else 0
        return [math.pow(v + offset, power) for v in values]

    def inverse_min_max(
        self,
        scaled: List[float],
        original_min: float,
        original_max: float,
        feature_range: Tuple[float, float] = (0.0, 1.0),
    ) -> List[float]:
        """Inverse min-max transform back to original scale.
        
        Args:
            scaled: Scaled values to inverse transform.
            original_min: Original data minimum.
            original_max: Original data maximum.
            feature_range: The feature range used during scaling.
        
        Returns:
            Values in original scale.
        """
        if original_min == original_max:
            return [original_min] * len(scaled)
        target_min, target_max = feature_range
        return [
            original_min + (v - target_min) / (target_max - target_min) * (original_max - original_min)
            for v in scaled
        ]
