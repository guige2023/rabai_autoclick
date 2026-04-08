"""Data normalization action module for RabAI AutoClick.

Provides data normalization with multiple strategies:
min-max, z-score, log, decimal, and robust scaling.
"""

import sys
import os
import math
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NormalizeStrategy(Enum):
    """Normalization strategies."""
    MIN_MAX = "min_max"
    Z_SCORE = "z_score"
    LOG = "log"
    DECIMAL = "decimal"
    ROBUST = "robust"
    PERCENTILE = "percentile"


class DataNormalizeAction(BaseAction):
    """Normalize numerical data using various scaling methods.
    
    Supports min-max, z-score, log, decimal, robust,
    and percentile normalization.
    """
    action_type = "data_normalize"
    display_name = "数据归一化"
    description = "数值数据归一化：最小最大/z分数/对数/小数/鲁棒缩放"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Normalize numerical data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: list of numbers or dicts with numeric fields
                - field: str, field name to normalize (for dict data)
                - strategy: str (min_max/z_score/log/decimal/robust/percentile)
                - output_min: float, min value for min_max output
                - output_max: float, max value for min_max output
                - precision: int, decimal precision
                - save_to_var: str
        
        Returns:
            ActionResult with normalized data.
        """
        data = params.get('data', [])
        field = params.get('field', None)
        strategy = params.get('strategy', 'min_max')
        output_min = params.get('output_min', 0.0)
        output_max = params.get('output_max', 1.0)
        precision = params.get('precision', 6)
        save_to_var = params.get('save_to_var', None)

        if not data:
            return ActionResult(success=False, message="No data provided")

        try:
            if isinstance(data[0], dict) and field:
                result = self._normalize_field(data, field, strategy, output_min, output_max, precision)
            elif isinstance(data[0], (int, float)):
                result = self._normalize_list(data, strategy, output_min, output_max, precision)
            else:
                return ActionResult(success=False, message="Unsupported data format")

            if save_to_var and hasattr(context, 'vars'):
                context.vars[save_to_var] = result

            return ActionResult(
                success=True,
                message=f"Normalized {len(result)} values using {strategy}",
                data=result
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalization error: {e}")

    def _normalize_field(
        self, data: List[Dict], field: str, strategy: str,
        output_min: float, output_max: float, precision: int
    ) -> List[Dict]:
        """Normalize a specific field in list of dicts."""
        values = [item.get(field, 0) for item in data]
        normalized = self._normalize_list(values, strategy, output_min, output_max, precision)
        result = []
        for item, norm_val in zip(data, normalized):
            new_item = dict(item)
            new_item[field] = norm_val
            result.append(new_item)
        return result

    def _normalize_list(
        self, values: List, strategy: str,
        output_min: float, output_max: float, precision: int
    ) -> List[float]:
        """Normalize a list of numbers."""
        if not values:
            return []

        values = [float(v) for v in values]

        if strategy == 'min_max':
            min_v, max_v = min(values), max(values)
            if max_v == min_v:
                return [output_min] * len(values)
            return [
                round(output_min + (v - min_v) / (max_v - min_v) * (output_max - output_min), precision)
                for v in values
            ]

        elif strategy == 'z_score':
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = math.sqrt(variance) if variance > 0 else 1.0
            return [round((v - mean) / std, precision) for v in values]

        elif strategy == 'log':
            return [round(math.log1p(v) if v >= 0 else -math.log1p(-v), precision) for v in values]

        elif strategy == 'decimal':
            max_abs = max(abs(v) for v in values)
            if max_abs == 0:
                return [0.0] * len(values)
            divisor = 10 ** len(str(int(max_abs)))
            return [round(v / divisor, precision) for v in values]

        elif strategy == 'robust':
            sorted_vals = sorted(values)
            median = sorted_vals[len(sorted_vals) // 2]
            q1 = sorted_vals[len(sorted_vals) // 4]
            q3 = sorted_vals[3 * len(sorted_vals) // 4]
            iqr = q3 - q1 if q3 != q1 else 1.0
            return [round((v - median) / iqr, precision) for v in values]

        elif strategy == 'percentile':
            sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
            n = len(values)
            result = [0.0] * n
            for rank, idx in enumerate(sorted_indices):
                result[idx] = round((rank + 1) / n, precision)
            return result

        return values

    def get_required_params(self) -> List[str]:
        return ['data', 'strategy']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'output_min': 0.0,
            'output_max': 1.0,
            'precision': 6,
            'save_to_var': None,
        }
