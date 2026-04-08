"""
Data Scaler Action Module.

Scales numeric data using standardization (z-score), normalization (min-max),
robust scaling, or log transformation. Handles outliers and preserves data shape.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class ScaleResult:
    """Result of scaling operation."""
    records: list[dict[str, Any]]
    scaled_fields: list[str]
    scaler_params: dict[str, dict[str, float]]


class DataScalerAction(BaseAction):
    """Scale numeric data for ML preprocessing."""

    def __init__(self) -> None:
        super().__init__("data_scaler")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Scale numeric fields in records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - fields: List of field names to scale
                - method: standardization, normalization, robust, log
                - output_suffix: Suffix for scaled fields (default: _scaled)
                - clip_min: Optional clip minimum
                - clip_max: Optional clip maximum

        Returns:
            ScaleResult with scaled records and scaler parameters
        """
        import math

        records = params.get("records", [])
        fields = params.get("fields", [])
        method = params.get("method", "standardization")
        output_suffix = params.get("output_suffix", "_scaled")
        clip_min = params.get("clip_min")
        clip_max = params.get("clip_max")

        if not records or not fields:
            return ScaleResult(records, [], {})

        scaler_params: dict[str, dict[str, float]] = {}

        for field in fields:
            values = [r.get(field) for r in records if isinstance(r, dict) and r.get(field) is not None and isinstance(r.get(field), (int, float))]
            if not values:
                continue

            if method == "standardization":
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = math.sqrt(variance) if variance > 0 else 1.0
                scaler_params[field] = {"mean": mean, "std": std}
                for r in records:
                    if isinstance(r, dict) and isinstance(r.get(field), (int, float)):
                        scaled = (r[field] - mean) / std
                        if clip_min is not None or clip_max is not None:
                            scaled = max(clip_min if clip_min is not None else scaled, min(clip_max if clip_max is not None else scaled, scaled))
                        r[f"{field}{output_suffix}"] = scaled

            elif method == "normalization":
                min_val, max_val = min(values), max(values)
                range_val = max_val - min_val if max_val != min_val else 1.0
                scaler_params[field] = {"min": min_val, "max": max_val}
                for r in records:
                    if isinstance(r, dict) and isinstance(r.get(field), (int, float)):
                        scaled = (r[field] - min_val) / range_val
                        if clip_min is not None or clip_max is not None:
                            scaled = max(clip_min if clip_min is not None else scaled, min(clip_max if clip_max is not None else scaled, scaled))
                        r[f"{field}{output_suffix}"] = scaled

            elif method == "robust":
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                q1 = sorted_vals[n // 4]
                q3 = sorted_vals[3 * n // 4]
                iqr = q3 - q1 if q3 != q1 else 1.0
                median = sorted_vals[n // 2]
                scaler_params[field] = {"median": median, "iqr": iqr}
                for r in records:
                    if isinstance(r, dict) and isinstance(r.get(field), (int, float)):
                        scaled = (r[field] - median) / iqr
                        if clip_min is not None or clip_max is not None:
                            scaled = max(clip_min if clip_min is not None else scaled, min(clip_max if clip_max is not None else scaled, scaled))
                        r[f"{field}{output_suffix}"] = scaled

            elif method == "log":
                scaler_params[field] = {"type": "log"}
                for r in records:
                    if isinstance(r, dict) and isinstance(r.get(field), (int, float)) and r[field] > 0:
                        r[f"{field}{output_suffix}"] = math.log(r[field])
                    elif isinstance(r, dict) and r.get(field) is not None:
                        r[f"{field}{output_suffix}"] = None

        return ScaleResult(
            records=records,
            scaled_fields=fields,
            scaler_params=scaler_params
        )

    def inverse_transform(self, records: list[dict], field: str, scaled_value: float, scaler_params: dict[str, dict[str, float]], method: str = "standardization") -> float:
        """Inverse transform a scaled value."""
        params = scaler_params.get(field, {})
        if method == "standardization":
            mean = params.get("mean", 0)
            std = params.get("std", 1)
            return scaled_value * std + mean
        elif method == "normalization":
            min_val = params.get("min", 0)
            max_val = params.get("max", 1)
            return scaled_value * (max_val - min_val) + min_val
        elif method == "robust":
            median = params.get("median", 0)
            iqr = params.get("iqr", 1)
            return scaled_value * iqr + median
        return scaled_value
