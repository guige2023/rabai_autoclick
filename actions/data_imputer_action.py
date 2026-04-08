"""
Data Imputer Action Module.

Imputes missing values using strategies: mean, median, mode, forward fill,
backward fill, interpolation, KNN, or constant values.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class ImputeResult:
    """Result of imputation."""
    records: list[dict[str, Any]]
    fields_imputed: dict[str, int]
    original_count: int
    imputed_count: int


class DataImputerAction(BaseAction):
    """Impute missing values in datasets."""

    def __init__(self) -> None:
        super().__init__("data_imputer")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Impute missing values in records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - strategy: per_field or global
                - strategies: Dict mapping field -> strategy
                    strategies: mean, median, mode, ffill, bfill, interpolate, constant, knn
                - constant_values: Dict of field -> constant value for constant strategy
                - limit: Max consecutive values to fill (for ffill/bfill)

        Returns:
            ImputeResult with imputed records and statistics
        """
        records = params.get("records", [])
        strategy = params.get("strategy", "per_field")
        global_strategy = params.get("global_strategy", "mean")
        strategies = params.get("strategies", {})
        constant_values = params.get("constant_values", {})
        limit = params.get("limit", None)

        if not records:
            return ImputeResult([], {}, 0, 0)

        all_fields = set()
        for r in records:
            if isinstance(r, dict):
                all_fields.update(r.keys())

        fields_imputed: dict[str, int] = {}

        if strategy == "global":
            for field in all_fields:
                values = [r.get(field) for r in records if isinstance(r, dict) and r.get(field) is not None]
                if not values:
                    continue
                strat = strategies.get(field, global_strategy)
                imputed_count = self._impute_field(records, field, strat, constant_values.get(field), limit)
                if imputed_count > 0:
                    fields_imputed[field] = imputed_count
        else:
            for field in all_fields:
                values = [r.get(field) for r in records if isinstance(r, dict) and r.get(field) is not None]
                if not values:
                    continue
                strat = strategies.get(field, "mean")
                imputed_count = self._impute_field(records, field, strat, constant_values.get(field), limit)
                if imputed_count > 0:
                    fields_imputed[field] = imputed_count

        total_imputed = sum(fields_imputed.values())
        return ImputeResult(records, fields_imputed, len(records), total_imputed)

    def _impute_field(self, records: list[dict], field: str, strategy: str, constant: Any = None, limit: Optional[int] = None) -> int:
        """Impute a single field with given strategy."""
        import statistics

        values = [r.get(field) for r in records if isinstance(r, dict) and r.get(field) is not None]
        if not values:
            return 0

        imputed_count = 0

        if strategy == "mean":
            mean_val = sum(v for v in values if isinstance(v, (int, float))) / len(values) if values else 0
            for r in records:
                if isinstance(r, dict) and r.get(field) is None:
                    r[field] = mean_val
                    imputed_count += 1

        elif strategy == "median":
            num_values = [v for v in values if isinstance(v, (int, float))]
            med = statistics.median(num_values) if num_values else 0
            for r in records:
                if isinstance(r, dict) and r.get(field) is None:
                    r[field] = med
                    imputed_count += 1

        elif strategy == "mode":
            from collections import Counter
            counter = Counter(values)
            mode_val = counter.most_common(1)[0][0] if counter else None
            for r in records:
                if isinstance(r, dict) and r.get(field) is None:
                    r[field] = mode_val
                    imputed_count += 1

        elif strategy == "ffill":
            last_val = None
            consecutive = 0
            for r in records:
                if isinstance(r, dict):
                    if r.get(field) is not None:
                        last_val = r[field]
                        consecutive = 0
                    elif last_val is not None and (limit is None or consecutive < limit):
                        r[field] = last_val
                        imputed_count += 1
                        consecutive += 1

        elif strategy == "bfill":
            last_val = None
            consecutive = 0
            for r in reversed(records):
                if isinstance(r, dict):
                    if r.get(field) is not None:
                        last_val = r[field]
                        consecutive = 0
                    elif last_val is not None and (limit is None or consecutive < limit):
                        r[field] = last_val
                        imputed_count += 1
                        consecutive += 1

        elif strategy == "interpolate":
            prev_val = None
            prev_idx = -1
            for i, r in enumerate(records):
                if isinstance(r, dict):
                    if r.get(field) is not None:
                        if prev_val is not None and prev_idx >= 0:
                            step = (r[field] - prev_val) / (i - prev_idx)
                            for j in range(prev_idx + 1, i):
                                if isinstance(records[j], dict) and records[j].get(field) is None:
                                    records[j][field] = prev_val + step * (j - prev_idx)
                                    imputed_count += 1
                        prev_val = r[field]
                        prev_idx = i

        elif strategy == "constant":
            const_val = constant if constant is not None else ""
            for r in records:
                if isinstance(r, dict) and r.get(field) is None:
                    r[field] = const_val
                    imputed_count += 1

        return imputed_count
