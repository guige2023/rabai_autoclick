"""
Data Binner Action Module.

Bins continuous data into discrete categories using equal-width,
equal-frequency, or custom bin boundaries. Supports bin labels and edge handling.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class BinningResult:
    """Result of binning operation."""
    records: list[dict[str, Any]]
    bin_edges: list[float]
    bin_labels: list[str]
    distribution: dict[str, int]


class DataBinnerAction(BaseAction):
    """Bin continuous data into discrete categories."""

    def __init__(self) -> None:
        super().__init__("data_binner")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Bin data values.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - field: Field name to bin
                - bins: Number of bins (for equal-width/frequency) or list of edges
                - strategy: equal_width, equal_frequency, custom
                - labels: Custom bin labels
                - include_lowest: Include edge value in first bin
                - right: Right-closed intervals (default: True)

        Returns:
            BinningResult with binned records and distribution
        """
        records = params.get("records", [])
        field = params.get("field", "")
        bins = params.get("bins", 5)
        strategy = params.get("strategy", "equal_width")
        labels = params.get("labels")
        include_lowest = params.get("include_lowest", False)
        right = params.get("right", True)

        if not field or not records:
            return {"error": "Field and records required"}

        values = [r.get(field) for r in records if isinstance(r, dict) and r.get(field) is not None]
        if not values:
            return {"error": f"No values found for field '{field}'"}

        if isinstance(bins, list):
            bin_edges = bins
        elif strategy == "equal_width":
            min_val, max_val = min(values), max(values)
            if min_val == max_val:
                bin_edges = [min_val, min_val + 1]
            else:
                bin_edges = [min_val + (max_val - min_val) * i / bins for i in range(bins + 1)]
        elif strategy == "equal_frequency":
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            bin_edges = [sorted_vals[int(n * i / bins)] for i in range(bins + 1)]
            bin_edges = sorted(set(bin_edges))
        else:
            min_val, max_val = min(values), max(values)
            bin_edges = [min_val + (max_val - min_val) * i / bins for i in range(bins + 1)]

        if labels:
            bin_labels = labels
        else:
            bin_labels = [f"bin_{i}" for i in range(len(bin_edges) - 1)]

        import math
        binned_records = []
        distribution: dict[str, int] = {label: 0 for label in bin_labels}

        for r in records:
            if isinstance(r, dict):
                new_r = dict(r)
                value = r.get(field)

                if value is None:
                    new_r[f"{field}_bin"] = None
                    new_r[f"{field}_bin_index"] = None
                else:
                    bin_idx = self._find_bin(value, bin_edges, right, include_lowest)
                    if bin_idx is not None and bin_idx < len(bin_labels):
                        new_r[f"{field}_bin"] = bin_labels[bin_idx]
                        new_r[f"{field}_bin_index"] = bin_idx
                        distribution[bin_labels[bin_idx]] = distribution.get(bin_labels[bin_idx], 0) + 1
                    else:
                        new_r[f"{field}_bin"] = None
                        new_r[f"{field}_bin_index"] = None

                binned_records.append(new_r)
            else:
                binned_records.append(r)

        return BinningResult(
            records=binned_records,
            bin_edges=bin_edges,
            bin_labels=bin_labels,
            distribution=distribution
        )

    def _find_bin(self, value: float, edges: list[float], right: bool, include_lowest: bool) -> Optional[int]:
        """Find bin index for a value."""
        import math

        if include_lowest and value == edges[0]:
            return 0

        for i in range(len(edges) - 1):
            if right:
                if edges[i] < value <= edges[i + 1]:
                    return i
            else:
                if edges[i] <= value < edges[i + 1]:
                    return i

        if value == edges[-1] and not right:
            return len(edges) - 2

        return None
