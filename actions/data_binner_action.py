"""
Data Binner Action Module.

Binning and bucketing for continuous data,
supports equal-width, equal-frequency, and custom binning.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass
import logging
import math

logger = logging.getLogger(__name__)


@dataclass
class Bin:
    """Single bin definition."""
    index: int
    label: str
    min_value: float
    max_value: float
    count: int = 0


class DataBinnerAction:
    """
    Data binning for continuous to categorical conversion.

    Supports equal-width, equal-frequency, and
    custom bin definitions.

    Example:
        binner = DataBinnerAction(n_bins=5, strategy="equal_width")
        binner.fit(data)
        binned = binner.transform(data)
        bin_labels = binner.get_bin_labels(data)
    """

    def __init__(
        self,
        n_bins: int = 5,
        strategy: str = "equal_width",
        custom_edges: Optional[list[float]] = None,
    ) -> None:
        self.n_bins = n_bins
        self.strategy = strategy
        self.custom_edges = custom_edges
        self._bins: list[Bin] = []
        self._fitted = False

    def fit(self, data: list[float]) -> "DataBinnerAction":
        """Fit binner to data."""
        if not data:
            return self

        min_val = min(data)
        max_val = max(data)

        if self.strategy == "equal_width":
            self._create_equal_width_bins(min_val, max_val)
        elif self.strategy == "equal_frequency":
            self._create_equal_frequency_bins(data)
        elif self.strategy == "custom" and self.custom_edges:
            self._create_custom_bins(self.custom_edges)
        else:
            self._create_equal_width_bins(min_val, max_val)

        self._fitted = True
        return self

    def transform(
        self,
        data: list[float],
        labels: bool = False,
    ) -> list[Union[int, str]]:
        """Transform data to bin indices or labels."""
        if not self._bins:
            logger.warning("Binner not fitted, returning data as-is")
            return data

        results = []
        for value in data:
            bin_idx = self._find_bin(value)
            if labels:
                results.append(self._bins[bin_idx].label if bin_idx >= 0 else "NA")
            else:
                results.append(bin_idx)

        return results

    def fit_transform(
        self,
        data: list[float],
        labels: bool = False,
    ) -> list[Union[int, str]]:
        """Fit and transform in one step."""
        self.fit(data)
        return self.transform(data, labels)

    def get_bin_labels(
        self,
        data: list[float],
    ) -> list[str]:
        """Get bin labels for data."""
        return self.transform(data, labels=True)

    def get_bin_counts(self) -> dict[str, int]:
        """Get count of items in each bin."""
        counts = {}
        for b in self._bins:
            counts[b.label] = b.count
        return counts

    def _create_equal_width_bins(
        self,
        min_val: float,
        max_val: float,
    ) -> None:
        """Create equal-width bins."""
        if min_val == max_val:
            self._bins = [Bin(0, f"[{min_val}]", min_val, max_val)]
            return

        bin_width = (max_val - min_val) / self.n_bins

        for i in range(self.n_bins):
            bin_min = min_val + i * bin_width
            bin_max = min_val + (i + 1) * bin_width

            if i == self.n_bins - 1:
                bin_max = max_val + 0.001

            self._bins.append(Bin(
                index=i,
                label=f"[{bin_min:.2f}, {bin_max:.2f})",
                min_value=bin_min,
                max_value=bin_max,
            ))

    def _create_equal_frequency_bins(
        self,
        data: list[float],
    ) -> None:
        """Create equal-frequency bins."""
        sorted_data = sorted(data)
        n = len(sorted_data)
        bin_size = n / self.n_bins

        for i in range(self.n_bins):
            start_idx = int(i * bin_size)
            end_idx = int((i + 1) * bin_size)

            if i == self.n_bins - 1:
                end_idx = n

            bin_min = sorted_data[start_idx] if start_idx < n else 0
            bin_max = sorted_data[end_idx - 1] if end_idx > 0 else 0

            self._bins.append(Bin(
                index=i,
                label=f"[{bin_min:.2f}, {bin_max:.2f}]",
                min_value=bin_min,
                max_value=bin_max,
            ))

    def _create_custom_bins(
        self,
        edges: list[float],
    ) -> None:
        """Create bins from custom edge values."""
        for i in range(len(edges) - 1):
            self._bins.append(Bin(
                index=i,
                label=f"[{edges[i]:.2f}, {edges[i+1]:.2f})",
                min_value=edges[i],
                max_value=edges[i + 1],
            ))

    def _find_bin(self, value: float) -> int:
        """Find bin index for a value."""
        for i, bin_def in enumerate(self._bins):
            if bin_def.min_value <= value < bin_def.max_value:
                bin_def.count += 1
                return i

        if self._bins and value >= self._bins[-1].max_value:
            self._bins[-1].count += 1
            return len(self._bins) - 1

        return -1

    @property
    def bin_count(self) -> int:
        """Number of bins."""
        return len(self._bins)

    @property
    def is_fitted(self) -> bool:
        """Whether binner has been fitted."""
        return self._fitted
