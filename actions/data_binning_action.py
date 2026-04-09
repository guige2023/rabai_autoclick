"""Data Binning Action Module.

Bins continuous data into discrete categories using equal-width,
equal-frequency, or custom boundary strategies.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
import math
import logging

logger = logging.getLogger(__name__)


@dataclass
class Bin:
    """A single bin with its boundaries."""
    index: int
    label: str
    lower: float
    upper: float
    count: int = 0


class DataBinningAction:
    """Bins continuous numerical data into discrete categories.
    
    Supports equal-width bins, equal-frequency (quantile) bins,
    and custom bin boundaries.
    """

    def __init__(self) -> None:
        self._bins: List[Bin] = []

    def equal_width(
        self,
        values: List[float],
        num_bins: int = 5,
        labels: Optional[List[str]] = None,
    ) -> List[Bin]:
        """Create bins of equal width.
        
        Args:
            values: List of numerical values.
            num_bins: Number of bins to create.
            labels: Optional custom labels for each bin.
        
        Returns:
            List of Bin objects with computed boundaries.
        """
        if not values:
            return []
        min_val = min(values)
        max_val = max(values)
        if min_val == max_val:
            return [Bin(index=0, label=labels[0] if labels else "0", lower=min_val, upper=max_val)]

        width = (max_val - min_val) / num_bins
        bins: List[Bin] = []
        for i in range(num_bins):
            lower = min_val + i * width
            upper = min_val + (i + 1) * width if i < num_bins - 1 else max_val + 1e-9
            label = labels[i] if labels else f"Bin{i+1}"
            bins.append(Bin(index=i, label=label, lower=lower, upper=upper))

        for v in values:
            for b in bins:
                if b.lower <= v < b.upper or (b == bins[-1] and v == max_val):
                    b.count += 1
                    break

        self._bins = bins
        return bins

    def equal_frequency(
        self,
        values: List[float],
        num_bins: int = 5,
        labels: Optional[List[str]] = None,
    ) -> List[Bin]:
        """Create bins with equal number of items (quantiles).
        
        Args:
            values: List of numerical values.
            num_bins: Number of bins.
            labels: Optional custom labels.
        
        Returns:
            List of Bin objects with computed boundaries.
        """
        if not values:
            return []
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        bins: List[Bin] = []
        bin_size = math.ceil(n / num_bins)

        for i in range(num_bins):
            start = i * bin_size
            end = min((i + 1) * bin_size, n)
            if start >= n:
                break
            lower = sorted_vals[start]
            upper = sorted_vals[end - 1] if end > start else lower
            label = labels[i] if labels else f"Q{i+1}"
            bins.append(Bin(
                index=i, label=label,
                lower=lower, upper=upper,
                count=end - start,
            ))

        self._bins = bins
        return bins

    def custom_boundaries(
        self,
        values: List[float],
        boundaries: List[float],
        labels: Optional[List[str]] = None,
    ) -> List[Bin]:
        """Create bins with custom boundary values.
        
        Args:
            values: List of numerical values.
            boundaries: Sorted list of boundary values.
            labels: Optional custom labels.
        
        Returns:
            List of Bin objects.
        """
        if not values or len(boundaries) < 2:
            return []
        bins: List[Bin] = []
        for i in range(len(boundaries) - 1):
            label = labels[i] if labels else f"Bin{i+1}"
            bins.append(Bin(
                index=i, label=label,
                lower=boundaries[i], upper=boundaries[i + 1],
            ))

        for v in values:
            for b in bins:
                if b.lower <= v < b.upper:
                    b.count += 1
                    break
            else:
                # Out of bounds
                pass

        self._bins = bins
        return bins

    def transform(self, value: float) -> Optional[str]:
        """Assign a value to a bin label.
        
        Args:
            value: The value to bin.
        
        Returns:
            Bin label string, or None if out of bounds.
        """
        for b in self._bins:
            if b.lower <= value < b.upper:
                return b.label
        if self._bins and value == self._bins[-1].upper:
            return self._bins[-1].label
        return None

    def transform_batch(self, values: List[float]) -> List[Optional[str]]:
        """Assign values to bin labels in batch."""
        return [self.transform(v) for v in values]

    def get_bin_distribution(self) -> Dict[str, int]:
        """Get count per bin."""
        return {b.label: b.count for b in self._bins}
