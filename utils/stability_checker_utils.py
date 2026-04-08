"""
Stability Checker Utilities

Provides utilities for checking UI element stability
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class StabilityReport:
    """Report on element stability."""
    element_id: str
    is_stable: bool
    position_variance: float
    size_variance: float
    samples: int


class StabilityChecker:
    """
    Checks if UI elements are stable.
    
    Monitors element position/size over time
    to detect animations or loading states.
    """

    def __init__(self, threshold: float = 5.0) -> None:
        self._threshold = threshold
        self._samples: dict[str, list[tuple[int, int, int, int]]] = {}

    def add_sample(
        self,
        element_id: str,
        bounds: tuple[int, int, int, int],
    ) -> None:
        """Add a bounds sample for an element."""
        if element_id not in self._samples:
            self._samples[element_id] = []
        self._samples[element_id].append(bounds)
        if len(self._samples[element_id]) > 10:
            self._samples[element_id].pop(0)

    def check_stability(self, element_id: str) -> StabilityReport:
        """
        Check stability of an element.
        
        Args:
            element_id: Element to check.
            
        Returns:
            StabilityReport with analysis.
        """
        samples = self._samples.get(element_id, [])
        if len(samples) < 2:
            return StabilityReport(
                element_id=element_id,
                is_stable=True,
                position_variance=0.0,
                size_variance=0.0,
                samples=len(samples),
            )

        x_vals = [s[0] for s in samples]
        y_vals = [s[1] for s in samples]
        w_vals = [s[2] for s in samples]
        h_vals = [s[3] for s in samples]

        x_var = max(x_vals) - min(x_vals)
        y_var = max(y_vals) - min(y_vals)
        w_var = max(w_vals) - min(w_vals)
        h_var = max(h_vals) - min(h_vals)

        pos_var = (x_var ** 2 + y_var ** 2) ** 0.5
        size_var = w_var + h_var

        return StabilityReport(
            element_id=element_id,
            is_stable=(pos_var <= self._threshold and size_var <= self._threshold),
            position_variance=pos_var,
            size_variance=size_var,
            samples=len(samples),
        )

    def clear_samples(self, element_id: str | None = None) -> None:
        """Clear samples for element or all elements."""
        if element_id:
            self._samples.pop(element_id, None)
        else:
            self._samples.clear()
