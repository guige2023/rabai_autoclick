"""
Adaptive threshold utilities for image matching.

Provides adaptive threshold calculation for image matching
that adjusts based on image characteristics and lighting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import math


@dataclass
class MatchThreshold:
    """Adaptive match threshold with context."""
    threshold: float
    confidence: float
    method: str
    adaptation_level: str  # "low", "medium", "high"


class AdaptiveThresholdEngine:
    """Computes adaptive thresholds for image matching."""

    def __init__(
        self,
        base_threshold: float = 0.8,
        adaptation_enabled: bool = True,
    ):
        self.base_threshold = base_threshold
        self.adaptation_enabled = adaptation_enabled

    def compute_threshold(
        self,
        similarity_score: float,
        image_variance: float,
        edge_density: float,
        brightness: float,
    ) -> MatchThreshold:
        """Compute adaptive threshold based on image characteristics.

        Args:
            similarity_score: Base similarity score
            image_variance: Variance of pixel values (texture indicator)
            edge_density: Ratio of edge pixels
            brightness: Average brightness [0, 1]

        Returns:
            MatchThreshold with adaptive values
        """
        adaptation = "low"
        method = "fixed"

        if not self.adaptation_enabled:
            return MatchThreshold(
                threshold=self.base_threshold,
                confidence=1.0,
                method="fixed",
                adaptation_level="low",
            )

        # High texture images need lower thresholds
        if image_variance > 1000:
            threshold = self.base_threshold - 0.05
            adaptation = "medium"
            method = "texture_adjusted"
        elif image_variance > 5000:
            threshold = self.base_threshold - 0.1
            adaptation = "high"
            method = "high_texture"

        # Very bright or dark images need adjustment
        if brightness < 0.1 or brightness > 0.9:
            threshold = min(1.0, threshold + 0.05)
            adaptation = "high"
            method = "lighting_adjusted"

        # High edge density suggests structured content
        if edge_density > 0.3:
            threshold = min(1.0, threshold + 0.03)

        # Adjust based on similarity score stability
        confidence = self._compute_confidence(similarity_score, image_variance)

        return MatchThreshold(
            threshold=max(0.5, min(1.0, threshold)),
            confidence=confidence,
            method=method,
            adaptation_level=adaptation,
        )

    def _compute_confidence(
        self,
        similarity: float,
        variance: float,
    ) -> float:
        """Compute confidence in the threshold decision."""
        base_confidence = 0.9

        # High variance reduces confidence
        if variance > 5000:
            base_confidence -= 0.2
        elif variance > 1000:
            base_confidence -= 0.1

        # Very high or low similarity increases confidence in decision
        if similarity > 0.9 or similarity < 0.3:
            base_confidence += 0.05

        return max(0.0, min(1.0, base_confidence))

    def should_retry_with_lower_threshold(
        self,
        current_score: float,
        current_threshold: float,
        attempts: int,
    ) -> bool:
        """Determine if matching should be retried with a lower threshold."""
        if attempts >= 3:
            return False

        gap = current_threshold - current_score
        if gap < 0.05:
            return True

        return False

    def compute_pyramid_threshold(
        self,
        level: int,
        base_threshold: float = 0.8,
    ) -> float:
        """Compute threshold for pyramid level (lower resolution = lower threshold).

        Args:
            level: Pyramid level (0 = full resolution)
            base_threshold: Base threshold for level 0

        Returns:
            Threshold for this pyramid level
        """
        reduction = level * 0.02
        return max(0.6, base_threshold - reduction)


__all__ = ["AdaptiveThresholdEngine", "MatchThreshold"]
