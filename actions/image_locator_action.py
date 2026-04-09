"""
Image Locator Action Module.

Locates elements on screen using template matching and
visual fingerprinting for image-based automation.
"""

import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class MatchResult:
    """Result of an image match search."""
    found: bool
    x: float
    y: float
    confidence: float
    bounds: Tuple[int, int, int, int]


class ImageLocator:
    """Locates images on screen using template matching."""

    def __init__(self, threshold: float = 0.8):
        """
        Initialize image locator.

        Args:
            threshold: Matching confidence threshold (0-1).
        """
        self.threshold = threshold

    def find_in_image(
        self,
        haystack: dict,
        needle: dict,
    ) -> MatchResult:
        """
        Find a template image within a larger image.

        Args:
            haystack: Larger image dict with 'pixels', 'width', 'height'.
            needle: Template image dict with same structure.

        Returns:
            MatchResult with location and confidence.
        """
        h_width = haystack.get("width", 0)
        h_height = haystack.get("height", 0)
        n_width = needle.get("width", 0)
        n_height = needle.get("height", 0)

        if n_width > h_width or n_height > h_height:
            return MatchResult(False, 0, 0, 0.0, (0, 0, 0, 0))

        haystack_pixels = haystack.get("pixels", [])
        needle_pixels = needle.get("pixels", [])

        best_x, best_y, best_conf = 0, 0, 0.0

        step_x = max(1, n_width // 8)
        step_y = max(1, n_height // 8)

        for y in range(0, h_height - n_height, step_y):
            for x in range(0, h_width - n_width, step_x):
                conf = self._match_region(
                    haystack_pixels, needle_pixels,
                    h_width, n_width, n_height, x, y
                )
                if conf > best_conf:
                    best_conf = conf
                    best_x = x
                    best_y = y

        if best_conf >= self.threshold:
            return MatchResult(
                found=True,
                x=best_x,
                y=best_y,
                confidence=best_conf,
                bounds=(best_x, best_y, best_x + n_width, best_y + n_height),
            )

        return MatchResult(False, 0, 0, 0.0, (0, 0, 0, 0))

    def _match_region(
        self,
        haystack: list,
        needle: list,
        haystack_width: int,
        needle_width: int,
        needle_height: int,
        start_x: int,
        start_y: int,
    ) -> float:
        """Compute match confidence for a region."""
        matches = 0
        total = 0

        for ny in range(needle_height):
            for nx in range(needle_width):
                hx = start_x + nx
                hy = start_y + ny
                h_idx = hy * haystack_width + hx
                n_idx = ny * needle_width + nx

                if h_idx < len(haystack) and n_idx < len(needle):
                    if self._pixel_match(haystack[h_idx], needle[n_idx]):
                        matches += 1
                    total += 1

        return matches / total if total > 0 else 0.0

    @staticmethod
    def _pixel_match(p1: Tuple, p2: Tuple, tolerance: int = 30) -> bool:
        """Check if two pixels match within tolerance."""
        if len(p1) != len(p2):
            return False
        return all(abs(int(a) - int(b)) <= tolerance for a, b in zip(p1, p2))

    def find_all_matches(
        self,
        haystack: dict,
        needle: dict,
        max_matches: int = 10,
    ) -> list[MatchResult]:
        """
        Find all occurrences of a template in an image.

        Args:
            haystack: Larger image.
            needle: Template image.
            max_matches: Maximum matches to return.

        Returns:
            List of MatchResult objects.
        """
        h_width = haystack.get("width", 0)
        h_height = haystack.get("height", 0)
        n_width = needle.get("width", 0)
        n_height = needle.get("height", 0)

        if n_width > h_width or n_height > h_height:
            return []

        haystack_pixels = haystack.get("pixels", [])
        needle_pixels = needle.get("pixels", [])

        matches = []

        step_x = max(1, n_width // 4)
        step_y = max(1, n_height // 4)

        for y in range(0, h_height - n_height, step_y):
            for x in range(0, h_width - n_width, step_x):
                conf = self._match_region(
                    haystack_pixels, needle_pixels,
                    h_width, n_width, n_height, x, y
                )

                if conf >= self.threshold:
                    matches.append(MatchResult(
                        found=True,
                        x=x,
                        y=y,
                        confidence=conf,
                        bounds=(x, y, x + n_width, y + n_height),
                    ))

                    if len(matches) >= max_matches:
                        return matches

        return matches
