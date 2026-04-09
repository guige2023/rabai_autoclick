"""
Screenshot Difference Detection Action Module.

Compares screenshots to detect visual changes using pixel
diffing and structural similarity algorithms.
"""

import math
from typing import Optional, Tuple


class ImageComparator:
    """Compares two images and computes similarity metrics."""

    def __init__(self, tolerance: int = 10):
        """
        Initialize comparator.

        Args:
            tolerance: Pixel value tolerance for matching.
        """
        self.tolerance = tolerance

    def pixel_diff_count(self, img1: dict, img2: dict) -> int:
        """
        Count number of differing pixels.

        Args:
            img1: First image as dict with 'width', 'height', 'pixels'.
            img2: Second image.

        Returns:
            Number of differing pixels.
        """
        w1, h1 = img1.get("width", 0), img1.get("height", 0)
        w2, h2 = img2.get("width", 0), img2.get("height", 0)

        if w1 != w2 or h1 != h2:
            return max(w1 * h1, w2 * h2)

        p1 = img1.get("pixels", [])
        p2 = img2.get("pixels", [])

        if len(p1) != len(p2):
            return abs(len(p1) - len(p2))

        diff_count = 0
        for a, b in zip(p1, p2):
            if self._pixel_differs(a, b):
                diff_count += 1
        return diff_count

    def _pixel_differs(self, p1: Tuple, p2: Tuple) -> bool:
        """Check if two pixels differ beyond tolerance."""
        if len(p1) != len(p2):
            return True
        return any(abs(int(a) - int(b)) > self.tolerance for a, b in zip(p1, p2))

    def structural_similarity(self, img1: dict, img2: dict) -> float:
        """
        Compute structural similarity index (simplified SSIM).

        Args:
            img1: First image.
            img2: Second image.

        Returns:
            Similarity score between 0.0 and 1.0.
        """
        w1, h1 = img1.get("width", 0), img1.get("height", 0)
        w2, h2 = img2.get("width", 0), img2.get("height", 0)

        if w1 != w2 or h1 != h2:
            return 0.0

        p1 = img1.get("pixels", [])
        p2 = img2.get("pixels", [])

        if not p1 or not p2:
            return 0.0

        total_pixels = len(p1)
        matching = sum(1 for a, b in zip(p1, p2) if not self._pixel_differs(a, b))
        return matching / total_pixels

    def diff_regions(self, img1: dict, img2: dict, block_size: int = 8) -> list[dict]:
        """
        Find rectangular regions where images differ.

        Args:
            img1: First image.
            img2: Second image.
            block_size: Size of blocks to check.

        Returns:
            List of dicts with 'x', 'y', 'width', 'height' of diff regions.
        """
        w = min(img1.get("width", 0), img2.get("width", 0))
        h = min(img1.get("height", 0), img2.get("height", 0))

        if w == 0 or h == 0:
            return []

        p1 = img1.get("pixels", [])
        p2 = img2.get("pixels", [])

        regions = []
        for y in range(0, h, block_size):
            for x in range(0, w, block_size):
                block_diff = self._block_diff(p1, p2, x, y, w, block_size)
                if block_diff > self.tolerance:
                    regions.append({
                        "x": x,
                        "y": y,
                        "width": block_size,
                        "height": block_size,
                    })
        return regions

    def _block_diff(
        self, p1: list, p2: list, x: int, y: int, width: int, block_size: int
    ) -> float:
        """Compute average difference in a block."""
        diff_sum = 0.0
        count = 0
        for dy in range(block_size):
            for dx in range(block_size):
                px, py = x + dx, y + dy
                if py * width + px < len(p1) and py * width + px < len(p2):
                    idx = py * width + px
                    diff_sum += self._pixel_euclidean(p1[idx], p2[idx])
                    count += 1
        return diff_sum / count if count > 0 else 0.0

    def _pixel_euclidean(self, p1: Tuple, p2: Tuple) -> float:
        """Compute Euclidean distance between pixels."""
        if len(p1) != len(p2):
            return 255.0 * len(p1)
        return math.sqrt(sum((int(a) - int(b)) ** 2 for a, b in zip(p1, p2)))


class ScreenshotDiff:
    """Main class for screenshot difference operations."""

    def __init__(self, tolerance: int = 10, similarity_threshold: float = 0.95):
        """
        Initialize screenshot differ.

        Args:
            tolerance: Pixel tolerance for matching.
            similarity_threshold: Threshold for "similar enough".
        """
        self.comparator = ImageComparator(tolerance=tolerance)
        self.similarity_threshold = similarity_threshold

    def are_similar(self, img1: dict, img2: dict) -> bool:
        """
        Check if two screenshots are similar enough.

        Args:
            img1: First image.
            img2: Second image.

        Returns:
            True if similar, False otherwise.
        """
        sim = self.comparator.structural_similarity(img1, img2)
        return sim >= self.similarity_threshold

    def diff_summary(self, img1: dict, img2: dict) -> dict:
        """
        Get a full diff summary.

        Args:
            img1: First image.
            img2: Second image.

        Returns:
            Dictionary with all diff metrics.
        """
        sim = self.comparator.structural_similarity(img1, img2)
        diff_count = self.comparator.pixel_diff_count(img1, img2)
        regions = self.comparator.diff_regions(img1, img2)

        total_pixels = img1.get("width", 0) * img1.get("height", 0)
        diff_ratio = diff_count / total_pixels if total_pixels > 0 else 1.0

        return {
            "similar": sim >= self.similarity_threshold,
            "similarity": round(sim, 4),
            "diff_pixels": diff_count,
            "diff_ratio": round(diff_ratio, 4),
            "diff_regions": regions,
            "dimensions_match": (
                img1.get("width") == img2.get("width")
                and img1.get("height") == img2.get("height")
            ),
        }
