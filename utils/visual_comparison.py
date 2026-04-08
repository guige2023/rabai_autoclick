"""
Visual Comparison Utilities.

Compare screenshots and visual elements for automation testing,
including pixel diff, structural similarity, and region matching.

Usage:
    from utils.visual_comparison import VisualComparator, compare_images

    comparator = VisualComparator()
    diff = comparator.compare(img1, img2)
    print(f"Difference: {diff.score}")
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False


@dataclass
class ComparisonResult:
    """Result of comparing two images."""
    score: float  # 0.0 = identical, 1.0 = completely different
    diff_pixels: int
    total_pixels: int
    diff_regions: list = None

    def __post_init__(self) -> None:
        if self.diff_regions is None:
            self.diff_regions = []

    @property
    def percent_different(self) -> float:
        """Return percentage of pixels that differ."""
        if self.total_pixels == 0:
            return 0.0
        return (self.diff_pixels / self.total_pixels) * 100


class VisualComparator:
    """
    Compare images visually for automation testing.

    Supports pixel-level comparison, difference highlighting,
    and threshold-based matching.

    Example:
        comparator = VisualComparator()
        result = comparator.compare(image_a, image_b)
        if result.score < 0.01:
            print("Images are identical")
    """

    def __init__(
        self,
        threshold: int = 10,
    ) -> None:
        """
        Initialize the comparator.

        Args:
            threshold: Pixel difference threshold (0-255).
        """
        self._threshold = threshold

    def compare(
        self,
        img1: "Image.Image",
        img2: "Image.Image",
    ) -> ComparisonResult:
        """
        Compare two images.

        Args:
            img1: First PIL Image.
            img2: Second PIL Image.

        Returns:
            ComparisonResult with score and diff info.
        """
        if not HAS_PIL:
            return ComparisonResult(score=1.0, diff_pixels=0, total_pixels=0)

        if img1.size != img2.size:
            img2 = img2.resize(img1.size)

        pixels1 = list(img1.getdata())
        pixels2 = list(img2.getdata())

        diff_count = 0
        total = len(pixels1)

        for p1, p2 in zip(pixels1, pixels2):
            if isinstance(p1, int):
                p1 = (p1,)
            if isinstance(p2, int):
                p2 = (p2,)

            diff = sum(abs(a - b) for a, b in zip(p1[:3], p2[:3]))
            if diff > self._threshold:
                diff_count += 1

        score = diff_count / total if total > 0 else 0.0

        return ComparisonResult(
            score=score,
            diff_pixels=diff_count,
            total_pixels=total,
        )

    def compare_regions(
        self,
        img1: "Image.Image",
        img2: "Image.Image",
        region: Tuple[int, int, int, int],
    ) -> ComparisonResult:
        """
        Compare specific regions of two images.

        Args:
            img1: First PIL Image.
            img2: Second PIL Image.
            region: (x, y, width, height) region to compare.

        Returns:
            ComparisonResult for the region.
        """
        x, y, w, h = region

        r1 = img1.crop((x, y, x + w, y + h))
        r2 = img2.crop((x, y, x + w, y + h))

        return self.compare(r1, r2)

    def find_diff_regions(
        self,
        img1: "Image.Image",
        img2: "Image.Image",
        block_size: int = 16,
    ) -> List[Tuple[int, int, int, int]]:
        """
        Find rectangular regions where images differ.

        Args:
            img1: First PIL Image.
            img2: Second PIL Image.
            block_size: Size of blocks to check.

        Returns:
            List of (x, y, w, h) regions that differ.
        """
        if not HAS_PIL:
            return []

        if img1.size != img2.size:
            img2 = img2.resize(img1.size)

        regions = []
        w, h = img1.size

        for y in range(0, h, block_size):
            for x in range(0, w, block_size):
                bx = min(x + block_size, w)
                by = min(y + block_size, h)

                r1 = img1.crop((x, y, bx, by))
                r2 = img2.crop((x, y, bx, by))

                result = self.compare(r1, r2)
                if result.score > 0.1:
                    regions.append((x, y, bx - x, by - y))

        return regions

    def highlight_differences(
        self,
        img1: "Image.Image",
        img2: "Image.Image",
        color: Tuple[int, int, int] = (255, 0, 0),
    ) -> "Image.Image":
        """
        Create an image highlighting the differences.

        Args:
            img1: First PIL Image.
            img2: Second PIL Image.
            color: RGB color for highlighting differences.

        Returns:
            PIL Image with differences highlighted.
        """
        if not HAS_PIL:
            return img1

        result = self.compare(img1, img2)

        diff_img = img2.copy()
        overlay = Image.new("RGBA", img1.size, (0, 0, 0, 0))
        from PIL import ImageDraw
        draw = ImageDraw.Draw(overlay)

        regions = self.find_diff_regions(img1, img2)
        for x, y, w, h in regions:
            draw.rectangle([x, y, x + w, y + h], fill=(*color, 128))

        if diff_img.mode != "RGBA":
            diff_img = diff_img.convert("RGBA")

        return Image.alpha_composite(diff_img, overlay)


def compare_images(
    img1: "Image.Image",
    img2: "Image.Image",
) -> ComparisonResult:
    """
    Quick image comparison.

    Args:
        img1: First PIL Image.
        img2: Second PIL Image.

    Returns:
        ComparisonResult.
    """
    comparator = VisualComparator()
    return comparator.compare(img1, img2)
