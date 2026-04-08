"""Image analysis utilities for visual difference detection and similarity scoring.

Provides tools for comparing two images, detecting visual changes,
computing similarity metrics, and finding changed regions — useful
for verifying automation results or detecting UI updates.

Example:
    >>> from utils.image_analysis_utils import compare_images, find_differences, similarity_score
    >>> diff = compare_images('before.png', 'after.png')
    >>> print(f"Difference: {diff['percent_diff']:.1f}%")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

__all__ = [
    "ImageDiff",
    "compare_images",
    "find_differences",
    "similarity_score",
    "histogram_similarity",
    "detect_change_regions",
    "ImageAnalyzer",
]


@dataclass
class ImageDiff:
    """Result of comparing two images."""

    width: int
    height: int
    identical: bool
    diff_pixel_count: int
    total_pixels: int
    percent_diff: float
    max_color_distance: float


# Try to import image processing libraries
try:
    import cv2
    import numpy as np

    _CV2_AVAILABLE = True
except ImportError:
    _CV2_AVAILABLE = False


def compare_images(
    image1_path: str | bytes,
    image2_path: str | bytes,
    mode: str = "pixel",
) -> ImageDiff:
    """Compare two images and return a diff summary.

    Args:
        image1_path: Path or bytes for the first image.
        image2_path: Path or bytes for the second image.
        mode: Comparison mode - 'pixel' (default) or 'histogram'.

    Returns:
        ImageDiff with comparison statistics.
    """
    if not _CV2_AVAILABLE:
        return ImageDiff(0, 0, True, 0, 0, 0.0, 0.0)

    img1 = _load_image(image1_path)
    img2 = _load_image(image2_path)

    if img1 is None or img2 is None:
        return ImageDiff(0, 0, False, 0, 0, 100.0, 255.0)

    # Resize to match
    if img1.shape[:2] != img2.shape[:2]:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))

    h, w = img1.shape[:2]
    total_pixels = h * w

    if mode == "pixel":
        # Per-pixel absolute difference
        diff = cv2.absdiff(img1, img2)
        diff_gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
        diff_pixel_count = int((diff_gray > 0).sum())
        max_dist = float(diff_gray.max()) if diff_pixel_count > 0 else 0.0
    else:
        # Histogram-based comparison
        h1 = cv2.calcHist([img1], [0], None, [256], [0, 256])
        h2 = cv2.calcHist([img2], [0], None, [256], [0, 256])
        h1 = cv2.normalize(h1, h1).flatten()
        h2 = cv2.normalize(h2, h2).flatten()
        corr = cv2.compareHist(
            cv2.mat(type(h1)(), 1, h1),
            cv2.mat(type(h2)(), 1, h2),
            cv2.HISTCMP_CORREL,
        )
        diff_pixel_count = int((1 - corr) * total_pixels)
        max_dist = float(1 - corr) * 255

    percent = 100.0 * diff_pixel_count / total_pixels
    return ImageDiff(
        width=w,
        height=h,
        identical=(diff_pixel_count == 0),
        diff_pixel_count=diff_pixel_count,
        total_pixels=total_pixels,
        percent_diff=percent,
        max_color_distance=max_dist,
    )


def similarity_score(
    image1_path: str | bytes,
    image2_path: str | bytes,
) -> float:
    """Compute a similarity score between 0.0 (identical) and 1.0 (completely different).

    Args:
        image1_path: Path or bytes for the first image.
        image2_path: Path or bytes for the second image.

    Returns:
        Similarity score (0.0 = identical, 1.0 = completely different).
    """
    diff = compare_images(image1_path, image2_path)
    return min(1.0, diff.percent_diff / 100.0)


def histogram_similarity(
    image1_path: str | bytes,
    image2_path: str | bytes,
) -> float:
    """Compute histogram-based similarity between two images.

    Returns:
        Similarity score from 0.0 (different) to 1.0 (similar).
    """
    if not _CV2_AVAILABLE:
        return 0.0

    img1 = _load_image(image1_path)
    img2 = _load_image(image2_path)
    if img1 is None or img2 is None:
        return 0.0

    # Compute color histograms for each channel
    similarities = []
    for i in range(3):
        h1 = cv2.calcHist([img1], [i], None, [256], [0, 256])
        h2 = cv2.calcHist([img2], [i], None, [256], [0, 256])
        h1 = cv2.normalize(h1, h1).flatten()
        h2 = cv2.normalize(h2, h2).flatten()
        corr = cv2.compareHist(
            h1.reshape(1, -1).astype("float32"),
            h2.reshape(1, -1).astype("float32"),
            cv2.HISTCMP_CORREL,
        )
        similarities.append(max(0.0, corr))

    return sum(similarities) / len(similarities)


def find_differences(
    image1_path: str | bytes,
    image2_path: str | bytes,
    threshold: int = 30,
) -> list[tuple[int, int]]:
    """Find individual differing pixel coordinates between two images.

    Args:
        image1_path: First image path or bytes.
        image2_path: Second image path or bytes.
        threshold: Color distance threshold for considering a pixel different.

    Returns:
        List of (x, y) coordinates of differing pixels.
    """
    if not _CV2_AVAILABLE:
        return []

    img1 = _load_image(image1_path)
    img2 = _load_image(image2_path)
    if img1 is None or img2 is None:
        return []

    if img1.shape[:2] != img2.shape[:2]:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))

    diff = cv2.absdiff(img1, img2)
    diff_gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
    locations = (diff_gray > threshold).nonzero()

    return [(int(x), int(y)) for y, x in zip(*locations)]


@dataclass
class ChangeRegion:
    """A rectangular region where visual change was detected."""

    x: int
    y: int
    width: int
    height: int
    intensity: float  # 0.0 to 1.0, how much the region changed


def detect_change_regions(
    image1_path: str | bytes,
    image2_path: str | bytes,
    grid_size: int = 32,
    threshold: float = 0.1,
) -> list[ChangeRegion]:
    """Detect rectangular regions of change between two images.

    Uses a grid-based approach to identify changed regions.

    Args:
        image1_path: First image path or bytes.
        image2_path: Second image path or bytes.
        grid_size: Size of grid cells for change detection.
        threshold: Minimum change intensity to report a region.

    Returns:
        List of ChangeRegion objects.
    """
    if not _CV2_AVAILABLE:
        return []

    img1 = _load_image(image1_path)
    img2 = _load_image(image2_path)
    if img1 is None or img2 is None:
        return []

    if img1.shape[:2] != img2.shape[:2]:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))

    diff = cv2.absdiff(img1, img2)
    diff_gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)

    h, w = img1.shape[:2]
    regions: list[ChangeRegion] = []

    for gy in range(0, h, grid_size):
        for gx in range(0, w, grid_size):
            cell = diff_gray[gy : min(gy + grid_size, h), gx : min(gx + grid_size, w)]
            avg_intensity = cell.mean() / 255.0
            if avg_intensity >= threshold:
                regions.append(
                    ChangeRegion(
                        x=gx,
                        y=gy,
                        width=min(grid_size, w - gx),
                        height=min(grid_size, h - gy),
                        intensity=avg_intensity,
                    )
                )

    return regions


class ImageAnalyzer:
    """Stateful image analyzer for batch comparison.

    Example:
        >>> analyzer = ImageAnalyzer('baseline.png')
        >>> analyzer.compare('check1.png')
        >>> analyzer.compare('check2.png')
        >>> for result in analyzer.results:
        ...     print(f"{result.name}: {result.percent_diff:.1f}%")
    """

    def __init__(self, baseline_path: str | bytes):
        self.baseline_path = baseline_path
        self.baseline_image = _load_image(baseline_path)
        self.results: list[ImageDiff] = []
        self.names: list[str] = []

    def compare(self, image_path: str | bytes, name: str = "") -> ImageDiff:
        """Compare the baseline to another image.

        Args:
            image_path: Image to compare.
            name: Optional name for this comparison.

        Returns:
            ImageDiff result.
        """
        diff = compare_images(self.baseline_path, image_path)
        self.results.append(diff)
        self.names.append(name)
        return diff

    def all_identical(self) -> bool:
        return all(r.identical for r in self.results)


# -------------------------------------------------------------------
# Internal helpers
# -------------------------------------------------------------------

def _load_image(source: str | bytes):
    """Load an image from a path or bytes, returning None on failure."""
    if not _CV2_AVAILABLE:
        return None

    if isinstance(source, str):
        return cv2.imread(source, cv2.IMREAD_COLOR)
    elif isinstance(source, bytes):
        import numpy as np

        arr = np.frombuffer(source, dtype=np.uint8)
        return cv2.imdecode(arr, cv2.IMREAD_COLOR)
    return None
