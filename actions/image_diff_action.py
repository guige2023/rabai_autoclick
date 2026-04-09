"""
Image Diff Action Module

Compares images and generates visual diffs for
UI testing and automation verification.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class DiffMode(Enum):
    """Image diff modes."""

    PIXEL = "pixel"
    STRUCTURAL = "structural"
    HISTOGRAM = "histogram"
    FEATURE = "feature"


@dataclass
class DiffRegion:
    """Region of difference."""

    x: int
    y: int
    width: int
    height: int
    severity: float = 1.0


@dataclass
class DiffResult:
    """Result of image comparison."""

    is_different: bool
    similarity: float
    diff_regions: List[DiffRegion] = field(default_factory=list)
    diff_percentage: float = 0.0


@dataclass
class ImageDiffConfig:
    """Configuration for image diff."""

    mode: DiffMode = DiffMode.PIXEL
    threshold: float = 0.95
    pixel_tolerance: int = 10
    generate_visual_diff: bool = True


class ImageDiffer:
    """
    Compares images and generates diffs.

    Supports pixel diff, structural similarity,
    histogram comparison, and visual diff generation.
    """

    def __init__(
        self,
        config: Optional[ImageDiffConfig] = None,
        image_loader: Optional[Callable[[Any], Any]] = None,
    ):
        self.config = config or ImageDiffConfig()
        self.image_loader = image_loader or self._default_loader

    def _default_loader(self, source: Any) -> Optional[Any]:
        """Default image loader."""
        try:
            from PIL import Image
            import io

            if isinstance(source, str):
                return Image.open(source)
            elif isinstance(source, bytes):
                return Image.open(io.BytesIO(source))
            return source
        except Exception:
            return None

    def compare(
        self,
        image1: Any,
        image2: Any,
        mode: Optional[DiffMode] = None,
    ) -> DiffResult:
        """
        Compare two images.

        Args:
            image1: First image
            image2: Second image
            mode: Comparison mode

        Returns:
            DiffResult with comparison details
        """
        img1 = self.image_loader(image1)
        img2 = self.image_loader(image2)

        if img1 is None or img2 is None:
            return DiffResult(is_different=True, similarity=0.0)

        mode = mode or self.config.mode

        if mode == DiffMode.PIXEL:
            return self._pixel_diff(img1, img2)
        elif mode == DiffMode.HISTOGRAM:
            return self._histogram_diff(img1, img2)
        elif mode == DiffMode.STRUCTURAL:
            return self._structural_diff(img1, img2)
        else:
            return self._pixel_diff(img1, img2)

    def _pixel_diff(self, img1: Any, img2: Any) -> DiffResult:
        """Pixel-level comparison."""
        import numpy as np

        arr1 = np.array(img1.convert("RGB"))
        arr2 = np.array(img2.convert("RGB"))

        if arr1.shape != arr2.shape:
            from PIL import Image
            img2 = img2.resize(img1.size)
            arr2 = np.array(img2.convert("RGB"))

        diff = np.abs(arr1.astype(int) - arr2.astype(int))
        diff_mask = np.any(diff > self.config.pixel_tolerance, axis=2)

        diff_pixels = np.sum(diff_mask)
        total_pixels = diff_mask.size
        diff_percentage = (diff_pixels / total_pixels) * 100

        similarity = 1.0 - (diff_pixels / total_pixels)

        return DiffResult(
            is_different=diff_pixels > 0,
            similarity=float(similarity),
            diff_percentage=float(diff_percentage),
        )

    def _histogram_diff(self, img1: Any, img2: Any) -> DiffResult:
        """Histogram-based comparison."""
        import numpy as np

        hist1 = np.array(img1.convert("RGB").histogram())
        hist2 = np.array(img2.convert("RGB").histogram())

        if len(hist1) != len(hist2):
            return DiffResult(is_different=True, similarity=0.0)

        correlation = np.corrcoef(hist1, hist2)[0, 1]
        similarity = max(0.0, float(correlation))

        return DiffResult(
            is_different=similarity < self.config.threshold,
            similarity=similarity,
        )

    def _structural_diff(self, img1: Any, img2: Any) -> DiffResult:
        """Structural similarity comparison."""
        import numpy as np

        arr1 = np.array(img1.convert("L"))
        arr2 = np.array(img2.convert("L"))

        if arr1.shape != arr2.shape:
            from PIL import Image
            img2 = img2.resize(img1.size)
            arr2 = np.array(img2.convert("L"))

        mean1, mean2 = arr1.mean(), arr2.mean()
        var1, var2 = arr1.var(), arr2.var()
        cov = np.mean((arr1 - mean1) * (arr2 - mean2))

        c1, c2 = (0.01 * 255) ** 2, (0.03 * 255) ** 2

        ssim = (
            (2 * mean1 * mean2 + c1)
            * (2 * cov + c2)
            / ((mean1 ** 2 + mean2 ** 2 + c1) * (var1 + var2 + c2))
        )

        return DiffResult(
            is_different=float(ssim) < self.config.threshold,
            similarity=float(ssim),
        )

    def generate_visual_diff(
        self,
        image1: Any,
        image2: Any,
        output_path: Optional[str] = None,
    ) -> Optional[bytes]:
        """
        Generate visual diff image.

        Args:
            image1: First image
            image2: Second image
            output_path: Optional output path

        Returns:
            Diff image bytes or None
        """
        if not self.config.generate_visual_diff:
            return None

        img1 = self.image_loader(image1)
        img2 = self.image_loader(image2)

        if img1 is None or img2 is None:
            return None

        try:
            import numpy as np
            from PIL import Image

            arr1 = np.array(img1.convert("RGB"))
            arr2 = np.array(img2.convert("RGB"))

            if arr1.shape != arr2.shape:
                img2 = img2.resize(img1.size)
                arr2 = np.array(img2.convert("RGB"))

            diff = np.abs(arr1.astype(int) - arr2.astype(int))
            diff_mask = np.any(diff > self.config.pixel_tolerance, axis=2)

            diff_img = np.zeros_like(arr1)
            diff_img[diff_mask] = [255, 0, 0]
            diff_img[~diff_mask] = (arr1[~diff_mask] * 0.5).astype(np.uint8)

            diff_pil = Image.fromarray(diff_img.astype(np.uint8))

            if output_path:
                diff_pil.save(output_path)
            else:
                from io import BytesIO
                buf = BytesIO()
                diff_pil.save(buf, format="PNG")
                return buf.getvalue()

        except Exception as e:
            logger.error(f"Visual diff generation failed: {e}")

        return None


def create_image_differ(
    config: Optional[ImageDiffConfig] = None,
) -> ImageDiffer:
    """Factory function."""
    return ImageDiffer(config=config)
