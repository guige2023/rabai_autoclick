"""
Visual Comparison Action Module

Provides screenshot comparison, visual diff detection, and
template matching for UI automation verification.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import hashlib
import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ComparisonMethod(Enum):
    """Visual comparison methods."""

    PIXEL_DIFF = "pixel_diff"
    HISTOGRAM = "histogram"
    FEATURE_MATCH = "feature_match"
    TEMPLATE_MATCH = "template_match"
    STRUCTURAL_SIMILARITY = "structural_similarity"


@dataclass
class DiffRegion:
    """Represents a region of difference between two images."""

    x: int
    y: int
    width: int
    height: int
    severity: float  # 0.0 to 1.0
    pixel_count: int


@dataclass
class ComparisonResult:
    """Result of visual comparison."""

    method: ComparisonMethod
    is_match: bool
    similarity_score: float  # 0.0 to 1.0
    diff_regions: List[DiffRegion] = field(default_factory=list)
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TemplateMatch:
    """Represents a template match result."""

    x: int
    y: int
    width: int
    height: int
    confidence: float
    template_name: str


@dataclass
class VisualComparisonConfig:
    """Configuration for visual comparison."""

    match_threshold: float = 0.85
    diff_tolerance: int = 10
    min_diff_region_size: int = 10
    max_diff_regions: int = 100
    enable_ssIM: bool = True
    template_scale_range: Tuple[float, float] = (0.8, 1.2)


class VisualComparator:
    """
    Compares visual elements for UI automation testing.

    Supports pixel diff, histogram comparison, template matching,
    and structural similarity index (SSIM) analysis.
    """

    def __init__(
        self,
        config: Optional[VisualComparisonConfig] = None,
        image_loader: Optional[Callable[[str], Any]] = None,
    ):
        self.config = config or VisualComparisonConfig()
        self.image_loader = image_loader or self._default_image_loader
        self._template_cache: Dict[str, Any] = {}

    def _default_image_loader(self, path: str) -> Any:
        """Default image loader using PIL."""
        try:
            from PIL import Image
            return Image.open(path)
        except ImportError:
            logger.warning("PIL not available, using raw pixel data")
            return None

    def load_image(self, source: Union[str, bytes, Any]) -> Optional[Any]:
        """
        Load an image from various sources.

        Args:
            source: File path, bytes, or image object

        Returns:
            Loaded image object
        """
        if isinstance(source, str):
            return self.image_loader(source)
        elif isinstance(source, bytes):
            try:
                from PIL import Image
                from io import BytesIO
                return Image.open(BytesIO(source))
            except Exception as e:
                logger.error(f"Failed to load image from bytes: {e}")
                return None
        else:
            return source

    def compare_images(
        self,
        image1: Union[str, bytes, Any],
        image2: Union[str, bytes, Any],
        method: ComparisonMethod = ComparisonMethod.PIXEL_DIFF,
    ) -> ComparisonResult:
        """
        Compare two images using the specified method.

        Args:
            image1: First image source
            image2: Second image source
            method: Comparison method to use

        Returns:
            ComparisonResult with match status and details
        """
        img1 = self.load_image(image1)
        img2 = self.load_image(image2)

        if img1 is None or img2 is None:
            return ComparisonResult(
                method=method,
                is_match=False,
                similarity_score=0.0,
                confidence=0.0,
            )

        if method == ComparisonMethod.PIXEL_DIFF:
            return self._pixel_diff_comparison(img1, img2)
        elif method == ComparisonMethod.HISTOGRAM:
            return self._histogram_comparison(img1, img2)
        elif method == ComparisonMethod.STRUCTURAL_SIMILARITY:
            return self._ssim_comparison(img1, img2)
        else:
            logger.warning(f"Unsupported method: {method}, falling back to pixel diff")
            return self._pixel_diff_comparison(img1, img2)

    def _pixel_diff_comparison(
        self, img1: Any, img2: Any
    ) -> ComparisonResult:
        """Compare images using pixel-level difference."""
        try:
            from PIL import Image, ImageChips

            if img1.size != img2.size:
                img2 = img2.resize(img1.size)

            diff_img = ImageChops.difference(img1, img2)
            diff_pixels = list(diff_img.getdata())

            diff_count = 0
            total_pixels = len(diff_pixels)
            diff_regions: List[DiffRegion] = []

            for i, pixel in enumerate(diff_pixels):
                x = i % img1.width
                y = i // img1.width

                if isinstance(pixel, tuple):
                    diff_val = sum(abs(a - b) for a, b in zip(pixel[:3], (0, 0, 0)))
                else:
                    diff_val = abs(pixel)

                if diff_val > self.config.diff_tolerance:
                    diff_count += 1

            similarity = 1.0 - (diff_count / total_pixels if total_pixels > 0 else 1.0)
            is_match = similarity >= self.config.match_threshold

            return ComparisonResult(
                method=ComparisonMethod.PIXEL_DIFF,
                is_match=is_match,
                similarity_score=similarity,
                diff_regions=diff_regions,
                confidence=0.95,
            )
        except Exception as e:
            logger.error(f"Pixel diff comparison failed: {e}")
            return ComparisonResult(
                method=ComparisonMethod.PIXEL_DIFF,
                is_match=False,
                similarity_score=0.0,
                confidence=0.0,
            )

    def _histogram_comparison(
        self, img1: Any, img2: Any
    ) -> ComparisonResult:
        """Compare images using histogram analysis."""
        try:
            from PIL import Image

            if img1.mode != "RGB":
                img1 = img1.convert("RGB")
            if img2.mode != "RGB":
                img2 = img2.convert("RGB")

            if img1.size != img2.size:
                img2 = img2.resize(img1.size)

            hist1 = img1.histogram()
            hist2 = img2.histogram()

            sum_sq_diff = sum((h1 - h2) ** 2 for h1, h2 in zip(hist1, hist2))
            max_sq_diff = sum(h ** 2 for h in hist1)

            similarity = 1.0 - math.sqrt(sum_sq_diff / max_sq_diff) if max_sq_diff > 0 else 0.0
            is_match = similarity >= self.config.match_threshold

            return ComparisonResult(
                method=ComparisonMethod.HISTOGRAM,
                is_match=is_match,
                similarity_score=similarity,
                confidence=0.8,
                metadata={"histogram_bins": len(hist1)},
            )
        except Exception as e:
            logger.error(f"Histogram comparison failed: {e}")
            return ComparisonResult(
                method=ComparisonMethod.HISTOGRAM,
                is_match=False,
                similarity_score=0.0,
                confidence=0.0,
            )

    def _ssim_comparison(
        self, img1: Any, img2: Any
    ) -> ComparisonResult:
        """Compare images using Structural Similarity Index."""
        try:
            import numpy as np

            if hasattr(img1, "tobytes"):
                arr1 = np.array(img1.convert("L"))
            else:
                arr1 = np.array(img1)

            if hasattr(img2, "tobytes"):
                arr2 = np.array(img2.convert("L"))
            else:
                arr2 = np.array(img2)

            if arr1.shape != arr2.shape:
                from PIL import Image
                img2_resized = Image.fromarray(arr2).resize(img1.size)
                arr2 = np.array(img2_resized)

            mean1 = np.mean(arr1)
            mean2 = np.mean(arr2)
            var1 = np.var(arr1)
            var2 = np.var(arr2)
            cov = np.mean((arr1 - mean1) * (arr2 - mean2))

            c1 = (0.01 * 255) ** 2
            c2 = (0.03 * 255) ** 2

            ssim = (
                (2 * mean1 * mean2 + c1)
                * (2 * cov + c2)
                / ((mean1 ** 2 + mean2 ** 2 + c1) * (var1 + var2 + c2))
            )

            return ComparisonResult(
                method=ComparisonMethod.STRUCTURAL_SIMILARITY,
                is_match=ssim >= self.config.match_threshold,
                similarity_score=float(ssim),
                confidence=0.9,
            )
        except Exception as e:
            logger.error(f"SSIM comparison failed: {e}")
            return self._histogram_comparison(img1, img2)

    def register_template(
        self,
        name: str,
        image: Union[str, bytes, Any],
    ) -> bool:
        """
        Register a template image for template matching.

        Args:
            name: Template identifier
            image: Template image source

        Returns:
            True if registration succeeded
        """
        loaded = self.load_image(image)
        if loaded is None:
            return False
        self._template_cache[name] = loaded
        logger.info(f"Registered template: {name}")
        return True

    def find_template(
        self,
        source: Union[str, bytes, Any],
        template_name: str,
    ) -> List[TemplateMatch]:
        """
        Find all occurrences of a template in the source image.

        Args:
            source: Source image to search in
            template_name: Name of registered template

        Returns:
            List of TemplateMatch results
        """
        if template_name not in self._template_cache:
            logger.warning(f"Template not found: {template_name}")
            return []

        source_img = self.load_image(source)
        template = self._template_cache[template_name]

        if source_img is None:
            return []

        matches: List[TemplateMatch] = []

        try:
            import numpy as np
            from PIL import Image

            source_arr = np.array(source_img.convert("L"))
            template_arr = np.array(template.convert("L"))

            if source_arr.shape[0] < template_arr.shape[0] or source_arr.shape[1] < template_arr.shape[1]:
                return []

            h, w = template_arr.shape

            for scale in self.config.template_scale_range:
                scaled_h = int(h * scale)
                scaled_w = int(w * scale)

                if scaled_h > source_arr.shape[0] or scaled_w > source_arr.shape[1]:
                    continue

                scaled_template = np.array(
                    Image.fromarray(template_arr).resize((scaled_w, scaled_h))
                )

                for y in range(0, source_arr.shape[0] - scaled_h, max(scaled_h // 4, 1)):
                    for x in range(0, source_arr.shape[1] - scaled_w, max(scaled_w // 4, 1)):
                        window = source_arr[y : y + scaled_h, x : x + scaled_w]

                        correlation = np.corrcoef(
                            window.flatten(), scaled_template.flatten()
                        )[0, 1]

                        if correlation >= self.config.match_threshold:
                            matches.append(
                                TemplateMatch(
                                    x=x,
                                    y=y,
                                    width=scaled_w,
                                    height=scaled_h,
                                    confidence=float(correlation),
                                    template_name=template_name,
                                )
                            )

        except Exception as e:
            logger.error(f"Template matching failed: {e}")

        return matches

    def compute_image_hash(
        self,
        image: Union[str, bytes, Any],
        algorithm: str = "md5",
    ) -> Optional[str]:
        """
        Compute a hash of an image for quick comparison.

        Args:
            image: Image source
            algorithm: Hash algorithm (md5, sha1, sha256)

        Returns:
            Hex digest of image hash
        """
        img = self.load_image(image)
        if img is None:
            return None

        try:
            import numpy as np
            arr = np.array(img.convert("L").resize((8, 8)))
            hash_input = arr.tobytes()

            if algorithm == "md5":
                return hashlib.md5(hash_input).hexdigest()
            elif algorithm == "sha1":
                return hashlib.sha1(hash_input).hexdigest()
            elif algorithm == "sha256":
                return hashlib.sha256(hash_input).hexdigest()
            else:
                return None
        except Exception as e:
            logger.error(f"Image hashing failed: {e}")
            return None


def create_visual_comparator(
    config: Optional[VisualComparisonConfig] = None,
) -> VisualComparator:
    """Factory function to create a VisualComparator."""
    return VisualComparator(config=config)
