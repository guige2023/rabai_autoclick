"""
Image Recognition Action Module

Provides image-based element recognition, template matching,
and visual pattern detection for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class RecognitionMethod(Enum):
    """Image recognition methods."""

    TEMPLATE_MATCH = "template_match"
    FEATURE_MATCH = "feature_match"
    HISTOGRAM_COMPARE = "histogram_compare"
    EDGE_DETECTION = "edge_detection"
    ML_CLASSIFY = "ml_classify"


@dataclass
class RecognitionResult:
    """Result of image recognition."""

    found: bool
    confidence: float = 0.0
    bounds: Optional[Tuple[int, int, int, int]] = None
    method: RecognitionMethod = RecognitionMethod.TEMPLATE_MATCH
    label: Optional[str] = None


@dataclass
class Template:
    """Image template for matching."""

    name: str
    image_data: Any
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecognitionConfig:
    """Configuration for image recognition."""

    default_threshold: float = 0.8
    multi_match_threshold: float = 0.75
    max_matches: int = 10
    scale_range: Tuple[float, float] = (0.5, 2.0)
    scale_step: float = 0.1
    cache_templates: bool = True
    preprocessing: List[str] = field(default_factory=lambda: ["grayscale"])


class ImageRecognizer:
    """
    Recognizes elements in images using various methods.

    Supports template matching, feature detection, histogram
    comparison, and integrates with ML models for classification.
    """

    def __init__(
        self,
        config: Optional[RecognitionConfig] = None,
        image_loader: Optional[Callable[[Any], Any]] = None,
    ):
        self.config = config or RecognitionConfig()
        self.image_loader = image_loader or self._default_loader
        self._templates: Dict[str, Template] = {}
        self._cache: Dict[str, Any] = {}

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
        except Exception as e:
            logger.error(f"Image load failed: {e}")
            return None

    def register_template(
        self,
        name: str,
        image: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Register an image template.

        Args:
            name: Template identifier
            image: Image data
            metadata: Optional metadata

        Returns:
            True if registration succeeded
        """
        loaded = self.image_loader(image)
        if loaded is None:
            return False

        checksum = self._compute_checksum(loaded)

        template = Template(
            name=name,
            image_data=loaded,
            checksum=checksum,
            metadata=metadata or {},
        )

        self._templates[name] = template
        logger.info(f"Registered template: {name}")
        return True

    def _compute_checksum(self, image: Any) -> Optional[str]:
        """Compute MD5 checksum of image."""
        try:
            if hasattr(image, "tobytes"):
                data = image.tobytes()
            elif hasattr(image, "convert"):
                data = image.convert("RGB").tobytes()
            else:
                return None
            return hashlib.md5(data).hexdigest()
        except Exception:
            return None

    def find_template(
        self,
        source: Any,
        template_name: str,
        method: RecognitionMethod = RecognitionMethod.TEMPLATE_MATCH,
    ) -> RecognitionResult:
        """
        Find a template in source image.

        Args:
            source: Source image
            template_name: Name of registered template
            method: Recognition method to use

        Returns:
            RecognitionResult with match details
        """
        if template_name not in self._templates:
            return RecognitionResult(found=False, method=method)

        source_img = self.image_loader(source)
        template = self._templates[template_name]

        if source_img is None:
            return RecognitionResult(found=False, method=method)

        if method == RecognitionMethod.TEMPLATE_MATCH:
            return self._template_match(source_img, template.image_data)
        elif method == RecognitionMethod.HISTOGRAM_COMPARE:
            return self._histogram_compare(source_img, template.image_data)
        else:
            return self._template_match(source_img, template.image_data)

    def find_all_templates(
        self,
        source: Any,
        template_name: str,
    ) -> List[RecognitionResult]:
        """
        Find all occurrences of a template.

        Args:
            source: Source image
            template_name: Template to find

        Returns:
            List of RecognitionResult for each match
        """
        if template_name not in self._templates:
            return []

        source_img = self.image_loader(source)
        template = self._templates[template_name]

        if source_img is None:
            return []

        return self._find_all_matches(source_img, template.image_data)

    def _template_match(
        self,
        source: Any,
        template: Any,
    ) -> RecognitionResult:
        """Template matching using normalized cross-correlation."""
        try:
            import numpy as np

            source_arr = self._image_to_array(source)
            template_arr = self._image_to_array(template)

            if source_arr.shape[0] < template_arr.shape[0] or source_arr.shape[1] < template_arr.shape[1]:
                return RecognitionResult(found=False, method=RecognitionMethod.TEMPLATE_MATCH)

            best_match = 0.0
            best_location = None

            for scale in self._generate_scales(source_arr, template_arr):
                scaled_template = self._scale_image(template_arr, scale)
                if scaled_template.shape[0] > source_arr.shape[0] or scaled_template.shape[1] > source_arr.shape[1]:
                    continue

                corr = self._normalized_cross_correlation(source_arr, scaled_template)
                max_corr = np.max(corr)

                if max_corr > best_match:
                    best_match = max_corr
                    y, x = np.unravel_index(np.argmax(corr), corr.shape)
                    best_location = (x, y, scaled_template.shape[1], scaled_template.shape[0])

            if best_match >= self.config.default_threshold and best_location:
                return RecognitionResult(
                    found=True,
                    confidence=float(best_match),
                    bounds=best_location,
                    method=RecognitionMethod.TEMPLATE_MATCH,
                    label=self._templates[template_name].name if hasattr(self, '_templates') else None,
                )

            return RecognitionResult(found=False, confidence=float(best_match))

        except Exception as e:
            logger.error(f"Template match failed: {e}")
            return RecognitionResult(found=False)

    def _histogram_compare(
        self,
        source: Any,
        template: Any,
    ) -> RecognitionResult:
        """Compare histograms of images."""
        try:
            import numpy as np

            source_hist = self._compute_histogram(source)
            template_hist = self._compute_histogram(template)

            if source_hist is None or template_hist is None:
                return RecognitionResult(found=False)

            correlation = np.corrcoef(source_hist, template_hist)[0, 1]

            return RecognitionResult(
                found=correlation >= self.config.default_threshold,
                confidence=float(correlation) if not np.isnan(correlation) else 0.0,
                method=RecognitionMethod.HISTOGRAM_COMPARE,
            )
        except Exception as e:
            logger.error(f"Histogram compare failed: {e}")
            return RecognitionResult(found=False)

    def _find_all_matches(
        self,
        source: Any,
        template: Any,
    ) -> List[RecognitionResult]:
        """Find all template matches in source."""
        try:
            import numpy as np

            source_arr = self._image_to_array(source)
            template_arr = self._image_to_array(template)

            corr = self._normalized_cross_correlation(source_arr, template_arr)

            threshold = self.config.multi_match_threshold
            locations = np.where(corr >= threshold)

            results = []
            for y, x in zip(*locations):
                if len(results) >= self.config.max_matches:
                    break
                results.append(RecognitionResult(
                    found=True,
                    confidence=float(corr[y, x]),
                    bounds=(x, y, template_arr.shape[1], template_arr.shape[0]),
                    method=RecognitionMethod.TEMPLATE_MATCH,
                ))

            return results
        except Exception as e:
            logger.error(f"Find all matches failed: {e}")
            return []

    def _image_to_array(self, image: Any) -> Any:
        """Convert PIL Image to numpy array."""
        import numpy as np
        from PIL import Image

        if isinstance(image, np.ndarray):
            return image

        if hasattr(image, "convert"):
            return np.array(image.convert("L"))
        return np.array(image)

    def _compute_histogram(self, image: Any) -> Optional[Any]:
        """Compute image histogram."""
        import numpy as np

        arr = self._image_to_array(image)
        if arr is None:
            return None

        hist, _ = np.histogram(arr.flatten(), bins=256, range=(0, 256))
        return hist

    def _generate_scales(self, source: Any, template: Any) -> List[float]:
        """Generate scale factors for multi-scale matching."""
        import numpy as np

        source_size = np.sqrt(source.shape[0] ** 2 + source.shape[1] ** 2)
        template_size = np.sqrt(template.shape[0] ** 2 + template.shape[1] ** 2)

        scale = source_size / template_size

        min_scale, max_scale = self.config.scale_range
        scales = []

        current = max(min_scale, scale * 0.5)
        while current <= min(max_scale, scale * 2):
            scales.append(current)
            current += self.config.scale_step

        return sorted(set(scales))

    def _scale_image(self, arr: Any, scale: float) -> Any:
        """Scale an image array."""
        import numpy as np
        from PIL import Image

        h, w = arr.shape[:2]
        new_h, new_w = int(h * scale), int(w * scale)

        if len(arr.shape) == 2:
            img = Image.fromarray(arr)
            resized = img.resize((new_w, new_h), Image.BILINEAR)
            return np.array(resized)
        return np.array(Image.fromarray(arr).resize((new_w, new_h)))

    def _normalized_cross_correlation(self, source: Any, template: Any) -> Any:
        """Compute normalized cross-correlation."""
        import numpy as np
        from scipy.signal import correlate2d

        if len(source.shape) == 3:
            source = np.mean(source, axis=2)

        source = source.astype(np.float64)
        template = template.astype(np.float64)

        source_mean = source - np.mean(source)
        template_mean = template - np.mean(template)

        source_std = np.std(source)
        template_std = np.std(template)

        if source_std == 0 or template_std == 0:
            return np.zeros((source.shape[0] - template.shape[0] + 1,
                            source.shape[1] - template.shape[1] + 1))

        source_norm = source_mean / source_std
        template_norm = template_mean / template_std

        corr = correlate2d(source_norm, template_norm, mode="valid")
        corr = corr / (template.shape[0] * template.shape[1])

        return corr

    def get_template(self, name: str) -> Optional[Template]:
        """Get a registered template."""
        return self._templates.get(name)

    def list_templates(self) -> List[str]:
        """List all registered template names."""
        return list(self._templates.keys())

    def unregister_template(self, name: str) -> bool:
        """Unregister a template."""
        if name in self._templates:
            del self._templates[name]
            return True
        return False


def create_image_recognizer(
    config: Optional[RecognitionConfig] = None,
) -> ImageRecognizer:
    """Factory function to create an ImageRecognizer."""
    return ImageRecognizer(config=config)
