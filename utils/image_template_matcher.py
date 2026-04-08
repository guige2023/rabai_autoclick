"""
Image Template Matcher Utility

Matches template images within screenshots for visual automation.
Uses feature-based matching and multi-scale search.

Example:
    >>> matcher = ImageTemplateMatcher()
    >>> result = matcher.find_template("button.png", screenshot)
    >>> if result:
    ...     print(f"Found at {result['x']}, {result['y']}")
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class TemplateMatch:
    """Result of a template match operation."""
    x: int
    y: int
    width: int
    height: int
    confidence: float  # 0.0 to 1.0
    template_name: str


class ImageTemplateMatcher:
    """
    Matches template images within larger screenshots.

    Supports multiple matching methods and confidence thresholds.
    """

    def __init__(
        self,
        default_threshold: float = 0.8,
        multi_scale: bool = True,
    ) -> None:
        self.default_threshold = default_threshold
        self.multi_scale = multi_scale
        self._scale_factors = [0.5, 0.75, 1.0, 1.25, 1.5] if multi_scale else [1.0]

    def find_template(
        self,
        template_path: str,
        screenshot_path: str,
        threshold: Optional[float] = None,
    ) -> Optional[TemplateMatch]:
        """
        Find a template image within a screenshot.

        Args:
            template_path: Path to template image file.
            screenshot_path: Path to screenshot image.
            threshold: Match confidence threshold.

        Returns:
            TemplateMatch with location if found, None otherwise.
        """
        import numpy as np
        from PIL import Image

        threshold = threshold or self.default_threshold

        try:
            template = Image.open(template_path).convert("RGB")
            screenshot = Image.open(screenshot_path).convert("RGB")
            template_arr = np.array(template)
            screen_arr = np.array(screenshot)

            best_match: Optional[TemplateMatch] = None

            for scale in self._scale_factors:
                match = self._match_at_scale(
                    template_arr, screen_arr, template_path, scale
                )
                if match and (best_match is None or match.confidence > best_match.confidence):
                    best_match = match

            if best_match and best_match.confidence >= threshold:
                return best_match
        except Exception:
            pass

        return None

    def _match_at_scale(
        self,
        template: "np.ndarray",
        screenshot: "np.ndarray",
        template_name: str,
        scale: float,
    ) -> Optional[TemplateMatch]:
        """Attempt template matching at a specific scale."""
        import numpy as np
        from PIL import Image

        try:
            h, w = template.shape[:2]
            scaled_h, scaled_w = int(h * scale), int(w * scale)

            if scaled_h > screenshot.shape[0] or scaled_w > screenshot.shape[1]:
                return None

            scaled_template = np.array(
                Image.fromarray(template).resize((scaled_w, scaled_h))
            )

            # Simple normalized cross-correlation
            match_map = self._normalized_cross_correlation(scaled_template, screenshot)

            if match_map.size == 0:
                return None

            max_idx = match_map.argmax()
            max_conf = match_map.flat[max_idx]
            max_y, max_x = np.unravel_index(max_idx, match_map.shape)

            return TemplateMatch(
                x=int(max_x / scale),
                y=int(max_y / scale),
                width=scaled_w,
                height=scaled_h,
                confidence=float(max_conf),
                template_name=template_name,
            )
        except Exception:
            return None

    def _normalized_cross_correlation(
        self,
        template: "np.ndarray",
        image: "np.ndarray",
    ) -> "np.ndarray":
        """Compute NCC between template and image at all positions."""
        import numpy as np

        t_h, t_w = template.shape[:2]
        i_h, i_w = image.shape[:2]

        if t_h > i_h or t_w > i_w:
            return np.array([])

        # Grayscale
        if len(template.shape) == 3:
            template = np.mean(template, axis=2)
        if len(image.shape) == 3:
            image = np.mean(image, axis=2)

        # Normalize template
        t_mean = template.mean()
        t_std = template.std() + 1e-8
        t_norm = (template - t_mean) / t_std

        # Compute NCC at each position
        result = np.zeros((i_h - t_h + 1, i_w - t_w + 1))

        for y in range(result.shape[0]):
            for x in range(result.shape[1]):
                window = image[y:y + t_h, x:x + t_w]
                w_mean = window.mean()
                w_std = window.std() + 1e-8
                w_norm = (window - w_mean) / w_std
                corr = (t_norm * w_norm).sum() / (t_h * t_w)
                result[y, x] = (corr + 1) / 2  # Normalize to 0-1

        return result

    def find_all_templates(
        self,
        template_path: str,
        screenshot_path: str,
        threshold: Optional[float] = None,
        max_results: int = 10,
    ) -> list[TemplateMatch]:
        """
        Find all occurrences of a template in a screenshot.

        Args:
            template_path: Path to template image.
            screenshot_path: Path to screenshot.
            threshold: Match confidence threshold.
            max_results: Maximum number of matches to return.

        Returns:
            List of TemplateMatch objects.
        """
        import numpy as np
        from PIL import Image

        threshold = threshold or self.default_threshold

        try:
            template = Image.open(template_path).convert("RGB")
            screenshot = Image.open(screenshot_path).convert("RGB")
            template_arr = np.array(template)
            screen_arr = np.array(screenshot)

            match_map = self._normalized_cross_correlation(template_arr, screen_arr)

            if match_map.size == 0:
                return []

            h, w = template_arr.shape[:2]
            results: list[TemplateMatch] = []
            flat = match_map.copy()

            while len(results) < max_results:
                max_idx = flat.argmax()
                max_val = flat.flat[max_idx]
                if max_val < threshold:
                    break

                max_y, max_x = np.unravel_index(max_idx, flat.shape)
                results.append(TemplateMatch(
                    x=int(max_x),
                    y=int(max_y),
                    width=w,
                    height=h,
                    confidence=float(max_val),
                    template_name=template_path,
                ))

                # Suppress neighborhood
                y_start = max(0, max_y - h)
                y_end = min(flat.shape[0], max_y + h + 1)
                x_start = max(0, max_x - w)
                x_end = min(flat.shape[1], max_x + w + 1)
                flat[y_start:y_end, x_start:x_end] = 0

            return results
        except Exception:
            return []
