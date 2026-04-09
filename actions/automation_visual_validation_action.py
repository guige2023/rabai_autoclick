"""Automation Visual Validation Action Module.

Provides visual comparison and validation for UI automation including
screenshot comparison, layout validation, color verification, and
accessibility visual checks.
"""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Visual validation strictness levels."""
    STRICT = "strict"
    MODERATE = "moderate"
    LENIENT = "lenient"
    SNAPSHOT = "snapshot"


@dataclass
class VisualRegion:
    """A region of interest in an image."""
    x: int
    y: int
    width: int
    height: int
    name: str = ""


@dataclass
class VisualDiff:
    """Difference between two images."""
    left_image: str
    right_image: str
    diff_image: Optional[str] = None
    pixel_diff_count: int = 0
    diff_percentage: float = 0.0
    diff_regions: List[VisualRegion] = field(default_factory=list)


@dataclass
class VisualValidationResult:
    """Result of a visual validation check."""
    passed: bool
    diff: Optional[VisualDiff] = None
    validation_level: ValidationLevel = ValidationLevel.MODERATE
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class VisualConfig:
    """Configuration for visual validation."""
    validation_level: ValidationLevel = ValidationLevel.MODERATE
    pixel_threshold: float = 0.01
    color_tolerance: int = 10
    wait_before_capture_ms: int = 500
    max_retries: int = 3
    generate_diff_image: bool = True


class PixelComparator:
    """Compare images at pixel level."""

    def __init__(self, tolerance: int = 10):
        self._tolerance = tolerance

    def compare_regions(self, img1_data: bytes, img2_data: bytes,
                       region: Optional[VisualRegion] = None) -> Tuple[int, int]:
        """Compare two image regions and return diff pixel count and total pixels."""
        try:
            from PIL import Image
            import numpy as np

            img1 = Image.open(io.BytesIO(img1_data)).convert("RGB")
            img2 = Image.open(io.BytesIO(img2_data)).convert("RGB")

            if img1.size != img2.size:
                img2 = img2.resize(img1.size)

            if region:
                x1, y1 = region.x, region.y
                x2, y2 = x1 + region.width, y1 + region.height
                img1 = img1.crop((x1, y1, x2, y2))
                img2 = img2.crop((x1, y1, x2, y2))

            arr1 = np.array(img1)
            arr2 = np.array(img2)

            if arr1.shape != arr2.shape:
                min_shape = tuple(min(s1, s2) for s1, s2 in zip(arr1.shape, arr2.shape))
                arr1 = arr1[:min_shape[0], :min_shape[1], :min_shape[2] if len(min_shape) > 2 else 3]
                arr2 = arr2[:min_shape[0], :min_shape[1], :min_shape[2] if len(min_shape) > 2 else 3]

            diff = np.abs(arr1.astype(int) - arr2.astype(int))
            diff_pixels = np.sum(np.any(diff > self._tolerance, axis=2))
            total_pixels = arr1.shape[0] * arr1.shape[1]

            return int(diff_pixels), int(total_pixels)

        except ImportError:
            logger.warning("PIL/numpy not available, using hash comparison")
            hash1 = hashlib.md5(img1_data).hexdigest()
            hash2 = hashlib.md5(img2_data).hexdigest()
            return 0 if hash1 == hash2 else 1, 1

    def generate_diff_image(self, img1_data: bytes, img2_data: bytes) -> Optional[bytes]:
        """Generate a diff visualization image."""
        try:
            from PIL import Image
            import numpy as np

            img1 = Image.open(io.BytesIO(img1_data)).convert("RGB")
            img2 = Image.open(io.BytesIO(img2_data)).convert("RGB")

            if img1.size != img2.size:
                img2 = img2.resize(img1.size)

            arr1 = np.array(img1)
            arr2 = np.array(img2)

            diff = np.abs(arr1.astype(int) - arr2.astype(int))
            mask = np.any(diff > self._tolerance, axis=2)

            diff_img = Image.new("RGB", img1.size, (255, 255, 255))
            diff_arr = np.array(diff_img)

            diff_arr[mask] = [255, 0, 0]
            diff_arr[~mask] = (arr1[~mask] * 0.5 + arr2[~mask] * 0.5).astype(np.uint8)

            result = Image.fromarray(diff_arr)
            output = io.BytesIO()
            result.save(output, format="PNG")
            return output.getvalue()

        except ImportError:
            logger.warning("Cannot generate diff image without PIL")
            return None


class LayoutValidator:
    """Validate layout and positioning of UI elements."""

    @staticmethod
    def validate_element_positions(
        reference: Dict[str, Tuple[int, int, int, int]],
        actual: Dict[str, Tuple[int, int, int, int]],
        tolerance: int = 5
    ) -> Dict[str, bool]:
        """Validate that elements are within tolerance of expected positions."""
        results = {}
        for element_id, ref_bounds in reference.items():
            if element_id not in actual:
                results[element_id] = False
                continue

            act_bounds = actual[element_id]
            x_match = abs(ref_bounds[0] - act_bounds[0]) <= tolerance
            y_match = abs(ref_bounds[1] - act_bounds[1]) <= tolerance
            results[element_id] = x_match and y_match

        return results

    @staticmethod
    def calculate_layout_score(
        reference: Dict[str, Any],
        actual: Dict[str, Any]
    ) -> float:
        """Calculate a layout similarity score between 0 and 1."""
        if not reference or not actual:
            return 0.0

        matching_keys = set(reference.keys()) & set(actual.keys())
        total_keys = set(reference.keys()) | set(actual.keys())

        if not total_keys:
            return 1.0

        key_score = len(matching_keys) / len(total_keys)
        position_scores = []

        for key in matching_keys:
            ref = reference.get(key, {})
            act = actual.get(key, {})
            ref_pos = ref.get("position", (0, 0))
            act_pos = act.get("position", (0, 0))
            dist = ((ref_pos[0] - act_pos[0]) ** 2 + (ref_pos[1] - act_pos[1]) ** 2) ** 0.5
            position_scores.append(max(0, 1 - dist / 1000))

        position_score = sum(position_scores) / len(position_scores) if position_scores else 0.0
        return (key_score + position_score) / 2


class AutomationVisualValidationAction(BaseAction):
    """Action for visual validation in automation."""

    def __init__(self):
        super().__init__(name="automation_visual_validation")
        self._config = VisualConfig()
        self._comparator = PixelComparator(tolerance=self._config.color_tolerance)
        self._snapshot_registry: Dict[str, bytes] = {}
        self._validation_history: List[VisualValidationResult] = []

    def configure(self, config: VisualConfig):
        """Configure visual validation settings."""
        self._config = config
        self._comparator = PixelComparator(tolerance=config.color_tolerance)

    def validate(
        self,
        actual_image: bytes,
        reference_image: Optional[bytes] = None,
        reference_id: Optional[str] = None,
        regions: Optional[List[VisualRegion]] = None
    ) -> VisualValidationResult:
        """Validate actual image against reference."""
        if reference_id and reference_id in self._snapshot_registry:
            reference_image = self._snapshot_registry[reference_id]
        elif not reference_image:
            return VisualValidationResult(
                passed=False,
                validation_level=self._config.validation_level,
                metadata={"error": "No reference image provided"}
            )

        total_diff_pixels = 0
        total_pixels = 0
        diff_regions = []

        if regions:
            for region in regions:
                diff_px, total_px = self._comparator.compare_regions(
                    actual_image, reference_image, region
                )
                total_diff_pixels += diff_px
                total_pixels += total_px
                if diff_px > 0:
                    diff_regions.append(region)
        else:
            total_diff_pixels, total_pixels = self._comparator.compare_regions(
                actual_image, reference_image, None
            )

        diff_percentage = total_diff_pixels / total_pixels if total_pixels > 0 else 0.0

        passed = self._evaluate_pass_criteria(diff_percentage, total_diff_pixels)

        diff_image = None
        if self._config.generate_diff_image:
            diff_image = self._comparator.generate_diff_image(actual_image, reference_image)

        diff = VisualDiff(
            diff_image=diff_image,
            pixel_diff_count=total_diff_pixels,
            diff_percentage=diff_percentage,
            diff_regions=diff_regions
        )

        return VisualValidationResult(
            passed=passed,
            diff=diff,
            validation_level=self._config.validation_level,
            metadata={
                "pixel_diff_count": total_diff_pixels,
                "total_pixels": total_pixels,
                "diff_percentage": diff_percentage
            }
        )

    def _evaluate_pass_criteria(self, diff_percentage: float, pixel_count: int) -> bool:
        """Evaluate whether validation passes based on configured level."""
        if self._config.validation_level == ValidationLevel.STRICT:
            return diff_percentage < 0.001 and pixel_count < 10
        elif self._config.validation_level == ValidationLevel.MODERATE:
            return diff_percentage < self._config.pixel_threshold
        elif self._config.validation_level == ValidationLevel.SNAPSHOT:
            return True
        else:
            return diff_percentage < 0.05

    def save_snapshot(self, snapshot_id: str, image_data: bytes):
        """Save a reference snapshot."""
        self._snapshot_registry[snapshot_id] = image_data

    def clear_snapshot(self, snapshot_id: str):
        """Remove a saved snapshot."""
        if snapshot_id in self._snapshot_registry:
            del self._snapshot_registry[snapshot_id]

    def get_history(self) -> List[VisualValidationResult]:
        """Get validation history."""
        return self._validation_history.copy()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute visual validation action."""
        try:
            actual_image = params.get("actual_image")
            if not actual_image:
                return ActionResult(success=False, error="actual_image is required")

            reference_image = params.get("reference_image")
            reference_id = params.get("reference_id")
            regions = params.get("regions")

            result = self.validate(actual_image, reference_image, reference_id, regions)
            self._validation_history.append(result)

            return ActionResult(
                success=result.passed,
                data={
                    "passed": result.passed,
                    "diff_percentage": result.diff.diff_percentage if result.diff else 0.0,
                    "pixel_diff_count": result.diff.pixel_diff_count if result.diff else 0,
                    "validation_level": result.validation_level.value
                }
            )
        except Exception as e:
            logger.exception("Visual validation failed")
            return ActionResult(success=False, error=str(e))
