"""Automation Screenshot Comparison Action Module.

Provides screenshot comparison for visual regression testing with
pixel diffing, perceptual hashing, layout comparison, and automated
baseline management for UI automation workflows.
"""

from __future__ import annotations

import hashlib
import io
import logging
import os
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ComparisonMethod(Enum):
    """Screenshot comparison methods."""
    PIXEL_DIFF = "pixel_diff"
    PERCEPTUAL_HASH = "perceptual_hash"
    HISTOGRAM = "histogram"
    FEATURE_MATCH = "feature_match"
    SSIM = "ssim"


class DiffLevel(Enum):
    """Level of detected differences."""
    IDENTICAL = "identical"
    MINOR = "minor"
    MODERATE = "moderate"
    MAJOR = "major"
    COMPLETELY_DIFFERENT = "completely_different"


@dataclass
class DiffRegion:
    """A region where differences were detected."""
    x: int
    y: int
    width: int
    height: int
    severity: float


@dataclass
class ComparisonResult:
    """Result of screenshot comparison."""
    baseline_id: str
    comparison_id: str
    timestamp: datetime
    method: ComparisonMethod
    diff_level: DiffLevel
    diff_percentage: float
    diff_regions: List[DiffRegion]
    diff_image_path: Optional[str] = None
    baseline_hash: Optional[str] = None
    comparison_hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BaselineMetadata:
    """Metadata for a baseline screenshot."""
    baseline_id: str
    image_path: str
    created_at: datetime
    resolution: Tuple[int, int]
    hash: str
    tags: Set[str] = field(default_factory=set)
    description: Optional[str] = None


@dataclass
class ScreenshotComparisonConfig:
    """Configuration for screenshot comparison."""
    method: ComparisonMethod = ComparisonMethod.PIXEL_DIFF
    threshold: float = 0.01
    diff_regions_enabled: bool = True
    generate_diff_image: bool = True
    perceptual_hash_threshold: float = 0.1
    histogram_threshold: float = 0.05
    baseline_dir: str = "./baselines"
    diff_output_dir: str = "./diffs"


class PerceptualHasher:
    """Perceptual hashing for image similarity."""

    @staticmethod
    def compute_dhash(image_data: bytes) -> str:
        """Compute difference hash for an image."""
        try:
            from PIL import Image
            import numpy as np

            img = Image.open(io.BytesIO(image_data)).convert("L")
            img = img.resize((9, 8), Image.LANCZOS)

            arr = np.array(img)
            diff = arr[:, :-1] < arr[:, 1:]

            bits = "".join("1" if d else "0" for row in diff for d in row)
            return hex(int(bits, 2))[2:]
        except ImportError:
            logger.warning("PIL not available for perceptual hashing")
            return hashlib.md5(image_data).hexdigest()

    @staticmethod
    def hamming_distance(hash1: str, hash2: str) -> int:
        """Calculate Hamming distance between two hashes."""
        if len(hash1) != len(hash2):
            min_len = min(len(hash1), len(hash2))
            return abs(len(hash1) - len(hash2)) * 4 + sum(
                c1 != c2 for c1, c2 in zip(hash1[:min_len], hash2[:min_len])
            )
        return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

    @staticmethod
    def similarity(hash1: str, hash2: str) -> float:
        """Calculate similarity score between 0 and 1."""
        max_dist = len(hash1) * 4
        dist = PerceptualHasher.hamming_distance(hash1, hash2)
        return 1.0 - (dist / max_dist)


class PixelDiffAnalyzer:
    """Analyze pixel-level differences between images."""

    @staticmethod
    def compare(
        baseline_data: bytes,
        comparison_data: bytes,
        threshold: float = 0.01
    ) -> Tuple[float, List[DiffRegion]]:
        """Compare two images and return diff percentage and regions."""
        try:
            from PIL import Image
            import numpy as np

            baseline = Image.open(io.BytesIO(baseline_data)).convert("RGB")
            comparison = Image.open(io.BytesIO(comparison_data)).convert("RGB")

            if baseline.size != comparison.size:
                comparison = comparison.resize(baseline.size, Image.LANCZOS)

            arr_baseline = np.array(baseline)
            arr_comparison = np.array(comparison)

            if arr_baseline.shape != arr_comparison.shape:
                min_shape = tuple(min(s1, s2) for s1, s2 in zip(arr_baseline.shape, arr_comparison.shape))
                arr_baseline = arr_baseline[:min_shape[0], :min_shape[1]]
                arr_comparison = arr_comparison[:min_shape[0], :min_shape[1]]

            diff = np.abs(arr_baseline.astype(int) - arr_comparison.astype(int))
            threshold_array = diff > 10
            diff_pixels = np.sum(np.any(threshold_array, axis=2))
            total_pixels = arr_baseline.shape[0] * arr_baseline.shape[1]

            diff_percentage = diff_pixels / total_pixels if total_pixels > 0 else 0.0

            regions = []
            if diff_percentage >= threshold:
                rows_with_diff = np.any(threshold_array, axis=(1, 2))
                cols_with_diff = np.any(threshold_array, axis=(0, 2))

                if np.any(rows_with_diff) and np.any(cols_with_diff):
                    y_min, y_max = np.where(rows_with_diff)[0][[0, -1]]
                    x_min, x_max = np.where(cols_with_diff)[0][[0, -1]]
                    regions.append(DiffRegion(
                        x=int(x_min), y=int(y_min),
                        width=int(x_max - x_min + 1),
                        height=int(y_max - y_min + 1),
                        severity=float(diff_percentage)
                    ))

            return float(diff_percentage), regions

        except ImportError:
            logger.warning("PIL not available for pixel diff")
            hash1 = hashlib.md5(baseline_data).hexdigest()
            hash2 = hashlib.md5(comparison_data).hexdigest()
            diff_pixels = 1 if hash1 != hash2 else 0
            return float(diff_pixels), []


class HistogramComparator:
    """Compare images using color histograms."""

    @staticmethod
    def compare(baseline_data: bytes, comparison_data: bytes) -> float:
        """Compare using histogram correlation."""
        try:
            from PIL import Image
            import numpy as np

            baseline = Image.open(io.BytesIO(baseline_data)).convert("RGB")
            comparison = Image.open(io.BytesIO(comparison_data)).convert("RGB")

            baseline_hist = np.array(baseline.histogram())
            comparison_hist = np.array(comparison.histogram())

            if len(baseline_hist) != len(comparison_hist):
                return 1.0

            baseline_hist = baseline_hist / (baseline_hist.sum() + 1e-10)
            comparison_hist = comparison_hist / (comparison_hist.sum() + 1e-10)

            correlation = np.corrcoef(baseline_hist, comparison_hist)[0, 1]
            return 1.0 - max(0.0, min(1.0, correlation))

        except ImportError:
            return 0.0


class AutomationScreenshotComparisonAction(BaseAction):
    """Action for screenshot comparison in automation."""

    def __init__(self):
        super().__init__(name="automation_screenshot_comparison")
        self._config = ScreenshotComparisonConfig()
        self._baselines: Dict[str, BaselineMetadata] = {}
        self._results: List[ComparisonResult] = []
        self._lock = threading.Lock()

    def configure(self, config: ScreenshotComparisonConfig):
        """Configure screenshot comparison settings."""
        self._config = config

    def add_baseline(
        self,
        baseline_id: str,
        image_data: bytes,
        tags: Optional[Set[str]] = None,
        description: Optional[str] = None
    ) -> ActionResult:
        """Add a baseline screenshot."""
        try:
            with self._lock:
                if baseline_id in self._baselines:
                    return ActionResult(success=False, error=f"Baseline {baseline_id} already exists")

                from PIL import Image
                img = Image.open(io.BytesIO(image_data))
                resolution = img.size

                img_hash = PerceptualHasher.compute_dhash(image_data)

                os.makedirs(self._config.baseline_dir, exist_ok=True)
                image_path = os.path.join(self._config.baseline_dir, f"{baseline_id}.png")

                with open(image_path, "wb") as f:
                    f.write(image_data)

                metadata = BaselineMetadata(
                    baseline_id=baseline_id,
                    image_path=image_path,
                    created_at=datetime.now(),
                    resolution=resolution,
                    hash=img_hash,
                    tags=tags or set(),
                    description=description
                )

                self._baselines[baseline_id] = metadata
                return ActionResult(success=True, data={
                    "baseline_id": baseline_id,
                    "resolution": resolution
                })
        except Exception as e:
            logger.exception("Failed to add baseline")
            return ActionResult(success=False, error=str(e))

    def compare(
        self,
        baseline_id: str,
        comparison_id: str,
        comparison_data: bytes
    ) -> ComparisonResult:
        """Compare screenshot against baseline."""
        with self._lock:
            if baseline_id not in self._baselines:
                raise ValueError(f"Baseline {baseline_id} not found")

            baseline = self._baselines[baseline_id]

            with open(baseline.image_path, "rb") as f:
                baseline_data = f.read()

        baseline_hash = PerceptualHasher.compute_dhash(baseline_data)
        comparison_hash = PerceptualHasher.compute_dhash(comparison_data)

        if self._config.method == ComparisonMethod.PIXEL_DIFF:
            diff_percentage, regions = PixelDiffAnalyzer.compare(
                baseline_data, comparison_data, self._config.threshold
            )
        elif self._config.method == ComparisonMethod.PERCEPTUAL_HASH:
            similarity = PerceptualHasher.similarity(baseline_hash, comparison_hash)
            diff_percentage = 1.0 - similarity
            regions = []
        elif self._config.method == ComparisonMethod.HISTOGRAM:
            diff_percentage = HistogramComparator.compare(baseline_data, comparison_data)
            regions = []
        else:
            diff_percentage, regions = PixelDiffAnalyzer.compare(
                baseline_data, comparison_data, self._config.threshold
            )

        if diff_percentage < 0.001:
            diff_level = DiffLevel.IDENTICAL
        elif diff_percentage < 0.01:
            diff_level = DiffLevel.MINOR
        elif diff_percentage < 0.05:
            diff_level = DiffLevel.MODERATE
        elif diff_percentage < 0.2:
            diff_level = DiffLevel.MAJOR
        else:
            diff_level = DiffLevel.COMPLETELY_DIFFERENT

        diff_image_path = None
        if self._config.generate_diff_image and diff_percentage >= self._config.threshold:
            diff_image_path = self._generate_diff_image(baseline_data, comparison_data, comparison_id)

        result = ComparisonResult(
            baseline_id=baseline_id,
            comparison_id=comparison_id,
            timestamp=datetime.now(),
            method=self._config.method,
            diff_level=diff_level,
            diff_percentage=diff_percentage,
            diff_regions=regions,
            diff_image_path=diff_image_path,
            baseline_hash=baseline_hash,
            comparison_hash=comparison_hash
        )

        with self._lock:
            self._results.append(result)

        return result

    def _generate_diff_image(
        self,
        baseline_data: bytes,
        comparison_data: bytes,
        comparison_id: str
    ) -> Optional[str]:
        """Generate a visual diff image."""
        try:
            from PIL import Image
            import numpy as np

            baseline = Image.open(io.BytesIO(baseline_data)).convert("RGB")
            comparison = Image.open(io.BytesIO(comparison_data)).convert("RGB")

            if baseline.size != comparison.size:
                comparison = comparison.resize(baseline.size, Image.LANCZOS)

            arr_baseline = np.array(baseline)
            arr_comparison = np.array(comparison)

            diff = np.abs(arr_baseline.astype(int) - arr_comparison.astype(int))
            mask = np.any(diff > 10, axis=2)

            diff_img = np.zeros_like(arr_baseline)
            diff_img[mask] = [255, 0, 0]
            diff_img[~mask] = ((arr_baseline[~mask].astype(float) * 0.5
                               + arr_comparison[~mask].astype(float) * 0.5)).astype(np.uint8)

            os.makedirs(self._config.diff_output_dir, exist_ok=True)
            diff_path = os.path.join(self._config.diff_output_dir, f"diff_{comparison_id}.png")

            result = Image.fromarray(diff_img)
            result.save(diff_path)

            return diff_path

        except Exception as e:
            logger.warning(f"Failed to generate diff image: {e}")
            return None

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute screenshot comparison."""
        try:
            action = params.get("action", "compare")

            if action == "add_baseline":
                import base64
                image_data = params["image_data"]
                if isinstance(image_data, str):
                    image_data = base64.b64decode(image_data)
                return self.add_baseline(
                    params["baseline_id"],
                    image_data,
                    set(params.get("tags", [])),
                    params.get("description")
                )
            elif action == "compare":
                import base64
                comparison_data = params["comparison_data"]
                if isinstance(comparison_data, str):
                    comparison_data = base64.b64decode(comparison_data)
                result = self.compare(
                    params["baseline_id"],
                    params["comparison_id"],
                    comparison_data
                )
                return ActionResult(
                    success=result.diff_level in (DiffLevel.IDENTICAL, DiffLevel.MINOR),
                    data={
                        "baseline_id": result.baseline_id,
                        "comparison_id": result.comparison_id,
                        "diff_level": result.diff_level.value,
                        "diff_percentage": result.diff_percentage,
                        "diff_regions": [
                            {"x": r.x, "y": r.y, "width": r.width, "height": r.height}
                            for r in result.diff_regions
                        ],
                        "diff_image": result.diff_image_path
                    }
                )
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            logger.exception("Screenshot comparison failed")
            return ActionResult(success=False, error=str(e))

    def get_baselines(self) -> List[BaselineMetadata]:
        """Get all registered baselines."""
        with self._lock:
            return list(self._baselines.values())

    def get_results(self) -> List[ComparisonResult]:
        """Get all comparison results."""
        with self._lock:
            return self._results.copy()
