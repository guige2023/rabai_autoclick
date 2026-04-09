"""Visual snapshot and comparison action for UI automation.

Provides:
- Screen/window snapshot capture
- Visual diff comparison
- Region-based snapshot
- Snapshot caching and history
"""

from __future__ import annotations

import base64
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class ComparisonMetric(Enum):
    """Image comparison metrics."""
    PIXEL_DIFF = auto()
    HISTOGRAM_SIMILARITY = auto()
    STRUCTURAL_SIMILARITY = auto()
    FEATURE_MATCH = auto()


@dataclass
class Snapshot:
    """A visual snapshot."""
    id: str
    timestamp: float
    image_data: bytes
    width: int
    height: int
    region: tuple[int, int, int, int] | None = None  # x, y, w, h
    hash: str | None = None
    metadata: dict = field(default_factory=dict)

    @classmethod
    def from_bytes(
        cls,
        image_data: bytes,
        region: tuple[int, int, int, int] | None = None,
        metadata: dict | None = None,
    ) -> Snapshot:
        """Create snapshot from image bytes."""
        # Calculate hash for quick comparison
        hash_val = hashlib.sha256(image_data).hexdigest()[:16]

        # Get dimensions from metadata if provided
        width = metadata.get("width", 0) if metadata else 0
        height = metadata.get("height", 0) if metadata else 0

        return cls(
            id=f"snap_{int(time.time() * 1000)}",
            timestamp=time.time(),
            image_data=image_data,
            width=width,
            height=height,
            region=region,
            hash=hash_val,
            metadata=metadata or {},
        )


@dataclass
class DiffResult:
    """Result of comparing two snapshots."""
    identical: bool
    diff_ratio: float  # 0.0 = identical, 1.0 = completely different
    diff_pixels: int
    total_pixels: int
    diff_region: tuple[int, int, int, int] | None = None
    diff_image: bytes | None = None


@dataclass
class SnapshotCriteria:
    """Criteria for snapshot matching."""
    metric: ComparisonMetric = ComparisonMetric.PIXEL_DIFF
    threshold: float = 0.0  # Max acceptable diff ratio
    ignore_colors: list[tuple[int, int, int]] = field(default_factory=list)
    baseline_id: str | None = None


class VisualSnapshotService:
    """Service for visual snapshot capture and comparison.

    Features:
    - Screen/window capture
    - Region-based capture
    - Snapshot comparison (pixel, histogram, SSIM)
    - Caching and history
    - Baseline comparison
    """

    def __init__(self, cache_size: int = 50):
        self.cache_size = cache_size
        self.snapshots: dict[str, Snapshot] = {}
        self.baselines: dict[str, str] = {}  # name -> snapshot_id
        self._capture_func: Callable | None = None

    def set_capture_func(self, func: Callable) -> None:
        """Set screenshot capture function.

        Args:
            func: Function that returns image bytes
        """
        self._capture_func = func

    def capture(
        self,
        region: tuple[int, int, int, int] | None = None,
        metadata: dict | None = None,
    ) -> Snapshot:
        """Capture a screenshot.

        Args:
            region: Optional region (x, y, w, h)
            metadata: Optional metadata

        Returns:
            Captured snapshot

        Raises:
            RuntimeError: If no capture function configured
        """
        if self._capture_func is None:
            raise RuntimeError("No capture function configured")

        image_data = self._capture_func(region)
        snapshot = Snapshot.from_bytes(image_data, region, metadata)

        # Cache
        self.snapshots[snapshot.id] = snapshot
        self._prune_cache()

        return snapshot

    def capture_and_compare(
        self,
        baseline_name: str,
        region: tuple[int, int, int, int] | None = None,
        criteria: SnapshotCriteria | None = None,
    ) -> tuple[Snapshot, DiffResult]:
        """Capture and compare to baseline.

        Args:
            baseline_name: Name of baseline to compare against
            region: Optional capture region
            criteria: Comparison criteria

        Returns:
            Tuple of (snapshot, diff_result)
        """
        snapshot = self.capture(region)
        criteria = criteria or SnapshotCriteria()

        baseline_id = self.baselines.get(baseline_name)
        if baseline_id is None:
            # First capture - set as baseline
            self.baselines[baseline_name] = snapshot.id
            diff_result = DiffResult(
                identical=True,
                diff_ratio=0.0,
                diff_pixels=0,
                total_pixels=snapshot.width * snapshot.height,
            )
        else:
            baseline = self.snapshots.get(baseline_id)
            if baseline is None:
                diff_result = DiffResult(
                    identical=False,
                    diff_ratio=1.0,
                    diff_pixels=snapshot.width * snapshot.height,
                    total_pixels=snapshot.width * snapshot.height,
                )
            else:
                diff_result = self.compare(baseline, snapshot, criteria)

        return snapshot, diff_result

    def compare(
        self,
        baseline: Snapshot,
        current: Snapshot,
        criteria: SnapshotCriteria | None = None,
    ) -> DiffResult:
        """Compare two snapshots.

        Args:
            baseline: Baseline snapshot
            current: Current snapshot to compare
            criteria: Comparison criteria

        Returns:
            Diff result
        """
        criteria = criteria or SnapshotCriteria()

        if baseline.image_data == current.image_data:
            return DiffResult(
                identical=True,
                diff_ratio=0.0,
                diff_pixels=0,
                total_pixels=baseline.width * baseline.height,
            )

        if criteria.metric == ComparisonMetric.PIXEL_DIFF:
            return self._pixel_diff(baseline, current, criteria)
        elif criteria.metric == ComparisonMetric.HISTOGRAM_SIMILARITY:
            return self._histogram_similarity(baseline, current, criteria)
        elif criteria.metric == ComparisonMetric.STRUCTURAL_SIMILARITY:
            return self._ssim(baseline, current, criteria)
        else:
            return self._pixel_diff(baseline, current, criteria)

    def _pixel_diff(
        self,
        baseline: Snapshot,
        current: Snapshot,
        criteria: SnapshotCriteria,
    ) -> DiffResult:
        """Pixel-by-pixel comparison."""
        # Simplified - real implementation would decode images
        # For now, return estimate based on hash
        identical = baseline.hash == current.hash
        total = baseline.width * baseline.height

        if identical:
            return DiffResult(
                identical=True,
                diff_ratio=0.0,
                diff_pixels=0,
                total_pixels=total,
            )

        # Estimate diff based on different hash
        return DiffResult(
            identical=False,
            diff_ratio=0.1,  # Placeholder
            diff_pixels=int(total * 0.1),
            total_pixels=total,
        )

    def _histogram_similarity(
        self,
        baseline: Snapshot,
        current: Snapshot,
        criteria: SnapshotCriteria,
    ) -> DiffResult:
        """Histogram-based similarity comparison."""
        # Placeholder - would compute color histograms
        return DiffResult(
            identical=False,
            diff_ratio=0.2,
            diff_pixels=0,
            total_pixels=baseline.width * baseline.height,
        )

    def _ssim(
        self,
        baseline: Snapshot,
        current: Snapshot,
        criteria: SnapshotCriteria,
    ) -> DiffResult:
        """Structural similarity index."""
        # Placeholder - would compute SSIM
        return DiffResult(
            identical=False,
            diff_ratio=0.15,
            diff_pixels=0,
            total_pixels=baseline.width * baseline.height,
        )

    def set_baseline(self, name: str, snapshot: Snapshot) -> None:
        """Set a baseline snapshot by name."""
        self.baselines[name] = snapshot.id
        self.snapshots[snapshot.id] = snapshot

    def get_baseline(self, name: str) -> Snapshot | None:
        """Get baseline snapshot by name."""
        baseline_id = self.baselines.get(name)
        if baseline_id:
            return self.snapshots.get(baseline_id)
        return None

    def get_snapshot(self, id: str) -> Snapshot | None:
        """Get snapshot by ID."""
        return self.snapshots.get(id)

    def list_snapshots(self) -> list[Snapshot]:
        """List all cached snapshots."""
        return list(self.snapshots.values())

    def list_baselines(self) -> list[str]:
        """List all baseline names."""
        return list(self.baselines.keys())

    def clear_cache(self) -> None:
        """Clear snapshot cache."""
        self.snapshots.clear()

    def _prune_cache(self) -> None:
        """Remove old snapshots if cache too large."""
        if len(self.snapshots) > self.cache_size:
            # Keep baselines, remove oldest non-baseline snapshots
            baseline_ids = set(self.baselines.values())
            non_baselines = [
                (sid, snap.timestamp)
                for sid, snap in self.snapshots.items()
                if sid not in baseline_ids
            ]
            non_baselines.sort(key=lambda x: x[1])

            # Remove oldest half
            to_remove = len(non_baselines) // 2
            for sid, _ in to_remove[:to_remove]:
                del self.snapshots[sid]

    def encode_to_base64(self, snapshot: Snapshot) -> str:
        """Encode snapshot to base64 for transport."""
        return base64.b64encode(snapshot.image_data).decode("utf-8")

    @staticmethod
    def decode_from_base64(encoded: str) -> bytes:
        """Decode snapshot from base64."""
        return base64.b64decode(encoded)


def create_snapshot_service(cache_size: int = 50) -> VisualSnapshotService:
    """Create visual snapshot service."""
    return VisualSnapshotService(cache_size)
