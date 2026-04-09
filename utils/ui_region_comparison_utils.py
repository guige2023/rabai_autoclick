"""
UI Region Comparison Utilities

Compare UI regions between two snapshots to identify what changed,
useful for detecting UI updates, verifying render results, and
tracking down rendering bugs.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple, Callable


@dataclass
class RegionDiff:
    """Difference found between two UI regions."""
    region_type: str  # 'added', 'removed', 'modified', 'moved'
    x: float
    y: float
    width: float
    height: float
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    confidence: float = 1.0


@dataclass
class RegionComparisonResult:
    """Result of comparing two UI regions."""
    is_identical: bool
    diff_count: int
    added_count: int
    removed_count: int
    modified_count: int
    moved_count: int
    diffs: List[RegionDiff]
    similarity_score: float  # 0.0 to 1.0


def compute_region_overlap(
    ax: float, ay: float, aw: float, ah: float,
    bx: float, by: float, bw: float, bh: float,
) -> float:
    """Compute the IoU (Intersection over Union) of two regions."""
    x1 = max(ax, bx)
    y1 = max(ay, by)
    x2 = min(ax + aw, bx + bw)
    y2 = min(ay + ah, by + bh)

    if x2 <= x1 or y2 <= y1:
        return 0.0

    intersection = (x2 - x1) * (y2 - y1)
    union = aw * ah + bw * bh - intersection
    return intersection / union if union > 0 else 0.0


def compute_pixel_diff_ratio(
    pixels_a: List[float],  # grayscale values
    pixels_b: List[float],
    threshold: float = 0.1,
) -> Tuple[float, int]:
    """
    Compute the ratio of pixels that differ between two images.

    Returns:
        Tuple of (diff_ratio, count_of_different_pixels).
    """
    if len(pixels_a) != len(pixels_b):
        min_len = min(len(pixels_a), len(pixels_b))
        pixels_a = pixels_a[:min_len]
        pixels_b = pixels_b[:min_len]

    different = sum(1 for a, b in zip(pixels_a, pixels_b) if abs(a - b) > threshold)
    return different / len(pixels_a) if pixels_a else 0.0, different


class UIRegionComparator:
    """Compare UI regions to detect changes."""

    def __init__(
        self,
        pixel_diff_threshold: float = 0.1,
        min_diff_ratio_to_report: float = 0.01,
        iou_threshold: float = 0.5,
    ):
        self.pixel_diff_threshold = pixel_diff_threshold
        self.min_diff_ratio_to_report = min_diff_ratio_to_report
        self.iou_threshold = iou_threshold

    def compare_regions(
        self,
        region_a: Tuple[float, float, float, float],  # x, y, w, h
        region_b: Tuple[float, float, float, float],
        pixel_diff_ratio: float,
    ) -> RegionComparisonResult:
        """
        Compare two regions and return diff summary.

        Args:
            region_a: First region bounds.
            region_b: Second region bounds.
            pixel_diff_ratio: Pre-computed pixel difference ratio.

        Returns:
            RegionComparisonResult.
        """
        is_identical = pixel_diff_ratio < self.min_diff_ratio_to_report

        # Compute IoU
        iou = compute_region_overlap(
            *region_a, *region_b
        )

        # Determine type of diff
        ax, ay, aw, ah = region_a
        bx, by, bw, bh = region_b

        diffs: List[RegionDiff] = []

        if iou < 0.1:
            # Regions are essentially different
            if aw * ah > 0 and bw * bh > 0:
                diffs.append(RegionDiff(
                    region_type="modified",
                    x=min(ax, bx),
                    y=min(ay, by),
                    width=max(ax + aw, bx + bw) - min(ax, bx),
                    height=max(ay + ah, by + bh) - min(ay, by),
                    confidence=1.0 - iou,
                ))
        elif abs(ax - bx) > 5 or abs(ay - by) > 5:
            # Moved
            diffs.append(RegionDiff(
                region_type="moved",
                x=bx, y=by, width=bw, height=bh,
                old_value=f"pos=({ax:.0f},{ay:.0f})",
                new_value=f"pos=({bx:.0f},{by:.0f})",
            ))

        added_count = removed_count = modified_count = moved_count = 0
        for d in diffs:
            if d.region_type == "added":
                added_count += 1
            elif d.region_type == "removed":
                removed_count += 1
            elif d.region_type == "modified":
                modified_count += 1
            elif d.region_type == "moved":
                moved_count += 1

        return RegionComparisonResult(
            is_identical=is_identical,
            diff_count=len(diffs),
            added_count=added_count,
            removed_count=removed_count,
            modified_count=modified_count,
            moved_count=moved_count,
            diffs=diffs,
            similarity_score=1.0 - pixel_diff_ratio,
        )
