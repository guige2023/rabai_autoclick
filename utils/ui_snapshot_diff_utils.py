"""
UI snapshot comparison utilities.

Compare UI snapshots to detect changes and regressions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class DiffRegion:
    """Region of difference between snapshots."""
    x: int
    y: int
    width: int
    height: int
    change_type: str
    similarity: float


@dataclass
class SnapshotDiff:
    """Result of comparing two snapshots."""
    is_identical: bool
    similarity: float
    changed_regions: list[DiffRegion]
    added_elements: int = 0
    removed_elements: int = 0


class PixelMatcher:
    """Match pixels between two image regions."""
    
    def __init__(self, tolerance: int = 10):
        self.tolerance = tolerance
    
    def pixels_match(self, p1: tuple[int, int, int], p2: tuple[int, int, int]) -> bool:
        """Check if two pixels match within tolerance."""
        return (
            abs(p1[0] - p2[0]) <= self.tolerance and
            abs(p1[1] - p2[1]) <= self.tolerance and
            abs(p1[2] - p2[2]) <= self.tolerance
        )


class SnapshotComparator:
    """Compare UI snapshots for differences."""
    
    def __init__(self, pixel_tolerance: int = 10):
        self.pixel_matcher = PixelMatcher(tolerance=pixel_tolerance)
        self._grid_size = 16
    
    def compare(
        self,
        before: list[list[tuple[int, int, int]]],
        after: list[list[tuple[int, int, int]]]
    ) -> SnapshotDiff:
        """Compare two snapshots."""
        if len(before) != len(after) or (before and len(before[0]) != len(after[0])):
            return SnapshotDiff(
                is_identical=False,
                similarity=0.0,
                changed_regions=[],
                added_elements=0,
                removed_elements=0
            )
        
        changed_pixels = []
        
        for y in range(len(before)):
            for x in range(len(before[0])):
                if not self.pixel_matcher.pixels_match(before[y][x], after[y][x]):
                    changed_pixels.append((x, y))
        
        total_pixels = len(before) * len(before[0])
        changed_ratio = len(changed_pixels) / total_pixels if total_pixels > 0 else 0
        similarity = 1.0 - changed_ratio
        
        regions = self._cluster_changes(changed_pixels)
        
        return SnapshotDiff(
            is_identical=len(changed_pixels) == 0,
            similarity=similarity,
            changed_regions=regions
        )
    
    def _cluster_changes(self, pixels: list[tuple[int, int]]) -> list[DiffRegion]:
        """Cluster changed pixels into regions."""
        if not pixels:
            return []
        
        visited = set()
        regions = []
        
        for px, py in pixels:
            if (px, py) in visited:
                continue
            
            region_pixels = self._flood_fill(pixels, px, py, visited)
            
            if region_pixels:
                xs = [p[0] for p in region_pixels]
                ys = [p[1] for p in region_pixels]
                
                regions.append(DiffRegion(
                    x=min(xs),
                    y=min(ys),
                    width=max(xs) - min(xs) + 1,
                    height=max(ys) - min(ys) + 1,
                    change_type="pixel_diff",
                    similarity=0.0
                ))
        
        return regions
    
    def _flood_fill(
        self,
        pixels: list[tuple[int, int]],
        start_x: int,
        start_y: int,
        visited: set
    ) -> list[tuple[int, int]]:
        """Flood fill to find connected changed pixels."""
        pixel_set = set(pixels)
        result = []
        stack = [(start_x, start_y)]
        
        while stack:
            x, y = stack.pop()
            
            if (x, y) in visited or (x, y) not in pixel_set:
                continue
            
            visited.add((x, y))
            result.append((x, y))
            
            stack.append((x + 1, y))
            stack.append((x - 1, y))
            stack.append((x, y + 1))
            stack.append((x, y - 1))
        
        return result
    
    def calculate_ssim(
        self,
        before: list[list[tuple[int, int, int]]],
        after: list[list[tuple[int, int, int]]]
    ) -> float:
        """Calculate Structural Similarity Index."""
        if len(before) != len(after) or len(before[0]) != len(after[0]):
            return 0.0
        
        total_ssim = 0.0
        count = 0
        
        for y in range(len(before)):
            for x in range(len(before[0])):
                ssim = self._ssim_pixel(before[y][x], after[y][x])
                total_ssim += ssim
                count += 1
        
        return total_ssim / count if count > 0 else 0.0
    
    def _ssim_pixel(
        self,
        p1: tuple[int, int, int],
        p2: tuple[int, int, int]
    ) -> float:
        """Calculate SSIM for a single pixel."""
        c1 = (0.01 * 255) ** 2
        c2 = (0.03 * 255) ** 2
        
        l1 = float(p1[0]) / 255
        l2 = float(p2[0]) / 255
        
        l_mean = (l1 + l2) / 2
        l_var = ((l1 - l_mean) ** 2 + (l2 - l_mean) ** 2) / 2
        
        l_score = (2 * l_mean + c1) / (l_mean ** 2 + c1)
        c_score = (2 * math.sqrt(l_var) + c2) / (l_var + c2)
        
        return l_score * c_score
