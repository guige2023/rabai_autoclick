"""
Region growing algorithm utilities for element detection.

Implements region growing algorithm to expand from seed points
and group connected pixels/elements based on similarity criteria.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class RegionGrowConfig:
    """Configuration for region growing."""
    threshold: float = 10.0
    max_region_size: int = 10000
    similarity_fn: Optional[Callable[[any, any], float]] = None
    include_diagonal: bool = False


@dataclass
class Region:
    """A grown region containing elements or pixels."""
    id: int
    seed_x: int
    seed_y: int
    pixels: list[tuple[int, int]] = field(default_factory=list)
    avg_color: tuple[float, float, float] = (0.0, 0.0, 0.0)
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)  # x, y, width, height

    @property
    def size(self) -> int:
        return len(self.pixels)

    @property
    def centroid(self) -> tuple[float, float]:
        if not self.pixels:
            return (0.0, 0.0)
        cx = sum(p[0] for p in self.pixels) / len(self.pixels)
        cy = sum(p[1] for p in self.pixels) / len(self.pixels)
        return (cx, cy)


class RegionGrower:
    """Region growing algorithm for pixel/element clustering."""

    def __init__(self, config: Optional[RegionGrowConfig] = None):
        self.config = config or RegionGrowConfig()

    def grow_from_seed(
        self,
        seed_x: int,
        seed_y: int,
        pixel_data: dict[tuple[int, int], any],
        mask: Optional[set[tuple[int, int]]] = None,
    ) -> Region:
        """Grow a region from a seed point.

        Args:
            seed_x: Seed X coordinate
            seed_y: Seed Y coordinate
            pixel_data: Dict mapping (x,y) to pixel values (e.g., color tuple)
            mask: Optional set of allowed coordinates to include

        Returns:
            Grown Region
        """
        if mask is None:
            mask = set(pixel_data.keys())

        seed_value = pixel_data.get((seed_x, seed_y))
        if seed_value is None:
            return Region(id=0, seed_x=seed_x, seed_y=seed_y)

        visited: set[tuple[int, int]] = set()
        region_pixels: list[tuple[int, int]] = []
        queue: list[tuple[int, int]] = [(seed_x, seed_y)]

        directions = [(0, 1), (0, -1), (1, 0), (-1, 0)]
        if self.config.include_diagonal:
            directions += [(1, 1), (1, -1), (-1, 1), (-1, -1)]

        while queue and len(region_pixels) < self.config.max_region_size:
            x, y = queue.pop(0)

            if (x, y) in visited:
                continue
            if (x, y) not in mask:
                continue
            if (x, y) not in pixel_data:
                continue

            pixel_value = pixel_data[(x, y)]
            similarity = self._compute_similarity(seed_value, pixel_value)

            if similarity >= self.config.threshold:
                visited.add((x, y))
                region_pixels.append((x, y))

                for dx, dy in directions:
                    nx, ny = x + dx, y + dy
                    if (nx, ny) not in visited:
                        queue.append((nx, ny))

        # Compute region properties
        region = Region(
            id=0,
            seed_x=seed_x,
            seed_y=seed_y,
            pixels=region_pixels,
        )

        if region_pixels:
            region.bounds = self._compute_bounds(region_pixels)
            region.avg_color = self._compute_avg_color(region_pixels, pixel_data)

        return region

    def grow_multiple_seeds(
        self,
        seeds: list[tuple[int, int]],
        pixel_data: dict[tuple[int, int], any],
        mask: Optional[set[tuple[int, int]]] = None,
    ) -> list[Region]:
        """Grow regions from multiple seeds."""
        regions = []
        for i, seed in enumerate(seeds):
            region = self.grow_from_seed(seed[0], seed[1], pixel_data, mask)
            region.id = i

            # Remove grown region pixels from mask for next seed
            if mask is not None and region.pixels:
                mask = mask - set(region.pixels)

            regions.append(region)
        return regions

    def _compute_similarity(self, val1: any, val2: any) -> float:
        """Compute similarity between two values."""
        if self.config.similarity_fn:
            return self.config.similarity_fn(val1, val2)

        # Default: assume numeric values
        if isinstance(val1, tuple) and isinstance(val2, tuple):
            diff = sum((a - b) ** 2 for a, b in zip(val1, val2)) ** 0.5
            return max(0.0, 100.0 - diff)
        elif isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
            return max(0.0, 100.0 - abs(val1 - val2))

        return 1.0 if val1 == val2 else 0.0

    def _compute_bounds(self, pixels: list[tuple[int, int]]) -> tuple[int, int, int, int]:
        """Compute bounding box of pixels."""
        xs = [p[0] for p in pixels]
        ys = [p[1] for p in pixels]
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        return (min_x, min_y, max_x - min_x + 1, max_y - min_y + 1)

    def _compute_avg_color(
        self,
        pixels: list[tuple[int, int]],
        pixel_data: dict[tuple[int, int], any],
    ) -> tuple[float, float, float]:
        """Compute average color of region."""
        color_sum = [0.0, 0.0, 0.0]
        count = 0
        for px, py in pixels:
            val = pixel_data.get((px, py))
            if isinstance(val, tuple) and len(val) >= 3:
                for i in range(3):
                    color_sum[i] += val[i]
                count += 1

        if count == 0:
            return (0.0, 0.0, 0.0)
        return tuple(c / count for c in color_sum)


__all__ = ["RegionGrower", "Region", "RegionGrowConfig"]
