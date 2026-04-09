"""
Image Segmentation Utilities for UI Automation.

This module provides utilities for segmenting images into
regions for targeted element detection and processing.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Dict, Any, Callable
from enum import Enum
import math


class SegmentationStrategy(Enum):
    """Image segmentation strategies."""
    GRID = "grid"
    QUADTREE = "quadtree"
    ADAPTIVE = "adaptive"
    EDGE_BASED = "edge_based"
    REGION_GROW = "region_grow"


@dataclass
class ImageRegion:
    """Represents a rectangular region in an image."""
    x: int
    y: int
    width: int
    height: int
    label: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def area(self) -> int:
        return self.width * self.height

    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.x + self.width, self.y + self.height)

    def contains_point(self, px: int, py: int) -> bool:
        return self.x <= px < self.x + self.width and self.y <= py < self.y + self.height

    def intersection(self, other: 'ImageRegion') -> Optional['ImageRegion']:
        """Calculate intersection with another region."""
        x1 = max(self.x, other.x)
        y1 = max(self.y, other.y)
        x2 = min(self.x + self.width, other.x + other.width)
        y2 = min(self.y + self.height, other.y + other.height)

        if x1 < x2 and y1 < y2:
            return ImageRegion(
                x=x1, y=y1,
                width=x2 - x1, height=y2 - y1
            )
        return None

    def union(self, other: 'ImageRegion') -> 'ImageRegion':
        """Calculate minimum bounding region containing both."""
        x1 = min(self.x, other.x)
        y1 = min(self.y, other.y)
        x2 = max(self.x + self.width, other.x + other.width)
        y2 = max(self.y + self.height, other.y + other.height)
        return ImageRegion(x=x1, y=y1, width=x2 - x1, height=y2 - y1)


@dataclass
class SegmentationResult:
    """Result of image segmentation."""
    regions: List[ImageRegion]
    strategy: SegmentationStrategy
    total_pixels: int
    coverage_ratio: float


class GridSegmenter:
    """
    Segment image into grid regions.
    """

    def __init__(self, cell_width: int, cell_height: int):
        """
        Initialize grid segmenter.

        Args:
            cell_width: Width of each cell
            cell_height: Height of each cell
        """
        self.cell_width = cell_width
        self.cell_height = cell_height

    def segment(
        self,
        image_width: int,
        image_height: int,
        overlap: int = 0
    ) -> SegmentationResult:
        """
        Segment image into grid.

        Args:
            image_width: Image width
            image_height: Image height
            overlap: Overlap between cells (pixels)

        Returns:
            SegmentationResult
        """
        regions = []

        for y in range(0, image_height, self.cell_height - overlap):
            for x in range(0, image_width, self.cell_width - overlap):
                width = min(self.cell_width, image_width - x)
                height = min(self.cell_height, image_height - y)

                regions.append(ImageRegion(
                    x=x, y=y,
                    width=width, height=height,
                    label=f"grid_{x}_{y}"
                ))

        total_pixels = image_width * image_height
        covered = sum(r.area for r in regions)

        return SegmentationResult(
            regions=regions,
            strategy=SegmentationStrategy.GRID,
            total_pixels=total_pixels,
            coverage_ratio=covered / total_pixels if total_pixels > 0 else 0.0
        )


class QuadtreeSegmenter:
    """
    Segment image using quadtree subdivision.
    """

    def __init__(
        self,
        min_region_size: int = 64,
        max_depth: int = 6
    ):
        """
        Initialize quadtree segmenter.

        Args:
            min_region_size: Minimum region dimension
            max_depth: Maximum subdivision depth
        """
        self.min_region_size = min_region_size
        self.max_depth = max_depth

    def segment(
        self,
        image_width: int,
        image_height: int,
        should_split: Optional[Callable[[ImageRegion], bool]] = None
    ) -> SegmentationResult:
        """
        Segment image using quadtree.

        Args:
            image_width: Image width
            image_height: Image height
            should_split: Optional function to determine if region should split

        Returns:
            SegmentationResult
        """
        regions = []
        self._split_region(
            ImageRegion(0, 0, image_width, image_height),
            regions,
            depth=0,
            should_split=should_split
        )

        total_pixels = image_width * image_height
        covered = sum(r.area for r in regions)

        return SegmentationResult(
            regions=regions,
            strategy=SegmentationStrategy.QUADTREE,
            total_pixels=total_pixels,
            coverage_ratio=covered / total_pixels if total_pixels > 0 else 0.0
        )

    def _split_region(
        self,
        region: ImageRegion,
        regions: List[ImageRegion],
        depth: int,
        should_split: Optional[Callable]
    ) -> None:
        """Recursively split region."""
        if depth >= self.max_depth:
            regions.append(region)
            return

        if region.width <= self.min_region_size or region.height <= self.min_region_size:
            regions.append(region)
            return

        if should_split and not should_split(region):
            regions.append(region)
            return

        half_w = region.width // 2
        half_h = region.height // 2

        quadrants = [
            ImageRegion(region.x, region.y, half_w, half_h),
            ImageRegion(region.x + half_w, region.y, region.width - half_w, half_h),
            ImageRegion(region.x, region.y + half_h, half_w, region.height - half_h),
            ImageRegion(region.x + half_w, region.y + half_h, region.width - half_w, region.height - half_h),
        ]

        for quadrant in quadrants:
            self._split_region(quadrant, regions, depth + 1, should_split)


class AdaptiveSegmenter:
    """
    Adaptively segment image based on content.
    """

    def __init__(
        self,
        target_region_count: int = 16,
        min_region_size: int = 32
    ):
        """
        Initialize adaptive segmenter.

        Args:
            target_region_count: Target number of regions
            min_region_size: Minimum region dimension
        """
        self.target_region_count = target_region_count
        self.min_region_size = min_region_size

    def segment(
        self,
        image_width: int,
        image_height: int
    ) -> SegmentationResult:
        """
        Adaptively segment image.

        Args:
            image_width: Image width
            image_height: Image height

        Returns:
            SegmentationResult
        """
        aspect = image_width / image_height if image_height > 0 else 1.0
        cols = max(1, int(math.sqrt(self.target_region_count * aspect)))
        rows = max(1, self.target_region_count // cols)

        cell_w = max(self.min_region_size, image_width // cols)
        cell_h = max(self.min_region_size, image_height // rows)

        segmenter = GridSegmenter(cell_w, cell_h)
        return segmenter.segment(image_width, image_height)


def segment_image(
    image_width: int,
    image_height: int,
    strategy: SegmentationStrategy = SegmentationStrategy.GRID,
    **kwargs
) -> SegmentationResult:
    """
    Segment image with specified strategy.

    Args:
        image_width: Image width
        image_height: Image height
        strategy: Segmentation strategy
        **kwargs: Strategy-specific parameters

    Returns:
        SegmentationResult
    """
    if strategy == SegmentationStrategy.GRID:
        segmenter = GridSegmenter(
            cell_width=kwargs.get("cell_width", 256),
            cell_height=kwargs.get("cell_height", 256)
        )
        overlap = kwargs.get("overlap", 0)
        return segmenter.segment(image_width, image_height, overlap)

    elif strategy == SegmentationStrategy.QUADTREE:
        segmenter = QuadtreeSegmenter(
            min_region_size=kwargs.get("min_region_size", 64),
            max_depth=kwargs.get("max_depth", 6)
        )
        return segmenter.segment(image_width, image_height)

    elif strategy == SegmentationStrategy.ADAPTIVE:
        segmenter = AdaptiveSegmenter(
            target_region_count=kwargs.get("target_region_count", 16),
            min_region_size=kwargs.get("min_region_size", 32)
        )
        return segmenter.segment(image_width, image_height)

    else:
        segmenter = GridSegmenter(256, 256)
        return segmenter.segment(image_width, image_height)
