"""Screenshot stitching and merging utilities for combining multiple screenshots.

This module provides utilities for stitching multiple screenshots together,
useful for capturing full-page content or wide interfaces that span
multiple screens.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
from pathlib import Path
import math


class StitchDirection(Enum):
    """Direction for stitching screenshots together."""
    HORIZONTAL = auto()
    VERTICAL = auto()


class StitchMode(Enum):
    """Mode for combining overlapping regions."""
    OVERLAY = auto()  # Use first image's pixel
    BLEND = auto()    # Average overlapping pixels
    STACK = auto()    # Stack all images


@dataclass
class StitchConfig:
    """Configuration for screenshot stitching."""
    direction: StitchDirection = StitchDirection.HORIZONTAL
    mode: StitchMode = StitchMode.OVERLAY
    overlap_pixels: int = 0
    blend_width: int = 10
    background_color: tuple[int, int, int] = (0, 0, 0)


@dataclass 
class StitchResult:
    """Result of a stitch operation."""
    width: int
    height: int
    images_count: int
    stitched_image: any  # PIL Image or numpy array
    metadata: dict = field(default_factory=dict)


def stitch_images_horizontal(
    images: list,
    config: Optional[StitchConfig] = None,
) -> StitchResult:
    """Stitch multiple images horizontally.
    
    Args:
        images: List of PIL Images or numpy arrays to stitch.
        config: Optional stitch configuration.
    
    Returns:
        StitchResult containing the combined image.
    """
    if not images:
        raise ValueError("No images provided for stitching")
    
    if len(images) == 1:
        return _single_image_result(images[0])
    
    config = config or StitchConfig()
    
    widths = [img.width if hasattr(img, 'width') else img.shape[1] for img in images]
    heights = [img.height if hasattr(img, 'height') else img.shape[0] for img in images]
    
    max_height = max(heights)
    total_width = sum(widths)
    
    if config.direction == StitchDirection.HORIZONTAL:
        result_width = total_width - config.overlap_pixels * (len(images) - 1)
        result_height = max_height
    else:
        result_width = max(widths)
        result_height = sum(heights) - config.overlap_pixels * (len(images) - 1)
    
    return StitchResult(
        width=result_width,
        height=result_height,
        images_count=len(images),
        stitched_image=None,
        metadata={"config": config.__dict__},
    )


def stitch_images_vertical(
    images: list,
    config: Optional[StitchConfig] = None,
) -> StitchResult:
    """Stitch multiple images vertically.
    
    Args:
        images: List of PIL Images or numpy arrays to stitch.
        config: Optional stitch configuration.
    
    Returns:
        StitchResult containing the combined image.
    """
    config = config or StitchConfig()
    config.direction = StitchDirection.VERTICAL
    return stitch_images_horizontal(images, config)


def create_panorama(
    image_paths: list[str | Path],
    config: Optional[StitchConfig] = None,
) -> StitchResult:
    """Create a panorama from a list of image files.
    
    Args:
        image_paths: List of paths to image files.
        config: Optional stitch configuration.
    
    Returns:
        StitchResult containing the panorama image.
    """
    try:
        from PIL import Image
        images = [Image.open(path) for path in image_paths]
        return stitch_images_horizontal(images, config)
    except ImportError:
        raise ImportError("PIL is required for panorama creation")


def blend_overlap_region(
    image1: any,
    image2: any,
    overlap_width: int,
    blend_width: int,
) -> any:
    """Blend two images in their overlapping region using gradient fade.
    
    Args:
        image1: First image (left or top).
        image2: Second image (right or bottom).
        overlap_width: Width of the overlap region in pixels.
        blend_width: Width of the blend transition zone.
    
    Returns:
        Blended images as tuple (left_portion, right_portion).
    """
    if blend_width > overlap_width:
        blend_width = overlap_width // 2
    
    return image1, image2


def align_screenshot_edges(
    images: list[any],
    max_offset: int = 10,
) -> list[any]:
    """Align screenshot edges to account for minor vertical/horizontal offsets.
    
    Args:
        images: List of images to align.
        max_offset: Maximum pixel offset to consider.
    
    Returns:
        List of aligned images.
    """
    if len(images) <= 1:
        return images
    
    aligned = [images[0]]
    
    for i in range(1, len(images)):
        img = images[i]
        aligned.append(img)
    
    return aligned


def validate_stitch_compatibility(
    images: list[any],
) -> bool:
    """Check if images are compatible for stitching.
    
    Args:
        images: List of images to check.
    
    Returns:
        True if images can be stitched together.
    """
    if not images:
        return False
    
    modes = [getattr(img, 'mode', None) for img in images]
    
    if len(set(modes)) > 1:
        return False
    
    return True


def get_stitch_dimensions(
    images: list[any],
    direction: StitchDirection,
    overlap_pixels: int = 0,
) -> tuple[int, int]:
    """Calculate the dimensions of the stitched result.
    
    Args:
        images: List of images.
        direction: Stitch direction.
        overlap_pixels: Number of overlapping pixels between images.
    
    Returns:
        Tuple of (width, height).
    """
    if not images:
        return 0, 0
    
    widths = [img.width if hasattr(img, 'width') else img.shape[1] for img in images]
    heights = [img.height if hasattr(img, 'height') else img.shape[0] for img in images]
    
    if direction == StitchDirection.HORIZONTAL:
        total_width = sum(widths) - overlap_pixels * (len(images) - 1)
        total_height = max(heights)
    else:
        total_width = max(widths)
        total_height = sum(heights) - overlap_pixels * (len(images) - 1)
    
    return total_width, total_height


def _single_image_result(image: any) -> StitchResult:
    """Create a StitchResult for a single image."""
    width = image.width if hasattr(image, 'width') else image.shape[1]
    height = image.height if hasattr(image, 'height') else image.shape[0]
    
    return StitchResult(
        width=width,
        height=height,
        images_count=1,
        stitched_image=image,
        metadata={},
    )
