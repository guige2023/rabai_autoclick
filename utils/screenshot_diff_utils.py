"""Screenshot difference utilities for comparing screenshots.

This module provides utilities for computing differences between screenshots,
useful for visual testing, change detection, and automation verification.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List
import io


@dataclass
class DiffResult:
    """Result of screenshot comparison."""
    is_identical: bool
    diff_percentage: float
    diff_pixels: int
    total_pixels: int
    diff_image: Optional[bytes]
    changed_regions: List[Tuple[int, int, int, int]]


@dataclass
class DiffConfig:
    """Configuration for diff computation."""
    threshold: int = 30
    highlight_color: Tuple[int, int, int] = (255, 0, 0)
    compute_diff_image: bool = True
    find_changed_regions: bool = True
    min_region_area: int = 100


def compare_screenshots(
    image1_data: bytes,
    image2_data: bytes,
    config: Optional[DiffConfig] = None,
) -> DiffResult:
    """Compare two screenshots and compute differences.
    
    Args:
        image1_data: First screenshot bytes.
        image2_data: Second screenshot bytes.
        config: Diff configuration.
    
    Returns:
        DiffResult with comparison results.
    """
    try:
        import numpy as np
        from PIL import Image
        import cv2
        import io
        
        config = config or DiffConfig()
        
        img1 = Image.open(io.BytesIO(image1_data)).convert("RGB")
        img2 = Image.open(io.BytesIO(image2_data)).convert("RGB")
        
        if img1.size != img2.size:
            img2 = img2.resize(img1.size, Image.LANCZOS)
        
        arr1 = np.array(img1)
        arr2 = np.array(img2)
        
        diff_mask = np.abs(arr1.astype(int) - arr2.astype(int)).max(axis=2)
        changed_pixels = diff_mask > config.threshold
        
        diff_pixels = int(np.sum(changed_pixels))
        total_pixels = arr1.shape[0] * arr1.shape[1]
        diff_percentage = (diff_pixels / total_pixels * 100) if total_pixels > 0 else 0
        
        diff_image = None
        if config.compute_diff_image:
            diff_image = _create_diff_image(img1, img2, diff_mask, config)
        
        changed_regions = []
        if config.find_changed_regions:
            changed_regions = _find_changed_regions(
                changed_pixels,
                config.min_region_area,
            )
        
        return DiffResult(
            is_identical=diff_pixels == 0,
            diff_percentage=float(diff_percentage),
            diff_pixels=diff_pixels,
            total_pixels=total_pixels,
            diff_image=diff_image,
            changed_regions=changed_regions,
        )
    except ImportError:
        raise ImportError("PIL and numpy are required for screenshot comparison")


def _create_diff_image(img1, img2, diff_mask, config) -> bytes:
    """Create visual diff image."""
    import numpy as np
    from PIL import Image
    import io
    
    arr1 = np.array(img1)
    arr2 = np.array(img2)
    
    diff_rgb = np.zeros_like(arr1)
    diff_rgb[:, :, 0] = config.highlight_color[0]
    diff_rgb[:, :, 1] = config.highlight_color[1]
    diff_rgb[:, :, 2] = config.highlight_color[2]
    
    mask_3d = np.stack([diff_mask] * 3, axis=2)
    
    result = np.where(mask_3d, diff_rgb, arr1)
    
    diff_img = Image.fromarray(result.astype(np.uint8))
    
    output = io.BytesIO()
    diff_img.save(output, format="PNG")
    return output.getvalue()


def _find_changed_regions(
    changed_pixels: np.ndarray,
    min_area: int,
) -> List[Tuple[int, int, int, int]]:
    """Find bounding boxes of changed regions."""
    import cv2
    
    changed_u8 = (changed_pixels * 255).astype(np.uint8)
    
    contours, _ = cv2.findContours(
        changed_u8,
        cv2.RETR_EXTERNAL,
        cv2.CHAIN_APPROX_SIMPLE,
    )
    
    regions = []
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w * h >= min_area:
            regions.append((int(x), int(y), int(w), int(h)))
    
    return regions


def compare_screenshot_regions(
    image1_data: bytes,
    image2_data: bytes,
    region: Tuple[int, int, int, int],
    config: Optional[DiffConfig] = None,
) -> DiffResult:
    """Compare specific regions of two screenshots.
    
    Args:
        image1_data: First screenshot bytes.
        image2_data: Second screenshot bytes.
        region: (x, y, width, height) region to compare.
        config: Diff configuration.
    
    Returns:
        DiffResult for the specified region only.
    """
    try:
        from PIL import Image
        import io
        
        config = config or DiffConfig()
        x, y, w, h = region
        
        img1 = Image.open(io.BytesIO(image1_data)).convert("RGB")
        img2 = Image.open(io.BytesIO(image2_data)).convert("RGB")
        
        img1_cropped = img1.crop((x, y, x + w, y + h))
        img2_cropped = img2.crop((x, y, x + w, y + h))
        
        output1 = io.BytesIO()
        output2 = io.BytesIO()
        img1_cropped.save(output1, format="PNG")
        img2_cropped.save(output2, format="PNG")
        
        return compare_screenshots(
            output1.getvalue(),
            output2.getvalue(),
            config,
        )
    except ImportError:
        raise ImportError("PIL is required for region comparison")


def batch_compare(
    baseline_data: bytes,
    comparison_data_list: list[bytes],
    config: Optional[DiffConfig] = None,
) -> list[DiffResult]:
    """Compare baseline against multiple screenshots.
    
    Args:
        baseline_data: Baseline screenshot bytes.
        comparison_data_list: List of screenshots to compare.
        config: Diff configuration.
    
    Returns:
        List of DiffResult for each comparison.
    """
    return [
        compare_screenshots(baseline_data, comp_data, config)
        for comp_data in comparison_data_list
    ]


def find_identical_screenshots(
    screenshots: list[bytes],
    config: Optional[DiffConfig] = None,
) -> List[Tuple[int, int]]:
    """Find pairs of identical screenshots.
    
    Args:
        screenshots: List of screenshot bytes.
        config: Diff configuration.
    
    Returns:
        List of (index1, index2) pairs that are identical.
    """
    identical_pairs = []
    
    for i in range(len(screenshots)):
        for j in range(i + 1, len(screenshots)):
            result = compare_screenshots(
                screenshots[i],
                screenshots[j],
                config,
            )
            if result.is_identical:
                identical_pairs.append((i, j))
    
    return identical_pairs
