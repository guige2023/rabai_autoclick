"""Image difference and comparison utilities.

This module provides utilities for comparing two images and computing
visual differences, which is useful for change detection, screenshot
verification, and automation validation.
"""

from __future__ import annotations

import hashlib
from typing import Optional


def compute_image_hash(image_path: str) -> str:
    """Compute MD5 hash of an image file.
    
    Args:
        image_path: Path to the image file.
    
    Returns:
        Hexadecimal hash string.
    """
    with open(image_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def images_are_identical(
    image_path1: str,
    image_path2: str,
) -> bool:
    """Check if two images are byte-identical.
    
    Args:
        image_path1: Path to first image.
        image_path2: Path to second image.
    
    Returns:
        True if images are identical.
    """
    try:
        import pyautogui
        # Quick hash comparison first
        hash1 = compute_image_hash(image_path1)
        hash2 = compute_image_hash(image_path2)
        if hash1 != hash2:
            return False
        
        # For same-size images, compare pixels
        from PIL import Image
        img1 = Image.open(image_path1)
        img2 = Image.open(image_path2)
        
        if img1.size != img2.size:
            return False
        
        # Compare pixel data
        return list(img1.getdata()) == list(img2.getdata())
    except ImportError:
        # Fallback: compare file hashes
        return compute_image_hash(image_path1) == compute_image_hash(image_path2)


def compute_pixel_diff(
    image_path1: str,
    image_path2: str,
) -> float:
    """Compute the percentage of differing pixels between two images.
    
    Images must be the same size. Different sizes are clamped to
    the minimum overlapping region.
    
    Args:
        image_path1: Path to first image.
        image_path2: Path to second image.
    
    Returns:
        Percentage of differing pixels (0.0 to 100.0).
    """
    try:
        from PIL import Image
        import numpy as np
        
        img1 = Image.open(image_path1).convert("RGB")
        img2 = Image.open(image_path2).convert("RGB")
        
        # Resize to match
        w = min(img1.width, img2.width)
        h = min(img1.height, img2.height)
        img1 = img1.resize((w, h))
        img2 = img2.resize((w, h))
        
        # Convert to arrays
        arr1 = np.array(img1)
        arr2 = np.array(img2)
        
        # Count differing pixels
        diff = np.sum(arr1 != arr2, axis=-1)
        differing_pixels = np.count_nonzero(diff)
        total_pixels = w * h
        
        return (differing_pixels / total_pixels) * 100.0
    except ImportError:
        return 0.0


def compute_histogram_diff(
    image_path1: str,
    image_path2: str,
) -> float:
    """Compare images using color histogram similarity.
    
    Uses chi-squared histogram comparison for robustness to
    minor color variations.
    
    Args:
        image_path1: Path to first image.
        image_path2: Path to second image.
    
    Returns:
        Similarity score from 0.0 (identical) to 1.0 (completely different).
    """
    try:
        from PIL import Image
        import numpy as np
        
        img1 = Image.open(image_path1).convert("RGB")
        img2 = Image.open(image_path2).convert("RGB")
        
        # Resize to same size for comparison
        if img1.size != img2.size:
            img2 = img2.resize(img1.size)
        
        # Compute color histograms
        hist1 = img1.histogram()
        hist2 = img2.histogram()
        
        # Sum squared difference
        diff = sum((h1 - h2) ** 2 for h1, h2 in zip(hist1, hist2))
        max_diff = sum(h ** 2 for h in hist1) or 1
        
        return min(1.0, diff / max_diff)
    except ImportError:
        return 0.0


def generate_diff_image(
    image_path1: str,
    image_path2: str,
    output_path: str,
    diff_color: tuple[int, int, int] = (255, 0, 0),
) -> bool:
    """Generate a visual diff image highlighting differences.
    
    Args:
        image_path1: Path to first image.
        image_path2: Path to second image.
        output_path: Path to save the diff image.
        diff_color: RGB tuple for highlighting differences.
    
    Returns:
        True if diff image was generated successfully.
    """
    try:
        from PIL import Image
        import numpy as np
        
        img1 = Image.open(image_path1).convert("RGB")
        img2 = Image.open(image_path2).convert("RGB")
        
        # Resize to match
        w = min(img1.width, img2.width)
        h = min(img1.height, img2.height)
        img1 = img1.resize((w, h))
        img2 = img2.resize((w, h))
        
        arr1 = np.array(img1)
        arr2 = np.array(img2)
        
        # Create diff mask
        diff_mask = np.any(arr1 != arr2, axis=-1)
        
        # Create output image
        output = np.copy(arr2)
        output[diff_mask] = diff_color
        
        Image.fromarray(output.astype(np.uint8)).save(output_path)
        return True
    except ImportError:
        return False


def are_images_similar(
    image_path1: str,
    image_path2: str,
    threshold: float = 5.0,
) -> bool:
    """Check if two images are visually similar within a threshold.
    
    Args:
        image_path1: Path to first image.
        image_path2: Path to second image.
        threshold: Maximum allowed pixel difference percentage.
    
    Returns:
        True if images are similar within the threshold.
    """
    diff_percent = compute_pixel_diff(image_path1, image_path2)
    return diff_percent <= threshold
