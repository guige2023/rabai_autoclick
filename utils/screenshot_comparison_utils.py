"""Screenshot comparison utilities for visual diff detection."""

from typing import Optional, Tuple, List, Dict, Any
import numpy as np


def compare_screenshots(
    img1: np.ndarray,
    img2: np.ndarray,
    threshold: float = 0.05
) -> Tuple[bool, float, Optional[np.ndarray]]:
    """Compare two screenshots and return diff info.
    
    Args:
        img1: First screenshot.
        img2: Second screenshot.
        threshold: Difference threshold (0-1).
    
    Returns:
        Tuple of (is_different, diff_ratio, diff_image).
    """
    import cv2
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    diff = cv2.absdiff(gray1, gray2)
    diff_ratio = np.sum(diff > 30) / diff.size
    is_different = diff_ratio > threshold
    diff_color = cv2.cvtColor(diff, cv2.COLOR_GRAY2BGR)
    return is_different, diff_ratio, diff_color


def find_changed_regions(
    img1: np.ndarray,
    img2: np.ndarray,
    block_size: int = 16,
    threshold: float = 0.1
) -> List[Tuple[int, int, int, int]]:
    """Find rectangular regions that differ between screenshots.
    
    Args:
        img1: First screenshot.
        img2: Second screenshot.
        block_size: Size of blocks to check.
        threshold: Change threshold per block.
    
    Returns:
        List of (x, y, w, h) rectangles of changed regions.
    """
    import cv2
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    diff = cv2.absdiff(gray1, gray2)
    h, w = diff.shape
    regions = []
    for y in range(0, h - block_size + 1, block_size):
        for x in range(0, w - block_size + 1, block_size):
            block = diff[y:y+block_size, x:x+block_size]
            if np.mean(block > 30) > threshold:
                regions.append((x, y, block_size, block_size))
    return regions


def screenshot_similarity(
    img1: np.ndarray,
    img2: np.ndarray
) -> float:
    """Calculate structural similarity between screenshots.
    
    Args:
        img1: First screenshot.
        img2: Second screenshot.
    
    Returns:
        Similarity score (0-1, higher is more similar).
    """
    import cv2
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    score = cv2.matchTemplate(gray1, gray2, cv2.TM_CCOEFF_NORMED)[0][0]
    return float(max(0, score))


def highlight_differences(
    img1: np.ndarray,
    img2: np.ndarray,
    color: Tuple[int, int, int] = (0, 0, 255),
    thickness: int = 2
) -> np.ndarray:
    """Draw highlighted difference regions on image.
    
    Args:
        img1: First screenshot.
        img2: Second screenshot.
        color: BGR color for highlighting.
        thickness: Border thickness.
    
    Returns:
        Image with differences highlighted.
    """
    import cv2
    result = img2.copy()
    regions = find_changed_regions(img1, img2)
    for x, y, w, h in regions:
        cv2.rectangle(result, (x, y), (x + w, y + h), color, thickness)
    return result


def get_screenshot_hash(
    image: np.ndarray,
    hash_size: int = 8
) -> str:
    """Generate perceptual hash of screenshot.
    
    Args:
        image: Input image.
        hash_size: Hash grid size.
    
    Returns:
        Hex string hash.
    """
    import cv2
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    resized = cv2.resize(gray, (hash_size + 1, hash_size))
    diff = resized[:, 1:] > resized[:, :-1]
    bits = "".join("1" if b else "0" for b in diff.flatten())
    return hex(int(bits, 2))[2:]
