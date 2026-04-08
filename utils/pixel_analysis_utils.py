"""Pixel analysis utilities for image inspection and color extraction."""

from typing import Tuple, Optional, List, Dict, Any
import numpy as np


def get_pixel_bgr(image: np.ndarray, x: int, y: int) -> Tuple[int, int, int]:
    """Get BGR value of pixel at coordinates.
    
    Args:
        image: Input image.
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        (B, G, R) tuple.
    """
    h, w = image.shape[:2]
    if x < 0 or x >= w or y < 0 or y >= h:
        raise ValueError(f"Coordinates ({x}, {y}) out of bounds ({w}x{h})")
    return tuple(int(c) for c in image[y, x][:3])


def get_pixel_rgb(image: np.ndarray, x: int, y: int) -> Tuple[int, int, int]:
    """Get RGB value of pixel at coordinates.
    
    Args:
        image: Input image.
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        (R, G, B) tuple.
    """
    b, g, r = get_pixel_bgr(image, x, y)
    return (r, g, b)


def get_region_average_color(
    image: np.ndarray,
    x: int, y: int,
    width: int, height: int
) -> Tuple[int, int, int]:
    """Get average BGR color of region.
    
    Args:
        image: Input image.
        x, y: Top-left corner.
        width, height: Region dimensions.
    
    Returns:
        (B, G, R) average color.
    """
    h, w = image.shape[:2]
    x2 = min(x + width, w)
    y2 = min(y + height, h)
    region = image[y:min(y, y2), x:min(x, x2)]
    if region.size == 0:
        return (0, 0, 0)
    return tuple(int(c) for c in region.mean(axis=(0, 1))[:3])


def find_pixels_by_color(
    image: np.ndarray,
    target: Tuple[int, int, int],
    tolerance: int = 10,
    channels: str = "bgr"
) -> List[Tuple[int, int]]:
    """Find all pixels matching target color within tolerance.
    
    Args:
        image: Input image.
        target: Target (B, G, R) or (R, G, B) depending on channels.
        tolerance: Color tolerance.
        channels: "bgr" or "rgb".
    
    Returns:
        List of (x, y) coordinates.
    """
    import cv2
    if channels == "rgb":
        target = target[::-1]
    lower = np.array([max(0, c - tolerance) for c in target], dtype=np.uint8)
    upper = np.array([min(255, c + tolerance) for c in target], dtype=np.uint8)
    mask = cv2.inRange(image, lower, upper)
    coords = np.where(mask > 0)
    return list(zip(coords[1], coords[0]))


def analyze_histogram(
    image: np.ndarray,
    channel: str = "gray"
) -> Dict[str, Any]:
    """Analyze image histogram.
    
    Args:
        image: Input image.
        channel: "gray", "r", "g", or "b".
    
    Returns:
        Dict with histogram data.
    """
    import cv2
    if channel == "gray":
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        hist = cv2.calcHist([gray], [0], None, [256], [0, 256]).flatten()
    else:
        c_idx = {"r": 2, "g": 1, "b": 0}[channel]
        hist = cv2.calcHist([image], [c_idx], None, [256], [0, 256]).flatten()
    return {
        "histogram": hist,
        "total": int(hist.sum()),
        "mean": float(np.average(np.arange(256), weights=hist)),
    }


def detect_edges_canny(
    image: np.ndarray,
    threshold1: int = 50,
    threshold2: int = 150
) -> np.ndarray:
    """Detect edges using Canny algorithm.
    
    Args:
        image: Input image.
        threshold1: Lower threshold.
        threshold2: Upper threshold.
    
    Returns:
        Edge map.
    """
    import cv2
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return cv2.Canny(gray, threshold1, threshold2)


def find_contours(
    image: np.ndarray,
    min_area: int = 10
) -> List[Dict[str, Any]]:
    """Find contours in image.
    
    Args:
        image: Input image or edge map.
        min_area: Minimum contour area.
    
    Returns:
        List of contour info dicts.
    """
    import cv2
    contours, _ = cv2.findContours(image, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    result = []
    for c in contours:
        area = cv2.contourArea(c)
        if area < min_area:
            continue
        x, y, w, h = cv2.boundingRect(c)
        result.append({
            "area": area,
            "bbox": (x, y, w, h),
            "center": (x + w // 2, y + h // 2),
            "points": c.tolist(),
        })
    return result


def is_uniform_color(
    image: np.ndarray,
    x: int, y: int,
    width: int, height: int,
    variance_threshold: float = 100.0
) -> bool:
    """Check if region has uniform color.
    
    Args:
        image: Input image.
        x, y: Top-left corner.
        width, height: Region size.
        variance_threshold: Max variance for uniform认定.
    
    Returns:
        True if region is uniform.
    """
    region = get_region_average_color(image, x, y, width, height)
    h, w = image.shape[:2]
    x2 = min(x + width, w)
    y2 = min(y + height, h)
    region_pixels = image[y:min(y, y2), x:min(x, x2)].reshape(-1, 3)
    if len(region_pixels) == 0:
        return True
    variance = np.var(region_pixels, axis=0).mean()
    return variance < variance_threshold
