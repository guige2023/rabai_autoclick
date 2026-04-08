"""Image filter utilities for image processing operations."""

from typing import Tuple, Optional
import numpy as np


def grayscale(image: np.ndarray) -> np.ndarray:
    """Convert image to grayscale.
    
    Args:
        image: Input BGR image.
    
    Returns:
        Grayscale image.
    """
    import cv2
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)


def blur(image: np.ndarray, kernel_size: int = 5) -> np.ndarray:
    """Apply Gaussian blur to image.
    
    Args:
        image: Input image.
        kernel_size: Blur kernel size (odd).
    
    Returns:
        Blurred image.
    """
    import cv2
    if kernel_size % 2 == 0:
        kernel_size += 1
    return cv2.GaussianBlur(image, (kernel_size, kernel_size), 0)


def sharpen(image: np.ndarray, strength: float = 1.0) -> np.ndarray:
    """Sharpen image.
    
    Args:
        image: Input image.
        strength: Sharpen strength.
    
    Returns:
        Sharpened image.
    """
    import cv2
    kernel = np.array([
        [0, -1, 0],
        [-1, 5, -1],
        [0, -1, 0]
    ]) * strength
    return cv2.filter2D(image, -1, kernel / kernel.sum())


def adjust_brightness(image: np.ndarray, factor: float) -> np.ndarray:
    """Adjust image brightness.
    
    Args:
        image: Input image.
        factor: Brightness factor (1.0 = no change).
    
    Returns:
        Adjusted image.
    """
    import cv2
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    hsv[:, :, 2] = np.clip(hsv[:, :, 2] * factor, 0, 255).astype(np.uint8)
    return cv2.cvtColor(hsv, cv2.COLOR_HSV2BGR)


def adjust_contrast(image: np.ndarray, factor: float) -> np.ndarray:
    """Adjust image contrast.
    
    Args:
        image: Input image.
        factor: Contrast factor (1.0 = no change).
    
    Returns:
        Adjusted image.
    """
    import cv2
    return cv2.convertScaleAbs(image, alpha=factor, beta=0)


def adjust_saturation(image: np.ndarray, factor: float) -> np.ndarray:
    """Adjust color saturation.
    
    Args:
        image: Input image.
        factor: Saturation factor (1.0 = no change).
    
    Returns:
        Adjusted image.
    """
    import cv2
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    hsv[:, :, 1] = np.clip(hsv[:, :, 1] * factor, 0, 255).astype(np.uint8)
    return cv2.cvtColor(hsv, cv2.COLOR_HSV2BGR)


def crop_region(
    image: np.ndarray,
    x: int, y: int,
    width: int, height: int
) -> np.ndarray:
    """Crop a rectangular region from image.
    
    Args:
        image: Input image.
        x, y: Top-left corner.
        width, height: Region dimensions.
    
    Returns:
        Cropped image.
    """
    h, w = image.shape[:2]
    x1 = max(0, min(x, w))
    y1 = max(0, min(y, h))
    x2 = max(0, min(x + width, w))
    y2 = max(0, min(y + height, h))
    return image[y1:y2, x1:x2]


def resize_with_aspect(
    image: np.ndarray,
    target_width: Optional[int] = None,
    target_height: Optional[int] = None
) -> np.ndarray:
    """Resize image while preserving aspect ratio.
    
    Args:
        image: Input image.
        target_width: Target width.
        target_height: Target height.
    
    Returns:
        Resized image.
    """
    import cv2
    h, w = image.shape[:2]
    if target_width and target_height:
        aspect = min(target_width / w, target_height / h)
    elif target_width:
        aspect = target_width / w
    elif target_height:
        aspect = target_height / h
    else:
        return image
    new_w = int(w * aspect)
    new_h = int(h * aspect)
    return cv2.resize(image, (new_w, new_h))


def apply_threshold(
    gray: np.ndarray,
    threshold: int = 127,
    max_val: int = 255
) -> np.ndarray:
    """Apply binary threshold to grayscale image.
    
    Args:
        gray: Grayscale input image.
        threshold: Threshold value.
        max_val: Maximum value for pixels above threshold.
    
    Returns:
        Binary image.
    """
    import cv2
    _, result = cv2.threshold(gray, threshold, max_val, cv2.THRESH_BINARY)
    return result
