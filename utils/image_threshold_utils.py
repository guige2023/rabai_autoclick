"""Image threshold utilities for image binarization and segmentation.

This module provides utilities for applying various thresholding techniques
to images, useful for simplifying images for pattern matching, OCR preprocessing,
and element detection in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple
import io


class ThresholdMethod(Enum):
    """Method of thresholding to apply."""
    BINARY = auto()           # Simple binary threshold
    BINARY_INVERSE = auto()    # Inverse binary threshold
    TRUNCATE = auto()          # Truncate threshold
    TO_ZERO = auto()          # Threshold to zero
    TO_ZERO_INVERSE = auto()  # Inverse threshold to zero
    OTSU = auto()              # Otsu's automatic thresholding
    ADAPTIVE_MEAN = auto()    # Adaptive mean threshold
    ADAPTIVE_GAUSSIAN = auto() # Adaptive Gaussian threshold


@dataclass
class ThresholdConfig:
    """Configuration for thresholding."""
    method: ThresholdMethod = ThresholdMethod.BINARY
    threshold_value: int = 127
    max_value: int = 255
    block_size: int = 11
    c_constant: int = 2


def apply_threshold(
    image_data: bytes,
    config: Optional[ThresholdConfig] = None,
) -> bytes:
    """Apply thresholding to an image.
    
    Args:
        image_data: Raw image bytes.
        config: Threshold configuration.
    
    Returns:
        Thresholded image bytes.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        config = config or ThresholdConfig()
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        if config.method == ThresholdMethod.OTSU:
            _, result = cv2.threshold(
                img, 0, config.max_value,
                cv2.THRESH_BINARY + cv2.THRESH_OTSU,
            )
        elif config.method == ThresholdMethod.ADAPTIVE_MEAN:
            result = cv2.adaptiveThreshold(
                img, config.max_value,
                cv2.ADAPTIVE_THRESH_MEAN_C,
                cv2.THRESH_BINARY,
                config.block_size,
                config.c_constant,
            )
        elif config.method == ThresholdMethod.ADAPTIVE_GAUSSIAN:
            result = cv2.adaptiveThreshold(
                img, config.max_value,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                config.block_size,
                config.c_constant,
            )
        else:
            threshold_type = _get_cv2_threshold_type(config.method)
            _, result = cv2.threshold(
                img, config.threshold_value, config.max_value, threshold_type,
            )
        
        _, encoded = cv2.imencode(".png", result)
        return encoded.tobytes()
    except ImportError:
        raise ImportError("OpenCV is required for thresholding")


def apply_binary_threshold(
    image_data: bytes,
    threshold: int = 127,
) -> bytes:
    """Apply simple binary threshold.
    
    Args:
        image_data: Raw image bytes.
        threshold: Threshold value (0-255).
    
    Returns:
        Binary thresholded image bytes.
    """
    config = ThresholdConfig(
        method=ThresholdMethod.BINARY,
        threshold_value=threshold,
    )
    return apply_threshold(image_data, config)


def apply_otsu_threshold(image_data: bytes) -> bytes:
    """Apply Otsu's automatic thresholding.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Otsu thresholded image bytes.
    """
    config = ThresholdConfig(method=ThresholdMethod.OTSU)
    return apply_threshold(image_data, config)


def apply_adaptive_threshold(
    image_data: bytes,
    block_size: int = 11,
    method: ThresholdMethod = ThresholdMethod.ADAPTIVE_MEAN,
) -> bytes:
    """Apply adaptive thresholding.
    
    Args:
        image_data: Raw image bytes.
        block_size: Size of pixel neighborhood.
        method: Adaptive method to use.
    
    Returns:
        Adaptive thresholded image bytes.
    """
    config = ThresholdConfig(
        method=method,
        block_size=block_size,
    )
    return apply_threshold(image_data, config)


def auto_threshold_otsu(image_data: bytes) -> Tuple[bytes, int]:
    """Apply Otsu's threshold and return the calculated threshold value.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Tuple of (thresholded_image_bytes, calculated_threshold).
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        threshold, result = cv2.threshold(
            img, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU,
        )
        
        _, encoded = cv2.imencode(".png", result)
        return encoded.tobytes(), int(threshold)
    except ImportError:
        raise ImportError("OpenCV is required for Otsu thresholding")


def segment_by_color(
    image_data: bytes,
    lower_bound: Tuple[int, int, int],
    upper_bound: Tuple[int, int, int],
) -> bytes:
    """Segment image by color range (color thresholding).
    
    Args:
        image_data: Raw image bytes.
        lower_bound: Lower HSV bounds (h, s, v).
        upper_bound: Upper HSV bounds (h, s, v).
    
    Returns:
        Mask image bytes (white where color is in range).
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            img = np.array(Image.open(io.BytesIO(image_data)).convert("RGB"))
            img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        
        lower = np.array(lower_bound)
        upper = np.array(upper_bound)
        
        mask = cv2.inRange(hsv, lower, upper)
        
        _, encoded = cv2.imencode(".png", mask)
        return encoded.tobytes()
    except ImportError:
        raise ImportError("OpenCV is required for color segmentation")


def double_threshold(
    image_data: bytes,
    low_threshold: int = 50,
    high_threshold: int = 150,
) -> bytes:
    """Apply double threshold to identify strong and weak edges.
    
    Args:
        image_data: Raw image bytes.
        low_threshold: Low threshold value.
        high_threshold: High threshold value.
    
    Returns:
        Double thresholded image bytes.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        strong = 255
        weak = 75
        
        _, strong_img = cv2.threshold(img, high_threshold, strong, cv2.THRESH_BINARY)
        _, weak_img = cv2.threshold(img, low_threshold, weak, cv2.THRESH_BINARY)
        
        result = np.zeros_like(img)
        result[(strong_img == strong) | (weak_img == weak)] = 255
        
        _, encoded = cv2.imencode(".png", result)
        return encoded.tobytes()
    except ImportError:
        raise ImportError("OpenCV is required for double thresholding")


def _get_cv2_threshold_type(method: ThresholdMethod) -> int:
    """Map ThresholdMethod to OpenCV threshold type."""
    mapping = {
        ThresholdMethod.BINARY: cv2.THRESH_BINARY,
        ThresholdMethod.BINARY_INVERSE: cv2.THRESH_BINARY_INV,
        ThresholdMethod.THRESHOLD: cv2.THRESH_TRUNC,
        ThresholdMethod.TO_ZERO: cv2.THRESH_TOZERO,
        ThresholdMethod.TO_ZERO_INVERSE: cv2.THRESH_TOZERO_INV,
    }
    return mapping.get(method, cv2.THRESH_BINARY)
