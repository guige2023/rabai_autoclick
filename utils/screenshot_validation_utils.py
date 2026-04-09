"""Screenshot validation utilities for verifying capture quality.

This module provides utilities for validating screenshots including
checking for errors, verifying content presence, and assessing
capture success for automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class ValidationError(Enum):
    """Type of validation error."""
    EMPTY_IMAGE = auto()
    CORRUPTED_IMAGE = auto()
    TOO_SMALL = auto()
    UNEXPECTED_SIZE = auto()
    ALL_BLACK = auto()
    ALL_WHITE = auto()
    BLURRY = auto()
    MISSING_CONTENT = auto()


@dataclass
class ValidationResult:
    """Result of screenshot validation."""
    is_valid: bool
    errors: List[ValidationError]
    warnings: List[str]
    metrics: dict


@dataclass
class ValidationConfig:
    """Configuration for validation."""
    min_width: int = 100
    min_height: int = 100
    expected_width: Optional[int] = None
    expected_height: Optional[int] = None
    check_blur: bool = True
    blur_threshold: float = 50.0
    check_content: bool = True
    min_content_threshold: float = 0.1


def validate_screenshot(
    image_data: bytes,
    config: Optional[ValidationConfig] = None,
) -> ValidationResult:
    """Validate a screenshot for quality and correctness.
    
    Args:
        image_data: Screenshot bytes.
        config: Validation configuration.
    
    Returns:
        ValidationResult with validation status.
    """
    try:
        from PIL import Image
        import io
        
        config = config or ValidationConfig()
        
        errors = []
        warnings = []
        metrics = {}
        
        if not image_data or len(image_data) == 0:
            errors.append(ValidationError.EMPTY_IMAGE)
            return ValidationResult(False, errors, warnings, metrics)
        
        try:
            img = Image.open(io.BytesIO(image_data))
            width, height = img.size
            metrics["width"] = width
            metrics["height"] = height
            metrics["format"] = img.format
            metrics["mode"] = img.mode
        except Exception:
            errors.append(ValidationError.CORRUPTED_IMAGE)
            return ValidationResult(False, errors, warnings, metrics)
        
        if width < config.min_width or height < config.min_height:
            errors.append(ValidationError.TOO_SMALL)
            warnings.append(f"Image too small: {width}x{height}")
        
        if config.expected_width and width != config.expected_width:
            warnings.append(f"Width mismatch: expected {config.expected_width}, got {width}")
        
        if config.expected_height and height != config.expected_height:
            warnings.append(f"Height mismatch: expected {config.expected_height}, got {height}")
        
        import numpy as np
        gray = np.array(img.convert("L"))
        
        mean_brightness = gray.mean() / 255.0
        metrics["brightness"] = mean_brightness
        
        if mean_brightness < 0.01:
            errors.append(ValidationError.ALL_BLACK)
        elif mean_brightness > 0.99:
            errors.append(ValidationError.ALL_WHITE)
        
        brightness_values = gray / 255.0
        variance = np.var(brightness_values)
        metrics["contrast_variance"] = float(variance)
        
        if config.check_content and variance < 0.001:
            errors.append(ValidationError.MISSING_CONTENT)
        
        if config.check_blur:
            try:
                import cv2
                blur_score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
                metrics["blur_score"] = blur_score
                
                if blur_score < config.blur_threshold:
                    warnings.append(f"Image may be blurry: score {blur_score:.1f}")
            except Exception:
                pass
        
        is_valid = len(errors) == 0
        
        return ValidationResult(is_valid, errors, warnings, metrics)
    except ImportError:
        return ValidationResult(False, [], ["PIL or numpy not available"], {})


def validate_region(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
) -> ValidationResult:
    """Validate a region of a screenshot.
    
    Args:
        image_data: Screenshot bytes.
        x: Region left edge.
        y: Region top edge.
        width: Region width.
        height: Region height.
    
    Returns:
        ValidationResult for the region.
    """
    try:
        from PIL import Image
        import io
        import numpy as np
        
        img = Image.open(io.BytesIO(image_data))
        
        x2 = min(x + width, img.width)
        y2 = min(y + height, img.height)
        
        region = img.crop((x, y, x2, y2))
        region_array = np.array(region.convert("L"))
        
        errors = []
        warnings = []
        metrics = {}
        
        mean_brightness = region_array.mean() / 255.0
        metrics["brightness"] = mean_brightness
        
        if mean_brightness < 0.01:
            errors.append(ValidationError.ALL_BLACK)
        elif mean_brightness > 0.99:
            errors.append(ValidationError.ALL_WHITE)
        
        is_valid = len(errors) == 0
        
        return ValidationResult(is_valid, errors, warnings, metrics)
    except ImportError:
        return ValidationResult(False, [], ["PIL not available"], {})


def check_screenshot_contains(
    image_data: bytes,
    expected_content: str,
) -> bool:
    """Check if screenshot contains expected content.
    
    Args:
        image_data: Screenshot bytes.
        expected_content: Content that should be present.
    
    Returns:
        True if content is detected.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        width, height = img.size
        if width < 100 or height < 100:
            return False
        
        gray = np.array(img.convert("L"))
        variance = float(np.var(gray) / (255 ** 2))
        
        return variance > 0.001
    except Exception:
        return False


def batch_validate(
    image_data_list: list[bytes],
    config: Optional[ValidationConfig] = None,
) -> list[ValidationResult]:
    """Validate multiple screenshots.
    
    Args:
        image_data_list: List of screenshot bytes.
        config: Validation configuration.
    
    Returns:
        List of ValidationResult for each screenshot.
    """
    return [validate_screenshot(img, config) for img in image_data_list]


def filter_valid_screenshots(
    image_data_list: list[bytes],
    config: Optional[ValidationConfig] = None,
) -> list[Tuple[int, bytes]]:
    """Filter to only valid screenshots.
    
    Args:
        image_data_list: List of screenshot bytes.
        config: Validation configuration.
    
    Returns:
        List of (index, image_data) tuples for valid screenshots.
    """
    valid = []
    for i, img_data in enumerate(image_data_list):
        result = validate_screenshot(img_data, config)
        if result.is_valid:
            valid.append((i, img_data))
    return valid
