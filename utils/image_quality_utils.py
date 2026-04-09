"""Image quality assessment utilities for screenshots.

This module provides utilities for assessing screenshot quality including
resolution, aspect ratio, file size, blur, noise, and overall quality
metrics for filtering and validation in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io


class QualityIssue(Enum):
    """Type of quality issue detected."""
    TOO_SMALL = auto()
    TOO_LARGE = auto()
    BLURRY = auto()
    TOO_DARK = auto()
    TOO_BRIGHT = auto()
    LOW_CONTRAST = auto()
    NOISY = auto()
    UNSUPPORTED_FORMAT = auto()


@dataclass
class QualityMetrics:
    """Quality metrics for an image."""
    width: int
    height: int
    aspect_ratio: float
    file_size_kb: float
    resolution_mp: float
    brightness: float
    contrast: float
    blur_score: float
    has_alpha: bool
    
    @property
    def quality_score(self) -> float:
        """Calculate overall quality score (0.0 to 1.0)."""
        score = 0.5
        
        if self.width >= 800 and self.height >= 600:
            score += 0.1
        
        if 0.5 <= self.aspect_ratio <= 2.5:
            score += 0.1
        
        if 0.1 <= self.brightness <= 0.9:
            score += 0.1
        
        if self.contrast >= 0.2:
            score += 0.1
        
        if self.blur_score >= 50:
            score += 0.1
        
        return min(1.0, score)


@dataclass
class QualityCheckResult:
    """Result of quality check."""
    passed: bool
    quality_score: float
    issues: list[QualityIssue]
    recommendations: list[str]


def assess_quality(
    image_data: bytes,
    min_width: int = 640,
    min_height: int = 480,
    max_width: int = 7680,
    max_height: int = 4320,
) -> QualityCheckResult:
    """Assess overall image quality.
    
    Args:
        image_data: Raw image bytes.
        min_width: Minimum acceptable width.
        min_height: Minimum acceptable height.
        max_width: Maximum acceptable width.
        max_height: Maximum acceptable height.
    
    Returns:
        QualityCheckResult with assessment.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        width, height = img.size
        aspect_ratio = width / height if height > 0 else 0
        resolution_mp = (width * height) / 1_000_000
        file_size_kb = len(image_data) / 1024
        
        gray = img.convert("L")
        import numpy as np
        gray_array = np.array(gray)
        
        brightness = float(gray_array.mean() / 255.0)
        contrast = float(gray_array.std() / 255.0)
        
        import cv2
        blur_score = float(cv2.Laplacian(gray_array, cv2.CV_64F).var())
        
        issues = []
        recommendations = []
        
        if width < min_width or height < min_height:
            issues.append(QualityIssue.TOO_SMALL)
            recommendations.append(f"Increase resolution to at least {min_width}x{min_height}")
        
        if width > max_width or height > max_height:
            issues.append(QualityIssue.TOO_LARGE)
            recommendations.append(f"Reduce resolution to at most {max_width}x{max_height}")
        
        if blur_score < 50:
            issues.append(QualityIssue.BLURRY)
            recommendations.append("Image appears blurry - adjust focus or lighting")
        
        if brightness < 0.1:
            issues.append(QualityIssue.TOO_DARK)
            recommendations.append("Image is too dark - increase lighting")
        elif brightness > 0.95:
            issues.append(QualityIssue.TOO_BRIGHT)
            recommendations.append("Image is overexposed - reduce lighting")
        
        if contrast < 0.15:
            issues.append(QualityIssue.LOW_CONTRAST)
            recommendations.append("Image has low contrast - improve lighting or use contrast enhancement")
        
        metrics = QualityMetrics(
            width=width,
            height=height,
            aspect_ratio=aspect_ratio,
            file_size_kb=file_size_kb,
            resolution_mp=resolution_mp,
            brightness=brightness,
            contrast=contrast,
            blur_score=blur_score,
            has_alpha=img.mode in ("RGBA", "LA", "P"),
        )
        
        passed = len(issues) == 0
        
        return QualityCheckResult(
            passed=passed,
            quality_score=metrics.quality_score,
            issues=issues,
            recommendations=recommendations,
        )
    except ImportError:
        raise ImportError("PIL and OpenCV are required for quality assessment")


def check_resolution(
    image_data: bytes,
    min_width: int = 640,
    min_height: int = 480,
) -> bool:
    """Check if image meets minimum resolution requirements.
    
    Args:
        image_data: Raw image bytes.
        min_width: Minimum width.
        min_height: Minimum height.
    
    Returns:
        True if resolution meets requirements.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        width, height = img.size
        
        return width >= min_width and height >= min_height
    except ImportError:
        raise ImportError("PIL is required for resolution check")


def check_brightness(
    image_data: bytes,
    min_brightness: float = 0.1,
    max_brightness: float = 0.95,
) -> bool:
    """Check if image brightness is within acceptable range.
    
    Args:
        image_data: Raw image bytes.
        min_brightness: Minimum acceptable brightness (0.0-1.0).
        max_brightness: Maximum acceptable brightness (0.0-1.0).
    
    Returns:
        True if brightness is acceptable.
    """
    try:
        from PIL import Image
        import numpy as np
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        gray_array = np.array(img)
        
        brightness = float(gray_array.mean() / 255.0)
        
        return min_brightness <= brightness <= max_brightness
    except ImportError:
        raise ImportError("PIL and numpy are required for brightness check")


def get_image_info(image_data: bytes) -> dict:
    """Get basic image information.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Dictionary with image information.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        width, height = img.size
        
        return {
            "width": width,
            "height": height,
            "aspect_ratio": width / height if height > 0 else 0,
            "resolution_mp": (width * height) / 1_000_000,
            "mode": img.mode,
            "format": img.format,
            "file_size_kb": len(image_data) / 1024,
        }
    except ImportError:
        raise ImportError("PIL is required for image info")


def filter_by_quality(
    image_data_list: list[bytes],
    min_quality_score: float = 0.6,
) -> list[tuple[int, bytes, float]]:
    """Filter images by quality score.
    
    Args:
        image_data_list: List of image bytes.
        min_quality_score: Minimum quality score (0.0-1.0).
    
    Returns:
        List of (index, image_data, quality_score) tuples for passing images.
    """
    results = []
    
    for i, img_data in enumerate(image_data_list):
        try:
            result = assess_quality(img_data)
            if result.quality_score >= min_quality_score:
                results.append((i, img_data, result.quality_score))
        except Exception:
            continue
    
    return results
