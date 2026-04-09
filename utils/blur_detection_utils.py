"""Blur detection utilities for image quality assessment.

This module provides utilities for detecting blur in images, useful for
assessing screenshot quality, detecting out-of-focus images, and filtering
low-quality captures in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional
import io


class BlurMetric(Enum):
    """Method for measuring blur."""
    LAPLACIAN = auto()    # Laplacian variance
    FFT = auto()          # Fast Fourier Transform
    Brenner = auto()      # Brenner gradient
    VARIANCE = auto()     # Local variance


@dataclass
class BlurResult:
    """Result of blur detection analysis."""
    is_blurry: bool
    score: float
    metric: BlurMetric
    threshold: float
    
    @property
    def blur_amount(self) -> str:
        """Get human-readable blur amount."""
        if self.score < self.threshold * 0.5:
            return "very sharp"
        elif self.score < self.threshold:
            return "sharp"
        elif self.score < self.threshold * 1.5:
            return "slightly blurry"
        else:
            return "very blurry"


def detect_blur(
    image_data: bytes,
    metric: BlurMetric = BlurMetric.LAPLACIAN,
    threshold: Optional[float] = None,
) -> BlurResult:
    """Detect if an image is blurry.
    
    Args:
        image_data: Raw image bytes.
        metric: Blur detection metric to use.
        threshold: Blur threshold (auto-calculated if None).
    
    Returns:
        BlurResult with detection results.
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
        
        if threshold is None:
            threshold = _get_default_threshold(metric)
        
        if metric == BlurMetric.LAPLACIAN:
            score = _laplacian_variance(img)
        elif metric == BlurMetric.FFT:
            score = _fft_blur_score(img)
        elif metric == BlurMetric.Brenner:
            score = _brenner_score(img)
        else:
            score = _laplacian_variance(img)
        
        is_blurry = score < threshold
        
        return BlurResult(
            is_blurry=is_blurry,
            score=float(score),
            metric=metric,
            threshold=threshold,
        )
    except ImportError:
        raise ImportError("OpenCV and numpy are required for blur detection")


def _laplacian_variance(img) -> float:
    """Calculate Laplacian variance for blur detection."""
    import cv2
    return float(cv2.Laplacian(img, cv2.CV_64F).var())


def _fft_blur_score(img) -> float:
    """Calculate blur score using FFT."""
    import numpy as np
    
    rows, cols = img.shape
    crow, ccol = rows // 2, cols // 2
    
    f = np.fft.fft2(img)
    fshift = np.fft.fftshift(f)
    magnitude = np.log(np.abs(fshift) + 1)
    
    center_region = magnitude[crow - 10:crow + 10, ccol - 10:ccol + 10]
    edge_energy = np.sum(magnitude) - np.sum(center_region)
    
    return float(edge_energy / (rows * cols))


def _brenner_score(img) -> float:
    """Calculate Brenner gradient score."""
    import numpy as np
    
    diff = img[:, 2:].astype(float) - img[:, :-2].astype(float)
    return float(np.sum(diff ** 2))


def _get_default_threshold(metric: BlurMetric) -> float:
    """Get default blur threshold for metric."""
    thresholds = {
        BlurMetric.LAPLACIAN: 100.0,
        BlurMetric.FFT: 10.0,
        BlurMetric.Brenner: 500.0,
        BlurMetric.VARIANCE: 50.0,
    }
    return thresholds.get(metric, 100.0)


def batch_blur_check(
    image_data_list: list[bytes],
    metric: BlurMetric = BlurMetric.LAPLACIAN,
    threshold: Optional[float] = None,
) -> list[BlurResult]:
    """Check blur for multiple images.
    
    Args:
        image_data_list: List of image bytes.
        metric: Blur detection metric.
        threshold: Optional threshold override.
    
    Returns:
        List of BlurResult for each image.
    """
    return [detect_blur(img, metric, threshold) for img in image_data_list]


def filter_blurry_images(
    image_data_list: list[bytes],
    metric: BlurMetric = BlurMetric.LAPLACIAN,
    threshold: Optional[float] = None,
) -> list[tuple[int, bytes, BlurResult]]:
    """Filter out blurry images from a list.
    
    Args:
        image_data_list: List of image bytes.
        metric: Blur detection metric.
        threshold: Optional threshold override.
    
    Returns:
        List of (index, image_data, blur_result) tuples for blurry images.
    """
    blurry = []
    for i, img_data in enumerate(image_data_list):
        result = detect_blur(img_data, metric, threshold)
        if result.is_blurry:
            blurry.append((i, img_data, result))
    return blurry


def suggest_focus_adjustment(
    image_data: bytes,
    current_focus: Optional[int] = None,
) -> Optional[str]:
    """Suggest focus adjustment based on blur analysis.
    
    Args:
        image_data: Raw image bytes.
        current_focus: Current focus setting if known.
    
    Returns:
        Suggestion string or None.
    """
    result = detect_blur(image_data)
    
    if result.is_blurry:
        ratio = result.threshold / result.score if result.score > 0 else float("inf")
        
        if ratio > 3:
            return "Increase focus significantly"
        elif ratio > 1.5:
            return "Increase focus slightly"
        else:
            return "Minor focus adjustment needed"
    
    return None
