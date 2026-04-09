"""Image histogram analysis utilities for screenshot comparison.

This module provides utilities for analyzing image histograms to detect
visual changes, assess image quality, and perform histogram-based matching
for UI automation verification.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List
from enum import Enum, auto
import math


class HistogramType(Enum):
    """Type of histogram analysis."""
    LUMINOSITY = auto()
    RGB = auto()
    RED = auto()
    GREEN = auto()
    BLUE = auto()
    HISTOGRAM_EQUALIZATION = auto()


@dataclass
class HistogramData:
    """Container for histogram data."""
    bins: List[int]
    range_min: int = 0
    range_max: int = 255
    total_pixels: int = 0
    
    @property
    def mean(self) -> float:
        """Calculate mean value from histogram."""
        if self.total_pixels == 0:
            return 0.0
        return sum(i * count for i, count in enumerate(self.bins)) / self.total_pixels
    
    @property
    def std_dev(self) -> float:
        """Calculate standard deviation."""
        if self.total_pixels == 0:
            return 0.0
        mean = self.mean
        variance = sum((i - mean) ** 2 * count for i, count in enumerate(self.bins)) / self.total_pixels
        return math.sqrt(variance)


@dataclass
class HistogramComparison:
    """Result of histogram comparison."""
    similarity: float  # 0.0 to 1.0 (1.0 = identical)
    chi_square: float
    intersection: float
    correlation: float


def calculate_histogram(
    image_data: bytes,
    bins: int = 256,
    histogram_type: HistogramType = HistogramType.RGB,
) -> HistogramData:
    """Calculate histogram from image data.
    
    Args:
        image_data: Raw image bytes.
        bins: Number of histogram bins.
        histogram_type: Type of histogram to calculate.
    
    Returns:
        HistogramData object.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if histogram_type == HistogramType.RGB:
            img_array = np.array(img.convert("RGB"))
            hist_r = np.histogram(img_array[:, :, 0], bins=bins, range=(0, 255))[0]
            hist_g = np.histogram(img_array[:, :, 1], bins=bins, range=(0, 255))[0]
            hist_b = np.histogram(img_array[:, :, 2], bins=bins, range=(0, 255))[0]
            bins_data = (hist_r + hist_g + hist_b).tolist()
        else:
            gray = img.convert("L")
            gray_array = np.array(gray)
            bins_data = np.histogram(gray_array, bins=bins, range=(0, 255))[0].tolist()
        
        return HistogramData(
            bins=bins_data,
            range_min=0,
            range_max=255,
            total_pixels=img.width * img.height,
        )
    except ImportError:
        raise ImportError("numpy and PIL are required for histogram calculation")


def compare_histograms(
    hist1: HistogramData,
    hist2: HistogramData,
) -> HistogramComparison:
    """Compare two histograms and return similarity metrics.
    
    Args:
        hist1: First histogram.
        hist2: Second histogram.
    
    Returns:
        HistogramComparison with similarity metrics.
    """
    import numpy as np
    
    if len(hist1.bins) != len(hist2.bins):
        raise ValueError("Histograms must have same number of bins")
    
    h1 = np.array(hist1.bins, dtype=float)
    h2 = np.array(hist2.bins, dtype=float)
    
    h1_norm = h1 / (h1.sum() + 1e-10)
    h2_norm = h2 / (h2.sum() + 1e-10)
    
    intersection = np.sum(np.minimum(h1_norm, h2_norm))
    
    chi_square = np.sum((h1_norm - h2_norm) ** 2 / (h1_norm + h2_norm + 1e-10))
    
    mean1, mean2 = h1_norm.mean(), h2_norm.mean()
    std1 = np.sqrt(((h1_norm - mean1) ** 2).mean())
    std2 = np.sqrt(((h2_norm - mean2) ** 2).mean())
    
    if std1 > 0 and std2 > 0:
        correlation = np.corrcoef(h1_norm, h2_norm)[0, 1]
    else:
        correlation = 0.0
    
    similarity = intersection
    
    return HistogramComparison(
        similarity=float(similarity),
        chi_square=float(chi_square),
        intersection=float(intersection),
        correlation=float(correlation),
    )


def detect_histogram_shift(
    baseline_data: bytes,
    current_data: bytes,
    threshold: float = 0.1,
) -> Tuple[bool, float]:
    """Detect if there's a significant histogram shift between images.
    
    Args:
        baseline_data: Baseline image bytes.
        current_data: Current image bytes to compare.
        threshold: Similarity threshold (0.0 to 1.0).
    
    Returns:
        Tuple of (has_shift, similarity_score).
    """
    baseline_hist = calculate_histogram(baseline_data)
    current_hist = calculate_histogram(current_data)
    
    comparison = compare_histograms(baseline_hist, current_hist)
    
    has_shift = comparison.similarity < threshold
    
    return has_shift, comparison.similarity


def equalize_histogram(image_data: bytes) -> bytes:
    """Apply histogram equalization to enhance image contrast.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Equalized image bytes.
    """
    try:
        import numpy as np
        from PIL import Image, ImageOps
        import io
        
        img = Image.open(io.BytesIO(image_data))
        gray = img.convert("L")
        equalized = ImageOps.equalize(gray)
        
        if img.mode == "RGB":
            img = img.convert("RGB")
            r, g, b = img.split()
            r_eq = ImageOps.equalize(r)
            g_eq = ImageOps.equalize(g)
            b_eq = ImageOps.equalize(b)
            equalized = Image.merge("RGB", (r_eq, g_eq, b_eq))
        
        output = io.BytesIO()
        equalized.save(output, format="PNG")
        return output.getvalue()
    except ImportError:
        raise ImportError("PIL is required for histogram equalization")


def calculate_brightness(
    image_data: bytes,
) -> float:
    """Calculate average brightness of an image (0.0 to 1.0).
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Brightness value (0.0 = black, 1.0 = white).
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        array = np.array(img)
        
        return float(array.mean() / 255.0)
    except ImportError:
        raise ImportError("PIL is required for brightness calculation")


def calculate_contrast(
    image_data: bytes,
) -> float:
    """Calculate contrast ratio of an image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        Contrast ratio.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("L")
        array = np.array(img)
        
        return float(array.std() / 255.0)
    except ImportError:
        raise ImportError("PIL is required for contrast calculation")


def analyze_region_histogram(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int,
) -> HistogramData:
    """Calculate histogram for a specific region of an image.
    
    Args:
        image_data: Raw image bytes.
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
    
    Returns:
        HistogramData for the region.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data)).convert("RGB")
        
        if x + width > img.width or y + height > img.height:
            width = min(width, img.width - x)
            height = min(height, img.height - y)
        
        region = img.crop((x, y, x + width, y + height))
        region_array = np.array(region)
        
        hist_r = np.histogram(region_array[:, :, 0], bins=256, range=(0, 255))[0]
        hist_g = np.histogram(region_array[:, :, 1], bins=256, range=(0, 255))[0]
        hist_b = np.histogram(region_array[:, :, 2], bins=256, range=(0, 255))[0]
        bins_data = (hist_r + hist_g + hist_b).tolist()
        
        return HistogramData(
            bins=bins_data,
            range_min=0,
            range_max=255,
            total_pixels=width * height,
        )
    except ImportError:
        raise ImportError("PIL is required for region histogram analysis")
