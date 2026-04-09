"""Image comparison scoring utilities for similarity measurement.

This module provides utilities for computing various similarity metrics
between images including SSIM, MSE, PSNR, and histogram comparison,
useful for image quality assessment and change detection.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import io


@dataclass
class SimilarityScore:
    """Complete similarity score between two images."""
    similarity: float  # 0.0 to 1.0 (1.0 = identical)
    mse: float  # Mean Squared Error (lower = more similar)
    psnr: float  # Peak Signal-to-Noise Ratio (higher = more similar)
    ssim: float  # Structural Similarity (higher = more similar)
    histogram_similarity: float  # Histogram comparison score


@dataclass
class ComparisonConfig:
    """Configuration for image comparison."""
    method: str = "all"  # all, mse, ssim, histogram
    histogram_bins: int = 256
    validate_size_match: bool = True


def compute_similarity(
    image1_data: bytes,
    image2_data: bytes,
    config: Optional[ComparisonConfig] = None,
) -> SimilarityScore:
    """Compute full similarity score between two images.
    
    Args:
        image1_data: First image bytes.
        image2_data: Second image bytes.
        config: Comparison configuration.
    
    Returns:
        SimilarityScore with all metrics.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        config = config or ComparisonConfig()
        
        img1 = Image.open(io.BytesIO(image1_data)).convert("RGB")
        img2 = Image.open(io.BytesIO(image2_data)).convert("RGB")
        
        if config.validate_size_match and img1.size != img2.size:
            img2 = img2.resize(img1.size, Image.LANCZOS)
        
        arr1 = np.array(img1).astype(float)
        arr2 = np.array(img2).astype(float)
        
        mse = _compute_mse(arr1, arr2)
        psnr = _compute_psnr(mse)
        ssim = _compute_ssim(arr1, arr2)
        hist_sim = _compute_histogram_similarity(arr1, arr2, config.histogram_bins)
        
        similarity = (ssim + hist_sim) / 2
        
        return SimilarityScore(
            similarity=float(similarity),
            mse=float(mse),
            psnr=float(psnr),
            ssim=float(ssim),
            histogram_similarity=float(hist_sim),
        )
    except ImportError:
        raise ImportError("numpy and PIL are required for image comparison")


def _compute_mse(arr1: "np.ndarray", arr2: "np.ndarray") -> float:
    """Compute Mean Squared Error."""
    import numpy as np
    return float(np.mean((arr1 - arr2) ** 2))


def _compute_psnr(mse: float) -> float:
    """Compute PSNR from MSE."""
    if mse == 0:
        return 100.0
    return float(10 * np.log10(255 ** 2 / mse))


def _compute_ssim(arr1: "np.ndarray", arr2: "np.ndarray", window_size: int = 8) -> float:
    """Compute Structural Similarity Index."""
    import numpy as np
    
    C1 = (0.01 * 255) ** 2
    C2 = (0.03 * 255) ** 2
    
    mu1 = np.mean(arr1, axis=(0, 1))
    mu2 = np.mean(arr2, axis=(0, 1))
    
    sigma1_sq = np.var(arr1, axis=(0, 1))
    sigma2_sq = np.var(arr2, axis=(0, 1))
    sigma12 = np.mean((arr1 - mu1) * (arr2 - mu2), axis=(0, 1))
    
    ssim_map = (
        (2 * mu1 * mu2 + C1) *
        (2 * sigma12 + C2) /
        ((mu1 ** 2 + mu2 ** 2 + C1) * (sigma1_sq + sigma2_sq + C2))
    )
    
    return float(np.mean(ssim_map))


def _compute_histogram_similarity(arr1: "np.ndarray", arr2: "np.ndarray", bins: int) -> float:
    """Compute histogram-based similarity."""
    import numpy as np
    
    similarities = []
    
    for c in range(3):
        hist1, _ = np.histogram(arr1[:, :, c], bins=bins, range=(0, 255))
        hist2, _ = np.histogram(arr2[:, :, c], bins=bins, range=(0, 255))
        
        hist1 = hist1 / (hist1.sum() + 1e-10)
        hist2 = hist2 / (hist2.sum() + 1e-10)
        
        intersection = np.sum(np.minimum(hist1, hist2))
        similarities.append(intersection)
    
    return float(np.mean(similarities))


def compute_mse(image1_data: bytes, image2_data: bytes) -> float:
    """Compute Mean Squared Error between two images.
    
    Args:
        image1_data: First image bytes.
        image2_data: Second image bytes.
    
    Returns:
        MSE value.
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img1 = Image.open(io.BytesIO(image1_data)).convert("RGB")
        img2 = Image.open(io.BytesIO(image2_data)).convert("RGB")
        
        if img1.size != img2.size:
            img2 = img2.resize(img1.size, Image.LANCZOS)
        
        arr1 = np.array(img1).astype(float)
        arr2 = np.array(img2).astype(float)
        
        return _compute_mse(arr1, arr2)
    except ImportError:
        raise ImportError("numpy and PIL are required for MSE computation")


def compute_ssim(image1_data: bytes, image2_data: bytes) -> float:
    """Compute Structural Similarity Index between two images.
    
    Args:
        image1_data: First image bytes.
        image2_data: Second image bytes.
    
    Returns:
        SSIM value (0.0 to 1.0).
    """
    try:
        import numpy as np
        from PIL import Image
        import io
        
        img1 = Image.open(io.BytesIO(image1_data)).convert("RGB")
        img2 = Image.open(io.BytesIO(image2_data)).convert("RGB")
        
        if img1.size != img2.size:
            img2 = img2.resize(img1.size, Image.LANCZOS)
        
        arr1 = np.array(img1).astype(float)
        arr2 = np.array(img2).astype(float)
        
        return _compute_ssim(arr1, arr2)
    except ImportError:
        raise ImportError("numpy and PIL are required for SSIM computation")


def are_images_similar(
    image1_data: bytes,
    image2_data: bytes,
    threshold: float = 0.95,
) -> bool:
    """Check if two images are similar based on SSIM.
    
    Args:
        image1_data: First image bytes.
        image2_data: Second image bytes.
        threshold: Similarity threshold (0.0 to 1.0).
    
    Returns:
        True if images are similar.
    """
    score = compute_similarity(image1_data, image2_data)
    return score.similarity >= threshold


def batch_compare(
    baseline_data: bytes,
    comparison_list: list[bytes],
    threshold: float = 0.95,
) -> list[Tuple[int, bool, SimilarityScore]]:
    """Compare baseline against multiple images.
    
    Args:
        baseline_data: Baseline image bytes.
        comparison_list: List of images to compare.
        threshold: Similarity threshold.
    
    Returns:
        List of (index, is_similar, score) tuples.
    """
    results = []
    for i, comp_data in enumerate(comparison_list):
        score = compute_similarity(baseline_data, comp_data)
        is_similar = score.similarity >= threshold
        results.append((i, is_similar, score))
    return results
