"""
Visual comparison utilities for screenshot diffing and analysis.

Provides visual diff generation, image similarity scoring,
and change detection for GUI automation testing.
"""

from __future__ import annotations

import cv2
import numpy as np
from typing import Optional, Tuple, List
from dataclasses import dataclass
import hashlib


@dataclass
class DiffRegion:
    """Region with detected difference."""
    x: int
    y: int
    width: int
    height: int
    similarity: float
    diff_pixels: int


@dataclass
class VisualDiffResult:
    """Result of visual comparison."""
    is_different: bool
    similarity_score: float
    diff_regions: List[DiffRegion]
    diff_image_path: Optional[str]
    diff_percentage: float


def compare_images(img1_path: str, img2_path: str,
                   threshold: float = 0.95) -> VisualDiffResult:
    """
    Compare two images and return differences.
    
    Args:
        img1_path: Path to first image.
        img2_path: Path to second image.
        threshold: Similarity threshold (0-1).
        
    Returns:
        VisualDiffResult with comparison details.
    """
    img1 = cv2.imread(img1_path)
    img2 = cv2.imread(img2_path)
    
    if img1 is None or img2 is None:
        return VisualDiffResult(
            is_different=True,
            similarity_score=0.0,
            diff_regions=[],
            diff_image_path=None,
            diff_percentage=100.0
        )
    
    return compare_image_arrays(img1, img2, threshold)


def compare_image_arrays(img1: np.ndarray, img2: np.ndarray,
                         threshold: float = 0.95) -> VisualDiffResult:
    """
    Compare two image arrays.
    
    Args:
        img1: First image array.
        img2: Second image array.
        threshold: Similarity threshold.
        
    Returns:
        VisualDiffResult with comparison details.
    """
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    
    diff = cv2.absdiff(gray1, gray2)
    _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
    
    total_pixels = gray1.shape[0] * gray1.shape[1]
    diff_pixels = np.count_nonzero(thresh)
    diff_percentage = (diff_pixels / total_pixels) * 100
    
    similarity = 1.0 - (diff_pixels / total_pixels)
    
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    regions = []
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w > 5 and h > 5:
            region_diff = np.count_nonzero(thresh[y:y+h, x:x+w])
            total_region = w * h
            regions.append(DiffRegion(
                x=int(x), y=int(y), width=int(w), height=int(h),
                similarity=1.0 - (region_diff / total_region),
                diff_pixels=int(region_diff)
            ))
    
    diff_img = cv2.bitwise_and(img1, img1, mask=thresh)
    diff_path = None
    if regions:
        timestamp = hashlib.md5(str(img1.tobytes()).encode()).hexdigest()[:8]
        diff_path = f"/tmp/visual_diff_{timestamp}.png"
        cv2.imwrite(diff_path, diff_img)
    
    return VisualDiffResult(
        is_different=diff_percentage > (1 - threshold) * 100,
        similarity_score=similarity,
        diff_regions=regions,
        diff_image_path=diff_path,
        diff_percentage=diff_percentage
    )


def generate_diff_image(img1: np.ndarray, img2: np.ndarray,
                       highlight_color: Tuple[int, int, int] = (0, 0, 255),
                       alpha: float = 0.5) -> np.ndarray:
    """
    Generate visual diff image with highlighted changes.
    
    Args:
        img1: First image.
        img2: Second image.
        highlight_color: BGR color for highlights.
        alpha: Blend factor.
        
    Returns:
        Composite diff image.
    """
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    
    diff = cv2.absdiff(gray1, gray2)
    _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
    
    highlighted = img2.copy()
    mask_colored = cv2.cvtColor(thresh, cv2.COLOR_GRAY2BGR)
    cv2.addWeighted(highlighted, 1 - alpha, mask_colored, alpha, 0, highlighted)
    
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    cv2.drawContours(highlighted, contours, -1, highlight_color, 2)
    
    return highlighted


def calculate_mse(img1: np.ndarray, img2: np.ndarray) -> float:
    """
    Calculate Mean Squared Error between images.
    
    Args:
        img1: First image.
        img2: Second image.
        
    Returns:
        MSE value.
    """
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY).astype(float)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY).astype(float)
    
    mse = np.mean((gray1 - gray2) ** 2)
    return float(mse)


def calculate_psnr(img1: np.ndarray, img2: np.ndarray) -> float:
    """
    Calculate Peak Signal-to-Noise Ratio.
    
    Args:
        img1: First image.
        img2: Second image.
        
    Returns:
        PSNR value in dB.
    """
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    
    mse = calculate_mse(img1, img2)
    if mse == 0:
        return float('inf')
    
    max_pixel = 255.0
    psnr = 20 * np.log10(max_pixel / np.sqrt(mse))
    return float(psnr)


def calculate_ssim(img1: np.ndarray, img2: np.ndarray) -> float:
    """
    Calculate Structural Similarity Index.
    
    Args:
        img1: First image.
        img2: Second image.
        
    Returns:
        SSIM value (0-1).
    """
    if img1.shape != img2.shape:
        img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
    
    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
    
    C1 = (0.01 * 255) ** 2
    C2 = (0.03 * 255) ** 2
    
    mu1 = cv2.GaussianBlur(gray1.astype(float), (11, 11), 1.5)
    mu2 = cv2.GaussianBlur(gray2.astype(float), (11, 11), 1.5)
    
    mu1_sq = mu1 ** 2
    mu2_sq = mu2 ** 2
    mu1_mu2 = mu1 * mu2
    
    sigma1_sq = cv2.GaussianBlur(gray1.astype(float) ** 2, (11, 11), 1.5) - mu1_sq
    sigma2_sq = cv2.GaussianBlur(gray2.astype(float) ** 2, (11, 11), 1.5) - mu2_sq
    sigma12 = cv2.GaussianBlur(gray1.astype(float) * gray2.astype(float), (11, 11), 1.5) - mu1_mu2
    
    ssim_map = ((2 * mu1_mu2 + C1) * (2 * sigma12 + C2)) / ((mu1_sq + mu2_sq + C1) * (sigma1_sq + sigma2_sq + C2))
    
    return float(np.mean(ssim_map))
