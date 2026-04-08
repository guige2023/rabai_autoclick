"""
Screenshot diff utilities for detecting visual changes.

Provides screenshot comparison, diff highlighting,
and change detection for automation testing.
"""

from __future__ import annotations

import cv2
import numpy as np
from typing import Optional, List, Tuple
from dataclasses import dataclass
import hashlib


@dataclass
class DiffResult:
    """Diff analysis result."""
    is_different: bool
    similarity: float
    diff_pixels: int
    total_pixels: int
    diff_percentage: float
    diff_image_path: Optional[str] = None


class ScreenshotDiff:
    """Compares screenshots and generates diffs."""
    
    def __init__(self, screenshot1_path: str, screenshot2_path: str):
        """
        Initialize screenshot diff.
        
        Args:
            screenshot1_path: Path to first screenshot.
            screenshot2_path: Path to second screenshot.
        """
        self.screenshot1_path = screenshot1_path
        self.screenshot2_path = screenshot2_path
        self.img1: Optional[np.ndarray] = None
        self.img2: Optional[np.ndarray] = None
        self._load_images()
    
    def _load_images(self) -> bool:
        """Load both screenshots."""
        try:
            self.img1 = cv2.imread(self.screenshot1_path)
            self.img2 = cv2.imread(self.screenshot2_path)
            return self.img1 is not None and self.img2 is not None
        except Exception:
            return False
    
    def compare(self, threshold: int = 30) -> DiffResult:
        """
        Compare screenshots and return diff result.
        
        Args:
            threshold: Pixel difference threshold.
            
        Returns:
            DiffResult.
        """
        if self.img1 is None or self.img2 is None:
            return DiffResult(
                is_different=True,
                similarity=0.0,
                diff_pixels=0,
                total_pixels=0,
                diff_percentage=100.0
            )
        
        if self.img1.shape != self.img2.shape:
            h = max(self.img1.shape[0], self.img2.shape[0])
            w = max(self.img1.shape[1], self.img2.shape[1])
            
            img1_resized = np.zeros((h, w, 3), dtype=np.uint8)
            img2_resized = np.zeros((h, w, 3), dtype=np.uint8)
            
            img1_resized[:self.img1.shape[0], :self.img1.shape[1]] = self.img1
            img2_resized[:self.img2.shape[0], :self.img2.shape[1]] = self.img2
            
            self.img1 = img1_resized
            self.img2 = img2_resized
        
        gray1 = cv2.cvtColor(self.img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(self.img2, cv2.COLOR_BGR2GRAY)
        
        diff = cv2.absdiff(gray1, gray2)
        _, thresh = cv2.threshold(diff, threshold, 255, cv2.THRESH_BINARY)
        
        total_pixels = gray1.shape[0] * gray1.shape[1]
        diff_pixels = np.count_nonzero(thresh)
        diff_percentage = (diff_pixels / total_pixels * 100) if total_pixels > 0 else 0
        similarity = 1.0 - (diff_pixels / total_pixels) if total_pixels > 0 else 0
        
        return DiffResult(
            is_different=diff_percentage > 0,
            similarity=float(similarity),
            diff_pixels=int(diff_pixels),
            total_pixels=int(total_pixels),
            diff_percentage=float(diff_percentage)
        )
    
    def get_diff_image(self, output_path: str,
                      highlight_color: Tuple[int, int, int] = (0, 0, 255),
                      alpha: float = 0.5) -> Optional[str]:
        """
        Generate diff visualization image.
        
        Args:
            output_path: Output file path.
            highlight_color: BGR color for highlights.
            alpha: Blend factor.
            
        Returns:
            Path to diff image or None.
        """
        if self.img1 is None or self.img2 is None:
            return None
        
        gray1 = cv2.cvtColor(self.img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(self.img2, cv2.COLOR_BGR2GRAY)
        
        diff = cv2.absdiff(gray1, gray2)
        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
        
        diff_colored = cv2.cvtColor(thresh, cv2.COLOR_GRAY2BGR)
        
        highlighted = cv2.addWeighted(self.img2, 1 - alpha, diff_colored, alpha, 0)
        
        try:
            cv2.imwrite(output_path, highlighted)
            return output_path
        except Exception:
            return None
    
    def get_diff_regions(self, min_area: int = 100) -> List[Tuple[int, int, int, int]]:
        """
        Get bounding boxes of diff regions.
        
        Args:
            min_area: Minimum region area.
            
        Returns:
            List of (x, y, w, h) tuples.
        """
        if self.img1 is None or self.img2 is None:
            return []
        
        gray1 = cv2.cvtColor(self.img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(self.img2, cv2.COLOR_BGR2GRAY)
        
        diff = cv2.absdiff(gray1, gray2)
        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
        
        contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        regions = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area >= min_area:
                x, y, w, h = cv2.boundingRect(contour)
                regions.append((x, y, w, h))
        
        return regions


def quick_diff(path1: str, path2: str, threshold: float = 0.95) -> bool:
    """
    Quick check if two screenshots are different.
    
    Args:
        path1: First screenshot path.
        path2: Second screenshot path.
        threshold: Similarity threshold.
        
    Returns:
        True if different, False otherwise.
    """
    diff = ScreenshotDiff(path1, path2)
    result = diff.compare()
    return result.diff_percentage > (1 - threshold) * 100
