"""
Screen analysis utilities for visual element detection and analysis.

Provides screen region analysis, color detection, and element
identification for automation workflows.
"""

from __future__ import annotations

import cv2
import numpy as np
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


@dataclass
class ScreenRegion:
    """Screen region definition."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def area(self) -> int:
        return self.width * self.height
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains(self, x: int, y: int) -> bool:
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height
    
    def to_tuple(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)


@dataclass
class ColorRegion:
    """Region with specific color characteristics."""
    region: ScreenRegion
    dominant_color: Tuple[int, int, int]
    color_count: int
    percentage: float


class ScreenAnalyzer:
    """Analyzes screen content."""
    
    def __init__(self, screenshot_path: str):
        """
        Initialize screen analyzer.
        
        Args:
            screenshot_path: Path to screenshot file.
        """
        self.screenshot_path = screenshot_path
        self.image = cv2.imread(screenshot_path)
        self.gray = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY) if self.image is not None else None
    
    def is_valid(self) -> bool:
        """Check if screenshot was loaded."""
        return self.image is not None
    
    def get_region(self, x: int, y: int, width: int, height: int) -> np.ndarray:
        """
        Extract region from screenshot.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            width: Region width.
            height: Region height.
            
        Returns:
            Image array or empty array.
        """
        if self.image is None:
            return np.array([])
        return self.image[y:y+height, x:x+width]
    
    def get_pixel_color(self, x: int, y: int) -> Optional[Tuple[int, int, int]]:
        """
        Get color of pixel at coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            BGR color tuple or None.
        """
        if self.image is None:
            return None
        if 0 <= y < self.image.shape[0] and 0 <= x < self.image.shape[1]:
            return tuple(self.image[y, x])
        return None
    
    def find_color_regions(self, target_color: Tuple[int, int, int],
                          tolerance: int = 30) -> List[ScreenRegion]:
        """
        Find regions matching target color.
        
        Args:
            target_color: BGR color to find.
            tolerance: Color matching tolerance.
            
        Returns:
            List of ScreenRegion with matching pixels.
        """
        if self.image is None:
            return []
        
        lower = np.array([max(0, c - tolerance) for c in target_color])
        upper = np.array([min(255, c + tolerance) for c in target_color])
        
        mask = cv2.inRange(self.image, lower, upper)
        
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        regions = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if w > 5 and h > 5:
                regions.append(ScreenRegion(x, y, w, h))
        
        return regions
    
    def find_bright_regions(self, threshold: int = 200) -> List[ScreenRegion]:
        """
        Find bright/highlighted regions.
        
        Args:
            threshold: Brightness threshold (0-255).
            
        Returns:
            List of bright ScreenRegion.
        """
        if self.gray is None:
            return []
        
        _, bright = cv2.threshold(self.gray, threshold, 255, cv2.THRESH_BINARY)
        
        contours, _ = cv2.findContours(bright, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        regions = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            if w > 10 and h > 10:
                regions.append(ScreenRegion(x, y, w, h))
        
        return regions
    
    def find_edges(self) -> np.ndarray:
        """
        Detect edges in screenshot.
        
        Returns:
            Edge map array.
        """
        if self.gray is None:
            return np.array([])
        return cv2.Canny(self.gray, 50, 150)
    
    def find_contours(self, min_area: int = 100) -> List[ScreenRegion]:
        """
        Find contours in screenshot.
        
        Args:
            min_area: Minimum contour area.
            
        Returns:
            List of ScreenRegion.
        """
        if self.image is None:
            return []
        
        gray = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        regions = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area >= min_area:
                x, y, w, h = cv2.boundingRect(contour)
                regions.append(ScreenRegion(x, y, w, h))
        
        return regions
    
    def get_histogram(self, region: Optional[ScreenRegion] = None) -> Dict[str, Any]:
        """
        Get color histogram for region or full image.
        
        Args:
            region: Optional region to analyze.
            
        Returns:
            Histogram data dict.
        """
        if self.image is None:
            return {}
        
        img = self.image
        if region:
            img = self.get_region(region.x, region.y, region.width, region.height)
        
        hist_b = cv2.calcHist([img], [0], None, [256], [0, 256])
        hist_g = cv2.calcHist([img], [1], None, [256], [0, 256])
        hist_r = cv2.calcHist([img], [2], None, [256], [0, 256])
        
        return {
            'blue': hist_b.flatten().tolist(),
            'green': hist_g.flatten().tolist(),
            'red': hist_r.flatten().tolist(),
        }
    
    def detect_text_regions(self) -> List[ScreenRegion]:
        """
        Detect potential text regions.
        
        Returns:
            List of text-like ScreenRegion.
        """
        if self.gray is None:
            return []
        
        _, binary = cv2.threshold(self.gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
        
        horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (25, 1))
        vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 25))
        
        horizontal = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel)
        vertical = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel)
        
        combined = cv2.addWeighted(horizontal, 0.5, vertical, 0.5, 0)
        
        contours, _ = cv2.findContours(combined, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        regions = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            aspect = w / h if h > 0 else 0
            if 0.1 <= aspect <= 20 and w > 20 and h > 5:
                regions.append(ScreenRegion(x, y, w, h))
        
        return regions
    
    def get_dominant_colors(self, num_colors: int = 5) -> List[Tuple[int, int, int]]:
        """
        Get dominant colors in image.
        
        Args:
            num_colors: Number of colors to extract.
            
        Returns:
            List of BGR color tuples.
        """
        if self.image is None:
            return []
        
        pixels = self.image.reshape(-1, 3).astype(np.float32)
        
        criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 100, 0.2)
        _, labels, centers = cv2.kmeans(pixels, num_colors, None, criteria, 10, cv2.KMEANS_RANDOM_CENTERS)
        
        return [tuple(map(int, c)) for c in centers]
    
    def compare_region(self, region: ScreenRegion,
                      reference_color: Tuple[int, int, int],
                      tolerance: int = 30) -> float:
        """
        Compare region color to reference.
        
        Args:
            region: Region to check.
            reference_color: Reference BGR color.
            tolerance: Matching tolerance.
            
        Returns:
            Similarity percentage (0-100).
        """
        if self.image is None:
            return 0.0
        
        roi = self.get_region(region.x, region.y, region.width, region.height)
        
        lower = np.array([max(0, c - tolerance) for c in reference_color])
        upper = np.array([min(255, c + tolerance) for c in reference_color])
        
        mask = cv2.inRange(roi, lower, upper)
        match_pixels = np.count_nonzero(mask)
        total_pixels = roi.shape[0] * roi.shape[1]
        
        return (match_pixels / total_pixels * 100) if total_pixels > 0 else 0.0
