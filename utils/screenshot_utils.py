"""
Screenshot Utilities for UI Automation.

This module provides utilities for capturing, comparing, and analyzing
screenshots during automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable
from pathlib import Path


class ImageFormat(Enum):
    """Supported screenshot formats."""
    PNG = auto()
    JPEG = auto()
    BMP = auto()
    WEBP = auto()


@dataclass
class Screenshot:
    """
    Screenshot data container.
    
    Attributes:
        image_data: Raw image bytes
        width: Image width in pixels
        height: Image height in pixels
        format: Image format
        captured_at: Capture timestamp
        region: Optional region coordinates (x, y, width, height)
    """
    image_data: bytes
    width: int
    height: int
    format: ImageFormat = ImageFormat.PNG
    captured_at: float = field(default_factory=time.time)
    region: Optional[tuple[int, int, int, int]] = None
    metadata: dict = field(default_factory=dict)
    
    @property
    def size_bytes(self) -> int:
        """Get image size in bytes."""
        return len(self.image_data)
    
    @property
    def aspect_ratio(self) -> float:
        """Get image aspect ratio."""
        return self.width / self.height if self.height > 0 else 0
    
    @property
    def hash(self) -> str:
        """Get SHA256 hash of image data."""
        return hashlib.sha256(self.image_data).hexdigest()


@dataclass
class ScreenshotConfig:
    """Configuration for screenshot capture."""
    format: ImageFormat = ImageFormat.PNG
    quality: int = 95  # For JPEG
    include_cursor: bool = False
    include_timestamp: bool = True
    max_width: Optional[int] = None
    max_height: Optional[int] = None


class ScreenshotCapture:
    """Handles screenshot capture operations."""
    
    def __init__(self, config: Optional[ScreenshotConfig] = None):
        self.config = config or ScreenshotConfig()
    
    def capture_full_screen(self) -> Screenshot:
        """
        Capture the full screen.
        
        Returns:
            Screenshot object
        """
        # Placeholder - actual implementation would use platform APIs
        return Screenshot(
            image_data=b"",
            width=1920,
            height=1080,
            format=self.config.format
        )
    
    def capture_region(
        self, 
        x: int, 
        y: int, 
        width: int, 
        height: int
    ) -> Screenshot:
        """
        Capture a specific screen region.
        
        Args:
            x: X coordinate
            y: Y coordinate
            width: Region width
            height: Region height
            
        Returns:
            Screenshot object
        """
        return Screenshot(
            image_data=b"",
            width=width,
            height=height,
            format=self.config.format,
            region=(x, y, width, height)
        )
    
    def capture_window(self, window_id: str) -> Screenshot:
        """
        Capture a specific window.
        
        Args:
            window_id: Window identifier
            
        Returns:
            Screenshot object
        """
        return Screenshot(
            image_data=b"",
            width=800,
            height=600,
            format=self.config.format
        )


class ScreenshotComparator:
    """Compares screenshots for visual regression testing."""
    
    def __init__(self, threshold: float = 0.05):
        """
        Initialize comparator.
        
        Args:
            threshold: Difference threshold (0.0 - 1.0) for considering images different
        """
        self.threshold = threshold
    
    def compare(self, screenshot1: Screenshot, screenshot2: Screenshot) -> 'ComparisonResult':
        """
        Compare two screenshots.
        
        Args:
            screenshot1: First screenshot
            screenshot2: Second screenshot
            
        Returns:
            ComparisonResult with difference metrics
        """
        # Check dimensions
        if screenshot1.width != screenshot2.width or screenshot1.height != screenshot2.height:
            return ComparisonResult(
                identical=False,
                pixel_diff=1.0,
                diff_percentage=100.0,
                diff_regions=[],
                error="Dimension mismatch"
            )
        
        # Calculate pixel difference (placeholder)
        pixel_diff = self._calculate_pixel_diff(
            screenshot1.image_data,
            screenshot2.image_data
        )
        
        diff_percentage = pixel_diff * 100
        
        return ComparisonResult(
            identical=pixel_diff < self.threshold,
            pixel_diff=pixel_diff,
            diff_percentage=diff_percentage,
            diff_regions=[]
        )
    
    def _calculate_pixel_diff(self, data1: bytes, data2: bytes) -> float:
        """Calculate normalized pixel difference between two images."""
        if len(data1) != len(data2):
            return 1.0
        
        if len(data1) == 0:
            return 0.0
        
        diff_count = sum(c1 != c2 for c1, c2 in zip(data1, data2))
        return diff_count / len(data1)
    
    def find_diff_regions(
        self, 
        screenshot1: Screenshot, 
        screenshot2: Screenshot,
        block_size: int = 16
    ) -> list[tuple[int, int, int, int]]:
        """
        Find rectangular regions that differ between screenshots.
        
        Args:
            screenshot1: First screenshot
            screenshot2: Second screenshot
            block_size: Size of blocks to check
            
        Returns:
            List of (x, y, width, height) tuples for differing regions
        """
        regions = []
        
        # Placeholder implementation
        return regions


@dataclass
class ComparisonResult:
    """Result of screenshot comparison."""
    identical: bool
    pixel_diff: float  # 0.0 - 1.0
    diff_percentage: float  # 0.0 - 100.0
    diff_regions: list[tuple[int, int, int, int]]
    error: Optional[str] = None


class ScreenshotManager:
    """
    Manages screenshot capture, storage, and retrieval.
    
    Example:
        manager = ScreenshotManager("./screenshots")
        manager.capture("homepage")
        screenshot = manager.get("homepage")
    """
    
    def __init__(self, storage_dir: str = "./screenshots"):
        self.storage_dir = Path(storage_dir)
        self._screenshots: dict[str, Screenshot] = {}
        self._capture = ScreenshotCapture()
    
    def capture(
        self, 
        name: str, 
        capture_func: Optional[Callable[[], Screenshot]] = None
    ) -> Screenshot:
        """
        Capture and store a screenshot.
        
        Args:
            name: Screenshot name/key
            capture_func: Optional custom capture function
            
        Returns:
            Captured Screenshot
        """
        if capture_func:
            screenshot = capture_func()
        else:
            screenshot = self._capture.capture_full_screen()
        
        self._screenshots[name] = screenshot
        return screenshot
    
    def get(self, name: str) -> Optional[Screenshot]:
        """Get a stored screenshot by name."""
        return self._screenshots.get(name)
    
    def save(self, name: str, path: Optional[str] = None) -> str:
        """
        Save a screenshot to disk.
        
        Args:
            name: Screenshot name
            path: Optional save path (defaults to storage_dir)
            
        Returns:
            Path where screenshot was saved
        """
        screenshot = self._screenshots.get(name)
        if not screenshot:
            raise ValueError(f"Screenshot not found: {name}")
        
        save_path = Path(path) if path else self.storage_dir / f"{name}.png"
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(save_path, "wb") as f:
            f.write(screenshot.image_data)
        
        return str(save_path)
    
    def load(self, name: str, path: str) -> Screenshot:
        """
        Load a screenshot from disk.
        
        Args:
            name: Name to store screenshot as
            path: Path to screenshot file
            
        Returns:
            Loaded Screenshot
        """
        with open(path, "rb") as f:
            data = f.read()
        
        screenshot = Screenshot(
            image_data=data,
            width=0,  # Would need image processing to determine
            height=0
        )
        
        self._screenshots[name] = screenshot
        return screenshot
    
    def list_screenshots(self) -> list[str]:
        """List all stored screenshot names."""
        return list(self._screenshots.keys())
    
    def delete(self, name: str) -> bool:
        """Delete a stored screenshot."""
        if name in self._screenshots:
            del self._screenshots[name]
            return True
        return False
