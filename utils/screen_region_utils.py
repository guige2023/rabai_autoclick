"""
Screen region utilities for region-based automation operations.

Provides region definition, capture, and analysis
for automation workflows.
"""

from __future__ import annotations

import subprocess
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum
import os


class RegionType(Enum):
    """Screen region types."""
    RECTANGLE = "rectangle"
    CIRCLE = "circle"
    POLYGON = "polygon"


@dataclass
class ScreenRegion:
    """Screen region definition."""
    id: str
    name: str
    x: int
    y: int
    width: int
    height: int
    region_type: RegionType = RegionType.RECTANGLE
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def area(self) -> int:
        return self.width * self.height
    
    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)
    
    def contains(self, x: int, y: int) -> bool:
        """Check if point is within region."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height
    
    def overlaps(self, other: 'ScreenRegion') -> bool:
        """Check if regions overlap."""
        return not (self.x + self.width < other.x or
                   other.x + other.width < self.x or
                   self.y + self.height < other.y or
                   other.y + other.height < self.y)


@dataclass
class RegionCaptureResult:
    """Result of region capture."""
    region: ScreenRegion
    path: str
    width: int
    height: int
    size_bytes: int


class RegionManager:
    """Manages screen regions for automation."""
    
    def __init__(self):
        self._regions: Dict[str, ScreenRegion] = {}
    
    def add(self, region: ScreenRegion) -> None:
        """Add region."""
        self._regions[region.id] = region
    
    def remove(self, region_id: str) -> bool:
        """Remove region."""
        if region_id in self._regions:
            del self._regions[region_id]
            return True
        return False
    
    def get(self, region_id: str) -> Optional[ScreenRegion]:
        """Get region by ID."""
        return self._regions.get(region_id)
    
    def get_by_name(self, name: str) -> Optional[ScreenRegion]:
        """Get region by name."""
        for region in self._regions.values():
            if region.name == name:
                return region
        return None
    
    def get_all(self) -> List[ScreenRegion]:
        """Get all regions."""
        return list(self._regions.values())
    
    def capture(self, region: ScreenRegion,
               output_dir: str = "/tmp") -> Optional[RegionCaptureResult]:
        """
        Capture region screenshot.
        
        Args:
            region: Region to capture.
            output_dir: Output directory.
            
        Returns:
            RegionCaptureResult or None.
        """
        timestamp = subprocess.run(
            ["date", "+%Y%m%d_%H%M%S"],
            capture_output=True,
            text=True
        ).stdout.strip()
        
        output_path = os.path.join(output_dir, f"region_{region.id}_{timestamp}.png")
        
        try:
            result = subprocess.run(
                [
                    "screencapture",
                    "-x",
                    "-R", f"{region.x},{region.y},{region.width},{region.height}",
                    output_path
                ],
                check=True,
                capture_output=True
            )
            
            if os.path.exists(output_path):
                stat = os.stat(output_path)
                import cv2
                img = cv2.imread(output_path)
                h, w = img.shape[:2] if img is not None else (0, 0)
                
                return RegionCaptureResult(
                    region=region,
                    path=output_path,
                    width=w,
                    height=h,
                    size_bytes=stat.st_size
                )
        except subprocess.CalledProcessError:
            pass
        
        return None
    
    def capture_all(self, output_dir: str = "/tmp") -> List[RegionCaptureResult]:
        """
        Capture all registered regions.
        
        Args:
            output_dir: Output directory.
            
        Returns:
            List of RegionCaptureResult.
        """
        results = []
        for region in self._regions.values():
            result = self.capture(region, output_dir)
            if result:
                results.append(result)
        return results
    
    def find_region_at(self, x: int, y: int) -> Optional[ScreenRegion]:
        """
        Find region containing point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            ScreenRegion or None.
        """
        for region in self._regions.values():
            if region.contains(x, y):
                return region
        return None
    
    def find_regions_overlapping(self, region: ScreenRegion) -> List[ScreenRegion]:
        """
        Find regions that overlap with given region.
        
        Args:
            region: Region to check.
            
        Returns:
            List of overlapping ScreenRegion.
        """
        overlapping = []
        for r in self._regions.values():
            if region.overlaps(r):
                overlapping.append(r)
        return overlapping


def create_region(id: str, name: str,
                 x: int, y: int, width: int, height: int,
                 tags: Optional[List[str]] = None) -> ScreenRegion:
    """
    Create a screen region.
    
    Args:
        id: Region ID.
        name: Region name.
        x: X coordinate.
        y: Y coordinate.
        width: Width.
        height: Height.
        tags: Optional tags.
        
    Returns:
        New ScreenRegion.
    """
    return ScreenRegion(
        id=id,
        name=name,
        x=x, y=y, width=width, height=height,
        tags=tags or []
    )


def get_display_region(display_index: int = 0) -> Optional[ScreenRegion]:
    """
    Get region for display.
    
    Args:
        display_index: Display index.
        
    Returns:
        ScreenRegion for display or None.
    """
    try:
        import Quartz
        screens = Quartz.NSScreen.screens()
        
        if display_index < len(screens):
            screen = screens[display_index]
            frame = screen.frame()
            
            return ScreenRegion(
                id=f"display_{display_index}",
                name=f"Display {display_index}",
                x=int(frame.origin.x),
                y=int(frame.origin.y),
                width=int(frame.size.width),
                height=int(frame.size.height)
            )
    except Exception:
        pass
    
    return None
