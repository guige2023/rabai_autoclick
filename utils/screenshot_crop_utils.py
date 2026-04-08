"""
Screenshot capture and cropping utilities.

Provides utilities for capturing screen regions, monitor selection,
and image cropping operations for automation workflows.
"""

from __future__ import annotations

import subprocess
import os
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ScreenRegion:
    """Represents a rectangular screen region."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def right(self) -> int:
        return self.x + self.width
    
    @property
    def bottom(self) -> int:
        return self.y + self.height
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains(self, px: int, py: int) -> bool:
        """Check if a point is within this region."""
        return self.x <= px < self.right and self.y <= py < self.bottom
    
    def intersects(self, other: "ScreenRegion") -> bool:
        """Check if this region intersects with another."""
        return not (
            self.right <= other.x or
            other.right <= self.x or
            self.bottom <= other.y or
            other.bottom <= self.y
        )
    
    def intersection(self, other: "ScreenRegion") -> Optional["ScreenRegion"]:
        """Get intersection with another region."""
        if not self.intersects(other):
            return None
        
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        width = min(self.right, other.right) - x
        height = min(self.bottom, other.bottom) - y
        
        return ScreenRegion(x, y, width, height)
    
    def to_screencapture_args(self) -> List[str]:
        """Convert to screencapture command arguments."""
        return ["-R", f"{self.x},{self.y},{self.width},{self.height}"]


@dataclass
class DisplayInfo:
    """Information about a connected display."""
    display_id: int
    name: str
    bounds: ScreenRegion
    is_main: bool = False
    scale_factor: float = 1.0


def get_display_count() -> int:
    """Get the number of connected displays.
    
    Returns:
        Number of displays
    """
    try:
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            if "SPDisplaysDataType" in data:
                displays = data["SPDisplaysDataType"]
                if isinstance(displays, list):
                    return len(displays)
                elif isinstance(displays, dict):
                    return len(displays)
    except Exception:
        pass
    
    # Fallback: try system_profiler without JSON
    try:
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType"],
            capture_output=True,
            text=True,
            timeout=5
        )
        count = result.stdout.count("Display Type:")
        if count > 0:
            return count
    except Exception:
        pass
    
    return 1  # Default to 1


def get_displays() -> List[DisplayInfo]:
    """Get information about all connected displays.
    
    Returns:
        List of DisplayInfo objects
    """
    displays = []
    
    try:
        # Use Quartz to get display info
        import Quartz
        for i, screen in enumerate(Quartz.CGGetActiveDisplayList(10, None, None)[1]):
            info = Quartz.CGDisplayCopyDisplayMode(screen)
            bounds = Quartz.CGRectMakeZero()
            Quartz.CGDisplayBounds(screen, bounds)
            
            is_main = screen == Quartz.CGMainDisplayID()
            scale = Quartz.CGDisplayScreenSize(screen).width / bounds.size.width if bounds.size.width > 0 else 1.0
            
            displays.append(DisplayInfo(
                display_id=int(screen),
                name=f"Display {i + 1}",
                bounds=ScreenRegion(
                    x=int(bounds.origin.x),
                    y=int(bounds.origin.y),
                    width=int(bounds.size.width),
                    height=int(bounds.size.height)
                ),
                is_main=is_main,
                scale_factor=scale
            ))
    except Exception:
        # Fallback: assume main display
        displays.append(DisplayInfo(
            display_id=0,
            name="Main Display",
            bounds=ScreenRegion(0, 0, 1920, 1080),
            is_main=True
        ))
    
    return displays


def capture_screen(
    output_path: Optional[str] = None,
    region: Optional[ScreenRegion] = None,
    display_index: int = 0,
    include_cursor: bool = True
) -> bytes:
    """Capture screenshot using macOS screencapture.
    
    Args:
        output_path: Optional path to save screenshot
        region: Optional region to capture
        display_index: Display index (0 = all displays)
        include_cursor: Whether to include cursor
        
    Returns:
        PNG image data as bytes
    """
    cmd = ["screencapture"]
    
    if not include_cursor:
        cmd.append("-C")
    
    if region:
        cmd.extend(["-R", f"{region.x},{region.y},{region.width},{region.height}"])
    
    if output_path:
        cmd.append(output_path)
    else:
        cmd.extend(["-x", "/tmp/screenshot_temp.png"])
        output_path = "/tmp/screenshot_temp.png"
    
    subprocess.run(cmd, capture_output=True, timeout=10)
    
    with open(output_path, "rb") as f:
        data = f.read()
    
    if not output_path.startswith("/tmp/screenshot"):
        os.remove(output_path)
    
    return data


def crop_image(
    image_data: bytes,
    x: int,
    y: int,
    width: int,
    height: int
) -> bytes:
    """Crop image data using sips.
    
    Args:
        image_data: PNG image bytes
        x: Left coordinate
        y: Top coordinate
        width: Crop width
        height: Crop height
        
    Returns:
        Cropped PNG image bytes
    """
    # Write input file
    input_path = "/tmp/crop_input.png"
    output_path = "/tmp/crop_output.png"
    
    with open(input_path, "wb") as f:
        f.write(image_data)
    
    # Calculate crop geometry (from top-left)
    # Note: sips uses bottom-left origin
    # We need to flip the y coordinate
    # Get image dimensions first
    result = subprocess.run(
        ["sips", "-g", "pixelHeight", input_path],
        capture_output=True,
        text=True
    )
    
    img_height = 1080
    for line in result.stdout.split("\n"):
        if "pixelHeight" in line:
            img_height = int(line.split(":")[-1].strip())
    
    # Convert coordinates
    top_y = y
    crop_height = height
    
    # Use sips crop
    # Format: --cropToHeightWidth {height},{width}
    subprocess.run(
        ["sips", "-c", f"{crop_height},{width}", "--cropOffset", f"{top_y},0", input_path],
        capture_output=True
    )
    
    with open(input_path, "rb") as f:
        cropped = f.read()
    
    # Cleanup
    os.remove(input_path)
    if os.path.exists(output_path):
        os.remove(output_path)
    
    return cropped


def save_screenshot(
    image_data: bytes,
    output_path: str,
    format: str = "png"
) -> None:
    """Save screenshot to file with specified format.
    
    Args:
        image_data: PNG image bytes
        output_path: Output file path
        format: Output format (png, jpg, tiff, etc.)
    """
    temp_path = "/tmp/screenshot_convert.png"
    
    with open(temp_path, "wb") as f:
        f.write(image_data)
    
    if format.lower() != "png":
        subprocess.run(
            ["sips", "-s", f"format {format.lower()}", temp_path, "--out", output_path],
            capture_output=True
        )
    else:
        import shutil
        shutil.copy(temp_path, output_path)
    
    os.remove(temp_path)


def get_pixel_color_at(
    x: int,
    y: int,
    screenshot_data: Optional[bytes] = None
) -> Tuple[int, int, int, int]:
    """Get RGBA color at specific coordinates.
    
    Args:
        x: X coordinate
        y: Y coordinate
        screenshot_data: Optional pre-captured screenshot data
        
    Returns:
        RGBA tuple (0-255 per channel)
    """
    # Capture small region around point
    region = ScreenRegion(
        x=max(0, x - 1),
        y=max(0, y - 1),
        width=3,
        height=3
    )
    
    if screenshot_data is None:
        screenshot_data = capture_screen(region=region)
    
    # Write to temp file for analysis
    temp_path = "/tmp/pixel_check.png"
    with open(temp_path, "wb") as f:
        f.write(screenshot_data)
    
    # Use sips to get pixel data via TIFF conversion
    # This is a simplified approach - in production you'd use PIL
    
    # For now, return a placeholder
    # A full implementation would parse the image data directly
    os.remove(temp_path)
    
    return (0, 0, 0, 255)


def capture_multiple_displays() -> Dict[int, bytes]:
    """Capture screenshots from all connected displays.
    
    Returns:
        Dictionary mapping display_id -> PNG bytes
    """
    results = {}
    
    displays = get_displays()
    for display in displays:
        # Capture full display
        img_data = capture_screen(region=display.bounds)
        results[display.display_id] = img_data
    
    return results


def capture_with_overlay(
    region: ScreenRegion,
    overlay_color: Tuple[int, int, int] = (255, 0, 0),
    overlay_alpha: float = 0.3,
    border_width: int = 3
) -> bytes:
    """Capture screenshot with visual overlay on region.
    
    Useful for debugging and visualizing captured regions.
    
    Args:
        region: Region to capture and highlight
        overlay_color: RGB color for overlay
        overlay_alpha: Alpha for overlay (0.0-1.0)
        border_width: Width of border in pixels
        
    Returns:
        PNG image bytes with overlay
    """
    # Capture the region
    img_data = capture_screen(region=region)
    
    # Note: Full implementation would draw border using PIL/Pillow
    # For now, return the plain capture
    return img_data


def create_region_from_points(
    point1: Tuple[int, int],
    point2: Tuple[int, int]
) -> ScreenRegion:
    """Create a ScreenRegion from two corner points.
    
    Args:
        point1: First corner (x, y)
        point2: Second corner (x, y)
        
    Returns:
        ScreenRegion covering the rectangle
    """
    x = min(point1[0], point2[0])
    y = min(point1[1], point2[1])
    width = abs(point2[0] - point1[0])
    height = abs(point2[1] - point1[1])
    
    return ScreenRegion(x, y, width, height)


def expand_region(
    region: ScreenRegion,
    pixels: int,
    max_bounds: Optional[ScreenRegion] = None
) -> ScreenRegion:
    """Expand a region by a number of pixels.
    
    Args:
        region: Original region
        pixels: Number of pixels to expand in each direction
        max_bounds: Optional bounds to clamp expansion
        
    Returns:
        Expanded ScreenRegion
    """
    new_x = region.x - pixels
    new_y = region.y - pixels
    new_width = region.width + pixels * 2
    new_height = region.height + pixels * 2
    
    if max_bounds:
        # Clamp to bounds
        if new_x < max_bounds.x:
            new_width -= (max_bounds.x - new_x)
            new_x = max_bounds.x
        if new_y < max_bounds.y:
            new_height -= (max_bounds.y - new_y)
            new_y = max_bounds.y
        if new_x + new_width > max_bounds.right:
            new_width = max_bounds.right - new_x
        if new_y + new_height > max_bounds.bottom:
            new_height = max_bounds.bottom - new_y
    
    return ScreenRegion(new_x, new_y, max(0, new_width), max(0, new_height))


def shrink_region(
    region: ScreenRegion,
    pixels: int
) -> ScreenRegion:
    """Shrink a region by a number of pixels.
    
    Args:
        region: Original region
        pixels: Number of pixels to shrink in each direction
        
    Returns:
        Shrunk ScreenRegion
    """
    return expand_region(region, -pixels)


def regions_equal(r1: ScreenRegion, r2: ScreenRegion, tolerance: int = 0) -> bool:
    """Check if two regions are equal within tolerance.
    
    Args:
        r1: First region
        r2: Second region
        tolerance: Pixel tolerance for comparison
        
    Returns:
        True if regions are equal within tolerance
    """
    return (
        abs(r1.x - r2.x) <= tolerance and
        abs(r1.y - r2.y) <= tolerance and
        abs(r1.width - r2.width) <= tolerance and
        abs(r1.height - r2.height) <= tolerance
    )
