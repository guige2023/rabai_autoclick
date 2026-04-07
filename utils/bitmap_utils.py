"""
Bitmap utilities for pixel-level image manipulation and analysis.

This module provides comprehensive bitmap operations including:
- Pixel access and modification
- Bitmap comparisons and difference calculation
- Region of interest (ROI) operations
- Bitmap filtering and transformations
- Histogram analysis

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, Iterator, List, Optional, Tuple, Union


@dataclass
class Bitmap:
    """
    A 2D bitmap representation with pixel-level operations.
    
    The bitmap stores pixel data as a 2D grid where each cell contains
    a grayscale intensity value (0-255) or a color tuple.
    
    Attributes:
        width: Width of the bitmap in pixels.
        height: Height of the bitmap in pixels.
        pixels: 2D list of pixel values.
        color_mode: Whether the bitmap is 'grayscale' or 'rgb'.
    
    Example:
        >>> bm = Bitmap(10, 10)
        >>> bm.set_pixel(5, 5, 255)
        >>> bm.get_pixel(5, 5)
        255
    """
    width: int = field(default=0)
    height: int = field(default=0)
    pixels: List[List[int]] = field(default_factory=list)
    color_mode: str = "grayscale"
    
    def __post_init__(self) -> None:
        if self.width > 0 and self.height > 0 and not self.pixels:
            self._initialize_empty()
    
    def _initialize_empty(self) -> None:
        """Initialize the pixel grid with zeros."""
        if self.color_mode == "grayscale":
            self.pixels = [[0 for _ in range(self.width)] for _ in range(self.height)]
        else:
            self.pixels = [[(0, 0, 0) for _ in range(self.width)] for _ in range(self.height)]
    
    def get_pixel(self, x: int, y: int) -> int:
        """
        Get the pixel value at coordinates (x, y).
        
        Args:
            x: Horizontal coordinate (0 = left).
            y: Vertical coordinate (0 = top).
            
        Returns:
            The pixel value, or 0 if out of bounds.
        """
        if not (0 <= x < self.width and 0 <= y < self.height):
            return 0
        return self.pixels[y][x]
    
    def set_pixel(self, x: int, y: int, value: int) -> None:
        """
        Set the pixel value at coordinates (x, y).
        
        Args:
            x: Horizontal coordinate.
            y: Vertical coordinate.
            value: The new pixel value (0-255 for grayscale).
        """
        if not (0 <= x < self.width and 0 <= y < self.height):
            return
        
        if self.color_mode == "grayscale":
            self.pixels[y][x] = max(0, min(255, int(value)))
        else:
            if isinstance(value, tuple) and len(value) == 3:
                r, g, b = value
                self.pixels[y][x] = (
                    max(0, min(255, int(r))),
                    max(0, min(255, int(g))),
                    max(0, min(255, int(b)))
                )
            else:
                self.pixels[y][x] = value
    
    def clear(self, value: int = 0) -> None:
        """
        Clear all pixels to a specified value.
        
        Args:
            value: The value to fill all pixels with.
        """
        self._initialize_empty()
        if value != 0:
            for y in range(self.height):
                for x in range(self.width):
                    self.set_pixel(x, y, value)
    
    def get_roi(self, x: int, y: int, w: int, h: int) -> Bitmap:
        """
        Extract a region of interest (ROI) from the bitmap.
        
        Args:
            x, y: Top-left corner of the ROI.
            w, h: Width and height of the ROI.
            
        Returns:
            A new Bitmap containing the ROI.
        """
        roi = Bitmap(w, h, color_mode=self.color_mode)
        
        for dy in range(h):
            for dx in range(w):
                src_x, src_y = x + dx, y + dy
                if self.color_mode == "grayscale":
                    roi.set_pixel(dx, dy, self.get_pixel(src_x, src_y))
                else:
                    roi.set_pixel(dx, dy, self.get_pixel(src_x, src_y))
        
        return roi
    
    def set_roi(self, x: int, y: int, roi: Bitmap) -> None:
        """
        Set a region of interest to match another bitmap.
        
        Args:
            x, y: Top-left corner where the ROI will be placed.
            roi: The Bitmap to copy into the ROI.
        """
        for dy in range(roi.height):
            for dx in range(roi.width):
                self.set_pixel(x + dx, y + dy, roi.get_pixel(dx, dy))
    
    def flip_horizontal(self) -> Bitmap:
        """
        Flip the bitmap horizontally (left-right mirror).
        
        Returns:
            A new Bitmap that is the horizontal mirror of this one.
        """
        result = Bitmap(self.width, self.height, color_mode=self.color_mode)
        
        for y in range(self.height):
            for x in range(self.width):
                result.set_pixel(self.width - 1 - x, y, self.get_pixel(x, y))
        
        return result
    
    def flip_vertical(self) -> Bitmap:
        """
        Flip the bitmap vertically (top-bottom mirror).
        
        Returns:
            A new Bitmap that is the vertical mirror of this one.
        """
        result = Bitmap(self.width, self.height, color_mode=self.color_mode)
        
        for y in range(self.height):
            for x in range(self.width):
                result.set_pixel(x, self.height - 1 - y, self.get_pixel(x, y))
        
        return result
    
    def rotate_90(self, clockwise: bool = True) -> Bitmap:
        """
        Rotate the bitmap by 90 degrees.
        
        Args:
            clockwise: If True, rotate clockwise; otherwise counter-clockwise.
            
        Returns:
            A new Bitmap rotated by 90 degrees.
        """
        if clockwise:
            result = Bitmap(self.height, self.width, color_mode=self.color_mode)
            for y in range(self.height):
                for x in range(self.width):
                    result.set_pixel(self.height - 1 - y, x, self.get_pixel(x, y))
        else:
            result = Bitmap(self.height, self.width, color_mode=self.color_mode)
            for y in range(self.height):
                for x in range(self.width):
                    result.set_pixel(y, self.width - 1 - x, self.get_pixel(x, y))
        
        return result
    
    def scale_nearest(self, new_width: int, new_height: int) -> Bitmap:
        """
        Scale the bitmap using nearest-neighbor interpolation.
        
        Args:
            new_width: Target width.
            new_height: Target height.
            
        Returns:
            A new scaled Bitmap.
        """
        if new_width <= 0 or new_height <= 0:
            raise ValueError(f"Dimensions must be positive, got {new_width}x{new_height}")
        
        result = Bitmap(new_width, new_height, color_mode=self.color_mode)
        
        for y in range(new_height):
            for x in range(new_width):
                src_x = int(x * self.width / new_width)
                src_y = int(y * self.height / new_height)
                src_x = min(src_x, self.width - 1)
                src_y = min(src_y, self.height - 1)
                result.set_pixel(x, y, self.get_pixel(src_x, src_y))
        
        return result
    
    def threshold(self, threshold_value: int) -> Bitmap:
        """
        Apply binary thresholding to the bitmap.
        
        Args:
            threshold_value: Pixels above this value become 255, below become 0.
            
        Returns:
            A new binary Bitmap.
        """
        result = Bitmap(self.width, self.height, color_mode=self.color_mode)
        
        for y in range(self.height):
            for x in range(self.width):
                result.set_pixel(x, y, 255 if self.get_pixel(x, y) >= threshold_value else 0)
        
        return result
    
    def invert(self) -> Bitmap:
        """
        Invert the bitmap (255 - value for grayscale).
        
        Returns:
            A new inverted Bitmap.
        """
        result = Bitmap(self.width, self.height, color_mode=self.color_mode)
        
        for y in range(self.height):
            for x in range(self.width):
                result.set_pixel(x, y, 255 - self.get_pixel(x, y))
        
        return result
    
    def histogram(self) -> List[int]:
        """
        Calculate the grayscale histogram of the bitmap.
        
        Returns:
            A list of 256 values representing the count of pixels
            at each intensity level.
        """
        hist = [0] * 256
        
        for y in range(self.height):
            for x in range(self.width):
                value = self.get_pixel(x, y)
                if isinstance(value, tuple):
                    value = int(0.299 * value[0] + 0.587 * value[1] + 0.114 * value[2])
                hist[value] += 1
        
        return hist
    
    def otsu_threshold(self) -> int:
        """
        Calculate the optimal threshold using Otsu's method.
        
        Returns:
            The optimal threshold value (0-255).
        """
        hist = self.histogram()
        total_pixels = self.width * self.height
        
        if total_pixels == 0:
            return 128
        
        sum_total = sum(i * h for i, h in enumerate(hist))
        
        var_max = 0.0
        threshold = 0
        
        sum_bg, weight_bg = 0.0, 0.0
        sum_fg, weight_fg = sum_total, float(total_pixels)
        
        for t in range(256):
            weight_bg += hist[t]
            
            if weight_bg == 0:
                continue
            
            weight_fg = total_pixels - weight_bg
            
            if weight_fg == 0:
                break
            
            sum_bg += t * hist[t]
            sum_fg = sum_total - sum_bg
            
            mean_bg = sum_bg / weight_bg
            mean_fg = sum_fg / weight_fg
            
            var_between = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2
            
            if var_between > var_max:
                var_max = var_between
                threshold = t
        
        return threshold
    
    def mean_intensity(self) -> float:
        """
        Calculate the mean pixel intensity of the entire bitmap.
        
        Returns:
            The average pixel value (0-255).
        """
        if self.width == 0 or self.height == 0:
            return 0.0
        
        total = sum(self.get_pixel(x, y) if self.color_mode == "grayscale"
                    else int(0.299 * self.get_pixel(x, y)[0] + 0.587 * self.get_pixel(x, y)[1] + 0.114 * self.get_pixel(x, y)[2])
                    for y in range(self.height) for x in range(self.width))
        
        return total / (self.width * self.height)
    
    def pixel_count(self) -> int:
        """
        Get the total number of pixels in the bitmap.
        
        Returns:
            Total pixel count (width * height).
        """
        return self.width * self.height
    
    def non_zero_count(self) -> int:
        """
        Count pixels that are not zero (non-black in grayscale).
        
        Returns:
            Number of non-zero pixels.
        """
        count = 0
        for y in range(self.height):
            for x in range(self.width):
                val = self.get_pixel(x, y)
                if isinstance(val, tuple):
                    if any(c > 0 for c in val):
                        count += 1
                elif val > 0:
                    count += 1
        return count
    
    def bounding_box(self) -> Tuple[int, int, int, int]:
        """
        Find the bounding box of non-zero pixels.
        
        Returns:
            Tuple of (min_x, min_y, max_x, max_y).
        """
        min_x, min_y = self.width, self.height
        max_x, max_y = -1, -1
        
        for y in range(self.height):
            for x in range(self.width):
                val = self.get_pixel(x, y)
                is_nonzero = val > 0 if isinstance(val, int) else any(c > 0 for c in val)
                
                if is_nonzero:
                    min_x = min(min_x, x)
                    min_y = min(min_y, y)
                    max_x = max(max_x, x)
                    max_y = max(max_y, y)
        
        if max_x == -1:
            return (0, 0, 0, 0)
        
        return (min_x, min_y, max_x, max_y)
    
    def crop_to_content(self, padding: int = 0) -> Bitmap:
        """
        Crop the bitmap to the smallest bounding box containing all content.
        
        Args:
            padding: Optional padding to add around the content.
            
        Returns:
            A new cropped Bitmap.
        """
        min_x, min_y, max_x, max_y = self.bounding_box()
        
        if max_x == -1:
            return Bitmap(0, 0, color_mode=self.color_mode)
        
        min_x = max(0, min_x - padding)
        min_y = max(0, min_y - padding)
        max_x = min(self.width - 1, max_x + padding)
        max_y = min(self.height - 1, max_y + padding)
        
        return self.get_roi(min_x, min_y, max_x - min_x + 1, max_y - min_y + 1)
    
    def difference(self, other: Bitmap) -> Bitmap:
        """
        Compute the absolute difference between two bitmaps.
        
        Args:
            other: The other Bitmap to compare with.
            
        Returns:
            A new Bitmap where each pixel is the absolute difference.
        """
        width = min(self.width, other.width)
        height = min(self.height, other.height)
        result = Bitmap(width, height, color_mode="grayscale")
        
        for y in range(height):
            for x in range(width):
                diff = abs(self.get_pixel(x, y) - other.get_pixel(x, y))
                result.set_pixel(x, y, diff)
        
        return result
    
    def iterate_pixels(self) -> Generator[Tuple[int, int, int], None, None]:
        """
        Iterate over all pixels with their coordinates.
        
        Yields:
            Tuples of (x, y, value).
        """
        for y in range(self.height):
            for x in range(self.width):
                yield (x, y, self.get_pixel(x, y))
    
    def apply_filter(self, kernel: List[List[float]]) -> Bitmap:
        """
        Apply a convolution filter to the bitmap.
        
        Args:
            kernel: A 2D list of filter coefficients. Should be square and odd-sized.
            
        Returns:
            A new filtered Bitmap.
        """
        k_size = len(kernel)
        k_half = k_size // 2
        
        result = Bitmap(self.width, self.height, color_mode=self.color_mode)
        
        for y in range(self.height):
            for x in range(self.width):
                total = 0.0
                
                for ky in range(k_size):
                    for kx in range(k_size):
                        src_x = x + kx - k_half
                        src_y = y + ky - k_half
                        
                        if 0 <= src_x < self.width and 0 <= src_y < self.height:
                            pixel_val = self.get_pixel(src_x, src_y)
                            if isinstance(pixel_val, int):
                                total += pixel_val * kernel[ky][kx]
                            else:
                                total += sum(c * kernel[ky][kx] for c in pixel_val)
                
                result.set_pixel(x, y, int(max(0, min(255, round(total)))))
        
        return result


def create_checkerboard(width: int, height: int, cell_size: int = 8) -> Bitmap:
    """
    Create a checkerboard pattern bitmap.
    
    Args:
        width: Width of the bitmap.
        height: Height of the bitmap.
        cell_size: Size of each checker cell in pixels.
        
    Returns:
        A new Bitmap with a checkerboard pattern.
    """
    bm = Bitmap(width, height)
    
    for y in range(height):
        for x in range(width):
            is_white = ((x // cell_size) + (y // cell_size)) % 2 == 0
            bm.set_pixel(x, y, 255 if is_white else 0)
    
    return bm


def create_gradient(width: int, height: int, direction: str = "horizontal") -> Bitmap:
    """
    Create a gradient bitmap.
    
    Args:
        width: Width of the bitmap.
        height: Height of the bitmap.
        direction: Gradient direction - "horizontal", "vertical", or "diagonal".
        
    Returns:
        A new Bitmap with a gradient.
    """
    bm = Bitmap(width, height)
    
    for y in range(height):
        for x in range(width):
            if direction == "horizontal":
                value = int(x * 255 / width)
            elif direction == "vertical":
                value = int(y * 255 / height)
            else:
                value = int((x + y) * 255 / (width + height))
            
            bm.set_pixel(x, y, value)
    
    return bm


def bitwise_and(a: Bitmap, b: Bitmap) -> Bitmap:
    """
    Perform bitwise AND between two bitmaps.
    
    Args:
        a: First bitmap.
        b: Second bitmap.
        
    Returns:
        A new Bitmap with the AND result.
    """
    width = min(a.width, b.width)
    height = min(a.height, b.height)
    result = Bitmap(width, height)
    
    for y in range(height):
        for x in range(width):
            result.set_pixel(x, y, a.get_pixel(x, y) & b.get_pixel(x, y))
    
    return result


def bitwise_or(a: Bitmap, b: Bitmap) -> Bitmap:
    """
    Perform bitwise OR between two bitmaps.
    
    Args:
        a: First bitmap.
        b: Second bitmap.
        
    Returns:
        A new Bitmap with the OR result.
    """
    width = max(a.width, b.width)
    height = max(a.height, b.height)
    result = Bitmap(width, height)
    
    for y in range(height):
        for x in range(width):
            val_a = a.get_pixel(x, y) if x < a.width and y < a.height else 0
            val_b = b.get_pixel(x, y) if x < b.width and y < b.height else 0
            result.set_pixel(x, y, val_a | val_b)
    
    return result


def bitwise_xor(a: Bitmap, b: Bitmap) -> Bitmap:
    """
    Perform bitwise XOR between two bitmaps.
    
    Args:
        a: First bitmap.
        b: Second bitmap.
        
    Returns:
        A new Bitmap with the XOR result.
    """
    width = min(a.width, b.width)
    height = min(a.height, b.height)
    result = Bitmap(width, height)
    
    for y in range(height):
        for x in range(width):
            result.set_pixel(x, y, a.get_pixel(x, y) ^ b.get_pixel(x, y))
    
    return result
