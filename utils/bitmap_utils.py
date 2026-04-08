"""Bitmap and pixel manipulation utilities.

Provides bitmap operations for image processing and
pixel-level manipulation in automation workflows.
"""

from typing import List, Tuple, Optional


RGBA = Tuple[int, int, int, int]
RGB = Tuple[int, int, int]


class Bitmap:
    """In-memory bitmap image representation.

    Example:
        bmp = Bitmap(width=100, height=100)
        bmp.set_pixel(50, 50, (255, 0, 0, 255))
        print(bmp.get_pixel(50, 50))
    """

    def __init__(self, width: int, height: int, color: RGBA = (0, 0, 0, 0)) -> None:
        self.width = width
        self.height = height
        self._pixels: List[List[RGBA]] = [
            [list(color) for _ in range(width)] for _ in range(height)
        ]

    def get_pixel(self, x: int, y: int) -> Optional[RGBA]:
        """Get pixel color at coordinates.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            RGBA tuple or None if out of bounds.
        """
        if 0 <= x < self.width and 0 <= y < self.height:
            return tuple(self._pixels[y][x])
        return None

    def set_pixel(self, x: int, y: int, color: RGBA) -> bool:
        """Set pixel color at coordinates.

        Args:
            x: X coordinate.
            y: Y coordinate.
            color: RGBA color tuple.

        Returns:
            True if set successfully.
        """
        if 0 <= x < self.width and 0 <= y < self.height:
            self._pixels[y][x] = list(color)
            return True
        return False

    def clear(self, color: RGBA = (0, 0, 0, 0)) -> None:
        """Clear bitmap with color.

        Args:
            color: RGBA color to fill with.
        """
        for y in range(self.height):
            for x in range(self.width):
                self._pixels[y][x] = list(color)

    def get_region(self, x: int, y: int, w: int, h: int) -> "Bitmap":
        """Extract a region as new bitmap.

        Args:
            x: Left coordinate.
            y: Top coordinate.
            w: Width.
            h: Height.

        Returns:
            New bitmap with region content.
        """
        region = Bitmap(w, h)
        for dy in range(h):
            for dx in range(w):
                pixel = self.get_pixel(x + dx, y + dy)
                if pixel:
                    region.set_pixel(dx, dy, pixel)
        return region

    def blit(self, source: "Bitmap", x: int, y: int) -> None:
        """Blit source bitmap onto this bitmap.

        Args:
            source: Source bitmap to copy.
            x: Destination X.
            y: Destination Y.
        """
        for sy in range(source.height):
            for sx in range(source.width):
                pixel = source.get_pixel(sx, sy)
                if pixel and pixel[3] > 0:
                    self.set_pixel(x + sx, y + sy, pixel)

    def to_list(self) -> List[List[RGBA]]:
        """Convert to 2D list.

        Returns:
            2D list of RGBA pixels.
        """
        return [[tuple(p) for p in row] for row in self._pixels]

    def for_each_pixel(self, func: callable) -> None:
        """Apply function to each pixel.

        Args:
            func: Function(x, y, color) -> new_color.
        """
        for y in range(self.height):
            for x in range(self.width):
                self._pixels[y][x] = list(func(x, y, tuple(self._pixels[y][x])))


def blend_pixels(c1: RGBA, c2: RGBA, alpha: float) -> RGBA:
    """Blend two pixels with alpha.

    Args:
        c1: First color.
        c2: Second color.
        alpha: Blend factor (0-1).

    Returns:
        Blended color.
    """
    return tuple(
        int(c1[i] * (1 - alpha) + c2[i] * alpha) for i in range(4)
    )


def rgb_to_hsv(rgb: RGB) -> Tuple[float, float, float]:
    """Convert RGB to HSV.

    Args:
        rgb: RGB tuple.

    Returns:
        HSV tuple (h: 0-360, s: 0-1, v: 0-1).
    """
    r, g, b = [x / 255.0 for x in rgb]
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    diff = max_c - min_c

    if max_c == min_c:
        h = 0.0
    elif max_c == r:
        h = (60 * ((g - b) / diff) + 360) % 360
    elif max_c == g:
        h = (60 * ((b - r) / diff) + 120) % 360
    else:
        h = (60 * ((r - g) / diff) + 240) % 360

    s = 0.0 if max_c == 0 else diff / max_c
    v = max_c

    return (h, s, v)


def hsv_to_rgb(hsv: Tuple[float, float, float]) -> RGB:
    """Convert HSV to RGB.

    Args:
        hsv: HSV tuple (h: 0-360, s: 0-1, v: 0-1).

    Returns:
        RGB tuple.
    """
    h, s, v = hsv
    c = v * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = v - c

    if 0 <= h < 60:
        r, g, b = c, x, 0
    elif 60 <= h < 120:
        r, g, b = x, c, 0
    elif 120 <= h < 180:
        r, g, b = 0, c, x
    elif 180 <= h < 240:
        r, g, b = 0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x

    return (int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))
