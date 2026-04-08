"""Color detection and matching utilities for screen-based automation.

Provides tools for capturing screen pixels, comparing colors within
tolerance, finding regions of a specific color, and generating
color-based highlight regions for visual debugging.

Example:
    >>> from utils.color_detection_utils import get_pixel_color, find_color_regions
    >>> color = get_pixel_color(100, 200)
    >>> regions = find_color_regions((0, 0, 800, 600), (255, 0, 0), tolerance=20)
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Optional

__all__ = [
    "RGBA",
    "get_pixel_color",
    "get_region_colors",
    "find_color_regions",
    "ColorRegion",
    "color_distance",
    "hex_to_rgb",
    "rgb_to_hex",
    "ColorMatcher",
]


@dataclass
class RGBA:
    """RGBA color value with components 0-255."""

    r: int
    g: int
    b: int
    a: int = 255

    def to_tuple(self) -> tuple[int, int, int, int]:
        return (self.r, self.g, self.b, self.a)

    def to_rgb_tuple(self) -> tuple[int, int, int]:
        return (self.r, self.g, self.b)

    def to_hex(self) -> str:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    @classmethod
    def from_hex(cls, hex_str: str) -> "RGBA":
        hex_str = hex_str.lstrip("#")
        if len(hex_str) == 6:
            return cls(r=int(hex_str[0:2], 16), g=int(hex_str[2:4], 16), b=int(hex_str[4:6], 16))
        elif len(hex_str) == 8:
            return cls(
                r=int(hex_str[0:2], 16),
                g=int(hex_str[2:4], 16),
                b=int(hex_str[4:6], 16),
                a=int(hex_str[6:8], 16),
            )
        raise ValueError(f"Invalid hex color: {hex_str}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RGBA):
            return NotImplemented
        return self.r == other.r and self.g == other.g and self.b == other.b and self.a == other.a

    def __hash__(self) -> int:
        return hash((self.r, self.g, self.b, self.a))


@dataclass
class ColorRegion:
    """A contiguous region of matching color."""

    x: int
    y: int
    width: int
    height: int
    center: tuple[int, int]
    color: RGBA
    pixel_count: int


def color_distance(c1: tuple[int, int, int], c2: tuple[int, int, int]) -> float:
    """Compute Euclidean distance between two RGB colors.

    Args:
        c1: First color as (R, G, B).
        c2: Second color as (R, G, B).

    Returns:
        Euclidean distance between the two colors.
    """
    return ((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2 + (c1[2] - c2[2]) ** 2) ** 0.5


def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """Convert a hex color string to an RGB tuple.

    Args:
        hex_color: Hex string like '#FF0000' or 'FF0000'.

    Returns:
        Tuple of (R, G, B).
    """
    return RGBA.from_hex(hex_color).to_rgb_tuple()


def rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB values to a hex color string.

    Args:
        r: Red component (0-255).
        g: Green component (0-255).
        b: Blue component (0-255).

    Returns:
        Hex string like '#FF0000'.
    """
    return f"#{r:02x}{g:02x}{b:02x}"


def get_pixel_color(x: int, y: int) -> Optional[RGBA]:
    """Get the RGBA color of a single pixel on screen.

    Args:
        x: Screen X coordinate.
        y: Screen Y coordinate.

    Returns:
        RGBA object, or None if the pixel could not be read.
    """
    import sys

    if sys.platform == "darwin":
        return _get_pixel_darwin(x, y)
    return None


def _get_pixel_darwin(x: int, y: int) -> Optional[RGBA]:
    """Use screencapture and PIL to read a single pixel on macOS."""
    try:
        import subprocess

        # Capture a 1x1 region at the specified point
        result = subprocess.run(
            [
                "screencapture",
                "-x",
                "-R",
                f"{x},{y},1,1",
                "-",
            ],
            capture_output=True,
            timeout=5,
        )
        if result.returncode != 0 or not result.stdout:
            return None

        try:
            from PIL import Image
            import io

            img = Image.open(io.BytesIO(result.stdout))
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            r, g, b, a = img.getpixel((0, 0))
            return RGBA(r=int(r), g=int(g), b=int(b), a=int(a))
        except ImportError:
            return None
    except Exception:
        return None


def get_region_colors(
    region: tuple[int, int, int, int],
    sample_rate: int = 1,
) -> list[RGBA]:
    """Capture all colors from a screen region.

    Args:
        region: Screen region as (x, y, width, height).
        sample_rate: Sample every Nth pixel (1 = all pixels, 2 = half, etc.).

    Returns:
        Flat list of RGBA values for sampled pixels.
    """
    import sys

    x, y, w, h = region
    colors: list[RGBA] = []

    if sys.platform == "darwin":
        try:
            result = subprocess.run(
                ["screencapture", "-x", "-R", f"{x},{y},{w},{h}", "-"],
                capture_output=True,
                timeout=10,
            )
            if result.returncode != 0:
                return []

            try:
                from PIL import Image
                import io

                img = Image.open(io.BytesIO(result.stdout))
                if img.mode != "RGBA":
                    img = img.convert("RGBA")

                pixels = list(img.getdata())
                for i, px in enumerate(pixels):
                    if i % sample_rate != 0:
                        continue
                    if isinstance(px, int):
                        r = g = b = px
                        a = 255
                    else:
                        r, g, b = px[0], px[1], px[2]
                        a = px[3] if len(px) > 3 else 255
                    colors.append(RGBA(r=r, g=g, b=b, a=a))
            except ImportError:
                pass
        except Exception:
            pass

    return colors


def find_color_regions(
    region: tuple[int, int, int, int],
    target_rgb: tuple[int, int, int],
    tolerance: int = 10,
    min_pixels: int = 1,
    sample_rate: int = 1,
) -> list[ColorRegion]:
    """Find contiguous regions matching a target color.

    Uses flood-fill on the sampled pixel grid to identify connected
    components of matching color.

    Args:
        region: Screen region as (x, y, width, height).
        target_rgb: Target color as (R, G, B).
        tolerance: Color distance tolerance.
        min_pixels: Minimum pixel count for a region to be reported.
        sample_rate: Sampling rate (1 = every pixel).

    Returns:
        List of ColorRegion objects.
    """
    import sys

    x, y, w, h = region
    colors = get_region_colors(region, sample_rate=sample_rate)
    if not colors:
        return []

    # Build sampled grid dimensions
    sw = (w + sample_rate - 1) // sample_rate
    sh = (h + sample_rate - 1) // sample_rate

    # Create color match grid
    grid = [[False] * sw for _ in range(sh)]
    for sy in range(sh):
        for sx in range(sw):
            idx = sy * sw + sx
            if idx < len(colors):
                dist = color_distance(colors[idx].to_rgb_tuple(), target_rgb)
                grid[sy][sx] = dist <= tolerance

    # Flood fill to find connected components
    visited = [[False] * sw for _ in range(sh)]
    regions: list[ColorRegion] = []

    def flood_fill(
        sx: int,
        sy: int,
    ) -> list[tuple[int, int]]:
        stack = [(sx, sy)]
        pixels: list[tuple[int, int]] = []
        while stack:
            cx, cy = stack.pop()
            if cx < 0 or cx >= sw or cy < 0 or cy >= sh:
                continue
            if visited[cy][cx] or not grid[cy][cx]:
                continue
            visited[cy][cx] = True
            pixels.append((cx, cy))
            stack.extend([(cx + 1, cy), (cx - 1, cy), (cx, cy + 1), (cx, cy - 1)])
        return pixels

    for sy in range(sh):
        for sx in range(sw):
            if grid[sy][sx] and not visited[sy][sx]:
                pixels = flood_fill(sx, sy)
                if len(pixels) < min_pixels:
                    continue

                xs = [p[0] for p in pixels]
                ys = [p[1] for p in pixels]
                min_x, max_x = min(xs), max(xs)
                min_y, max_y = min(ys), max(ys)

                # Convert back to screen coordinates
                screen_x = x + min_x * sample_rate
                screen_y = y + min_y * sample_rate
                screen_w = (max_x - min_x + 1) * sample_rate
                screen_h = (max_y - min_y + 1) * sample_rate

                regions.append(
                    ColorRegion(
                        x=screen_x,
                        y=screen_y,
                        width=screen_w,
                        height=screen_h,
                        center=(
                            screen_x + screen_w // 2,
                            screen_y + screen_h // 2,
                        ),
                        color=RGBA.from_hex(rgb_to_hex(*target_rgb)),
                        pixel_count=len(pixels),
                    )
                )

    return regions


class ColorMatcher:
    """Stateful color matcher for repeated matching on a fixed region."""

    def __init__(
        self,
        region: tuple[int, int, int, int],
        target_rgb: tuple[int, int, int],
        tolerance: int = 10,
    ):
        self.region = region
        self.target_rgb = target_rgb
        self.tolerance = tolerance

    def find(self) -> list[ColorRegion]:
        """Find all matching regions in the configured region."""
        return find_color_regions(
            self.region,
            self.target_rgb,
            tolerance=self.tolerance,
        )

    def wait_for(
        self,
        timeout: float = 10.0,
        poll_interval: float = 0.1,
    ) -> Optional[ColorRegion]:
        """Wait until a matching region appears.

        Args:
            timeout: Maximum time to wait in seconds.
            poll_interval: Time between checks.

        Returns:
            First ColorRegion found, or None if timeout.
        """
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            regions = self.find()
            if regions:
                return regions[0]
            time.sleep(poll_interval)
        return None
