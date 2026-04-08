"""
Screen Region Capture.

Capture specific screen regions as images with support for
monitor selection, coordinate transformation, and capture scheduling.

Usage:
    from utils.screen_region_capture import ScreenRegionCapture, capture

    capturer = ScreenRegionCapture()
    img = capturer.capture_region(x=100, y=100, width=500, height=300)
"""

from __future__ import annotations

from typing import Optional, Tuple, Dict, Any, Union, List, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass

try:
    import Quartz
    HAS_QUARTZ = True
except ImportError:
    HAS_QUARTZ = False


@dataclass
class ScreenRegion:
    """Defines a rectangular region of a screen."""
    x: int
    y: int
    width: int
    height: int
    display_id: Optional[int] = None

    @property
    def rect(self) -> Tuple[int, int, int, int]:
        """Return as (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)

    @property
    def rect_xywh(self) -> Tuple[int, int, int, int]:
        """Return as (x, y, w, h) for CGrect."""
        return (self.x, self.y, self.x + self.width, self.y + self.height)


class ScreenRegionCapture:
    """
    Capture specific regions of the screen.

    Supports capturing from specific monitors, coordinate transformation
    for Retina displays, and various output formats.

    Example:
        capturer = ScreenRegionCapture()
        region = ScreenRegion(x=100, y=100, width=400, height=300)
        img = capturer.capture(region)
    """

    def __init__(
        self,
        scale_factor: float = 2.0,
    ) -> None:
        """
        Initialize the screen region capturer.

        Args:
            scale_factor: Scale factor for Retina displays (default 2.0).
        """
        self._scale_factor = scale_factor

    def capture_region(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        display_id: Optional[int] = None,
    ) -> Optional["Image.Image"]:
        """
        Capture a screen region.

        Args:
            x: X coordinate of top-left corner.
            y: Y coordinate of top-left corner.
            width: Width of region.
            height: Height of region.
            display_id: Optional display ID.

        Returns:
            PIL Image or None on failure.
        """
        try:
            from PIL import Image
        except ImportError:
            return None

        region = ScreenRegion(x, y, width, height, display_id)
        return self.capture(region)

    def capture(
        self,
        region: ScreenRegion,
    ) -> Optional["Image.Image"]:
        """
        Capture a screen region.

        Args:
            region: ScreenRegion object defining the capture area.

        Returns:
            PIL Image or None on failure.
        """
        try:
            from PIL import Image
        except ImportError:
            return None

        if not HAS_QUARTZ:
            return self._capture_fallback(region)

        try:
            display_id = region.display_id or Quartz.CGMainDisplayID()

            x = int(region.x * self._scale_factor)
            y = int(region.y * self._scale_factor)
            w = int(region.width * self._scale_factor)
            h = int(region.height * self._scale_factor)

            rect = Quartz.CGRectMake(x, y, w, h)

            cg_image = Quartz.CGDisplayGrabImage(display_id, rect)
            if cg_image is None:
                return None

            return self._cgimage_to_pil(cg_image)
        except Exception:
            return None

    def _capture_fallback(
        self,
        region: ScreenRegion,
    ) -> Optional["Image.Image"]:
        """Fallback capture using screencapture command."""
        import subprocess
        import tempfile
        import os

        try:
            from PIL import Image
        except ImportError:
            return None

        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            path = f.name

        x, y = region.x, region.y
        w, h = region.width, region.height

        try:
            subprocess.run(
                ["screencapture", "-x", "-R", f"{x},{y},{w},{h}", path],
                check=True,
                capture_output=True,
            )
            img = Image.open(path)
            return img.copy()
        except Exception:
            return None
        finally:
            os.unlink(path)

    def _cgimage_to_pil(
        self,
        cg_image: Any,
    ) -> Optional["Image.Image"]:
        """Convert CGImage to PIL Image."""
        try:
            from PIL import Image
        except ImportError:
            return None

        width = cg_image.width
        height = cg_image.height
        bytes_per_pixel = 4
        bytes_per_row = cg_image.bytes_per_row
        bits_per_pixel = cg_image.bits_per_pixel

        if bits_per_pixel == 32:
            provider = cg_image.data_provider
            data = provider.copy_aspect_ratio()

            if data is None:
                return None

            img = Image.frombuffer(
                "RGBA",
                (width, height),
                data,
                "raw",
                "BGRA",
                bytes_per_row,
            )
            return img

        return None

    def capture_all_displays(
        self,
        region: ScreenRegion,
    ) -> List["Image.Image"]:
        """
        Capture a region across all displays.

        Args:
            region: ScreenRegion to capture.

        Returns:
            List of PIL Images from each display.
        """
        images: List["Image.Image"] = []

        if not HAS_QUARTZ:
            img = self.capture(region)
            if img:
                images.append(img)
            return images

        try:
            displays = Quartz.CGGetActiveDisplayList
            max_displays = 16
            display_ids = Quartz.CGDisplayID * max_displays
            count = [max_displays]
            displays(max_displays, display_ids, count)

            for i in range(count[0]):
                did = display_ids[i]
                region.display_id = did
                img = self.capture(region)
                if img:
                    images.append(img)

        except Exception:
            img = self.capture(region)
            if img:
                images.append(img)

        return images


def capture(
    x: int,
    y: int,
    width: int,
    height: int,
    scale_factor: float = 2.0,
) -> Optional["Image.Image"]:
    """
    Quick capture function.

    Args:
        x: X coordinate.
        y: Y coordinate.
        width: Width.
        height: Height.
        scale_factor: Scale factor for Retina.

    Returns:
        PIL Image or None.
    """
    capturer = ScreenRegionCapture(scale_factor=scale_factor)
    return capturer.capture_region(x, y, width, height)
