"""
Screenshot Capture Action Module.

Provides screenshot capture, region selection, annotation, and comparison
for browser and desktop automation workflows.

Example:
    >>> from screenshot_action import ScreenshotAction
    >>> action = ScreenshotAction()
    >>> path = action.capture_screen(region=(0, 0, 1920, 1080))
    >>> action.annotate(path, text="Error at step 3", color="red")
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class ScreenRegion:
    """Region of a screen defined by coordinates."""
    x: int
    y: int
    width: int
    height: int

    def to_tuple(self) -> tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)


@dataclass
class ScreenshotResult:
    """Result of a screenshot capture operation."""
    path: str
    width: int
    height: int
    size_bytes: int
    duration_ms: float
    error: Optional[str] = None


class ScreenshotAction:
    """Capture and annotate screenshots using native macOS tools."""

    def __init__(self, output_dir: str = "/tmp/screenshots"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def capture_screen(
        self,
        region: Optional[ScreenRegion] = None,
        output_name: Optional[str] = None,
        include_cursor: bool = True,
    ) -> ScreenshotResult:
        """
        Capture full screen or region.

        Args:
            region: Optional ScreenRegion to capture
            output_name: Custom filename (default: timestamp-based)
            include_cursor: Include cursor in capture (macOS only)

        Returns:
            ScreenshotResult with path and metadata
        """
        start = time.monotonic()
        if output_name is None:
            ts = int(time.time() * 1000)
            output_name = f"screenshot_{ts}.png"

        path = os.path.join(self.output_dir, output_name)

        if sys.platform == "darwin":
            result = self._capture_macos(path, region, include_cursor)
        elif sys.platform == "win32":
            result = self._capture_windows(path, region)
        else:
            result = self._capture_linux(path, region)

        result.duration_ms = (time.monotonic() - start) * 1000
        return result

    def _capture_macos(
        self,
        path: str,
        region: Optional[ScreenRegion],
        include_cursor: bool,
    ) -> ScreenshotResult:
        try:
            cmd = ["/usr/sbin/screencapture"]
            if not include_cursor:
                cmd.append("-C")
            if region:
                x, y, w, h = region.to_tuple()
                cmd.extend(["-x", "-R", f"{x},{y},{w},{h}"])
            cmd.extend(["-x", path])

            subprocess.run(cmd, check=True, capture_output=True)

            size = os.path.getsize(path)
            width, height = self._get_image_size(path)
            return ScreenshotResult(path=path, width=width, height=height, size_bytes=size, duration_ms=0)
        except subprocess.CalledProcessError as e:
            return ScreenshotResult(path=path, width=0, height=0, size_bytes=0, duration_ms=0, error=str(e.stderr))

    def _capture_windows(self, path: str, region: Optional[ScreenRegion]) -> ScreenshotResult:
        try:
            import mss
            with mss.mss() as sct:
                if region:
                    monitor = {"left": region.x, "top": region.y, "width": region.width, "height": region.height}
                else:
                    monitor = sct.monitors[0]
                img = sct.grab(monitor)
                mss.tools.to_png(img.rgb, img.size, output=path)
                size = os.path.getsize(path)
                return ScreenshotResult(path=path, width=monitor["width"], height=monitor["height"], size_bytes=size, duration_ms=0)
        except Exception as e:
            return ScreenshotResult(path=path, width=0, height=0, size_bytes=0, duration_ms=0, error=str(e))

    def _capture_linux(self, path: str, region: Optional[ScreenRegion]) -> ScreenshotResult:
        try:
            cmd = ["scrot"]
            if region:
                cmd.extend(["-a", f"{region.x},{region.y},{region.width},{region.height}"])
            cmd.append(path)
            subprocess.run(cmd, check=True, capture_output=True)
            size = os.path.getsize(path)
            width, height = self._get_image_size(path)
            return ScreenshotResult(path=path, width=width, height=height, size_bytes=size, duration_ms=0)
        except Exception as e:
            return ScreenshotResult(path=path, width=0, height=0, size_bytes=0, duration_ms=0, error=str(e))

    def _get_image_size(self, path: str) -> tuple[int, int]:
        try:
            import struct
            with open(path, "rb") as f:
                header = f.read(24)
                if header[1:4] == b"PNG":
                    w = struct.unpack(">I", header[16:20])[0]
                    h = struct.unpack(">I", header[20:24])[0]
                    return w, h
        except Exception:
            pass
        return 0, 0

    def capture_and_compare(
        self,
        region: Optional[ScreenRegion] = None,
        baseline_path: Optional[str] = None,
        threshold: float = 0.01,
    ) -> tuple[ScreenshotResult, bool, float]:
        """
        Capture and compare with baseline image.

        Returns:
            Tuple of (ScreenshotResult, is_match, diff_percentage)
        """
        result = self.capture_screen(region=region)
        if result.error:
            return result, False, 100.0

        if baseline_path and os.path.exists(baseline_path):
            diff = self._image_diff(result.path, baseline_path)
            is_match = diff <= threshold
            return result, is_match, diff

        return result, True, 0.0

    def _image_diff(self, img1: str, img2: str) -> float:
        try:
            import numpy as np
            try:
                from PIL import Image
            except ImportError:
                return 100.0

            im1 = Image.open(img1).convert("RGB")
            im2 = Image.open(img2).convert("RGB")

            if im1.size != im2.size:
                im2 = im2.resize(im1.size)

            arr1 = np.array(im1)
            arr2 = np.array(im2)

            diff = np.abs(arr1.astype(float) - arr2.astype(float)).mean()
            max_diff = 255.0
            return (diff / max_diff) * 100
        except Exception:
            return 100.0

    def annotate(
        self,
        image_path: str,
        text: Optional[str] = None,
        boxes: Optional[list[dict]] = None,
        color: str = "red",
        output_path: Optional[str] = None,
    ) -> str:
        """
        Annotate screenshot with text and/or bounding boxes.

        Args:
            image_path: Path to input image
            text: Optional text to overlay
            boxes: Optional list of {"x", "y", "width", "height", "label"}
            color: Annotation color (red, blue, green, yellow)
            output_path: Output path (default: overwrite input)

        Returns:
            Path to annotated image
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            return image_path

        output = output_path or image_path
        color_map = {"red": (255, 0, 0), "blue": (0, 0, 255), "green": (0, 255, 0), "yellow": (255, 255, 0)}
        rgb = color_map.get(color, (255, 0, 0))

        img = Image.open(image_path)
        draw = ImageDraw.Draw(img)

        try:
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 24)
        except Exception:
            font = ImageFont.load_default()

        if text:
            text_pos = (10, 10)
            draw.rectangle([text_pos, (text_pos[0] + 400, text_pos[1] + 40)], fill=(0, 0, 0, 180))
            draw.text(text_pos, text, fill=rgb, font=font)

        if boxes:
            for box in boxes:
                x = box.get("x", 0)
                y = box.get("y", 0)
                w = box.get("width", 100)
                h = box.get("height", 100)
                draw.rectangle([x, y, x + w, y + h], outline=rgb, width=3)
                if "label" in box:
                    draw.rectangle([x, y - 30, x + 200, y], fill=rgb)
                    draw.text((x + 5, y - 28), box["label"], fill=(255, 255, 255), font=font)

        img.save(output)
        return output

    def thumbnail(
        self,
        image_path: str,
        max_width: int = 200,
        max_height: int = 200,
        output_path: Optional[str] = None,
    ) -> str:
        """Generate thumbnail of an image."""
        try:
            from PIL import Image
        except ImportError:
            return image_path

        output = output_path or image_path
        img = Image.open(image_path)
        img.thumbnail((max_width, max_height), Image.LANCZOS)
        img.save(output)
        return output

    def capture_sequence(
        self,
        count: int,
        interval: float = 0.5,
        region: Optional[ScreenRegion] = None,
        prefix: str = "seq",
    ) -> list[ScreenshotResult]:
        """
        Capture a sequence of screenshots.

        Args:
            count: Number of screenshots
            interval: Seconds between captures
            region: Optional region to capture
            prefix: Filename prefix

        Returns:
            List of ScreenshotResult
        """
        results: list[ScreenshotResult] = []
        for i in range(count):
            ts = int(time.time() * 1000)
            name = f"{prefix}_{i:03d}_{ts}.png"
            result = self.capture_screen(region=region, output_name=name)
            results.append(result)
            if i < count - 1:
                time.sleep(interval)
        return results


if __name__ == "__main__":
    action = ScreenshotAction()
    result = action.capture_screen()
    print(f"Captured: {result.path}")
    if result.error:
        print(f"Error: {result.error}")
