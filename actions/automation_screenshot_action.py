"""
Screenshot capture automation module.

Provides screenshot capture with region selection, annotation,
and comparison capabilities for UI automation.

Author: Aito Auto Agent
"""

from __future__ import annotations

import subprocess
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Union


class ScreenshotFormat(Enum):
    """Screenshot file format."""
    PNG = "png"
    JPEG = "jpg"
    TIFF = "tiff"
    BMP = "bmp"


class ScreenshotRegion:
    """Represents a screen region to capture."""

    def __init__(
        self,
        x: int = 0,
        y: int = 0,
        width: Optional[int] = None,
        height: Optional[int] = None
    ):
        self.x = x
        self.y = y
        self.width = width
        self.height = height

    def to_tuple(self) -> tuple[int, int, int, int]:
        """Convert to (x, y, width, height) tuple."""
        return (self.x, self.y, self.width or 0, self.height or 0)

    @classmethod
    def from_display(cls, display_index: int = 0) -> ScreenshotRegion:
        """Create region for a specific display."""
        return cls(0, 0)


@dataclass
class ScreenshotMetadata:
    """Metadata about a screenshot."""
    timestamp: datetime
    region: Optional[ScreenshotRegion]
    display: int
    width: int
    height: int
    format: ScreenshotFormat
    file_size: int
    file_path: Optional[str] = None
    checksum: Optional[str] = None


class ScreenshotAutomator:
    """
    Screenshot capture automation.

    Provides cross-platform screenshot capture with region selection,
    file management, and comparison tools.

    Example:
        automator = ScreenshotAutomator(platform="macos")

        # Capture full screen
        path = automator.capture()

        # Capture region
        region = ScreenshotRegion(x=100, y=100, width=800, height=600)
        path = automator.capture(region=region)

        # Compare screenshots
        diff = automator.compare("before.png", "after.png")
    """

    def __init__(self, platform: str = "macos", output_dir: str = "/tmp/screenshots"):
        self._platform = platform
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._default_format = ScreenshotFormat.PNG

    def capture(
        self,
        region: Optional[ScreenshotRegion] = None,
        display: int = 0,
        format: Optional[ScreenshotFormat] = None,
        output_path: Optional[str] = None,
        include_cursor: bool = True
    ) -> Optional[str]:
        """
        Capture a screenshot.

        Args:
            region: Optional region to capture
            display: Display index for multi-monitor setups
            format: Output format
            output_path: Optional custom output path
            include_cursor: Whether to include cursor

        Returns:
            Path to saved screenshot or None on failure
        """
        if format is None:
            format = self._default_format

        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            output_path = str(self._output_dir / f"screenshot_{timestamp}.{format.value}")

        if self._platform == "macos":
            return self._capture_macos(region, output_path, include_cursor)
        elif self._platform == "windows":
            return self._capture_windows(region, output_path)
        else:
            return self._capture_x11(region, output_path)

    def _capture_macos(
        self,
        region: Optional[ScreenshotRegion],
        output_path: str,
        include_cursor: bool
    ) -> Optional[str]:
        """Capture screenshot on macOS using screencapture."""
        try:
            args = ["screencapture"]

            if not include_cursor:
                args.append("-C")

            if region:
                x, y, w, h = region.to_tuple()
                if w and h:
                    args.extend(["-R", f"{x},{y},{w},{h}"])

            args.append(output_path)

            result = subprocess.run(
                args,
                capture_output=True,
                timeout=30
            )

            if result.returncode == 0 and Path(output_path).exists():
                return output_path

        except Exception:
            pass

        return None

    def _capture_windows(
        self,
        region: Optional[ScreenshotRegion],
        output_path: str
    ) -> Optional[str]:
        """Capture screenshot on Windows using PowerShell."""
        try:
            script = f'''
            Add-Type -AssemblyName System.Windows.Forms
            Add-Type -AssemblyName System.Drawing

            $screen = [System.Windows.Forms.Screen]::AllScreens[{region.display if region else 0}]
            $bounds = $screen.Bounds

            $bitmap = New-Object System.Drawing.Bitmap($bounds.Width, $bounds.Height)
            $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
            $graphics.CopyFromScreen($bounds.Location, [System.Drawing.Point]::Empty, $bounds.Size)

            $bitmap.Save("{output_path}")
            $graphics.Dispose()
            $bitmap.Dispose()
            '''
            subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                timeout=30
            )

            if Path(output_path).exists():
                return output_path

        except Exception:
            pass

        return None

    def _capture_x11(
        self,
        region: Optional[ScreenshotRegion],
        output_path: str
    ) -> Optional[str]:
        """Capture screenshot on X11/Linux using scrot."""
        try:
            args = ["scrot"]

            if region:
                x, y, w, h = region.to_tuple()
                if w and h:
                    args.extend(["-a", f"{x},{y},{w},{h}"])

            args.append(output_path)

            result = subprocess.run(
                args,
                capture_output=True,
                timeout=30
            )

            if result.returncode == 0 and Path(output_path).exists():
                return output_path

        except Exception:
            pass

        return None

    def capture_sequence(
        self,
        count: int,
        interval_ms: int = 1000,
        prefix: str = "capture"
    ) -> list[str]:
        """
        Capture a sequence of screenshots.

        Args:
            count: Number of screenshots to capture
            interval_ms: Interval between captures in milliseconds
            prefix: Filename prefix

        Returns:
            List of paths to captured screenshots
        """
        paths = []
        interval_sec = interval_ms / 1000

        for i in range(count):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            path = str(self._output_dir / f"{prefix}_{i:03d}_{timestamp}.{self._default_format.value}")

            if self.capture(output_path=path):
                paths.append(path)

            if i < count - 1:
                time.sleep(interval_sec)

        return paths

    def compare(
        self,
        path1: str,
        path2: str,
        diff_output: Optional[str] = None,
        threshold: float = 0.05
    ) -> dict:
        """
        Compare two screenshots and generate diff.

        Args:
            path1: Path to first screenshot
            path2: Path to second screenshot
            diff_output: Optional path for diff image
            threshold: Pixel difference threshold (0-1)

        Returns:
            Dict with comparison results
        """
        try:
            from PIL import Image
            import numpy as np

            img1 = Image.open(path1).convert("RGB")
            img2 = Image.open(path2).convert("RGB")

            if img1.size != img2.size:
                img2 = img2.resize(img1.size)

            arr1 = np.array(img1)
            arr2 = np.array(img2)

            diff = np.abs(arr1.astype(int) - arr2.astype(int))
            diff_sum = np.sum(diff, axis=2)
            total_pixels = diff_sum.size
            changed_pixels = np.count_nonzero(diff_sum > 10)
            similarity = 1 - (changed_pixels / total_pixels)

            result = {
                "identical": changed_pixels == 0,
                "similarity": float(similarity),
                "changed_pixels": int(changed_pixels),
                "total_pixels": int(total_pixels),
                "changed_percentage": float(changed_pixels / total_pixels * 100)
            }

            if diff_output and similarity < (1 - threshold):
                diff_img = Image.fromarray(diff.astype(np.uint8))
                diff_img.save(diff_output)
                result["diff_image"] = diff_output

            return result

        except ImportError:
            return self._compare_simple(path1, path2)
        except Exception as e:
            return {"error": str(e)}

    def _compare_simple(self, path1: str, path2: str) -> dict:
        """Simple comparison using file hashing."""
        try:
            with open(path1, "rb") as f1:
                hash1 = hashlib.md5(f1.read()).hexdigest()
            with open(path2, "rb") as f2:
                hash2 = hashlib.md5(f2.read()).hexdigest()

            return {
                "identical": hash1 == hash2,
                "hash1": hash1,
                "hash2": hash2
            }
        except Exception:
            return {"error": "Comparison failed"}

    def annotate(
        self,
        image_path: str,
        annotations: list[dict],
        output_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Add annotations to a screenshot.

        Args:
            image_path: Path to input image
            annotations: List of annotation dicts with type, position, etc.
            output_path: Optional output path

        Returns:
            Path to annotated image or None
        """
        try:
            from PIL import Image, ImageDraw, ImageFont

            img = Image.open(image_path)
            draw = ImageDraw.Draw(img)

            for ann in annotations:
                ann_type = ann.get("type", "rectangle")

                if ann_type == "rectangle":
                    coords = ann.get("coordinates", [0, 0, 100, 100])
                    color = ann.get("color", "red")
                    width = ann.get("width", 2)
                    draw.rectangle(coords, outline=color, width=width)

                elif ann_type == "circle":
                    center = ann.get("center", [50, 50])
                    radius = ann.get("radius", 20)
                    color = ann.get("color", "red")
                    draw.ellipse(
                        [center[0] - radius, center[1] - radius,
                         center[0] + radius, center[1] + radius],
                        outline=color
                    )

                elif ann_type == "text":
                    position = ann.get("position", [10, 10])
                    text = ann.get("text", "")
                    color = ann.get("color", "red")
                    size = ann.get("size", 16)
                    draw.text(position, text, fill=color)

                elif ann_type == "arrow":
                    start = ann.get("start", [0, 0])
                    end = ann.get("end", [100, 100])
                    color = ann.get("color", "red")
                    width = ann.get("width", 2)
                    draw.line([start, end], fill=color, width=width)

            if output_path is None:
                path = Path(image_path)
                output_path = str(path.parent / f"{path.stem}_annotated{path.suffix}")

            img.save(output_path)
            return output_path

        except ImportError:
            return None
        except Exception:
            return None

    def wait_for_change(
        self,
        region: Optional[ScreenshotRegion] = None,
        timeout_ms: int = 30000,
        poll_interval_ms: int = 500,
        baseline_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Wait for screen to change from baseline.

        Args:
            region: Region to monitor
            timeout_ms: Maximum wait time
            poll_interval_ms: Time between checks
            baseline_path: Optional baseline screenshot

        Returns:
            Path to changed screenshot or None if timeout
        """
        start_time = time.time()
        timeout_sec = timeout_ms / 1000
        poll_sec = poll_interval_ms / 1000

        if baseline_path:
            baseline = baseline_path
        else:
            baseline = self.capture(region=region)
            if not baseline:
                return None

        while time.time() - start_time < timeout_sec:
            current = self.capture(region=region)
            if not current:
                time.sleep(poll_sec)
                continue

            result = self.compare(baseline, current)
            if not result.get("identical", False):
                return current

            Path(current).unlink(missing_ok=True)
            time.sleep(poll_sec)

        return None


def create_screenshot_automator(
    platform: str = "macos",
    output_dir: str = "/tmp/screenshots"
) -> ScreenshotAutomator:
    """Factory function to create a ScreenshotAutomator."""
    return ScreenshotAutomator(platform=platform, output_dir=output_dir)
