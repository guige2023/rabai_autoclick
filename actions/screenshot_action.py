"""Screenshot capture and analysis action.

This module provides screenshot capture capabilities including
full screen, region capture, and image analysis for automation.

Example:
    >>> action = ScreenshotAction()
    >>> result = action.execute(mode="full")
"""

from __future__ import annotations

import base64
import io
import time
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ScreenshotResult:
    """Result from screenshot capture."""
    success: bool
    path: Optional[str] = None
    image_data: Optional[str] = None
    size: Optional[tuple[int, int]] = None
    error: Optional[str] = None


class ScreenshotAction:
    """Screenshot capture and analysis action.

    Provides multiple screenshot modes including full screen,
    region capture, window capture, and basic image analysis.

    Example:
        >>> action = ScreenshotAction()
        >>> result = action.execute(mode="region", region=(0, 0, 800, 600))
    """

    def __init__(self) -> None:
        """Initialize screenshot action."""
        self._last_screenshot: Optional[Any] = None

    def execute(
        self,
        mode: str = "full",
        region: Optional[tuple[int, int, int, int]] = None,
        path: Optional[str] = None,
        encoding: str = "png",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute screenshot capture.

        Args:
            mode: Capture mode ('full', 'region', 'window').
            region: Region tuple (x, y, width, height).
            path: Optional save path.
            encoding: Image encoding ('png', 'jpg').
            **kwargs: Additional parameters.

        Returns:
            Screenshot result dictionary.

        Raises:
            ValueError: If mode is invalid.
        """
        try:
            from PIL import Image, ImageGrab
        except ImportError:
            return {
                "success": False,
                "error": "Pillow not installed. Run: pip install pillow",
            }

        mode = mode.lower()
        result: dict[str, Any] = {"mode": mode, "success": True}

        try:
            if mode == "full":
                img = ImageGrab.grab()
            elif mode == "region":
                if not region:
                    raise ValueError("region required for 'region' mode")
                img = ImageGrab.grab(bbox=region)
                result["region"] = region
            elif mode == "window":
                title = kwargs.get("title")
                img = self._capture_window(title)
            else:
                raise ValueError(f"Unknown mode: {mode}")

            self._last_screenshot = img
            result["size"] = img.size

            # Save if path provided
            if path:
                img.save(path, format=encoding.upper())
                result["path"] = path

            # Encode to base64
            buffer = io.BytesIO()
            img.save(buffer, format=encoding.upper())
            img_bytes = buffer.getvalue()
            result["image_data"] = base64.b64encode(img_bytes).decode()
            result["size_bytes"] = len(img_bytes)

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _capture_window(self, title: Optional[str] = None) -> Any:
        """Capture specific window by title.

        Args:
            title: Window title (partial match).

        Returns:
            PIL Image of window.
        """
        try:
            import pyscreeze
        except ImportError:
            import pyscreeze as _ps
            pyscreeze = _ps

        if title:
            # Try to locate window by title
            windows = self._find_windows(title)
            if windows:
                bbox = windows[0]
                from PIL import ImageGrab
                return ImageGrab.grab(bbox=bbox)

        # Fallback to full screen
        from PIL import ImageGrab
        return ImageGrab.grab()

    def _find_windows(self, title: str) -> list[tuple[int, int, int, int]]:
        """Find windows matching title.

        Args:
            title: Window title to search for.

        Returns:
            List of window bounding boxes.
        """
        try:
            import pywintypes
            import win32gui
            import win32con
        except ImportError:
            return []

        windows = []

        def callback(hwnd, _):
            if win32gui.IsWindowVisible(hwnd):
                window_title = win32gui.GetWindowText(hwnd)
                if title.lower() in window_title.lower():
                    rect = win32gui.GetWindowRect(hwnd)
                    windows.append(rect)
            return True

        try:
            win32gui.EnumWindows(callback, None)
        except Exception:
            pass

        return windows

    def analyze_image(
        self,
        image_path: Optional[str] = None,
        data: Optional[str] = None,
    ) -> dict[str, Any]:
        """Analyze screenshot for colors, brightness, etc.

        Args:
            image_path: Path to image file.
            data: Base64 encoded image data.

        Returns:
            Analysis result dictionary.
        """
        from PIL import Image
        import io

        result: dict[str, Any] = {"success": True}

        try:
            if data:
                img_data = base64.b64decode(data)
                img = Image.open(io.BytesIO(img_data))
            elif image_path:
                img = Image.open(image_path)
            else:
                raise ValueError("image_path or data required")

            # Convert to RGB if necessary
            if img.mode != "RGB":
                img = img.convert("RGB")

            result["size"] = img.size
            result["mode"] = img.mode

            # Sample colors
            pixels = list(img.getdata())
            if pixels:
                avg_color = tuple(
                    sum(c[i] for c in pixels) // len(pixels)
                    for i in range(3)
                )
                result["average_color"] = avg_color

                # Find most common color
                from collections import Counter
                counter = Counter(pixels)
                result["most_common_color"] = counter.most_common(1)[0][0]

            # Calculate brightness
            if pixels:
                brightness = sum(
                    0.299 * c[0] + 0.587 * c[1] + 0.114 * c[2]
                    for c in pixels
                ) / len(pixels)
                result["average_brightness"] = brightness

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def compare_screenshots(
        self,
        image1: str,
        image2: str,
        threshold: float = 0.95,
    ) -> dict[str, Any]:
        """Compare two screenshots for differences.

        Args:
            image1: First image path.
            image2: Second image path.
            threshold: Similarity threshold (0-1).

        Returns:
            Comparison result dictionary.
        """
        from PIL import Image
        import math

        result: dict[str, Any] = {"success": True}

        try:
            img1 = Image.open(image1).convert("RGB")
            img2 = Image.open(image2).convert("RGB")

            if img1.size != img2.size:
                result["match"] = False
                result["reason"] = "Different sizes"
                return result

            # Calculate pixel difference
            pixels1 = list(img1.getdata())
            pixels2 = list(img2.getdata())

            diff_count = 0
            for p1, p2 in zip(pixels1, pixels2):
                if p1 != p2:
                    diff_count += 1

            similarity = 1 - (diff_count / len(pixels1))
            result["similarity"] = similarity
            result["match"] = similarity >= threshold
            result["diff_pixels"] = diff_count
            result["diff_percent"] = (diff_count / len(pixels1)) * 100

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result
