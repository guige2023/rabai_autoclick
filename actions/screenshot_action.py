"""
Screenshot Action Module

Captures screenshots, regions, windows with various
options for automation documentation and debugging.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from io import BytesIO
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class CaptureFormat(Enum):
    """Screenshot capture formats."""

    PNG = "png"
    JPG = "jpg"
    BMP = "bmp"
    GIF = "gif"


@dataclass
class ScreenshotConfig:
    """Configuration for screenshot capture."""

    default_format: CaptureFormat = CaptureFormat.PNG
    default_quality: int = 90
    include_cursor: bool = False
    capture_delay: float = 0.1
    max_dimension: int = 4096


@dataclass
class Screenshot:
    """Represents a captured screenshot."""

    data: bytes
    format: CaptureFormat
    bounds: Tuple[int, int, int, int]
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ScreenshotCapture:
    """
    Captures screenshots for automation.

    Supports full screen, window, and region capture
    with various output formats.
    """

    def __init__(
        self,
        config: Optional[ScreenshotConfig] = None,
        capture_handler: Optional[Callable[..., Optional[bytes]]] = None,
    ):
        self.config = config or ScreenshotConfig()
        self.capture_handler = capture_handler or self._default_capture
        self._save_directory: Optional[str] = None

    def _default_capture(
        self,
        region: Optional[Tuple[int, int, int, int]] = None,
        format: CaptureFormat = CaptureFormat.PNG,
    ) -> Optional[bytes]:
        """Default capture using screencapture."""
        import subprocess

        cmd = ["screencapture", "-x"]

        if region:
            x, y, w, h = region
            cmd.extend(["-R", f"{x},{y},{w},{h}"])

        if format == CaptureFormat.PNG:
            cmd.append("-P")
        elif format == CaptureFormat.JPG:
            cmd.append("-tjpg")
        else:
            cmd.append("-P")

        try:
            result = subprocess.run(cmd, capture_output=True)
            return result.stdout
        except Exception as e:
            logger.error(f"Screenshot capture failed: {e}")
            return None

    def capture_screen(
        self,
        monitor_index: int = 0,
        format: Optional[CaptureFormat] = None,
    ) -> Optional[Screenshot]:
        """
        Capture full screen.

        Args:
            monitor_index: Monitor index
            format: Output format

        Returns:
            Screenshot object or None
        """
        format = format or self.config.default_format
        bounds = self._get_monitor_bounds(monitor_index)

        data = self.capture_handler(region=bounds, format=format)

        if data:
            return Screenshot(
                data=data,
                format=format,
                bounds=bounds,
                metadata={"monitor": monitor_index},
            )
        return None

    def capture_region(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        format: Optional[CaptureFormat] = None,
    ) -> Optional[Screenshot]:
        """
        Capture a screen region.

        Args:
            x: X coordinate
            y: Y coordinate
            width: Region width
            height: Region height
            format: Output format

        Returns:
            Screenshot object or None
        """
        format = format or self.config.default_format
        bounds = (x, y, width, height)

        data = self.capture_handler(region=bounds, format=format)

        if data:
            return Screenshot(
                data=data,
                format=format,
                bounds=bounds,
            )
        return None

    def capture_window(
        self,
        window_id: int,
        format: Optional[CaptureFormat] = None,
    ) -> Optional[Screenshot]:
        """
        Capture a window.

        Args:
            window_id: Window identifier
            format: Output format

        Returns:
            Screenshot object or None
        """
        format = format or self.config.default_format

        logger.info(f"Capturing window {window_id}")
        bounds = (0, 0, 800, 600)

        data = self.capture_handler(region=bounds, format=format)

        if data:
            return Screenshot(
                data=data,
                format=format,
                bounds=bounds,
                metadata={"window_id": window_id},
            )
        return None

    def _get_monitor_bounds(self, index: int) -> Tuple[int, int, int, int]:
        """Get bounds for a monitor."""
        return (0, 0, 1920, 1080)

    def save(
        self,
        screenshot: Screenshot,
        path: str,
    ) -> bool:
        """
        Save screenshot to file.

        Args:
            screenshot: Screenshot to save
            path: Output file path

        Returns:
            True if successful
        """
        try:
            with open(path, "wb") as f:
                f.write(screenshot.data)
            return True
        except Exception as e:
            logger.error(f"Save failed: {e}")
            return False

    def to_base64(self, screenshot: Screenshot) -> str:
        """Encode screenshot as base64."""
        return base64.b64encode(screenshot.data).decode("utf-8")

    def to_pil_image(self, screenshot: Screenshot) -> Optional[Any]:
        """Convert screenshot to PIL Image."""
        try:
            from PIL import Image
            return Image.open(BytesIO(screenshot.data))
        except Exception as e:
            logger.error(f"PIL conversion failed: {e}")
            return None


def create_screenshot_capture(
    config: Optional[ScreenshotConfig] = None,
) -> ScreenshotCapture:
    """Factory function to create ScreenshotCapture."""
    return ScreenshotCapture(config=config)
