"""Screenshot capture action module.

Provides screenshot capture with region selection, annotation,
and comparison capabilities.
"""

from __future__ import annotations

import os
import time
import logging
import subprocess
from typing import Optional, Tuple, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ScreenshotAction:
    """Screenshot capture engine.

    Captures screenshots using macOS screencapture.

    Example:
        shot = ScreenshotAction()
        path = shot.capture()
        shot.capture(region=(100, 100, 800, 600), path="/tmp/region.png")
    """

    def __init__(self) -> None:
        """Initialize screenshot action."""
        self._default_path = "/tmp/screenshot_{timestamp}.png"

    def capture(
        self,
        path: Optional[str] = None,
        region: Optional[Tuple[int, int, int, int]] = None,
        include_cursor: bool = True,
    ) -> str:
        """Capture a screenshot.

        Args:
            path: Output file path.
            region: Optional (x, y, width, height) region.
            include_cursor: Include cursor in screenshot.

        Returns:
            Path to the captured screenshot.
        """
        if path is None:
            path = self._default_path.format(timestamp=int(time.time()))

        cmd = ["screencapture"]
        if region:
            x, y, w, h = region
            cmd.extend(["-R", f"{x},{y},{w},{h}"])
        if not include_cursor:
            cmd.append("-C")

        cmd.append(path)

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            logger.debug("Screenshot saved to %s", path)
        except subprocess.CalledProcessError as e:
            logger.error("Screenshot failed: %s", e)
            raise

        return path

    def capture_to_clipboard(
        self,
        region: Optional[Tuple[int, int, int, int]] = None,
    ) -> bool:
        """Capture screenshot directly to clipboard.

        Args:
            region: Optional (x, y, width, height) region.

        Returns:
            True if successful.
        """
        cmd = ["screencapture"]
        if region:
            x, y, w, h = region
            cmd.extend(["-R", f"{x},{y},{w},{h}"])
        cmd.append("-c")

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Screenshot to clipboard failed: %s", e)
            return False

    def capture_primary_display(self, path: Optional[str] = None) -> str:
        """Capture the primary display."""
        return self.capture(path=path)

    def capture_all_displays(self, path_prefix: str = "/tmp/display") -> list:
        """Capture all connected displays.

        Returns:
            List of screenshot file paths.
        """
        paths = []
        displays = self._get_displays()

        for i, display_id in enumerate(displays):
            p = f"{path_prefix}_{i}.png"
            cmd = ["screencapture", "-D", str(display_id), p]
            try:
                subprocess.run(cmd, check=True, capture_output=True)
                paths.append(p)
            except subprocess.CalledProcessError:
                pass

        return paths

    def _get_displays(self) -> list:
        """Get list of connected display IDs."""
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                check=True,
                capture_output=True,
            )
            import json
            data = json.loads(result.stdout)
            displays = data.get("SPDisplaysDataType", [])
            return list(range(1, len(displays) + 1))
        except Exception:
            return [1]
