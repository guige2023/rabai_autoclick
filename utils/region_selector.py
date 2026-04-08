"""
Region Selector Utility

Interactive region selection for screen automation.
Allows users to define screen regions via keyboard shortcuts.

Example:
    >>> selector = RegionSelector()
    >>> region = selector.select_region()
    >>> print(f"Selected: {region.x}, {region.y}, {region.width}, {region.height}")
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Optional


@dataclass
class ScreenRegion:
    """A rectangular screen region."""
    x: int
    y: int
    width: int
    height: int

    def __str__(self) -> str:
        return f"{self.x},{self.y},{self.width},{self.height}"

    def to_tuple(self) -> tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)

    def center(self) -> tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)


class RegionSelector:
    """
    Allows interactive region selection using macOS screenshot tool.

    Uses `screencapture -i -R` for interactive selection.
    """

    def __init__(self) -> None:
        self._tmp_file: Optional[str] = None

    def select_region(self) -> Optional[ScreenRegion]:
        """
        Launch interactive region selector.

        Returns:
            ScreenRegion if selected, None if cancelled.
        """
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            self._tmp_file = f.name

        try:
            result = subprocess.run(
                [
                    "screencapture",
                    "-i",  # interactive
                    "-R", "0,0,0,0",  # initial rect (will be overridden by user)
                    "-s",  # only allow region selection
                    self._tmp_file,
                ],
                timeout=30.0,
            )

            if result.returncode != 0:
                return None

            # Read the saved region from file
            region = self._get_region_from_file(self._tmp_file)
            return region
        except Exception:
            return None
        finally:
            self._cleanup_tmp()

    def _get_region_from_file(self, path: str) -> Optional[ScreenRegion]:
        """Parse region from saved screenshot metadata."""
        try:
            from PIL import Image
            img = Image.open(path)
            return ScreenRegion(x=0, y=0, width=img.width, height=img.height)
        except Exception:
            return None

    def _cleanup_tmp(self) -> None:
        """Clean up temporary file."""
        if self._tmp_file and os.path.exists(self._tmp_file):
            try:
                os.remove(self._tmp_file)
            except Exception:
                pass
            self._tmp_file = None

    def select_multiple(self, count: int = 3) -> list[ScreenRegion]:
        """
        Select multiple regions sequentially.

        Args:
            count: Number of regions to select.

        Returns:
            List of ScreenRegions (may include None for cancelled).
        """
        regions: list[ScreenRegion] = []
        for _ in range(count):
            region = self.select_region()
            regions.append(region)
            if region is None:
                break
        return [r for r in regions if r is not None]
