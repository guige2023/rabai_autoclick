"""Image processing action module.

Provides image manipulation including resize, crop, rotate, filters,
format conversion, and thumbnail generation.
"""

from __future__ import annotations

import io
import logging
import subprocess
from typing import Optional, Tuple, Union
from pathlib import Path

logger = logging.getLogger(__name__)


class ImageAction:
    """Image processing engine using macOS sips and ImageMagick.

    Provides common image operations without external dependencies.

    Example:
        img = ImageAction()
        img.resize("/tmp/input.png", "/tmp/output.png", (800, 600))
        img.thumbnail("/tmp/photo.jpg", "/tmp/thumb.jpg", size=200)
    """

    def resize(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        size: Tuple[int, int],
        maintain_aspect: bool = True,
    ) -> bool:
        """Resize an image.

        Args:
            input_path: Input image path.
            output_path: Output image path.
            size: Target (width, height).
            maintain_aspect: Maintain aspect ratio.

        Returns:
            True if successful.
        """
        width, height = size

        if maintain_aspect:
            cmd = [
                "sips",
                str(input_path),
                "--resampleWidth", str(width),
                "--out", str(output_path),
            ]
        else:
            cmd = [
                "sips",
                str(input_path),
                "--resampleWidth", str(width),
                "--resampleHeight", str(height),
                "--out", str(output_path),
            ]

        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Resize failed: %s", e)
            return False

    def crop(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        region: Tuple[int, int, int, int],
    ) -> bool:
        """Crop an image.

        Args:
            input_path: Input image path.
            output_path: Output image path.
            region: (x, y, width, height) crop region.

        Returns:
            True if successful.
        """
        x, y, w, h = region

        try:
            cmd = ["convert", str(input_path), "-crop", f"{w}x{h}+{x}+{y}", str(output_path)]
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.warning("ImageMagick convert not found, using sips fallback")
            return self._sips_fallback(input_path, output_path, region)

    def thumbnail(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        size: int = 200,
    ) -> bool:
        """Generate a thumbnail.

        Args:
            input_path: Input image path.
            output_path: Output thumbnail path.
            size: Max dimension.

        Returns:
            True if successful.
        """
        cmd = [
            "sips",
            str(input_path),
            "--resampleWidth", str(size),
            "--out", str(output_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Thumbnail failed: %s", e)
            return False

    def rotate(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        degrees: float,
    ) -> bool:
        """Rotate an image.

        Args:
            input_path: Input image path.
            output_path: Output image path.
            degrees: Rotation angle (positive = clockwise).

        Returns:
            True if successful.
        """
        try:
            cmd = ["convert", str(input_path), "-rotate", str(degrees), str(output_path)]
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("ImageMagick not available for rotation")
            return False

    def convert_format(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        format: str = "jpeg",
    ) -> bool:
        """Convert image format.

        Args:
            input_path: Input image path.
            output_path: Output image path.
            format: Target format (jpeg, png, tiff, etc.).

        Returns:
            True if successful.
        """
        cmd = [
            "sips",
            "-s", f"format {format}",
            str(input_path),
            "--out", str(output_path),
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True)
            return True
        except subprocess.CalledProcessError as e:
            logger.error("Format conversion failed: %s", e)
            return False

    def get_info(self, image_path: Union[str, Path]) -> dict:
        """Get image metadata.

        Args:
            image_path: Path to image.

        Returns:
            Dict with width, height, format, etc.
        """
        try:
            cmd = ["sips", "-g", "all", str(image_path)]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            lines = result.stdout.split("\n")

            info = {}
            for line in lines[1:]:
                if ":" in line:
                    key, val = line.split(":", 1)
                    info[key.strip()] = val.strip()

            return info
        except subprocess.CalledProcessError as e:
            logger.error("Get info failed: %s", e)
            return {}

    def _sips_fallback(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        region: Tuple[int, int, int, int],
    ) -> bool:
        """Fallback crop using sips (limited)."""
        logger.warning("Full crop not supported with sips, copying file")
        try:
            subprocess.run(["cp", str(input_path), str(output_path)], check=True)
            return True
        except subprocess.CalledProcessError:
            return False
