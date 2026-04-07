"""
Image Processing Action Module.

Provides image loading, resizing, cropping, format conversion,
thumbnail generation, and basic image analysis utilities.

Example:
    >>> from image_processor_action import ImageProcessor
    >>> processor = ImageProcessor()
    >>> processor.resize("/tmp/input.png", (800, 600), "/tmp/output.png")
    >>> info = processor.get_info("/tmp/input.png")
"""
from __future__ import annotations

import io
import os
import subprocess
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ImageInfo:
    """Image metadata."""
    width: int
    height: int
    format: str
    mode: str
    size_bytes: int


class ImageProcessor:
    """Process images: resize, crop, convert, thumbnail."""

    def __init__(self):
        self._pillow_available = self._check_pillow()

    def _check_pillow(self) -> bool:
        try:
            from PIL import Image
            return True
        except ImportError:
            return False

    def get_info(self, path: str) -> Optional[ImageInfo]:
        """Get image metadata without loading full image."""
        if not os.path.exists(path):
            return None
        try:
            from PIL import Image
            with Image.open(path) as img:
                return ImageInfo(
                    width=img.width,
                    height=img.height,
                    format=img.format or "unknown",
                    mode=img.mode,
                    size_bytes=os.path.getsize(path),
                )
        except Exception:
            return None

    def resize(
        self,
        input_path: str,
        size: tuple[int, int],
        output_path: Optional[str] = None,
        maintain_aspect: bool = True,
    ) -> Optional[str]:
        """
        Resize image.

        Args:
            input_path: Source image path
            size: (width, height) target size
            output_path: Output path (default: overwrite input)
            maintain_aspect: Scale to fit within size, preserving aspect ratio

        Returns:
            Output path on success, None on failure
        """
        if not self._pillow_available:
            return self._resize_imagemagick(input_path, size, output_path)

        output = output_path or input_path
        try:
            from PIL import Image
            with Image.open(input_path) as img:
                if maintain_aspect:
                    img.thumbnail(size, Image.LANCZOS)
                else:
                    img = img.resize(size, Image.LANCZOS)
                img.save(output)
            return output
        except Exception:
            return None

    def _resize_imagemagick(
        self,
        input_path: str,
        size: tuple[int, int],
        output_path: Optional[str],
    ) -> Optional[str]:
        try:
            output = output_path or input_path
            subprocess.run(
                ["convert", input_path, "-resize", f"{size[0]}x{size[1]}", output],
                check=True,
                capture_output=True,
            )
            return output
        except Exception:
            return None

    def crop(
        self,
        input_path: str,
        region: tuple[int, int, int, int],
        output_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        Crop image to region.

        Args:
            input_path: Source image path
            region: (x, y, width, height)
            output_path: Output path

        Returns:
            Output path on success
        """
        output = output_path or input_path
        if self._pillow_available:
            try:
                from PIL import Image
                with Image.open(input_path) as img:
                    cropped = img.crop(region)
                    cropped.save(output)
                return output
            except Exception:
                return None
        return None

    def thumbnail(
        self,
        input_path: str,
        max_size: tuple[int, int] = (200, 200),
        output_path: Optional[str] = None,
    ) -> Optional[str]:
        """Generate thumbnail."""
        return self.resize(input_path, max_size, output_path, maintain_aspect=True)

    def convert_format(
        self,
        input_path: str,
        output_format: str,
        output_path: Optional[str] = None,
        quality: int = 85,
    ) -> Optional[str]:
        """
        Convert image to different format.

        Args:
            input_path: Source image
            output_format: Target format (JPEG, PNG, WEBP, etc.)
            output_path: Output path
            quality: JPEG quality (1-100)

        Returns:
            Output path
        """
        if not self._pillow_available:
            return None

        ext = output_format.lower()
        if output_path is None:
            base = os.path.splitext(input_path)[0]
            output_path = f"{base}.{ext}"

        try:
            from PIL import Image
            with Image.open(input_path) as img:
                if img.mode == "RGBA" and ext in ("jpg", "jpeg"):
                    img = img.convert("RGB")
                save_kwargs = {}
                if ext in ("jpg", "jpeg", "webp"):
                    save_kwargs["quality"] = quality
                img.save(output_path, format=output_format.upper(), **save_kwargs)
            return output_path
        except Exception:
            return None

    def to_grayscale(self, input_path: str, output_path: Optional[str] = None) -> Optional[str]:
        """Convert image to grayscale."""
        if not self._pillow_available:
            return None
        output = output_path or input_path
        try:
            from PIL import Image
            with Image.open(input_path) as img:
                gray = img.convert("L")
                gray.save(output)
            return output
        except Exception:
            return None

    def rotate(
        self,
        input_path: str,
        degrees: float,
        output_path: Optional[str] = None,
        expand: bool = True,
    ) -> Optional[str]:
        """Rotate image by degrees."""
        if not self._pillow_available:
            return None
        output = output_path or input_path
        try:
            from PIL import Image
            with Image.open(input_path) as img:
                rotated = img.rotate(degrees, expand=expand)
                rotated.save(output)
            return output
        except Exception:
            return None

    def flip(self, input_path: str, direction: str = "horizontal", output_path: Optional[str] = None) -> Optional[str]:
        """Flip image (horizontal, vertical, or both)."""
        if not self._pillow_available:
            return None
        output = output_path or input_path
        try:
            from PIL import Image
            with Image.open(input_path) as img:
                if direction == "horizontal":
                    flipped = img.transpose(Image.FLIP_LEFT_RIGHT)
                elif direction == "vertical":
                    flipped = img.transpose(Image.FLIP_TOP_BOTTOM)
                else:
                    flipped = img.transpose(Image.ROTATE_180)
                flipped.save(output)
            return output
        except Exception:
            return None

    def add_watermark(
        self,
        input_path: str,
        watermark_text: str,
        output_path: Optional[str] = None,
        opacity: float = 0.3,
        position: str = "bottom-right",
    ) -> Optional[str]:
        """Add text watermark to image."""
        if not self._pillow_available:
            return None
        output = output_path or input_path
        try:
            from PIL import Image, ImageDraw, ImageFont
            with Image.open(input_path) as img:
                watermark = Image.new("RGBA", img.size, (0, 0, 0, 0))
                draw = ImageDraw.Draw(watermark)
                try:
                    font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 36)
                except Exception:
                    font = ImageFont.load_default()
                bbox = draw.textbbox((0, 0), watermark_text, font=font)
                text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]

                margin = 20
                if position == "bottom-right":
                    x = img.width - text_w - margin
                    y = img.height - text_h - margin
                elif position == "top-right":
                    x = img.width - text_w - margin
                    y = margin
                elif position == "bottom-left":
                    x = margin
                    y = img.height - text_h - margin
                else:
                    x = margin
                    y = margin

                alpha = int(255 * opacity)
                draw.text((x, y), watermark_text, fill=(255, 255, 255, alpha), font=font)
                watermarked = Image.alpha_composite(img.convert("RGBA"), watermark)
                watermarked.convert("RGB").save(output)
            return output
        except Exception:
            return None

    def compress(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        quality: int = 75,
        max_width: Optional[int] = None,
    ) -> Optional[str]:
        """Compress image, optionally resizing."""
        if not self._pillow_available:
            return None
        output = output_path or input_path
        try:
            from PIL import Image
            with Image.open(input_path) as img:
                if max_width and img.width > max_width:
                    ratio = max_width / img.width
                    new_size = (max_width, int(img.height * ratio))
                    img = img.resize(new_size, Image.LANCZOS)
                if img.mode == "RGBA":
                    img = img.convert("RGB")
                img.save(output, "JPEG", quality=quality, optimize=True)
            return output
        except Exception:
            return None

    def create_sprite_sheet(
        self,
        image_paths: list[str],
        cols: int = 4,
        padding: int = 0,
        output_path: Optional[str] = None,
    ) -> Optional[str]:
        """Combine multiple images into a sprite sheet."""
        if not self._pillow_available or not image_paths:
            return None
        try:
            from PIL import Image
            images = [Image.open(p) for p in image_paths]
            if not images:
                return None
            thumb_w = max(img.width for img in images) + padding
            thumb_h = max(img.height for img in images) + padding
            rows = (len(images) + cols - 1) // cols
            sheet = Image.new("RGBA", (cols * thumb_w, rows * thumb_h), (0, 0, 0, 0))
            for i, img in enumerate(images):
                x = (i % cols) * thumb_w
                y = (i // cols) * thumb_h
                sheet.paste(img, (x, y))
            output = output_path or "/tmp/spritesheet.png"
            sheet.save(output)
            return output
        except Exception:
            return None


if __name__ == "__main__":
    processor = ImageProcessor()
    info = processor.get_info("/System/Library/CoreServices/Finder.app/Contents/Resources/Finder.icns")
    if info:
        print(f"Image: {info.width}x{info.height} {info.format}")
    else:
        print("Could not read image info")
