"""Image processor action for image manipulation and analysis.

This module provides image processing capabilities including
resizing, cropping, filtering, and format conversion.

Example:
    >>> action = ImageProcessorAction()
    >>> result = action.execute(operation="resize", path="/tmp/image.png", width=800)
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ImageInfo:
    """Image information."""
    width: int
    height: int
    format: str
    mode: str
    size_bytes: int


class ImageProcessorAction:
    """Image processing action.

    Provides image manipulation including resize, crop,
    rotate, filter, and format conversion.

    Example:
        >>> action = ImageProcessorAction()
        >>> result = action.execute(
        ...     operation="resize",
        ...     input_path="input.jpg",
        ...     output_path="output.png",
        ...     width=800
        ... )
    """

    def __init__(self) -> None:
        """Initialize image processor."""
        self._last_image: Optional[Any] = None

    def execute(
        self,
        operation: str,
        input_path: Optional[str] = None,
        output_path: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute image operation.

        Args:
            operation: Operation name (resize, crop, rotate, etc.).
            input_path: Input image path.
            output_path: Output image path.
            width: Target width.
            height: Target height.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid or image cannot be loaded.
        """
        try:
            from PIL import Image
        except ImportError:
            return {
                "success": False,
                "error": "Pillow not installed. Run: pip install pillow",
            }

        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        # Load image
        img = self._load_image(input_path, kwargs.get("image_data"))
        if img is None:
            return {"success": False, "error": "Could not load image"}

        self._last_image = img

        # Execute operation
        if op == "resize":
            if width is None and height is None:
                raise ValueError("width or height required for 'resize'")
            img = self._resize_image(img, width, height, kwargs.get("keep_aspect", True))
            result["resized"] = True

        elif op == "crop":
            box = kwargs.get("box")
            if not box:
                raise ValueError("box required for 'crop'")
            img = img.crop(box)
            result["cropped"] = True

        elif op == "rotate":
            angle = kwargs.get("angle", 90)
            img = img.rotate(angle, expand=kwargs.get("expand", True))
            result["rotated"] = angle

        elif op == "flip":
            direction = kwargs.get("direction", "horizontal")
            if direction == "horizontal":
                img = img.transpose(Image.FLIP_LEFT_RIGHT)
            else:
                img = img.transpose(Image.FLIP_TOP_BOTTOM)
            result["flipped"] = direction

        elif op == "grayscale":
            img = img.convert("L")
            result["converted"] = "grayscale"

        elif op == "blur":
            radius = kwargs.get("radius", 2)
            img = img.filter(Image.BLUR)
            result["blurred"] = radius

        elif op == "sharpen":
            img = img.filter(Image.SHARPEN)
            result["sharpened"] = True

        elif op == "thumbnail":
            if width is None and height is None:
                raise ValueError("width or height required for 'thumbnail'")
            img.thumbnail((width or 9999, height or 9999))
            result["thumbnailed"] = True

        elif op == "convert_format":
            fmt = kwargs.get("format", "PNG")
            output = io.BytesIO()
            img.save(output, format=fmt.upper())
            result["converted"] = fmt
            img = Image.open(output)

        elif op == "info":
            result["info"] = {
                "width": img.width,
                "height": img.height,
                "format": img.format,
                "mode": img.mode,
            }
            return result

        elif op == "thumbnail_grid":
            cells = kwargs.get("cells", 3)
            result["grid"] = self._create_thumbnail_grid(img, cells)
            return result

        else:
            raise ValueError(f"Unknown operation: {operation}")

        # Save output
        if output_path:
            img.save(output_path)
            result["output_path"] = output_path

        result["size"] = (img.width, img.height)
        self._last_image = img

        return result

    def _load_image(self, path: Optional[str], image_data: Optional[str]) -> Any:
        """Load image from path or data.

        Args:
            path: Image file path.
            image_data: Base64 image data.

        Returns:
            PIL Image object or None.
        """
        from PIL import Image
        import base64

        try:
            if path:
                return Image.open(path)
            elif image_data:
                img_data = base64.b64decode(image_data)
                return Image.open(io.BytesIO(img_data))
        except Exception:
            return None

        return None

    def _resize_image(
        self,
        img: Any,
        width: Optional[int],
        height: Optional[int],
        keep_aspect: bool,
    ) -> Any:
        """Resize image.

        Args:
            img: PIL Image.
            width: Target width.
            height: Target height.
            keep_aspect: Whether to maintain aspect ratio.

        Returns:
            Resized image.
        """
        if keep_aspect:
            img.thumbnail((width or 9999, height or 9999))
            return img
        else:
            return img.resize((width or img.width, height or img.height))

    def _create_thumbnail_grid(self, img: Any, cells: int) -> list[str]:
        """Create grid of thumbnails.

        Args:
            img: PIL Image.
            cells: Number of cells per row/column.

        Returns:
            List of thumbnail image data.
        """
        from PIL import Image
        import base64

        thumbnails: list[str] = []
        cell_width = img.width // cells
        cell_height = img.height // cells

        for i in range(cells):
            for j in range(cells):
                left = j * cell_width
                top = i * cell_height
                right = left + cell_width
                bottom = top + cell_height

                thumb = img.crop((left, top, right, bottom))
                thumb.thumbnail((100, 100))

                buffer = io.BytesIO()
                thumb.save(buffer, format="PNG")
                thumbnails.append(base64.b64encode(buffer.getvalue()).decode())

        return thumbnails

    def composite_images(
        self,
        images: list[str],
        layout: str = "horizontal",
        spacing: int = 0,
    ) -> dict[str, Any]:
        """Composite multiple images.

        Args:
            images: List of image paths or data.
            layout: Layout type ('horizontal', 'vertical', 'grid').
            spacing: Pixel spacing between images.

        Returns:
            Composite result dictionary.
        """
        from PIL import Image

        imgs = [self._load_image(p, None) for p in images]
        imgs = [img for img in imgs if img is not None]

        if not imgs:
            return {"success": False, "error": "No valid images"}

        if layout == "horizontal":
            total_width = sum(img.width for img in imgs) + spacing * (len(imgs) - 1)
            max_height = max(img.height for img in imgs)
            composite = Image.new("RGB", (total_width, max_height), (255, 255, 255))

            x = 0
            for img in imgs:
                composite.paste(img, (x, 0))
                x += img.width + spacing

        elif layout == "vertical":
            max_width = max(img.width for img in imgs)
            total_height = sum(img.height for img in imgs) + spacing * (len(imgs) - 1)
            composite = Image.new("RGB", (max_width, total_height), (255, 255, 255))

            y = 0
            for img in imgs:
                composite.paste(img, (0, y))
                y += img.height + spacing

        else:
            return {"success": False, "error": f"Unknown layout: {layout}"}

        self._last_image = composite
        return {"success": True, "composited": len(imgs)}
