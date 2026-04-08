"""
Screenshot Annotator Utilities.

Utilities for annotating screenshots with visual markers,
highlights, arrows, text labels, and region overlays.

Usage:
    from utils.screenshot_annotator import ScreenshotAnnotator, annotate

    annotator = ScreenshotAnnotator()
    annotated = annotator.add_rect(screenshot, (x, y, w, h), color="red")
    annotated = annotator.add_text(annotated, (x, y), "Click here")
    annotated.save("annotated.png")
"""

from __future__ import annotations

from typing import Optional, Tuple, List, Dict, Any, Union
from dataclasses import dataclass
from enum import Enum, auto
import io
import math

try:
    from PIL import Image, ImageDraw, ImageFont
    HAS_PIL = True
except ImportError:
    HAS_PIL = False


class AnnotationType(Enum):
    """Types of annotations that can be applied."""
    RECTANGLE = auto()
    ROUNDED_RECT = auto()
    CIRCLE = auto()
    ARROW = auto()
    LINE = auto()
    TEXT = auto()
    HIGHLIGHT = auto()
    BLUR = auto()
    NUMBERING = auto()


@dataclass
class Annotation:
    """Base annotation with common properties."""
    annotation_type: AnnotationType
    color: Tuple[int, int, int, int] = (255, 0, 0, 255)
    thickness: int = 2
    label: Optional[str] = None


@dataclass
class RectAnnotation(Annotation):
    """Rectangle annotation."""
    rect: Tuple[int, int, int, int] = (0, 0, 0, 0)  # x, y, width, height
    fill: Optional[Tuple[int, int, int, int]] = None
    corner_radius: int = 0


@dataclass
class CircleAnnotation(Annotation):
    """Circle annotation."""
    center: Tuple[int, int] = (0, 0)
    radius: int = 10


@dataclass
class ArrowAnnotation(Annotation):
    """Arrow annotation."""
    start: Tuple[int, int] = (0, 0)
    end: Tuple[int, int] = (0, 0)


@dataclass
class TextAnnotation(Annotation):
    """Text annotation."""
    position: Tuple[int, int] = (0, 0)
    text: str = ""
    font_size: int = 16
    font_path: Optional[str] = None
    background: Optional[Tuple[int, int, int, int]] = None


class ScreenshotAnnotator:
    """
    Annotate screenshots with visual markers.

    Supports rectangles, circles, arrows, lines, text labels,
    numbering, highlights, and blurs. All annotations are
    rendered using PIL.

    Example:
        annotator = ScreenshotAnnotator()
        img = annotator.load("screenshot.png")
        img = annotator.add_rect(img, (100, 100, 200, 50), color=(255, 0, 0, 255))
        img = annotator.add_numbered_circle(img, (150, 125), 1)
        img = annotator.add_text(img, (100, 80), "Step 1")
        img.save("annotated.png")
    """

    def __init__(
        self,
        default_color: Tuple[int, int, int, int] = (255, 0, 0, 255),
        default_thickness: int = 2,
    ) -> None:
        """
        Initialize the annotator.

        Args:
            default_color: Default annotation color (RGBA).
            default_thickness: Default line thickness in pixels.
        """
        self._default_color = default_color
        self._default_thickness = default_thickness
        self._font_cache: Dict[Tuple[str, int], ImageFont.FreeTypeFont] = {}

    def load(
        self,
        source: Union[str, bytes, "Image.Image"],
    ) -> "Image.Image":
        """
        Load an image from a file path, bytes, or PIL Image.

        Args:
            source: File path, bytes, or PIL Image.

        Returns:
            PIL Image object.
        """
        if not HAS_PIL:
            raise ImportError("PIL is required for screenshot annotation")

        if isinstance(source, Image.Image):
            return source.copy()
        elif isinstance(source, str):
            return Image.open(source).convert("RGBA")
        elif isinstance(source, bytes):
            return Image.open(io.BytesIO(source)).convert("RGBA")
        else:
            raise TypeError(f"Cannot load image from {type(source)}")

    def _get_font(self, size: int, path: Optional[str] = None) -> ImageFont.FreeTypeFont:
        """Get a cached font instance."""
        key = (path or "default", size)
        if key not in self._font_cache:
            try:
                if path:
                    self._font_cache[key] = ImageFont.truetype(path, size)
                else:
                    self._font_cache[key] = ImageFont.load_default()
            except Exception:
                self._font_cache[key] = ImageFont.load_default()
        return self._font_cache[key]

    def add_rect(
        self,
        image: "Image.Image",
        rect: Tuple[int, int, int, int],
        color: Optional[Tuple[int, int, int, int]] = None,
        fill: Optional[Tuple[int, int, int, int]] = None,
        thickness: int = -1,
        corner_radius: int = 0,
    ) -> "Image.Image":
        """
        Add a rectangle annotation to an image.

        Args:
            image: PIL Image to annotate.
            rect: (x, y, width, height) of the rectangle.
            color: Outline color (RGBA), uses default if None.
            fill: Fill color (RGBA) for solid rectangle.
            thickness: Line thickness (-1 for solid fill).
            corner_radius: Corner radius for rounded rectangles.

        Returns:
            Annotated PIL Image.
        """
        if color is None:
            color = self._default_color

        draw = ImageDraw.Draw(image)
        x, y, w, h = rect

        if corner_radius > 0 and fill is None:
            draw.rounded_rectangle([x, y, x + w, y + h], corner_radius, fill=fill, outline=color, width=thickness)
        elif fill is not None:
            draw.rectangle([x, y, x + w, y + h], fill=fill, outline=color, width=thickness)
        else:
            draw.rectangle([x, y, x + w, y + h], outline=color, width=thickness)

        return image

    def add_circle(
        self,
        image: "Image.Image",
        center: Tuple[int, int],
        radius: int,
        color: Optional[Tuple[int, int, int, int]] = None,
        fill: Optional[Tuple[int, int, int, int]] = None,
        thickness: int = 2,
    ) -> "Image.Image":
        """
        Add a circle annotation.

        Args:
            image: PIL Image to annotate.
            center: (x, y) center of the circle.
            radius: Radius in pixels.
            color: Outline color (RGBA).
            fill: Fill color (RGBA).
            thickness: Line thickness.

        Returns:
            Annotated PIL Image.
        """
        if color is None:
            color = self._default_color

        draw = ImageDraw.Draw(image)
        bbox = [center[0] - radius, center[1] - radius, center[0] + radius, center[1] + radius]
        draw.ellipse(bbox, fill=fill, outline=color, width=thickness)
        return image

    def add_arrow(
        self,
        image: "Image.Image",
        start: Tuple[int, int],
        end: Tuple[int, int],
        color: Optional[Tuple[int, int, int, int]] = None,
        thickness: int = 2,
        head_length: int = 15,
        head_angle: int = 30,
    ) -> "Image.Image":
        """
        Add an arrow annotation between two points.

        Args:
            image: PIL Image to annotate.
            start: (x, y) starting point.
            end: (x, y) ending point.
            color: Arrow color (RGBA).
            thickness: Line thickness.
            head_length: Length of the arrowhead.
            head_angle: Angle of the arrowhead in degrees.

        Returns:
            Annotated PIL Image.
        """
        if color is None:
            color = self._default_color

        draw = ImageDraw.Draw(image)

        angle = math.atan2(end[1] - start[1], end[0] - start[0])
        angle_rad = math.radians(head_angle)
        arrow_angle1 = angle + math.pi - angle_rad
        arrow_angle2 = angle + angle_rad

        p1 = (
            end[0] - head_length * math.cos(arrow_angle1),
            end[1] - head_length * math.sin(arrow_angle1),
        )
        p2 = (
            end[0] - head_length * math.cos(arrow_angle2),
            end[1] - head_length * math.sin(arrow_angle2),
        )

        draw.line([start, end], fill=color, width=thickness)
        draw.polygon([end, p1, p2], fill=color)
        return image

    def add_text(
        self,
        image: "Image.Image",
        position: Tuple[int, int],
        text: str,
        color: Optional[Tuple[int, int, int, int]] = None,
        font_size: int = 16,
        background: Optional[Tuple[int, int, int, int]] = None,
    ) -> "Image.Image":
        """
        Add a text label to an image.

        Args:
            image: PIL Image to annotate.
            position: (x, y) top-left position of the text.
            text: Text string to draw.
            color: Text color (RGBA).
            font_size: Font size in points.
            background: Optional background color (RGBA).

        Returns:
            Annotated PIL Image.
        """
        if color is None:
            color = self._default_color

        draw = ImageDraw.Draw(image)
        font = self._get_font(font_size)

        bbox = draw.textbbox(position, text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]

        if background:
            pad = 4
            draw.rectangle(
                [bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad],
                fill=background,
            )

        draw.text(position, text, fill=color, font=font)
        return image

    def add_numbered_circle(
        self,
        image: "Image.Image",
        center: Tuple[int, int],
        number: int,
        radius: int = 20,
        color: Optional[Tuple[int, int, int, int]] = None,
    ) -> "Image.Image":
        """
        Add a numbered circle badge (e.g., for step indicators).

        Args:
            image: PIL Image to annotate.
            center: (x, y) center of the circle.
            number: Number to display inside the circle.
            radius: Radius of the circle.
            color: Circle color (RGBA).

        Returns:
            Annotated PIL Image.
        """
        if color is None:
            color = self._default_color

        import math
        red = (255, 0, 0, 255)

        draw = ImageDraw.Draw(image)
        bbox = [center[0] - radius, center[1] - radius, center[0] + radius, center[1] + radius]
        draw.ellipse(bbox, fill=red)

        font = self._get_font(int(radius * 0.8))
        text = str(number)
        tbbox = draw.textbbox((0, 0), text, font=font)
        tw = tbbox[2] - tbbox[0]
        th = tbbox[3] - tbbox[1]
        tx = center[0] - tw // 2
        ty = center[1] - th // 2 - 2
        draw.text((tx, ty), text, fill=(255, 255, 255, 255), font=font)
        return image

    def add_highlight(
        self,
        image: "Image.Image",
        rect: Tuple[int, int, int, int],
        alpha: int = 80,
    ) -> "Image.Image":
        """
        Add a semi-transparent highlight overlay.

        Args:
            image: PIL Image to annotate.
            rect: (x, y, width, height) region to highlight.
            alpha: Opacity (0-255) of the highlight.

        Returns:
            Annotated PIL Image.
        """
        overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        x, y, w, h = rect
        draw.rectangle([x, y, x + w, y + h], fill=(255, 255, 0, alpha))
        return Image.alpha_composite(image, overlay)

    def annotate(
        self,
        image: Union[str, bytes, "Image.Image"],
        annotations: List[Dict[str, Any]],
    ) -> "Image.Image":
        """
        Apply multiple annotations from a list of dicts.

        Args:
            image: Source image (path, bytes, or PIL Image).
            annotations: List of annotation dicts with 'type' and parameters.

        Returns:
            Annotated PIL Image.
        """
        img = self.load(image)

        for ann in annotations:
            ann_type = ann.get("type")
            if ann_type == "rect":
                img = self.add_rect(img, **ann.get("params", {}))
            elif ann_type == "circle":
                img = self.add_circle(img, **ann.get("params", {}))
            elif ann_type == "arrow":
                img = self.add_arrow(img, **ann.get("params", {}))
            elif ann_type == "text":
                img = self.add_text(img, **ann.get("params", {}))
            elif ann_type == "number":
                img = self.add_numbered_circle(img, **ann.get("params", {}))
            elif ann_type == "highlight":
                img = self.add_highlight(img, **ann.get("params", {}))

        return img
