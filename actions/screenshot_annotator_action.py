"""
Screenshot Annotator Action Module.

Annotates screenshots with shapes, text, and highlights
for documentation and debugging purposes.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Annotation:
    """A screenshot annotation."""
    type: str
    params: dict


class ScreenshotAnnotator:
    """Annotates screenshots with shapes and text."""

    def __init__(self):
        """Initialize screenshot annotator."""
        self._annotations: list[Annotation] = []

    def add_rectangle(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        color: str = "red",
        line_width: int = 2,
    ) -> "ScreenshotAnnotator":
        """
        Add a rectangle annotation.

        Args:
            x: X position.
            y: Y position.
            width: Rectangle width.
            height: Rectangle height.
            color: Stroke color.
            line_width: Line width.

        Returns:
            Self for chaining.
        """
        self._annotations.append(Annotation(
            type="rectangle",
            params={
                "x": x, "y": y,
                "width": width, "height": height,
                "color": color, "line_width": line_width,
            },
        ))
        return self

    def add_circle(
        self,
        x: int,
        y: int,
        radius: int,
        color: str = "red",
        line_width: int = 2,
    ) -> "ScreenshotAnnotator":
        """Add a circle annotation."""
        self._annotations.append(Annotation(
            type="circle",
            params={
                "x": x, "y": y,
                "radius": radius,
                "color": color, "line_width": line_width,
            },
        ))
        return self

    def add_arrow(
        self,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
        color: str = "red",
        line_width: int = 2,
    ) -> "ScreenshotAnnotator":
        """Add an arrow annotation."""
        self._annotations.append(Annotation(
            type="arrow",
            params={
                "x1": x1, "y1": y1,
                "x2": x2, "y2": y2,
                "color": color, "line_width": line_width,
            },
        ))
        return self

    def add_text(
        self,
        x: int,
        y: int,
        text: str,
        color: str = "red",
        font_size: int = 16,
    ) -> "ScreenshotAnnotator":
        """Add a text annotation."""
        self._annotations.append(Annotation(
            type="text",
            params={
                "x": x, "y": y,
                "text": text,
                "color": color,
                "font_size": font_size,
            },
        ))
        return self

    def add_highlight(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
        color: str = "yellow",
        alpha: float = 0.3,
    ) -> "ScreenshotAnnotator":
        """Add a highlight annotation."""
        self._annotations.append(Annotation(
            type="highlight",
            params={
                "x": x, "y": y,
                "width": width, "height": height,
                "color": color, "alpha": alpha,
            },
        ))
        return self

    def clear(self) -> None:
        """Clear all annotations."""
        self._annotations.clear()

    def get_annotations(self) -> list[Annotation]:
        """Get all annotations."""
        return list(self._annotations)
