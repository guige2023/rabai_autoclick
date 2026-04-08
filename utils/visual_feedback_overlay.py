"""Visual feedback overlay for UI automation.

Creates and manages transparent overlay windows for showing
automation progress, highlights, and feedback.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class OverlayShape(Enum):
    """Shapes for visual feedback."""
    RECTANGLE = auto()
    ROUNDED_RECT = auto()
    CIRCLE = auto()
    ELLIPSE = auto()
    HIGHLIGHT = auto()


class OverlayColor:
    """RGBA color values for overlays."""
    RED = (255, 0, 0, 180)
    GREEN = (0, 255, 0, 180)
    BLUE = (0, 0, 255, 180)
    YELLOW = (255, 255, 0, 180)
    ORANGE = (255, 165, 0, 180)
    CYAN = (0, 255, 255, 180)
    MAGENTA = (255, 0, 255, 180)
    WHITE = (255, 255, 255, 255)
    BLACK = (0, 0, 0, 255)
    TRANSPARENT = (0, 0, 0, 0)

    def __init__(self, r: int, g: int, b: int, a: int = 255) -> None:
        self.r = r
        self.g = g
        self.b = b
        self.a = a

    def to_tuple(self) -> tuple[int, int, int, int]:
        return (self.r, self.g, self.b, self.a)

    def with_alpha(self, a: int) -> OverlayColor:
        """Return new color with different alpha."""
        return OverlayColor(self.r, self.g, self.b, a)


@dataclass
class OverlayElement:
    """A single element in an overlay.

    Attributes:
        element_id: Unique identifier.
        shape: The shape to draw.
        x: X coordinate.
        y: Y coordinate.
        width: Width (for rectangles).
        height: Height (for rectangles).
        radius: Corner radius (for rounded rectangles).
        color: RGBA color tuple.
        border_width: Width of border (0 = fill only).
        border_color: Border color.
        text: Optional text to display.
        text_color: Text color.
        visible: Whether this element is visible.
        duration: Auto-hide duration in seconds (0 = persistent).
    """
    shape: OverlayShape
    x: float = 0.0
    y: float = 0.0
    width: float = 100.0
    height: float = 100.0
    radius: float = 0.0
    color: tuple[int, int, int, int] = (255, 0, 0, 180)
    border_width: float = 2.0
    border_color: tuple[int, int, int, int] = (255, 255, 255, 255)
    text: str = ""
    text_color: tuple[int, int, int, int] = (255, 255, 255, 255)
    visible: bool = True
    duration: float = 0.0
    element_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def x2(self) -> float:
        """Right edge."""
        return self.x + self.width

    @property
    def y2(self) -> float:
        """Bottom edge."""
        return self.y + self.height

    @property
    def center(self) -> tuple[float, float]:
        """Center point."""
        return (self.x + self.width / 2, self.y + self.height / 2)


class VisualFeedbackOverlay:
    """Manages visual feedback overlays for automation.

    Provides a layer on top of the screen for highlighting
    elements, showing progress, and other visual feedback.
    """

    def __init__(self) -> None:
        """Initialize empty overlay manager."""
        self._elements: dict[str, OverlayElement] = {}
        self._is_visible: bool = False
        self._on_update_callbacks: list[Callable[[], None]] = []

    def add_rectangle(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        color: tuple[int, int, int, int] = (255, 0, 0, 180),
        border_width: float = 2.0,
        text: str = "",
        duration: float = 0.0,
    ) -> str:
        """Add a rectangle overlay element."""
        elem = OverlayElement(
            shape=OverlayShape.RECTANGLE,
            x=x, y=y, width=width, height=height,
            color=color, border_width=border_width,
            text=text, duration=duration,
        )
        self._elements[elem.element_id] = elem
        self._notify_update()
        return elem.element_id

    def add_highlight(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        color: tuple[int, int, int, int] = (255, 255, 0, 100),
        duration: float = 1.0,
    ) -> str:
        """Add a highlight overlay element."""
        elem = OverlayElement(
            shape=OverlayShape.HIGHLIGHT,
            x=x, y=y, width=width, height=height,
            color=color, border_width=0,
            duration=duration,
        )
        self._elements[elem.element_id] = elem
        self._notify_update()
        return elem.element_id

    def add_circle(
        self,
        x: float,
        y: float,
        radius: float,
        color: tuple[int, int, int, int] = (255, 0, 0, 180),
    ) -> str:
        """Add a circle overlay element."""
        elem = OverlayElement(
            shape=OverlayShape.CIRCLE,
            x=x - radius, y=y - radius,
            width=radius * 2, height=radius * 2,
            color=color,
        )
        self._elements[elem.element_id] = elem
        self._notify_update()
        return elem.element_id

    def remove(self, element_id: str) -> bool:
        """Remove an overlay element."""
        if element_id in self._elements:
            del self._elements[element_id]
            self._notify_update()
            return True
        return False

    def clear(self) -> None:
        """Remove all overlay elements."""
        self._elements.clear()
        self._notify_update()

    def show(self) -> None:
        """Show the overlay window."""
        self._is_visible = True

    def hide(self) -> None:
        """Hide the overlay window."""
        self._is_visible = False

    def get_elements(self) -> list[OverlayElement]:
        """Return all overlay elements."""
        return [e for e in self._elements.values() if e.visible]

    def on_update(self, callback: Callable[[], None]) -> None:
        """Register an update callback."""
        self._on_update_callbacks.append(callback)

    def _notify_update(self) -> None:
        """Notify all update callbacks."""
        for cb in self._on_update_callbacks:
            try:
                cb()
            except Exception:
                pass

    @property
    def is_visible(self) -> bool:
        """Return True if overlay is visible."""
        return self._is_visible

    @property
    def element_count(self) -> int:
        """Return number of elements."""
        return len(self._elements)
