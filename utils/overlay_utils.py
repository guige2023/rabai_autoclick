"""Overlay management utilities for drawing on screen."""

from typing import Tuple, Optional, List, Callable
import numpy as np


class OverlayManager:
    """Manage overlay windows for drawing on screen."""

    def __init__(self):
        """Initialize overlay manager."""
        self._overlays: dict = {}
        self._enabled = True

    def create_overlay(
        self,
        name: str,
        x: int,
        y: int,
        width: int,
        height: int,
        alpha: float = 1.0,
        always_on_top: bool = True
    ) -> bool:
        """Create an overlay window.
        
        Args:
            name: Overlay identifier.
            x, y: Position.
            width, height: Dimensions.
            alpha: Transparency (0-1).
            always_on_top: Keep above other windows.
        
        Returns:
            True if created successfully.
        """
        try:
            import tkinter as tk
            root = tk.Tk()
            root.overrideredirect(True)
            root.geometry(f"{width}x{height}+{x}+{y}")
            root.attributes("-alpha", alpha)
            if always_on_top:
                root.attributes("-topmost", True)
            canvas = tk.Canvas(root, width=width, height=height, bg="", highlightthickness=0)
            canvas.pack()
            self._overlays[name] = {"root": root, "canvas": canvas, "width": width, "height": height}
            return True
        except Exception:
            return False

    def draw_rectangle(
        self,
        name: str,
        x1: int, y1: int,
        x2: int, y2: int,
        color: str = "red",
        width: int = 2
    ) -> None:
        """Draw rectangle on overlay.
        
        Args:
            name: Overlay name.
            x1, y1: Top-left.
            x2, y2: Bottom-right.
            color: Stroke color.
            width: Line width.
        """
        if name not in self._overlays:
            return
        canvas = self._overlays[name]["canvas"]
        canvas.create_rectangle(x1, y1, x2, y2, outline=color, width=width, tags="shape")

    def draw_circle(
        self,
        name: str,
        x: int, y: int,
        radius: int,
        color: str = "red",
        fill: Optional[str] = None,
        width: int = 2
    ) -> None:
        """Draw circle on overlay."""
        if name not in self._overlays:
            return
        canvas = self._overlays[name]["canvas"]
        canvas.create_oval(x - radius, y - radius, x + radius, y + radius,
                         outline=color, fill=fill, width=width, tags="shape")

    def draw_line(
        self,
        name: str,
        x1: int, y1: int,
        x2: int, y2: int,
        color: str = "red",
        width: int = 2
    ) -> None:
        """Draw line on overlay."""
        if name not in self._overlays:
            return
        canvas = self._overlays[name]["canvas"]
        canvas.create_line(x1, y1, x2, y2, fill=color, width=width, tags="shape")

    def draw_text(
        self,
        name: str,
        x: int, y: int,
        text: str,
        color: str = "white",
        font_size: int = 12
    ) -> None:
        """Draw text on overlay."""
        if name not in self._overlays:
            return
        canvas = self._overlays[name]["canvas"]
        canvas.create_text(x, y, text=text, fill=color, font=("Arial", font_size), tags="text")

    def clear(self, name: str) -> None:
        """Clear all drawings from overlay."""
        if name not in self._overlays:
            return
        canvas = self._overlays[name]["canvas"]
        canvas.delete("shape")
        canvas.delete("text")

    def show(self, name: str) -> None:
        """Show overlay window."""
        if name in self._overlays:
            self._overlays[name]["root"].deiconify()

    def hide(self, name: str) -> None:
        """Hide overlay window."""
        if name in self._overlays:
            self._overlays[name]["root"].withdraw()

    def destroy(self, name: str) -> None:
        """Destroy overlay window."""
        if name in self._overlays:
            try:
                self._overlays[name]["root"].destroy()
            except Exception:
                pass
            del self._overlays[name]

    def destroy_all(self) -> None:
        """Destroy all overlays."""
        for name in list(self._overlays.keys()):
            self.destroy(name)


def draw_highlight_box(
    image: np.ndarray,
    x: int, y: int,
    width: int, height: int,
    color: Tuple[int, int, int] = (0, 255, 0),
    thickness: int = 2
) -> np.ndarray:
    """Draw a highlight box on an image.
    
    Args:
        image: Input image.
        x, y: Top-left corner.
        width, height: Box dimensions.
        color: BGR color.
        thickness: Line thickness.
    
    Returns:
        Image with box drawn.
    """
    import cv2
    result = image.copy()
    cv2.rectangle(result, (x, y), (x + width, y + height), color, thickness)
    return result


def draw_crosshair(
    image: np.ndarray,
    x: int, y: int,
    size: int = 10,
    color: Tuple[int, int, int] = (0, 255, 0),
    thickness: int = 2
) -> np.ndarray:
    """Draw crosshair at point.
    
    Args:
        image: Input image.
        x, y: Center point.
        size: Crosshair size.
        color: BGR color.
        thickness: Line thickness.
    
    Returns:
        Image with crosshair drawn.
    """
    import cv2
    result = image.copy()
    cv2.line(result, (x - size, y), (x + size, y), color, thickness)
    cv2.line(result, (x, y - size), (x, y + size), color, thickness)
    return result
