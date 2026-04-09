"""
Debug image utilities for visual debugging of automation workflows.

Provides functions for annotating images with bounding boxes, text,
paths, heatmaps, and other visual markers useful for debugging
UI detection and interaction pipelines.

Example:
    >>> from debug_image_utils import draw_bbox, draw_path, annotate_text, save_debug
    >>> debug_img = draw_bbox(image, (x, y, w, h), label="button")
    >>> save_debug(debug_img, "detection_result.png")
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, Tuple, Union

import numpy as np


# =============================================================================
# Types
# =============================================================================


class DrawMode(Enum):
    """Drawing modes for shapes."""
    FILLED = "filled"
    OUTLINE = "outline"
    BOTH = "both"


@dataclass
class BBox:
    """Bounding box with label and metadata."""
    x: int
    y: int
    width: int
    height: int
    label: str = ""
    color: Tuple[int, int, int] = (0, 255, 0)
    thickness: int = 2
    confidence: Optional[float] = None


@dataclass
class Point:
    """2D point."""
    x: float
    y: float


@dataclass
class DebugConfig:
    """Configuration for debug visualization."""
    font_scale: float = 0.5
    font_thickness: int = 1
    point_radius: int = 3
    line_thickness: int = 2
    alpha: float = 0.7
    show_confidence: bool = True
    show_timestamp: bool = True
    bbox_style: str = "box"


# =============================================================================
# Bounding Box Drawing
# =============================================================================


def draw_bbox(
    image: np.ndarray,
    bbox: Union[Tuple[int, int, int, int], BBox],
    label: str = "",
    color: Tuple[int, int, int] = (0, 255, 0),
    thickness: int = 2,
    confidence: Optional[float] = None,
) -> np.ndarray:
    """
    Draw a bounding box on an image.

    Args:
        image: Input image (H x W x 3).
        bbox: Bounding box as (x, y, w, h) or BBox object.
        label: Optional text label.
        color: RGB color tuple.
        thickness: Line thickness.
        confidence: Optional confidence score to display.

    Returns:
        Image with drawn bounding box.
    """
    if len(image.shape) != 3:
        raise ValueError("Image must be HxWx3")

    result = image.copy()

    if isinstance(bbox, BBox):
        x, y, w, h = bbox.x, bbox.y, bbox.width, bbox.height
        color = bbox.color
        thickness = bbox.thickness
        if not label and bbox.label:
            label = bbox.label
        if confidence is None and bbox.confidence:
            confidence = bbox.confidence
    else:
        x, y, w, h = bbox

    x2, y2 = x + w, y + h

    # Draw rectangle
    result = _draw_line(result, (x, y), (x2, y), color, thickness)
    result = _draw_line(result, (x2, y), (x2, y2), color, thickness)
    result = _draw_line(result, (x2, y2), (x, y2), color, thickness)
    result = _draw_line(result, (x, y2), (x, y), color, thickness)

    # Draw label
    if label or confidence is not None:
        label_text = label
        if confidence is not None and label:
            label_text = f"{label} ({confidence:.2f})"
        elif confidence is not None:
            label_text = f"{confidence:.2f}"

        result = draw_text(
            result, label_text, (x, y - 5),
            color=color, font_scale=0.4, thickness=1
        )

    return result


def draw_bboxes(
    image: np.ndarray,
    bboxes: List[Union[Tuple[int, int, int, int], BBox]],
    config: Optional[DebugConfig] = None,
) -> np.ndarray:
    """
    Draw multiple bounding boxes on an image.

    Args:
        image: Input image.
        bboxes: List of bounding boxes.
        config: Debug configuration.

    Returns:
        Image with all bounding boxes drawn.
    """
    if config is None:
        config = DebugConfig()

    result = image.copy()
    for bbox in bboxes:
        if isinstance(bbox, BBox):
            result = draw_bbox(
                result, bbox,
                color=bbox.color,
                thickness=bbox.thickness,
                confidence=bbox.confidence,
            )
        else:
            result = draw_bbox(result, bbox)

    return result


def draw_text(
    image: np.ndarray,
    text: str,
    position: Tuple[int, int],
    color: Tuple[int, int, int] = (255, 255, 255),
    font_scale: float = 0.5,
    thickness: int = 1,
    background: Optional[Tuple[int, int, int]] = None,
) -> np.ndarray:
    """
    Draw text on an image.

    Args:
        image: Input image.
        text: Text to draw.
        position: Top-left position (x, y).
        color: Text color.
        font_scale: Font scale factor.
        thickness: Text thickness.
        background: Optional background color.

    Returns:
        Image with text drawn.
    """
    result = image.copy()
    x, y = position

    # Simple font approximation (no external dependencies)
    char_width = int(6 * font_scale)
    char_height = int(10 * font_scale)

    # Background
    if background:
        bg_h = char_height + 2
        bg_w = len(text) * char_width + 2
        result = _fill_rect(
            result,
            (x, y - char_height),
            (x + bg_w, y + 2),
            background
        )

    # Draw each character (simplified)
    for i, char in enumerate(text):
        char_x = x + i * char_width
        _draw_char(result, char, (char_x, y), color, font_scale, thickness)

    return result


def _draw_char(
    image: np.ndarray,
    char: str,
    pos: Tuple[int, int],
    color: Tuple[int, int, int],
    scale: float,
    thickness: int,
) -> None:
    """Draw a single character (simplified bitmap font)."""
    x, y = pos
    # Very simplified - just draw a small rectangle for visibility
    size = max(1, int(3 * scale))
    for dx in range(size):
        for dy in range(size):
            nx, ny = x + dx, y - size + dy
            if 0 <= ny < image.shape[0] and 0 <= nx < image.shape[1]:
                image[ny, nx] = color


# =============================================================================
# Path and Trajectory Drawing
# =============================================================================


def draw_path(
    image: np.ndarray,
    points: List[Tuple[float, float]],
    color: Tuple[int, int, int] = (255, 0, 0),
    thickness: int = 2,
    draw_points: bool = True,
    point_radius: int = 3,
) -> np.ndarray:
    """
    Draw a path connecting a series of points.

    Args:
        image: Input image.
        points: List of (x, y) coordinates.
        color: Line color.
        thickness: Line thickness.
        draw_points: Whether to draw point markers.
        point_radius: Radius for point markers.

    Returns:
        Image with path drawn.
    """
    result = image.copy()

    if len(points) < 2:
        if draw_points and points:
            result = draw_points_as_circles(result, [points[0]], color, point_radius)
        return result

    # Draw lines
    for i in range(len(points) - 1):
        p1 = points[i]
        p2 = points[i + 1]
        result = _draw_line(result, p1, p2, color, thickness)

    # Draw points
    if draw_points:
        result = draw_points_as_circles(result, points, color, point_radius)

    return result


def draw_points_as_circles(
    image: np.ndarray,
    points: List[Tuple[float, float]],
    color: Tuple[int, int, int] = (0, 255, 0),
    radius: int = 3,
) -> np.ndarray:
    """
    Draw points as circles.

    Args:
        image: Input image.
        points: List of (x, y) coordinates.
        color: Circle color.
        radius: Circle radius.

    Returns:
        Image with circles drawn.
    """
    result = image.copy()
    h, w = image.shape[:2]

    for x, y in points:
        xi, yi = int(round(x)), int(round(y))
        for dx in range(-radius, radius + 1):
            for dy in range(-radius, radius + 1):
                if dx * dx + dy * dy <= radius * radius:
                    nx, ny = xi + dx, yi + dy
                    if 0 <= ny < h and 0 <= nx < w:
                        result[ny, nx] = color

    return result


def _draw_line(
    image: np.ndarray,
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    color: Tuple[int, int, int],
    thickness: int,
) -> np.ndarray:
    """Draw a line using Bresenham's algorithm."""
    result = image.copy()
    x1, y1 = int(round(p1[0])), int(round(p1[1]))
    x2, y2 = int(round(p2[0])), int(round(p2[1]))

    dx = abs(x2 - x1)
    dy = abs(y2 - y1)
    sx = 1 if x1 < x2 else -1
    sy = 1 if y1 < y2 else -1
    err = dx - dy

    h, w = image.shape[:2]

    while True:
        if 0 <= y1 < h and 0 <= x1 < w:
            result[y1, x1] = color

        if x1 == x2 and y1 == y2:
            break

        e2 = 2 * err
        if e2 > -dy:
            err -= dy
            x1 += sx
        if e2 < dx:
            err += dx
            y1 += sy

    return result


def _fill_rect(
    image: np.ndarray,
    p1: Tuple[int, int],
    p2: Tuple[int, int],
    color: Tuple[int, int, int],
) -> np.ndarray:
    """Fill a rectangle."""
    result = image.copy()
    x1, y1 = min(p1[0], p2[0]), min(p1[1], p2[1])
    x2, y2 = max(p1[0], p2[0]), max(p1[1], p2[1])

    h, w = image.shape[:2]
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(w, x2), min(h, y2)

    for y in range(y1, y2):
        for x in range(x1, x2):
            result[y, x] = color

    return result


# =============================================================================
# Heatmap Visualization
# =============================================================================


def create_heatmap(
    data: np.ndarray,
    colormap: str = "jet",
    min_val: Optional[float] = None,
    max_val: Optional[float] = None,
) -> np.ndarray:
    """
    Create a heatmap from a 2D array.

    Args:
        data: 2D array of values.
        colormap: Colormap name ('jet', 'hot', 'cool', 'gray').
        min_val: Minimum value (auto if None).
        max_val: Maximum value (auto if None).

    Returns:
        Heatmap image (H x W x 3).
    """
    if min_val is None:
        min_val = data.min()
    if max_val is None:
        max_val = data.max()

    if max_val == min_val:
        normalized = np.zeros_like(data)
    else:
        normalized = (data - min_val) / (max_val - min_val)
        normalized = np.clip(normalized, 0, 1)

    if colormap == "jet":
        heatmap = _jet_colormap(normalized)
    elif colormap == "hot":
        heatmap = _hot_colormap(normalized)
    elif colormap == "cool":
        heatmap = _cool_colormap(normalized)
    else:
        gray = (normalized * 255).astype(np.uint8)
        heatmap = np.stack([gray, gray, gray], axis=-1)
        return heatmap

    return heatmap


def _jet_colormap(t: np.ndarray) -> np.ndarray:
    """Jet colormap: blue -> cyan -> yellow -> red."""
    h, w = t.shape
    result = np.zeros((h, w, 3), dtype=np.uint8)

    r = np.clip(1.5 - np.abs(4 * t - 3), 0, 1)
    g = np.clip(1.5 - np.abs(4 * t - 2), 0, 1)
    b = np.clip(1.5 - np.abs(4 * t - 1), 0, 1)

    result[:, :, 0] = (r * 255).astype(np.uint8)
    result[:, :, 1] = (g * 255).astype(np.uint8)
    result[:, :, 2] = (b * 255).astype(np.uint8)

    return result


def _hot_colormap(t: np.ndarray) -> np.ndarray:
    """Hot colormap: black -> red -> yellow -> white."""
    h, w = t.shape
    result = np.zeros((h, w, 3), dtype=np.uint8)

    result[:, :, 0] = (np.clip(t * 3, 0, 1) * 255).astype(np.uint8)
    result[:, :, 1] = (np.clip(t * 3 - 1, 0, 1) * 255).astype(np.uint8)
    result[:, :, 2] = (np.clip(t * 3 - 2, 0, 1) * 255).astype(np.uint8)

    return result


def _cool_colormap(t: np.ndarray) -> np.ndarray:
    """Cool colormap: cyan -> magenta."""
    h, w = t.shape
    result = np.zeros((h, w, 3), dtype=np.uint8)

    result[:, :, 0] = (t * 255).astype(np.uint8)
    result[:, :, 1] = ((1 - t) * 255).astype(np.uint8)
    result[:, :, 2] = (255 - t * 127).astype(np.uint8)

    return result


def overlay_heatmap(
    image: np.ndarray,
    heatmap: np.ndarray,
    alpha: float = 0.5,
) -> np.ndarray:
    """
    Overlay a heatmap on an image.

    Args:
        image: Base image.
        heatmap: Heatmap to overlay.
        alpha: Blend factor.

    Returns:
        Blended image.
    """
    if len(image.shape) == 2:
        image = np.stack([image, image, image], axis=-1)

    if heatmap.shape[:2] != image.shape[:2]:
        heatmap = _resize_image(heatmap, image.shape[:2])

    result = (image.astype(float) * (1 - alpha) + heatmap.astype(float) * alpha)
    return np.clip(result, 0, 255).astype(np.uint8)


def _resize_image(image: np.ndarray, target_size: Tuple[int, int]) -> np.ndarray:
    """Simple nearest-neighbor resize."""
    target_h, target_w = target_size
    h, w = image.shape[:2]

    if len(image.shape) == 3:
        result = np.zeros((target_h, target_w, image.shape[2]), dtype=image.dtype)
        for i in range(target_h):
            for j in range(target_w):
                src_i = min(int(i * h / target_h), h - 1)
                src_j = min(int(j * w / target_w), w - 1)
                result[i, j] = image[src_i, src_j]
    else:
        result = np.zeros((target_h, target_w), dtype=image.dtype)
        for i in range(target_h):
            for j in range(target_w):
                src_i = min(int(i * h / target_h), h - 1)
                src_j = min(int(j * w / target_w), w - 1)
                result[i, j] = image[src_i, src_j]

    return result


# =============================================================================
# Grid and Overlay
# =============================================================================


def draw_grid(
    image: np.ndarray,
    grid_size: Tuple[int, int] = (50, 50),
    color: Tuple[int, int, int] = (128, 128, 128),
    thickness: int = 1,
) -> np.ndarray:
    """
    Draw a grid overlay on an image.

    Args:
        image: Input image.
        grid_size: Size of grid cells (height, width).
        color: Grid line color.
        thickness: Line thickness.

    Returns:
        Image with grid overlay.
    """
    result = image.copy()
    h, w = image.shape[:2]
    gh, gw = grid_size

    for y in range(0, h, gh):
        result = _draw_line(result, (0, y), (w, y), color, thickness)

    for x in range(0, w, gw):
        result = _draw_line(result, (x, 0), (x, h), color, thickness)

    return result


def draw_crosshair(
    image: np.ndarray,
    position: Tuple[int, int],
    size: int = 10,
    color: Tuple[int, int, int] = (255, 0, 0),
    thickness: int = 1,
) -> np.ndarray:
    """
    Draw a crosshair at a position.

    Args:
        image: Input image.
        position: Center position (x, y).
        size: Size of crosshair arms.
        color: Color.
        thickness: Line thickness.

    Returns:
        Image with crosshair.
    """
    result = image.copy()
    x, y = position

    result = _draw_line(result, (x - size, y), (x + size, y), color, thickness)
    result = _draw_line(result, (x, y - size), (x, y + size), color, thickness)

    return result


# =============================================================================
# Save and Export
# =============================================================================


def save_debug(
    image: np.ndarray,
    path: str,
    metadata: Optional[dict] = None,
) -> None:
    """
    Save a debug image with optional metadata.

    Args:
        image: Image to save.
        path: Output path.
        metadata: Optional metadata dictionary.
    """
    import json
    import os

    # Ensure directory exists
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    # Save image
    if path.endswith(".png"):
        _save_png(image, path)
    elif path.endswith(".jpg") or path.endswith(".jpeg"):
        _save_jpg(image, path)
    else:
        _save_png(image, path)

    # Save metadata if provided
    if metadata:
        meta_path = path.rsplit(".", 1)[0] + "_meta.json"
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)


def _save_png(image: np.ndarray, path: str) -> None:
    """Save as PNG (simplified - just writes raw bytes)."""
    import struct

    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("PNG save requires HxWx3 image")

    h, w = image.shape[:2]

    # Simple PPM format (compatible, can be renamed)
    ppm_path = path.replace(".png", ".ppm")
    with open(ppm_path, "wb") as f:
        f.write(f"P6\n{w} {h}\n255\n".encode())
        f.write(image.tobytes())


def _save_jpg(image: np.ndarray, path: str) -> None:
    """Save as JPG (simplified - uses PPM fallback)."""
    import struct

    if len(image.shape) != 3 or image.shape[2] != 3:
        raise ValueError("JPG save requires HxWx3 image")

    h, w = image.shape[:2]

    # Simple PPM format (compatible, can be renamed)
    ppm_path = path.replace(".jpg", ".ppm").replace(".jpeg", ".ppm")
    with open(ppm_path, "wb") as f:
        f.write(f"P6\n{w} {h}\n255\n".encode())
        f.write(image.tobytes())
