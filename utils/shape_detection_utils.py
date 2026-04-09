"""Shape detection utilities for identifying geometric shapes in images.

This module provides utilities for detecting and identifying geometric shapes
like rectangles, circles, triangles, and lines, useful for UI element
identification and verification in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class ShapeType(Enum):
    """Type of detected shape."""
    UNKNOWN = auto()
    RECTANGLE = auto()
    SQUARE = auto()
    CIRCLE = auto()
    ELLIPSE = auto()
    TRIANGLE = auto()
    LINE = auto()
    POLYGON = auto()


@dataclass
class DetectedShape:
    """A shape detected in an image."""
    shape_type: ShapeType
    bounding_box: Tuple[int, int, int, int]  # x, y, width, height
    center: Tuple[int, int]
    area: float
    perimeter: float
    confidence: float
    points: List[Tuple[int, int]]
    
    @property
    def aspect_ratio(self) -> float:
        """Get aspect ratio of bounding box."""
        x, y, w, h = self.bounding_box
        return w / h if h > 0 else 0


@dataclass
class ShapeDetectionConfig:
    """Configuration for shape detection."""
    min_area: float = 100.0
    max_area: float = float("inf")
    min_aspect_ratio: float = 0.1
    max_aspect_ratio: float = 10.0
    circularity_threshold: float = 0.7
    rectangularity_threshold: float = 0.8


def detect_shapes(
    image_data: bytes,
    config: Optional[ShapeDetectionConfig] = None,
) -> List[DetectedShape]:
    """Detect shapes in an image.
    
    Args:
        image_data: Raw image bytes.
        config: Shape detection configuration.
    
    Returns:
        List of detected shapes.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        config = config or ShapeDetectionConfig()
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        edges = cv2.Canny(img, 50, 150)
        
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE,
        )
        
        shapes = []
        for contour in contours:
            area = cv2.contourArea(contour)
            
            if area < config.min_area or area > config.max_area:
                continue
            
            x, y, w, h = cv2.boundingRect(contour)
            aspect_ratio = w / h if h > 0 else 0
            
            if aspect_ratio < config.min_aspect_ratio or aspect_ratio > config.max_aspect_ratio:
                continue
            
            perimeter = cv2.arcLength(contour, True)
            
            M = cv2.moments(contour)
            if M["m00"] != 0:
                cx = int(M["m10"] / M["m00"])
                cy = int(M["m01"] / M["m00"])
            else:
                cx, cy = x + w // 2, y + h // 2
            
            shape_type = _identify_shape(
                contour, area, perimeter, w, h, config
            )
            
            if shape_type != ShapeType.UNKNOWN:
                shapes.append(DetectedShape(
                    shape_type=shape_type,
                    bounding_box=(x, y, w, h),
                    center=(cx, cy),
                    area=float(area),
                    perimeter=float(perimeter),
                    confidence=_calculate_confidence(
                        contour, shape_type, area, w * h, config
                    ),
                    points=[tuple(p[0]) for p in contour],
                ))
        
        return shapes
    except ImportError:
        raise ImportError("OpenCV is required for shape detection")


def _identify_shape(contour, area, perimeter, width, height, config) -> ShapeType:
    """Identify the type of shape from contour."""
    import cv2
    
    circularity = 4 * 3.14159 * area / (perimeter * perimeter) if perimeter > 0 else 0
    
    hull = cv2.convexHull(contour)
    hull_area = cv2.contourArea(hull)
    rectangularity = area / hull_area if hull_area > 0 else 0
    
    if circularity > config.circularity_threshold:
        aspect_tolerance = 0.2
        if abs(width - height) / max(width, height) < aspect_tolerance:
            return ShapeType.CIRCLE
        else:
            return ShapeType.ELLIPSE
    
    if rectangularity > config.rectangularity_threshold:
        aspect_tolerance = 0.1
        if abs(width - height) / max(width, height) < aspect_tolerance:
            return ShapeType.SQUARE
        else:
            return ShapeType.RECTANGLE
    
    approx = cv2.approxPolyDP(contour, 0.04 * perimeter, True)
    
    if len(approx) == 3:
        return ShapeType.TRIANGLE
    elif len(approx) == 4:
        return ShapeType.RECTANGLE
    
    return ShapeType.POLYGON


def _calculate_confidence(contour, shape_type, area, bbox_area, config) -> float:
    """Calculate confidence score for shape detection."""
    import cv2
    
    perimeter = cv2.arcLength(contour, True)
    
    if shape_type in (ShapeType.CIRCLE, ShapeType.ELLIPSE):
        circularity = 4 * 3.14159 * area / (perimeter * perimeter) if perimeter > 0 else 0
        return circularity
    
    if shape_type in (ShapeType.RECTANGLE, ShapeType.SQUARE):
        hull = cv2.convexHull(contour)
        hull_area = cv2.contourArea(hull)
        rectangularity = area / hull_area if hull_area > 0 else 0
        return rectangularity
    
    return 0.5


def detect_circles(image_data: bytes) -> List[DetectedShape]:
    """Detect circles in an image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        List of detected circles.
    """
    shapes = detect_shapes(image_data)
    return [s for s in shapes if s.shape_type in (ShapeType.CIRCLE, ShapeType.ELLIPSE)]


def detect_rectangles(image_data: bytes) -> List[DetectedShape]:
    """Detect rectangles in an image.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        List of detected rectangles.
    """
    shapes = detect_shapes(image_data)
    return [s for s in shapes if s.shape_type in (ShapeType.RECTANGLE, ShapeType.SQUARE)]


def detect_lines_hough(
    image_data: bytes,
    rho: float = 1.0,
    theta: float = 1.0,
    threshold: int = 100,
) -> List[Tuple[Tuple[int, int], Tuple[int, int]]]:
    """Detect lines using Hough transform.
    
    Args:
        image_data: Raw image bytes.
        rho: Distance resolution.
        theta: Angle resolution.
        threshold: Accumulator threshold.
    
    Returns:
        List of lines as ((x1, y1), (x2, y2)) tuples.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        edges = cv2.Canny(img, 50, 150)
        
        lines = cv2.HoughLinesP(
            edges,
            rho, theta * 3.14159 / 180,
            threshold,
            minLineLength=50,
            maxLineGap=10,
        )
        
        if lines is None:
            return []
        
        return [((int(l[0][0]), int(l[0][1])), (int(l[0][2]), int(l[0][3]))) for l in lines]
    except ImportError:
        raise ImportError("OpenCV is required for line detection")


def find_largest_shape(
    image_data: bytes,
    shape_types: Optional[List[ShapeType]] = None,
) -> Optional[DetectedShape]:
    """Find the largest shape of specified types.
    
    Args:
        image_data: Raw image bytes.
        shape_types: Filter to these shape types (None = all types).
    
    Returns:
        Largest DetectedShape or None.
    """
    shapes = detect_shapes(image_data)
    
    if shape_types:
        shapes = [s for s in shapes if s.shape_type in shape_types]
    
    if not shapes:
        return None
    
    return max(shapes, key=lambda s: s.area)
