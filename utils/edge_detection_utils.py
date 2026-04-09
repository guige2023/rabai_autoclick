"""Edge detection utilities for finding contours and boundaries.

This module provides utilities for detecting edges and finding contours
in images, useful for element boundary detection, shape identification,
and UI element localization in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, List, Tuple
import io


class EdgeDetector(Enum):
    """Edge detection algorithm to use."""
    SOBEL = auto()      # Sobel edge detection
    SCHARR = auto()     # Scharr edge detection
    LAPLACIAN = auto()  # Laplacian of Gaussian
    CANNY = auto()      # Canny edge detector


class ContourApproximation(Enum):
    """Contour approximation method."""
    NONE = auto()           # Store all points
    SIMPLE = auto()          # Simple compression
    TOKES = auto()           # Approximate to tokens


@dataclass
class EdgeDetectionConfig:
    """Configuration for edge detection."""
    detector: EdgeDetector = EdgeDetector.CANNY
    low_threshold: int = 50
    high_threshold: int = 150
    aperture_size: int = 3
    l2_gradient: bool = True


@dataclass
class Contour:
    """Represents a contour found in an image."""
    points: List[Tuple[int, int]]
    area: float
    perimeter: float
    bounding_box: Tuple[int, int, int, int]  # x, y, width, height
    
    @property
    def center(self) -> Tuple[int, int]:
        """Get center point of contour."""
        x, y, w, h = self.bounding_box
        return (x + w // 2, y + h // 2)
    
    @property
    def aspect_ratio(self) -> float:
        """Get aspect ratio of bounding box."""
        x, y, w, h = self.bounding_box
        return w / h if h > 0 else 0


def detect_edges(
    image_data: bytes,
    config: Optional[EdgeDetectionConfig] = None,
) -> bytes:
    """Detect edges in an image.
    
    Args:
        image_data: Raw image bytes.
        config: Edge detection configuration.
    
    Returns:
        Image bytes with edges detected.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        config = config or EdgeDetectionConfig()
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)
        
        if img is None:
            pil_img = Image.open(io.BytesIO(image_data)).convert("L")
            img = np.array(pil_img)
        
        if config.detector == EdgeDetector.CANNY:
            edges = cv2.Canny(
                img,
                config.low_threshold,
                config.high_threshold,
            )
        elif config.detector == EdgeDetector.SOBEL:
            if len(img.shape) > 2:
                img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            sobelx = cv2.Sobel(img, cv2.CV_64F, 1, 0, ksize=config.aperture_size)
            sobely = cv2.Sobel(img, cv2.CV_64F, 0, 1, ksize=config.aperture_size)
            magnitude = np.sqrt(sobelx**2 + sobely**2)
            edges = np.uint8(magnitude / magnitude.max() * 255)
        elif config.detector == EdgeDetector.SCHARR:
            if len(img.shape) > 2:
                img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            scharrx = cv2.Scharr(img, cv2.CV_64F, 1, 0)
            scharry = cv2.Scharr(img, cv2.CV_64F, 0, 1)
            magnitude = np.sqrt(scharrx**2 + scharry**2)
            edges = np.uint8(magnitude / magnitude.max() * 255)
        elif config.detector == EdgeDetector.LAPLACIAN:
            blurred = cv2.GaussianBlur(img, (3, 3), 0)
            laplacian = cv2.Laplacian(blurred, cv2.CV_64F, ksize=config.aperture_size)
            edges = np.uint8(np.absolute(laplacian) / laplacian.max() * 255)
        else:
            edges = cv2.Canny(img, config.low_threshold, config.high_threshold)
        
        _, encoded = cv2.imencode(".png", edges)
        return encoded.tobytes()
    except ImportError:
        raise ImportError("OpenCV is required for edge detection")


def find_contours(
    image_data: bytes,
    min_area: float = 10.0,
    max_area: float = float("inf"),
) -> List[Contour]:
    """Find contours in an image.
    
    Args:
        image_data: Raw image bytes or edge-detected bytes.
        min_area: Minimum contour area to keep.
        max_area: Maximum contour area to keep.
    
    Returns:
        List of Contour objects.
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
        
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE,
        )
        
        result = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < min_area or area > max_area:
                continue
            
            perimeter = cv2.arcLength(contour, True)
            x, y, w, h = cv2.boundingRect(contour)
            
            result.append(Contour(
                points=[tuple(p[0]) for p in contour],
                area=float(area),
                perimeter=float(perimeter),
                bounding_box=(int(x), int(y), int(w), int(h)),
            ))
        
        return result
    except ImportError:
        raise ImportError("OpenCV is required for contour finding")


def detect_corners(
    image_data: bytes,
    max_corners: int = 100,
    quality_level: float = 0.01,
    min_distance: float = 10.0,
) -> List[Tuple[int, int]]:
    """Detect corners using Shi-Tomasi corner detection.
    
    Args:
        image_data: Raw image bytes.
        max_corners: Maximum number of corners to detect.
        quality_level: Quality threshold for corners.
        min_distance: Minimum Euclidean distance between corners.
    
    Returns:
        List of corner (x, y) coordinates.
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
        
        corners = cv2.goodFeaturesToTrack(
            img,
            maxCorners=max_corners,
            qualityLevel=quality_level,
            minDistance=min_distance,
        )
        
        if corners is None:
            return []
        
        return [(int(x[0][0]), int(x[0][1])) for x in corners]
    except ImportError:
        raise ImportError("OpenCV is required for corner detection")


def detect_lines(
    image_data: bytes,
    rho: float = 1.0,
    theta: float = 1.0,
    threshold: int = 100,
    min_line_length: float = 50.0,
    max_line_gap: float = 10.0,
) -> List[Tuple[Tuple[int, int], Tuple[int, int]]]:
    """Detect lines using Hough transform.
    
    Args:
        image_data: Raw image bytes.
        rho: Distance resolution of accumulator.
        theta: Angle resolution of accumulator.
        threshold: Accumulator threshold.
        min_line_length: Minimum line length.
        max_line_gap: Maximum gap between line segments.
    
    Returns:
        List of line segments as ((x1, y1), (x2, y2)) tuples.
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
            rho=rho,
            theta=theta * 3.14159 / 180,
            threshold=threshold,
            minLineLength=min_line_length,
            maxLineGap=max_line_gap,
        )
        
        if lines is None:
            return []
        
        return [((int(l[0][0]), int(l[0][1])), (int(l[0][2]), int(l[0][3]))) for l in lines]
    except ImportError:
        raise ImportError("OpenCV is required for line detection")


def find_largest_contour(
    image_data: bytes,
) -> Optional[Contour]:
    """Find the largest contour by area.
    
    Args:
        image_data: Raw image bytes.
    
    Returns:
        The largest Contour or None if no contours found.
    """
    contours = find_contours(image_data)
    if not contours:
        return None
    return max(contours, key=lambda c: c.area)
