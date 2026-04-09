"""Icon detection utilities for finding UI icons in screenshots.

This module provides utilities for detecting and recognizing icons in
screenshots, useful for UI element identification and verification
in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple
import io


@dataclass
class IconMatch:
    """Result of an icon match."""
    x: int
    y: int
    width: int
    height: int
    confidence: float
    template_name: str
    
    @property
    def center(self) -> Tuple[int, int]:
        """Get center point of match."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def bounding_box(self) -> Tuple[int, int, int, int]:
        """Get bounding box (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)


@dataclass
class IconDetectionConfig:
    """Configuration for icon detection."""
    confidence_threshold: float = 0.8
    scale_range: Tuple[float, float] = (0.5, 2.0)
    scale_steps: int = 10
    max_matches: int = 10


def find_icon(
    image_data: bytes,
    icon_template: bytes,
    config: Optional[IconDetectionConfig] = None,
) -> List[IconMatch]:
    """Find icon template in image using template matching.
    
    Args:
        image_data: Screenshot or target image bytes.
        icon_template: Icon template image bytes.
        config: Detection configuration.
    
    Returns:
        List of IconMatch objects.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        config = config or IconDetectionConfig()
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            img = np.array(Image.open(io.BytesIO(image_data)).convert("RGB"))
            img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        
        template_arr = np.frombuffer(icon_template, np.uint8)
        template = cv2.imdecode(template_arr, cv2.IMREAD_COLOR)
        
        if template is None:
            template = np.array(Image.open(io.BytesIO(icon_template)).convert("RGB"))
            template = cv2.cvtColor(template, cv2.COLOR_RGB2BGR)
        
        template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        img_gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        w, h = template_gray.shape[::-1]
        
        scale_min, scale_max = config.scale_range
        scales = np.linspace(scale_min, scale_max, config.scale_steps)
        
        matches = []
        
        for scale in scales:
            scaled_w = int(w * scale)
            scaled_h = int(h * scale)
            
            if scaled_w > img.shape[1] or scaled_h > img.shape[0]:
                continue
            
            scaled_template = cv2.resize(template_gray, (scaled_w, scaled_h))
            
            result = cv2.matchTemplate(
                img_gray,
                scaled_template,
                cv2.TM_CCOEFF_NORMED,
            )
            
            locations = np.where(result >= config.confidence_threshold)
            
            for pt in zip(*locations[::-1]):
                matches.append(IconMatch(
                    x=int(pt[0]),
                    y=int(pt[1]),
                    width=scaled_w,
                    height=scaled_h,
                    confidence=float(result[pt[1], pt[0]]),
                    template_name="icon_template",
                ))
        
        matches = _filter_overlapping_matches(matches, config.max_matches)
        
        return matches
    except ImportError:
        raise ImportError("OpenCV is required for icon detection")


def find_icons_batch(
    image_data: bytes,
    icon_templates: dict[str, bytes],
    config: Optional[IconDetectionConfig] = None,
) -> dict[str, List[IconMatch]]:
    """Find multiple icon templates in an image.
    
    Args:
        image_data: Screenshot or target image bytes.
        icon_templates: Dictionary mapping template name to template bytes.
        config: Detection configuration.
    
    Returns:
        Dictionary mapping template name to list of IconMatch objects.
    """
    results = {}
    
    for name, template in icon_templates.items():
        matches = find_icon(image_data, template, config)
        if matches:
            results[name] = matches
    
    return results


def detect_buttons(
    image_data: bytes,
    min_width: int = 30,
    min_height: int = 20,
) -> List[Tuple[int, int, int, int]]:
    """Detect button-like regions in an image.
    
    Args:
        image_data: Raw image bytes.
        min_width: Minimum button width.
        min_height: Minimum button height.
    
    Returns:
        List of button bounding boxes (x, y, width, height).
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            img = np.array(Image.open(io.BytesIO(image_data)).convert("RGB"))
            img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        
        edges = cv2.Canny(blurred, 50, 150)
        
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE,
        )
        
        buttons = []
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            
            if w >= min_width and h >= min_height:
                aspect_ratio = w / h if h > 0 else 0
                if 0.5 < aspect_ratio < 5.0:
                    buttons.append((x, y, w, h))
        
        return buttons
    except ImportError:
        raise ImportError("OpenCV is required for button detection")


def _filter_overlapping_matches(
    matches: List[IconMatch],
    max_matches: int,
) -> List[IconMatch]:
    """Filter overlapping matches, keeping highest confidence."""
    if not matches:
        return []
    
    matches.sort(key=lambda m: m.confidence, reverse=True)
    
    filtered = []
    for match in matches:
        is_overlap = False
        for existing in filtered:
            if _matches_overlap(match, existing):
                is_overlap = True
                break
        
        if not is_overlap:
            filtered.append(match)
            
        if len(filtered) >= max_matches:
            break
    
    return filtered


def _matches_overlap(m1: IconMatch, m2: IconMatch) -> bool:
    """Check if two matches overlap significantly."""
    x1 = max(m1.x, m2.x)
    y1 = max(m1.y, m2.y)
    x2 = min(m1.x + m1.width, m2.x + m2.width)
    y2 = min(m1.y + m1.height, m2.y + m2.height)
    
    if x1 >= x2 or y1 >= y2:
        return False
    
    overlap_area = (x2 - x1) * (y2 - y1)
    m1_area = m1.width * m1.height
    
    return overlap_area / m1_area > 0.5 if m1_area > 0 else False
