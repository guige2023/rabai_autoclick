"""Template matching utilities for finding images within screenshots.

This module provides advanced template matching utilities with multi-scale
search, confidence scoring, and region filtering for precise element
identification in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple
import io


@dataclass
class TemplateMatch:
    """Result of a template match."""
    x: int
    y: int
    width: int
    height: int
    confidence: float
    scale: float
    
    @property
    def center(self) -> Tuple[int, int]:
        """Get center point of match."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def bounding_box(self) -> Tuple[int, int, int, int]:
        """Get bounding box (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)


@dataclass
class TemplateMatchConfig:
    """Configuration for template matching."""
    confidence_threshold: float = 0.8
    scale_range: Tuple[float, float] = (0.5, 2.0)
    scale_steps: int = 10
    max_matches: int = 10
    match_filter: str = "best"  # "best", "nms", "all"


def find_template_multi_scale(
    screenshot_data: bytes,
    template_data: bytes,
    config: Optional[TemplateMatchConfig] = None,
) -> List[TemplateMatch]:
    """Find template in screenshot using multi-scale matching.
    
    Args:
        screenshot_data: Screenshot image bytes.
        template_data: Template image bytes.
        config: Matching configuration.
    
    Returns:
        List of TemplateMatch objects.
    """
    try:
        import cv2
        import numpy as np
        from PIL import Image
        import io
        
        config = config or TemplateMatchConfig()
        
        nparr = np.frombuffer(screenshot_data, np.uint8)
        screenshot = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if screenshot is None:
            screenshot_pil = Image.open(io.BytesIO(screenshot_data)).convert("RGB")
            screenshot = cv2.cvtColor(np.array(screenshot_pil), cv2.COLOR_RGB2BGR)
        
        template_arr = np.frombuffer(template_data, np.uint8)
        template = cv2.imdecode(template_arr, cv2.IMREAD_COLOR)
        
        if template is None:
            template_pil = Image.open(io.BytesIO(template_data)).convert("RGB")
            template = cv2.cvtColor(np.array(template_pil), cv2.COLOR_RGB2BGR)
        
        screenshot_gray = cv2.cvtColor(screenshot, cv2.COLOR_BGR2GRAY)
        template_gray = cv2.cvtColor(template, cv2.COLOR_BGR2GRAY)
        
        t_h, t_w = template_gray.shape[:2]
        
        scale_min, scale_max = config.scale_range
        scales = np.linspace(scale_min, scale_max, config.scale_steps)
        
        all_matches = []
        
        for scale in scales:
            scaled_w = int(t_w * scale)
            scaled_h = int(t_h * scale)
            
            if scaled_w > screenshot.shape[1] or scaled_h > screenshot.shape[0]:
                continue
            
            scaled_template = cv2.resize(template_gray, (scaled_w, scaled_h))
            
            result = cv2.matchTemplate(
                screenshot_gray,
                scaled_template,
                cv2.TM_CCOEFF_NORMED,
            )
            
            locations = np.where(result >= config.confidence_threshold)
            
            for pt in zip(*locations[::-1]):
                all_matches.append(TemplateMatch(
                    x=int(pt[0]),
                    y=int(pt[1]),
                    width=scaled_w,
                    height=scaled_h,
                    confidence=float(result[pt[1], pt[0]]),
                    scale=float(scale),
                ))
        
        if config.match_filter == "best":
            if all_matches:
                best = max(all_matches, key=lambda m: m.confidence)
                return [best]
            return []
        elif config.match_filter == "nms":
            return _filter_overlapping(all_matches, config.max_matches)
        else:
            sorted_matches = sorted(all_matches, key=lambda m: m.confidence, reverse=True)
            return sorted_matches[:config.max_matches]
    except ImportError:
        raise ImportError("OpenCV is required for template matching")


def _filter_overlapping(
    matches: List[TemplateMatch],
    max_matches: int,
) -> List[TemplateMatch]:
    """Filter overlapping matches using NMS."""
    if not matches:
        return []
    
    sorted_matches = sorted(matches, key=lambda m: m.confidence, reverse=True)
    
    filtered = []
    for match in sorted_matches:
        is_overlap = False
        for existing in filtered:
            if _iou(match, existing) > 0.5:
                is_overlap = True
                break
        
        if not is_overlap:
            filtered.append(match)
        
        if len(filtered) >= max_matches:
            break
    
    return filtered


def _iou(m1: TemplateMatch, m2: TemplateMatch) -> float:
    """Calculate IoU between two matches."""
    x1 = max(m1.x, m2.x)
    y1 = max(m1.y, m2.y)
    x2 = min(m1.x + m1.width, m2.x + m2.width)
    y2 = min(m1.y + m1.height, m2.y + m2.height)
    
    if x1 >= x2 or y1 >= y2:
        return 0.0
    
    inter_area = (x2 - x1) * (y2 - y1)
    m1_area = m1.width * m1.height
    m2_area = m2.width * m2.height
    union_area = m1_area + m2_area - inter_area
    
    return inter_area / union_area if union_area > 0 else 0.0


def find_all_templates(
    screenshot_data: bytes,
    template_data: bytes,
    confidence_threshold: float = 0.8,
) -> List[TemplateMatch]:
    """Find all occurrences of template at original scale.
    
    Args:
        screenshot_data: Screenshot image bytes.
        template_data: Template image bytes.
        confidence_threshold: Minimum confidence.
    
    Returns:
        List of all matches.
    """
    config = TemplateMatchConfig(
        confidence_threshold=confidence_threshold,
        match_filter="nms",
    )
    return find_template_multi_scale(screenshot_data, template_data, config)


def find_best_match(
    screenshot_data: bytes,
    template_data: bytes,
    confidence_threshold: float = 0.8,
) -> Optional[TemplateMatch]:
    """Find the best matching template.
    
    Args:
        screenshot_data: Screenshot image bytes.
        template_data: Template image bytes.
        confidence_threshold: Minimum confidence.
    
    Returns:
        Best TemplateMatch or None.
    """
    config = TemplateMatchConfig(
        confidence_threshold=confidence_threshold,
        match_filter="best",
    )
    matches = find_template_multi_scale(screenshot_data, template_data, config)
    return matches[0] if matches else None


def batch_template_match(
    screenshot_data: bytes,
    templates: dict[str, bytes],
    config: Optional[TemplateMatchConfig] = None,
) -> dict[str, List[TemplateMatch]]:
    """Match multiple templates against a screenshot.
    
    Args:
        screenshot_data: Screenshot image bytes.
        templates: Dictionary of template name to template bytes.
        config: Matching configuration.
    
    Returns:
        Dictionary of template name to list of matches.
    """
    results = {}
    for name, template_data in templates.items():
        matches = find_template_multi_scale(screenshot_data, template_data, config)
        results[name] = matches
    return results
