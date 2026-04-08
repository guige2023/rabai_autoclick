"""Image template matching utilities for finding images within screens.

This module provides utilities for template matching - finding where
an image appears on screen, useful for automation targeting.
"""

from __future__ import annotations

from typing import Optional
from dataclasses import dataclass


@dataclass
class MatchResult:
    """Result of a template match operation."""
    x: int
    y: int
    width: int
    height: int
    confidence: float
    
    @property
    def center(self) -> tuple[int, int]:
        """Get the center point of the match."""
        return (self.x + self.width // 2, self.y + self.height // 2)


def find_template(
    template_path: str,
    confidence: float = 0.8,
    region: Optional[tuple[int, int, int, int]] = None,  # (x, y, width, height)
) -> list[MatchResult]:
    """Find all occurrences of a template image on screen.
    
    Args:
        template_path: Path to the template image.
        confidence: Minimum confidence threshold (0.0 to 1.0).
        region: Optional region to search within.
    
    Returns:
        List of MatchResult objects for all matches found.
    """
    try:
        import cv2
        import numpy as np
        import pyautogui
        
        # Capture screenshot
        if region:
            screenshot = pyautogui.screenshot(region=region)
        else:
            screenshot = pyautogui.screenshot()
        
        # Convert to OpenCV format
        screenshot_cv = cv2.cvtColor(np.array(screenshot), cv2.COLOR_RGB2BGR)
        template = cv2.imread(template_path)
        
        if template is None:
            return []
        
        # Perform template matching
        result = cv2.matchTemplate(screenshot_cv, template, cv2.TM_CCOEFF_NORMED)
        
        # Find all matches above threshold
        locations = np.where(result >= confidence)
        
        matches = []
        h, w = template.shape[:2]
        
        for pt in zip(*locations[::-1]):
            match = MatchResult(
                x=int(pt[0]),
                y=int(pt[1]),
                width=w,
                height=h,
                confidence=float(result[pt[1], pt[0]]),
            )
            matches.append(match)
        
        return matches
    
    except ImportError:
        return []


def find_template_best_match(
    template_path: str,
    confidence: float = 0.8,
    region: Optional[tuple[int, int, int, int]] = None,
) -> Optional[MatchResult]:
    """Find the best (highest confidence) match of a template.
    
    Args:
        template_path: Path to the template image.
        confidence: Minimum confidence threshold.
        region: Optional region to search within.
    
    Returns:
        Best MatchResult, or None if no match found.
    """
    matches = find_template(template_path, confidence, region)
    
    if not matches:
        return None
    
    return max(matches, key=lambda m: m.confidence)


def find_all_templates(
    template_paths: list[str],
    confidence: float = 0.8,
    region: Optional[tuple[int, int, int, int]] = None,
) -> dict[str, list[MatchResult]]:
    """Find all matches for multiple templates.
    
    Args:
        template_paths: List of paths to template images.
        confidence: Minimum confidence threshold.
        region: Optional region to search within.
    
    Returns:
        Dictionary mapping template paths to their match results.
    """
    results = {}
    for path in template_paths:
        matches = find_template(path, confidence, region)
        if matches:
            results[path] = matches
    return results


def wait_for_template(
    template_path: str,
    confidence: float = 0.8,
    timeout: float = 10.0,
    poll_interval: float = 0.5,
    region: Optional[tuple[int, int, int, int]] = None,
) -> Optional[MatchResult]:
    """Wait for a template to appear on screen.
    
    Args:
        template_path: Path to the template image.
        confidence: Minimum confidence threshold.
        timeout: Maximum time to wait in seconds.
        poll_interval: Time between search attempts.
        region: Optional region to search within.
    
    Returns:
        MatchResult if template found, None if timeout.
    """
    import time
    
    start_time = time.monotonic()
    
    while time.monotonic() - start_time < timeout:
        match = find_template_best_match(template_path, confidence, region)
        if match:
            return match
        time.sleep(poll_interval)
    
    return None
