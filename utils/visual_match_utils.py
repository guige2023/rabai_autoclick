"""
Visual matching utilities using OpenCV template matching and feature detection.

Provides image-based element location and matching capabilities for
GUI automation on macOS.
"""

from __future__ import annotations

import cv2
import numpy as np
from typing import Optional, Tuple, Union
from dataclasses import dataclass
import os


@dataclass
class MatchResult:
    """Template match result."""
    x: int
    y: int
    width: int
    height: int
    confidence: float
    center_x: int
    center_y: int


def load_image(path: Union[str, np.ndarray]) -> Optional[np.ndarray]:
    """
    Load image from file or use existing array.
    
    Args:
        path: Image path or numpy array.
        
    Returns:
        RGB image array or None if failed.
    """
    if isinstance(path, np.ndarray):
        return cv2.cvtColor(path, cv2.COLOR_BGR2RGB) if len(path.shape) == 3 else path
    
    if not os.path.exists(path):
        return None
    
    img = cv2.imread(path)
    if img is None:
        return None
    return cv2.cvtColor(img, cv2.COLOR_BGR2RGB)


def find_template(screen_img: np.ndarray, template_img: np.ndarray,
                  threshold: float = 0.8) -> Optional[MatchResult]:
    """
    Find template in screenshot using normalized cross-correlation.
    
    Args:
        screen_img: Screenshot as numpy array.
        template_img: Template image to find.
        threshold: Match confidence threshold (0-1).
        
    Returns:
        MatchResult if found, None otherwise.
    """
    screen_gray = cv2.cvtColor(screen_img, cv2.COLOR_RGB2GRAY)
    template_gray = cv2.cvtColor(template_img, cv2.COLOR_RGB2GRAY)
    
    w, h = template_gray.shape[::-1]
    
    result = cv2.matchTemplate(screen_gray, template_gray, cv2.TM_CCOEFF_NORMED)
    min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(result)
    
    if max_val >= threshold:
        return MatchResult(
            x=max_loc[0],
            y=max_loc[1],
            width=w,
            height=h,
            confidence=float(max_val),
            center_x=max_loc[0] + w // 2,
            center_y=max_loc[1] + h // 2
        )
    return None


def find_all_templates(screen_img: np.ndarray, template_img: np.ndarray,
                       threshold: float = 0.8) -> list[MatchResult]:
    """
    Find all occurrences of template in screenshot.
    
    Args:
        screen_img: Screenshot as numpy array.
        template_img: Template image to find.
        threshold: Match confidence threshold (0-1).
        
    Returns:
        List of MatchResult for all matches found.
    """
    screen_gray = cv2.cvtColor(screen_img, cv2.COLOR_RGB2GRAY)
    template_gray = cv2.cvtColor(template_img, cv2.COLOR_RGB2GRAY)
    
    w, h = template_gray.shape[::-1]
    
    result = cv2.matchTemplate(screen_gray, template_gray, cv2.TM_CCOEFF_NORMED)
    
    locations = np.where(result >= threshold)
    matches = []
    
    for pt in zip(*locations[::-1]):
        matches.append(MatchResult(
            x=pt[0],
            y=pt[1],
            width=w,
            height=h,
            confidence=float(result[pt[1], pt[0]]),
            center_x=pt[0] + w // 2,
            center_y=pt[1] + h // 2
        ))
    
    return matches


def find_with_sift(screen_img: np.ndarray, template_img: np.ndarray,
                   threshold: float = 0.75) -> Optional[MatchResult]:
    """
    Find template using SIFT feature matching (rotation/scale invariant).
    
    Args:
        screen_img: Screenshot as numpy array.
        template_img: Template image to find.
        threshold: Match ratio threshold.
        
    Returns:
        MatchResult if found, None otherwise.
    """
    sift = cv2.SIFT_create()
    
    kp1, des1 = sift.detectAndCompute(cv2.cvtColor(screen_img, cv2.COLOR_RGB2GRAY), None)
    kp2, des2 = sift.detectAndCompute(cv2.cvtColor(template_img, cv2.COLOR_RGB2GRAY), None)
    
    if des1 is None or des2 is None or len(kp1) == 0 or len(kp2) == 0:
        return None
    
    bf = cv2.BFMatcher()
    matches = bf.knnMatch(des1, des2, k=2)
    
    good = [m for m, n in matches if m.distance < threshold * n.distance]
    
    if len(good) >= 4:
        src_pts = np.float32([kp1[m.queryIdx].pt for m in good]).reshape(-1, 1, 2)
        dst_pts = np.float32([kp2[m.trainIdx].pt for m in good]).reshape(-1, 1, 2)
        
        _, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, 5.0)
        
        h, w = template_img.shape[:2]
        pts = np.float32([[0, 0], [0, h-1], [w-1, h-1], [w-1, 0]]).reshape(-1, 1, 2)
        dst = cv2.perspectiveTransform(pts, _)
        
        x, y, xe, ye = map(int, [dst[:, 0, 0].min(), dst[:, 0, 1].min(),
                                   dst[:, 0, 0].max(), dst[:, 0, 1].max()])
        
        return MatchResult(
            x=x, y=y,
            width=xe - x, height=ye - y,
            confidence=float(len(good) / len(matches)),
            center_x=(x + xe) // 2, center_y=(y + ye) // 2
        )
    return None


def wait_for_template(screen_img: np.ndarray, template_img: np.ndarray,
                      timeout: float = 10.0, poll_interval: float = 0.5,
                      threshold: float = 0.8) -> Optional[MatchResult]:
    """
    Wait for template to appear in screen.
    
    Args:
        screen_img: Screenshot as numpy array.
        template_img: Template image to wait for.
        timeout: Maximum wait time in seconds.
        poll_interval: Time between checks.
        threshold: Match confidence threshold.
        
    Returns:
        MatchResult if found before timeout, None otherwise.
    """
    import time
    start = time.time()
    
    while time.time() - start < timeout:
        result = find_template(screen_img, template_img, threshold)
        if result:
            return result
        time.sleep(poll_interval)
    
    return None


def highlight_matches(screen_img: np.ndarray, matches: list[MatchResult],
                      color: Tuple[int, int, int] = (0, 255, 0),
                      thickness: int = 2) -> np.ndarray:
    """
    Draw rectangles around matches on screen image.
    
    Args:
        screen_img: Screenshot as numpy array.
        matches: List of MatchResult to highlight.
        color: BGR color tuple.
        thickness: Line thickness.
        
    Returns:
        Image with highlighted matches.
    """
    output = screen_img.copy()
    for match in matches:
        x1, y1 = match.x, match.y
        x2, y2 = match.x + match.width, match.y + match.height
        
        cv2.rectangle(output, (x1, y1), (x2, y2), color, thickness)
        
        label = f"{match.confidence:.2f}"
        cv2.putText(output, label, (x1, y1 - 5),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 1)
    
    return output


def calculate_similarity(img1: np.ndarray, img2: np.ndarray) -> float:
    """
    Calculate structural similarity between two images.
    
    Args:
        img1: First image.
        img2: Second image.
        
    Returns:
        Similarity score 0-1.
    """
    gray1 = cv2.cvtColor(img1, cv2.COLOR_RGB2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_RGB2GRAY)
    
    score = cv2.matchTemplate(gray1, gray2, cv2.TM_CCOEFF_NORMED)[0, 0]
    return float(max(0, score))
