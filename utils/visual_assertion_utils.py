"""
Visual assertion utilities for automation testing.

Provides screenshot comparison, visual diff, and
assertion helpers for automation testing.
"""

from __future__ import annotations

import cv2
import numpy as np
from typing import Optional, Tuple, List, Callable
from dataclasses import dataclass


@dataclass
class AssertionResult:
    """Result of visual assertion."""
    passed: bool
    message: str
    similarity: float = 0.0
    diff_percentage: float = 0.0


class VisualAssertions:
    """Provides visual assertion methods."""
    
    @staticmethod
    def assert_screenshots_match(
        screenshot1_path: str,
        screenshot2_path: str,
        threshold: float = 0.95
    ) -> AssertionResult:
        """
        Assert two screenshots match.
        
        Args:
            screenshot1_path: First screenshot.
            screenshot2_path: Second screenshot.
            threshold: Similarity threshold (0-1).
            
        Returns:
            AssertionResult.
        """
        img1 = cv2.imread(screenshot1_path)
        img2 = cv2.imread(screenshot2_path)
        
        if img1 is None:
            return AssertionResult(
                passed=False,
                message=f"Failed to load: {screenshot1_path}"
            )
        
        if img2 is None:
            return AssertionResult(
                passed=False,
                message=f"Failed to load: {screenshot2_path}"
            )
        
        if img1.shape != img2.shape:
            img2 = cv2.resize(img2, (img1.shape[1], img1.shape[0]))
        
        gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)
        
        diff = cv2.absdiff(gray1, gray2)
        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
        
        total_pixels = gray1.shape[0] * gray1.shape[1]
        diff_pixels = np.count_nonzero(thresh)
        diff_percentage = (diff_pixels / total_pixels * 100) if total_pixels > 0 else 0
        similarity = 1.0 - (diff_pixels / total_pixels) if total_pixels > 0 else 0
        
        passed = similarity >= threshold
        
        return AssertionResult(
            passed=passed,
            message=f"Similarity: {similarity*100:.1f}%, Diff: {diff_percentage:.1f}%",
            similarity=float(similarity),
            diff_percentage=float(diff_percentage)
        )
    
    @staticmethod
    def assert_element_visible(
        screenshot_path: str,
        region: Tuple[int, int, int, int],
        expected_color: Optional[Tuple[int, int, int]] = None,
        color_tolerance: int = 30
    ) -> AssertionResult:
        """
        Assert element is visible in region.
        
        Args:
            screenshot_path: Screenshot path.
            region: (x, y, w, h) region.
            expected_color: Optional expected color.
            color_tolerance: Color matching tolerance.
            
        Returns:
            AssertionResult.
        """
        img = cv2.imread(screenshot_path)
        
        if img is None:
            return AssertionResult(
                passed=False,
                message=f"Failed to load: {screenshot_path}"
            )
        
        x, y, w, h = region
        if y + h > img.shape[0] or x + w > img.shape[1]:
            return AssertionResult(
                passed=False,
                message=f"Region {region} exceeds image bounds"
            )
        
        roi = img[y:y+h, x:x+w]
        
        if expected_color is None:
            mean_color = cv2.mean(roi)[:3]
            brightness = sum(mean_color) / 3
            visible = brightness > 10
            return AssertionResult(
                passed=visible,
                message=f"Brightness: {brightness:.1f}"
            )
        
        lower = np.array([max(0, c - color_tolerance) for c in expected_color])
        upper = np.array([min(255, c + color_tolerance) for c in expected_color])
        
        mask = cv2.inRange(roi, lower, upper)
        match_ratio = np.count_nonzero(mask) / (w * h) if w * h > 0 else 0
        
        return AssertionResult(
            passed=match_ratio > 0.1,
            message=f"Color match ratio: {match_ratio*100:.1f}%",
            similarity=float(match_ratio)
        )
    
    @staticmethod
    def assert_screen_region_matches(
        screenshot_path: str,
        expected_path: str,
        region: Tuple[int, int, int, int]
    ) -> AssertionResult:
        """
        Assert region matches expected image.
        
        Args:
            screenshot_path: Screenshot path.
            expected_path: Expected image path.
            region: (x, y, w, h) region.
            
        Returns:
            AssertionResult.
        """
        screenshot = cv2.imread(screenshot_path)
        expected = cv2.imread(expected_path)
        
        if screenshot is None:
            return AssertionResult(passed=False, message=f"Failed to load: {screenshot_path}")
        if expected is None:
            return AssertionResult(passed=False, message=f"Failed to load: {expected_path}")
        
        x, y, w, h = region
        roi = screenshot[y:y+h, x:x+w]
        
        if roi.shape[:2] != expected.shape[:2]:
            expected = cv2.resize(expected, (roi.shape[1], roi.shape[0]))
        
        gray_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        gray_expected = cv2.cvtColor(expected, cv2.COLOR_BGR2GRAY)
        
        diff = cv2.absdiff(gray_roi, gray_expected)
        total_pixels = gray_roi.shape[0] * gray_roi.shape[1]
        diff_pixels = np.count_nonzero(diff)
        similarity = 1.0 - (diff_pixels / total_pixels) if total_pixels > 0 else 0
        
        return AssertionResult(
            passed=similarity > 0.9,
            message=f"Region similarity: {similarity*100:.1f}%",
            similarity=float(similarity)
        )


def assert_screenshots_match(
    screenshot1_path: str,
    screenshot2_path: str,
    threshold: float = 0.95
) -> AssertionResult:
    """
    Assert two screenshots match.
    
    Args:
        screenshot1_path: First screenshot.
        screenshot2_path: Second screenshot.
        threshold: Similarity threshold.
        
    Returns:
        AssertionResult.
    """
    return VisualAssertions.assert_screenshots_match(
        screenshot1_path, screenshot2_path, threshold
    )
