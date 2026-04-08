"""Template matching utilities for visual element detection.

Provides wrappers around OpenCV template matching for finding image
patterns on screen, multi-scale matching for resolution-independent
detection, and batch matching across a list of templates.

Example:
    >>> from utils.template_matching_utils import find_template, find_all
    >>> match = find_template('submit_button.png', threshold=0.8)
    >>> if match:
    ...     print(f"Found at {match['x']}, {match['y']}")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

__all__ = [
    "MatchResult",
    "MatchResults",
    "find_template",
    "find_all",
    "find_best",
    "match_in_region",
    "multi_scale_match",
    "TemplateMatcher",
]


@dataclass
class MatchResult:
    """A single template match result.

    Attributes:
        x: Top-left X coordinate of the match in the search image.
        y: Top-left Y coordinate of the match in the search image.
        width: Template width.
        height: Template height.
        confidence: Match confidence score (0.0-1.0).
        center: Center point (x, y) of the match.
        rect: Bounding rect as (x, y, width, height).
    """

    x: float
    y: float
    width: float
    height: float
    confidence: float

    @property
    def center(self) -> tuple[float, float]:
        return (self.x + self.width / 2, self.y + self.height / 2)

    @property
    def rect(self) -> tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)


# Type alias for a collection of results
MatchResults = list[MatchResult]


# Try to import OpenCV - gracefully degrade if not available
try:
    import cv2
    import numpy as np

    _CV2_AVAILABLE = True
except ImportError:
    _CV2_AVAILABLE = False


def _load_image(path: str) -> Optional["np.ndarray"]:
    """Load an image file, returning None if unavailable."""
    if not _CV2_AVAILABLE:
        return None
    img = cv2.imread(path, cv2.IMREAD_COLOR)
    return img


def _capture_screen(region: Optional[tuple[float, float, float, float]] = None) -> Optional["np.ndarray"]:
    """Capture the screen (or a region) and return as an OpenCV image."""
    if not _CV2_AVAILABLE:
        return None

    import subprocess

    if region is not None:
        x, y, w, h = map(int, region)
        args = ["screencapture", "-x", "-R", f"{x},{y},{w},{h}", "-"]
    else:
        args = ["screencapture", "-x", "-"]

    try:
        result = subprocess.run(args, capture_output=True, timeout=10)
        if result.returncode != 0:
            return None
        nparr = np.frombuffer(result.stdout, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        return img
    except Exception:
        return None


def find_template(
    template_path: str,
    region: Optional[tuple[float, float, float, float]] = None,
    threshold: float = 0.8,
) -> Optional[MatchResult]:
    """Find a template image on screen and return the best match.

    Args:
        template_path: Path to the template PNG/JPG image.
        region: Optional screen region to search within.
        threshold: Minimum confidence score (0.0-1.0).

    Returns:
        MatchResult with best match, or None if no match above threshold.
    """
    if not _CV2_AVAILABLE:
        return None

    template = _load_image(template_path)
    if template is None:
        return None

    screen = _capture_screen(region)
    if screen is None:
        return None

    result = cv2.matchTemplate(screen, template, cv2.TM_CCOEFF_NORMED)
    _, max_val, _, max_loc = cv2.minMaxLoc(result)

    if max_val < threshold:
        return None

    h, w = template.shape[:2]
    return MatchResult(
        x=float(max_loc[0]),
        y=float(max_loc[1]),
        width=float(w),
        height=float(h),
        confidence=float(max_val),
    )


def find_all(
    template_path: str,
    region: Optional[tuple[float, float, float, float]] = None,
    threshold: float = 0.8,
    max_results: int = 100,
) -> MatchResults:
    """Find all occurrences of a template in the search image.

    Args:
        template_path: Path to the template image.
        region: Optional screen region to search.
        threshold: Minimum confidence threshold.
        max_results: Maximum number of results to return.

    Returns:
        List of MatchResult objects.
    """
    if not _CV2_AVAILABLE:
        return []

    template = _load_image(template_path)
    if template is None:
        return []

    screen = _capture_screen(region)
    if screen is None:
        return []

    result = cv2.matchTemplate(screen, template, cv2.TM_CCOEFF_NORMED)
    h, w = template.shape[:2]

    locations = np.where(result >= threshold)
    matches: MatchResults = []

    for pt in zip(*locations[::-1]):
        matches.append(
            MatchResult(
                x=float(pt[0]),
                y=float(pt[1]),
                width=float(w),
                height=float(h),
                confidence=float(result[pt[1], pt[0]]),
            )
        )
        if len(matches) >= max_results:
            break

    # Sort by confidence
    matches.sort(key=lambda m: m.confidence, reverse=True)
    return matches


def find_best(
    template_path: str,
    region: Optional[tuple[float, float, float, float]] = None,
    threshold: float = 0.8,
) -> Optional[MatchResult]:
    """Alias for find_template to get the best single match."""
    return find_template(template_path, region, threshold)


def match_in_region(
    image: "np.ndarray",
    template: "np.ndarray",
    threshold: float = 0.8,
) -> MatchResults:
    """Match a template within a provided image array.

    Args:
        image: OpenCV image array (already captured).
        template: Template OpenCV image array.
        threshold: Minimum confidence.

    Returns:
        List of MatchResults.
    """
    if not _CV2_AVAILABLE:
        return []

    result = cv2.matchTemplate(image, template, cv2.TM_CCOEFF_NORMED)
    h, w = template.shape[:2]
    locations = np.where(result >= threshold)
    matches: MatchResults = []

    for pt in zip(*locations[::-1]):
        matches.append(
            MatchResult(
                x=float(pt[0]),
                y=float(pt[1]),
                width=float(w),
                height=float(h),
                confidence=float(result[pt[1], pt[0]]),
            )
        )

    return matches


def multi_scale_match(
    template_path: str,
    region: Optional[tuple[float, float, float, float]] = None,
    scales: Optional[list[float]] = None,
    threshold: float = 0.7,
) -> MatchResults:
    """Perform template matching at multiple scales for resolution robustness.

    Args:
        template_path: Path to template image.
        region: Screen region to search.
        scales: List of scale factors (default: 0.5 to 1.5 in 0.1 steps).
        threshold: Minimum confidence threshold.

    Returns:
        List of MatchResults across all scales.
    """
    if not _CV2_AVAILABLE:
        return []

    if scales is None:
        scales = [s / 10 for s in range(5, 16)]  # 0.5 to 1.5

    template = _load_image(template_path)
    if template is None:
        return []

    screen = _capture_screen(region)
    if screen is None:
        return []

    all_matches: MatchResults = []
    base_h, base_w = template.shape[:2]

    for scale in scales:
        if scale <= 0:
            continue
        scaled_h, scaled_w = int(base_h * scale), int(base_w * scale)
        if scaled_h <= 0 or scaled_w <= 0:
            continue

        resized = cv2.resize(template, (scaled_w, scaled_h))
        result = cv2.matchTemplate(screen, resized, cv2.TM_CCOEFF_NORMED)
        h, w = resized.shape[:2]

        locations = np.where(result >= threshold)
        for pt in zip(*locations[::-1]):
            all_matches.append(
                MatchResult(
                    x=float(pt[0]),
                    y=float(pt[1]),
                    width=float(w),
                    height=float(h),
                    confidence=float(result[pt[1], pt[0]]),
                )
            )

    # Deduplicate nearby matches and sort by confidence
    all_matches.sort(key=lambda m: m.confidence, reverse=True)
    return all_matches


class TemplateMatcher:
    """Stateful template matcher for repeated matching."""

    def __init__(
        self,
        template_path: str,
        region: Optional[tuple[float, float, float, float]] = None,
        threshold: float = 0.8,
        multi_scale: bool = False,
    ):
        self.template_path = template_path
        self.region = region
        self.threshold = threshold
        self.multi_scale = multi_scale

    def find(self) -> MatchResults:
        """Find all matches."""
        if self.multi_scale:
            return multi_scale_match(self.template_path, self.region, threshold=self.threshold)
        return find_all(self.template_path, self.region, threshold=self.threshold)

    def find_best(self) -> Optional[MatchResult]:
        """Find the best match."""
        if self.multi_scale:
            matches = multi_scale_match(self.template_path, self.region, threshold=self.threshold)
        else:
            matches = find_all(self.template_path, self.region, threshold=self.threshold)
        return matches[0] if matches else None

    def wait_for(
        self,
        timeout: float = 10.0,
        poll_interval: float = 0.2,
    ) -> Optional[MatchResult]:
        """Wait for the template to appear on screen."""
        import time

        deadline = time.time() + timeout
        while time.time() < deadline:
            match = self.find_best()
            if match:
                return match
            time.sleep(poll_interval)
        return None
