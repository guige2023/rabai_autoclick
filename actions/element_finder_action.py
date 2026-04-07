"""Element finder action for locating UI elements.

This module provides element location capabilities using
image recognition, template matching, and coordinate search.

Example:
    >>> action = ElementFinderAction()
    >>> result = action.execute(template="/path/to/button.png")
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class ElementMatch:
    """Represents a found element match."""
    x: int
    y: int
    width: int
    height: int
    confidence: float


@dataclass
class SearchRegion:
    """Region to search within."""
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0


class ElementFinderAction:
    """Element finder action using image recognition.

    Provides template matching and image-based element
    location with confidence scoring.

    Example:
        >>> action = ElementFinderAction()
        >>> result = action.execute(
        ...     template="submit_button.png",
        ...     confidence=0.8
        ... )
    """

    def __init__(self) -> None:
        """Initialize element finder."""
        self._last_matches: list[ElementMatch] = []

    def execute(
        self,
        template: str,
        screenshot: Optional[str] = None,
        region: Optional[tuple[int, int, int, int]] = None,
        confidence: float = 0.8,
        multiple: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute element search.

        Args:
            template: Path to template image.
            screenshot: Optional screenshot to search in.
            region: Search region (x, y, width, height).
            confidence: Minimum confidence threshold.
            multiple: Whether to find all matches.
            **kwargs: Additional parameters.

        Returns:
            Search result dictionary.

        Raises:
            ValueError: If template is missing.
        """
        try:
            import cv2
            import numpy as np
        except ImportError:
            return {
                "success": False,
                "error": "OpenCV not installed. Run: pip install opencv-python",
            }

        if not template:
            raise ValueError("template is required")

        result: dict[str, Any] = {"template": template, "success": True}

        try:
            # Load template
            template_img = cv2.imread(template, cv2.IMREAD_COLOR)
            if template_img is None:
                raise ValueError(f"Could not load template: {template}")
            template_h, template_w = template_img.shape[:2]

            # Load screenshot
            if screenshot:
                search_img = cv2.imread(screenshot, cv2.IMREAD_COLOR)
            else:
                search_img = self._capture_screen(region)

            if search_img is None:
                raise ValueError("Could not capture screenshot")

            # Template matching
            method = kwargs.get("method", cv2.TM_CCOEFF_NORMED)
            res = cv2.matchTemplate(search_img, template_img, method)
            min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)

            if multiple:
                # Find all matches above threshold
                locations = np.where(res >= confidence)
                matches = []
                for pt in zip(*locations[::-1]):
                    conf = res[pt[1], pt[0]]
                    matches.append(ElementMatch(
                        x=pt[0],
                        y=pt[1],
                        width=template_w,
                        height=template_h,
                        confidence=float(conf),
                    ))
                self._last_matches = matches
                result["count"] = len(matches)
                result["matches"] = [
                    {"x": m.x, "y": m.y, "w": m.width, "h": m.height, "conf": m.confidence}
                    for m in matches
                ]
            else:
                # Find best match
                if max_val >= confidence:
                    match = ElementMatch(
                        x=max_loc[0],
                        y=max_loc[1],
                        width=template_w,
                        height=template_h,
                        confidence=float(max_val),
                    )
                    self._last_matches = [match]
                    result["found"] = True
                    result["match"] = {
                        "x": match.x,
                        "y": match.y,
                        "w": match.width,
                        "h": match.height,
                        "conf": match.confidence,
                    }
                    result["center"] = (
                        match.x + match.width // 2,
                        match.y + match.height // 2,
                    )
                else:
                    result["found"] = False
                    result["confidence"] = float(max_val)

        except Exception as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _capture_screen(self, region: Optional[tuple[int, int, int, int]] = None) -> Any:
        """Capture screen for searching.

        Args:
            region: Optional capture region.

        Returns:
            OpenCV image.
        """
        try:
            import pyautogui
            import numpy as np
            from PIL import Image

            img = pyautogui.screenshot(region=region)
            return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        except Exception:
            return None

    def wait_for_element(
        self,
        template: str,
        timeout: float = 10.0,
        interval: float = 0.5,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Wait for element to appear on screen.

        Args:
            template: Template image path.
            timeout: Maximum wait time.
            interval: Check interval.
            **kwargs: Additional search parameters.

        Returns:
            Search result when found or timeout.
        """
        start_time = time.time()
        result: dict[str, Any] = {"found": False}

        while time.time() - start_time < timeout:
            result = self.execute(template=template, **kwargs)
            if result.get("found"):
                result["wait_time"] = time.time() - start_time
                return result
            time.sleep(interval)

        result["wait_time"] = timeout
        result["timeout"] = True
        return result

    def click_element(
        self,
        template: str,
        offset_x: int = 0,
        offset_y: int = 0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Find and click element.

        Args:
            template: Template image path.
            offset_x: Click offset from element center.
            offset_y: Click offset from element center.
            **kwargs: Additional search parameters.

        Returns:
            Click result dictionary.
        """
        result = self.execute(template=template, **kwargs)

        if result.get("found"):
            center = result["center"]
            click_x = center[0] + offset_x
            click_y = center[1] + offset_y

            try:
                import pyautogui
                pyautogui.click(click_x, click_y)
                result["clicked"] = (click_x, click_y)
            except Exception as e:
                result["click_error"] = str(e)

        return result

    def get_last_matches(self) -> list[ElementMatch]:
        """Get last found matches.

        Returns:
            List of ElementMatch objects.
        """
        return self._last_matches
