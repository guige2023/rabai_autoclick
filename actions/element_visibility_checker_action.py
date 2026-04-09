"""
Element Visibility Checker Action Module.

Checks if UI elements are visible, clickable, or obscured,
accounting for viewport boundaries, stacking order, and
CSS visibility/display properties.
"""

from typing import Optional, Tuple


class VisibilityResult:
    """Result of a visibility check."""

    def __init__(
        self,
        is_visible: bool,
        reason: str = "",
        viewport_bounds: Optional[Tuple[int, int, int, int]] = None,
       obscruing_elements: Optional[list] = None,
    ):
        """
        Initialize visibility result.

        Args:
            is_visible: Whether element is visible.
            reason: Reason for visibility status.
            viewport_bounds: Visible bounds of element.
            obscruing_elements: List of obscuring elements.
        """
        self.is_visible = is_visible
        self.reason = reason
        self.viewport_bounds = viewport_bounds
        self.obscuring_elements = obscruing_elements or []

    def __repr__(self) -> str:
        return f"VisibilityResult(visible={self.is_visible}, reason='{self.reason}')"


class ElementVisibilityChecker:
    """Checks if elements are visible and interactable."""

    def __init__(
        self,
        viewport_width: int,
        viewport_height: int,
        scroll_x: int = 0,
        scroll_y: int = 0,
    ):
        """
        Initialize visibility checker.

        Args:
            viewport_width: Viewport width in pixels.
            viewport_height: Viewport height in pixels.
            scroll_x: Horizontal scroll offset.
            scroll_y: Vertical scroll offset.
        """
        self.viewport_width = viewport_width
        self.viewport_height = viewport_height
        self.scroll_x = scroll_x
        self.scroll_y = scroll_y

    def check_visibility(
        self,
        element: dict,
        all_elements: Optional[list[dict]] = None,
    ) -> VisibilityResult:
        """
        Check if an element is visible.

        Args:
            element: Element dictionary with bounds, style, etc.
            all_elements: All elements for occlusion checking.

        Returns:
            VisibilityResult with visibility status.
        """
        display = element.get("style", {}).get("display", "block")
        visibility = element.get("style", {}).get("visibility", "visible")
        opacity = element.get("style", {}).get("opacity", "1.0")

        if display == "none":
            return VisibilityResult(False, "display: none")

        if visibility == "hidden":
            return VisibilityResult(False, "visibility: hidden")

        try:
            if float(opacity) <= 0:
                return VisibilityResult(False, "opacity: 0")
        except (ValueError, TypeError):
            pass

        bounds = element.get("bounds", (0, 0, 0, 0))
        x1, y1, x2, y2 = bounds

        view_x1 = x1 - self.scroll_x
        view_y1 = y1 - self.scroll_y
        view_x2 = x2 - self.scroll_x
        view_y2 = y2 - self.scroll_y

        visible_bounds = (
            max(0, view_x1),
            max(0, view_y1),
            min(self.viewport_width, view_x2),
            min(self.viewport_height, view_y2),
        )

        if visible_bounds[2] <= visible_bounds[0] or visible_bounds[3] <= visible_bounds[1]:
            return VisibilityResult(False, "outside viewport", visible_bounds)

        obscuring = []
        if all_elements:
            for other in all_elements:
                if other is element:
                    continue
                if self._is_obscuring(other, visible_bounds):
                    obscuring.append(other.get("tag", "unknown"))

        if obscuring:
            return VisibilityResult(
                False,
                f"obscured by {len(obscuring)} element(s)",
                visible_bounds,
                obscuring,
            )

        return VisibilityResult(True, "visible", visible_bounds)

    def is_clickable(
        self,
        element: dict,
        all_elements: Optional[list[dict]] = None,
    ) -> Tuple[bool, str]:
        """
        Check if an element is clickable.

        Args:
            element: Element dictionary.
            all_elements: All elements for occlusion check.

        Returns:
            Tuple of (is_clickable, reason).
        """
        vis_result = self.check_visibility(element, all_elements)

        if not vis_result.is_visible:
            return False, vis_result.reason

        enabled = element.get("enabled", True)
        if not enabled:
            return False, "element disabled"

        tag = element.get("tag", "").lower()
        if tag in {"input", "button", "a"}:
            return True, "clickable element"

        role = element.get("role", "")
        if role in {"button", "link", "menuitem", "checkbox", "radio"}:
            return True, f"interactive role: {role}"

        return True, "element in viewport"

    def _is_obscuring(
        self,
        other: dict,
        bounds: Tuple[int, int, int, int],
    ) -> bool:
        """Check if another element is obscuring the given bounds."""
        other_bounds = other.get("bounds", (0, 0, 0, 0))
        ox1, oy1, ox2, oy2 = other_bounds
        bx1, by1, bx2, by2 = bounds

        display = other.get("style", {}).get("display", "block")
        if display == "none":
            return False

        overlap = not (ox2 <= bx1 or ox1 >= bx2 or oy2 <= by1 or oy1 >= by2)

        if overlap and other.get("style", {}).get("opacity", "1.0") != "0":
            return True

        return False
