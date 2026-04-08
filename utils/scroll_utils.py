"""Scroll operation utilities for viewport and element scrolling.

Provides helpers for scrolling pages, elements, and custom
regions, with support for smooth scrolling, page-by-page
scrolling, and directional scroll operations.

Example:
    >>> from utils.scroll_utils import scroll_down, scroll_up, scroll_element
    >>> scroll_down(pages=2)
    >>> scroll_up()
    >>> scroll_element('AXScrollArea', direction='up')
"""

from __future__ import annotations

__all__ = [
    "scroll_down",
    "scroll_up",
    "scroll_left",
    "scroll_right",
    "scroll_element",
    "scroll_to_top",
    "scroll_to_bottom",
    "smooth_scroll",
    "page_scroll",
]


def scroll_down(pages: float = 1.0, clicks: int = 3) -> bool:
    """Scroll down by a number of pages or clicks.

    Args:
        pages: Number of page-heights to scroll.
        clicks: Number of wheel click events per page.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import scroll
    except ImportError:
        return False

    total_clicks = int(pages * clicks)
    for _ in range(total_clicks):
        scroll(delta_y=-3)
    return True


def scroll_up(pages: float = 1.0, clicks: int = 3) -> bool:
    """Scroll up by a number of pages or clicks."""
    try:
        from utils.input_simulation_utils import scroll
    except ImportError:
        return False

    total_clicks = int(pages * clicks)
    for _ in range(total_clicks):
        scroll(delta_y=3)
    return True


def scroll_left(pages: float = 1.0, clicks: int = 3) -> bool:
    """Scroll left by a number of pages."""
    try:
        from utils.input_simulation_utils import scroll
    except ImportError:
        return False

    total_clicks = int(pages * clicks)
    for _ in range(total_clicks):
        scroll(delta_x=-3)
    return True


def scroll_right(pages: float = 1.0, clicks: int = 3) -> bool:
    """Scroll right by a number of pages."""
    try:
        from utils.input_simulation_utils import scroll
    except ImportError:
        return False

    total_clicks = int(pages * clicks)
    for _ in range(total_clicks):
        scroll(delta_x=3)
    return True


def scroll_element(
    element_role: str = "AXScrollArea",
    direction: str = "down",
    amount: int = 3,
) -> bool:
    """Scroll a specific UI element by its accessibility role.

    Args:
        element_role: AXUIElement role (e.g., 'AXScrollArea', 'AXList').
        direction: Scroll direction ('up', 'down', 'left', 'right').
        amount: Number of scroll units.

    Returns:
        True if the element was found and scrolled.
    """
    import sys

    if sys.platform != "darwin":
        return False

    try:
        from utils.accessibility_utils import (
            get_focused_element,
            get_element_children,
            get_element_role,
            get_element_rect,
        )
    except ImportError:
        return False

    focused = get_focused_element()
    if focused is None:
        return False

    # Recursively search for scroll element
    def find_scroll_element(elem, depth=0):
        if depth > 5:
            return None
        role = get_element_role(elem)
        if role == element_role:
            return elem
        for child in get_element_children(elem):
            found = find_scroll_element(child, depth + 1)
            if found:
                return found
        return None

    scroll_elem = find_scroll_element(focused)
    if scroll_elem is None:
        return False

    # Use keyboard for scrolling (Page Up/Down/Left/Right)
    if direction == "down":
        key = "pagedown"
    elif direction == "up":
        key = "pageup"
    elif direction == "left":
        key = "left"
    elif direction == "right":
        key = "right"
    else:
        key = "down"

    try:
        from utils.input_simulation_utils import press_key
        for _ in range(amount):
            press_key(key)
        return True
    except Exception:
        return False


def scroll_to_top() -> bool:
    """Scroll to the top of the current view.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import press_key
    except ImportError:
        return False

    try:
        press_key("home")
        return True
    except Exception:
        return False


def scroll_to_bottom() -> bool:
    """Scroll to the bottom of the current view.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import press_key
    except ImportError:
        return False

    try:
        press_key("end")
        return True
    except Exception:
        return False


def smooth_scroll(
    dx: float = 0,
    dy: float = 0,
    steps: int = 10,
    interval: float = 0.02,
) -> bool:
    """Perform a smooth scroll with incremental steps.

    Args:
        dx: Horizontal scroll delta.
        dy: Vertical scroll delta.
        steps: Number of scroll steps.
        interval: Time between steps in seconds.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import scroll
    except ImportError:
        return False

    dx_step = dx / steps
    dy_step = dy / steps

    for _ in range(steps):
        scroll(delta_x=dx_step, delta_y=dy_step)
        if interval > 0:
            import time
            time.sleep(interval)

    return True


def page_scroll(direction: str = "down", count: int = 1) -> bool:
    """Scroll by full pages in the given direction.

    Args:
        direction: 'up', 'down', 'left', or 'right'.
        count: Number of pages.

    Returns:
        True if successful.
    """
    if direction == "down":
        return scroll_down(pages=count)
    elif direction == "up":
        return scroll_up(pages=count)
    elif direction == "left":
        return scroll_left(pages=count)
    elif direction == "right":
        return scroll_right(pages=count)
    return False
